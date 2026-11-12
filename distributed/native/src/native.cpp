#include "jobpolls.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <vector>

namespace {

constexpr const char *kVersion = "jobpolls-native 0.1.0";
constexpr size_t kBlockSize = 64 * 1024;

/*
 * Per-thread bump arena. Everything a batch needs (normalized buffers, hash
 * arrays) comes from here and is released in one shot on reset, so the hot path
 * does no per-item malloc/free and no GC-visible allocation.
 */
struct Arena {
    std::vector<void *> blocks;
    uint64_t total = 0;
    char *cur = nullptr;
    size_t used = 0;
    size_t cap = 0;

    void *alloc(size_t n) {
        n = (n + 15) & ~static_cast<size_t>(15);
        if (!cur || used + n > cap) {
            size_t block = std::max(n, kBlockSize);
            void *p = std::malloc(block);
            if (!p) return nullptr;
            blocks.push_back(p);
            total += block;
            cur = static_cast<char *>(p);
            used = 0;
            cap = block;
        }
        void *r = cur + used;
        used += n;
        return r;
    }

    void reset() {
        for (void *p : blocks) std::free(p);
        blocks.clear();
        total = 0;
        cur = nullptr;
        used = 0;
        cap = 0;
    }
};

thread_local Arena g_arena;

size_t normalize_into(const char *in, char *out, size_t cap) {
    if (!in || !out || cap == 0) return static_cast<size_t>(-1);

    size_t o = 0;
    bool pending_space = false;
    bool truncated = false;

    for (const auto *p = reinterpret_cast<const unsigned char *>(in); *p; ++p) {
        unsigned char c = *p;
        unsigned char emit = 0;
        bool is_char = false;

        if (c >= 0x80) {
            emit = c;
            is_char = true;
        } else if (std::isalnum(c)) {
            emit = static_cast<unsigned char>(std::tolower(c));
            is_char = true;
        }

        if (is_char) {
            if (pending_space && o > 0) {
                if (o + 1 >= cap) { truncated = true; break; }
                out[o++] = ' ';
            }
            pending_space = false;
            if (o + 1 >= cap) { truncated = true; break; }
            out[o++] = static_cast<char>(emit);
        } else {
            pending_space = true;
        }
    }

    out[o] = '\0';
    return truncated ? static_cast<size_t>(-1) : o;
}

uint64_t fnv1a(const char *begin, const char *end) {
    uint64_t h = 1469598103934665603ULL;
    for (const char *p = begin; p < end; ++p) {
        h ^= static_cast<unsigned char>(*p);
        h *= 1099511628211ULL;
    }
    return h;
}

uint64_t simhash_text(const char *text) {
    int votes[64] = {0};
    const char *p = text;

    while (*p) {
        while (*p == ' ') ++p;
        const char *start = p;
        while (*p && *p != ' ') ++p;
        if (p == start) break;

        uint64_t h = fnv1a(start, p);
        for (int bit = 0; bit < 64; ++bit) {
            votes[bit] += ((h >> bit) & 1ULL) ? 1 : -1;
        }
    }

    uint64_t out = 0;
    for (int bit = 0; bit < 64; ++bit) {
        if (votes[bit] > 0) out |= (1ULL << bit);
    }
    return out;
}

uint64_t simhash_normalized(const char *text) {
    size_t len = text ? std::strlen(text) : 0;
    char *buf = static_cast<char *>(g_arena.alloc(len + 1));
    if (!buf) return 0;
    size_t n = normalize_into(text ? text : "", buf, len + 1);
    if (n == static_cast<size_t>(-1)) { buf[len] = '\0'; }
    return simhash_text(buf);
}

uint32_t hamming(uint64_t a, uint64_t b) {
    return static_cast<uint32_t>(__builtin_popcountll(a ^ b));
}

}  // namespace

extern "C" {

const char *jp_version(void) { return kVersion; }

size_t jp_normalize(const char *in, char *out, size_t out_cap) {
    return normalize_into(in, out, out_cap);
}

uint64_t jp_simhash64(const char *text) { return simhash_normalized(text); }

uint32_t jp_hamming64(uint64_t a, uint64_t b) { return hamming(a, b); }

int jp_simhash_batch(const char *const *texts, size_t n, uint64_t *out_hashes) {
    if (!texts || !out_hashes) return JP_ERR;
    for (size_t i = 0; i < n; ++i) {
        out_hashes[i] = simhash_normalized(texts[i] ? texts[i] : "");
    }
    return JP_OK;
}

int jp_dedup_batch(const char *const *texts, size_t n, uint32_t threshold,
                   uint8_t *out_dup, uint64_t *out_hashes) {
    if (!texts || !out_dup) return JP_ERR;
    if (n == 0) return JP_OK;

    uint64_t *hashes = out_hashes;
    if (!hashes) {
        hashes = static_cast<uint64_t *>(g_arena.alloc(sizeof(uint64_t) * n));
        if (!hashes) return JP_ERR;
    }

    for (size_t i = 0; i < n; ++i) {
        hashes[i] = simhash_normalized(texts[i] ? texts[i] : "");
        out_dup[i] = 0;
    }

    for (size_t i = 1; i < n; ++i) {
        for (size_t j = 0; j < i; ++j) {
            if (hamming(hashes[i], hashes[j]) <= threshold) {
                out_dup[i] = 1;
                break;
            }
        }
    }

    return JP_OK;
}

uint64_t jp_arena_bytes(void) { return g_arena.total; }

void jp_arena_reset(void) { g_arena.reset(); }

}  // extern "C"
