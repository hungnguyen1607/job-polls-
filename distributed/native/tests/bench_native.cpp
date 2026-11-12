#include "jobpolls.h"

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

int main() {
    const int n = 2000;
    std::vector<std::string> titles;
    titles.reserve(n);
    for (int i = 0; i < n; ++i) {
        // ~20 distinct roles, each repeated with a suffix that normalizes away.
        titles.push_back("Software Engineer Intern " + std::to_string(i % 20));
    }

    std::vector<const char *> ptrs;
    ptrs.reserve(n);
    for (const auto &s : titles) ptrs.push_back(s.c_str());

    std::vector<unsigned char> dup(n, 0);
    std::vector<uint64_t> hashes(n, 0);

    jp_arena_reset();
    const auto start = std::chrono::steady_clock::now();
    const int rc = jp_dedup_batch(ptrs.data(), n, 3, dup.data(), hashes.data());
    const auto end = std::chrono::steady_clock::now();

    const double ms = std::chrono::duration<double, std::milli>(end - start).count();
    int duplicates = 0;
    for (unsigned char d : dup) duplicates += d;

    std::printf(
        "native dedup: %d rows in %.1f ms (%.0f rows/s), duplicates=%d, arena=%.2f MB, rc=%d\n",
        n, ms, n / (ms / 1000.0), duplicates, jp_arena_bytes() / (1024.0 * 1024.0), rc);
    return rc == JP_OK ? 0 : 1;
}
