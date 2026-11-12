#include "jobpolls.h"

#include <cassert>
#include <cstdio>
#include <cstring>

int main() {
    char buf[128];
    size_t n = jp_normalize("  Software   Developer, INTERN!  ", buf, sizeof buf);
    assert(n == std::strlen("software developer intern"));
    assert(std::strcmp(buf, "software developer intern") == 0);

    uint64_t a = jp_simhash64("Software Engineer Intern");
    uint64_t b = jp_simhash64("software   engineer intern!");
    assert(a == b);              // normalization makes them identical
    assert(jp_hamming64(a, b) == 0);

    uint64_t c = jp_simhash64("Mechanical Design Engineer");
    assert(jp_hamming64(a, c) > 0);

    const char *items[] = {
        "Software Engineer Intern",
        "software engineer intern",
        "Backend Developer Co-op",
        "Mechanical Engineer",
    };
    uint8_t dup[4] = {0};
    uint64_t hashes[4] = {0};
    assert(jp_dedup_batch(items, 4, 3, dup, hashes) == JP_OK);
    assert(dup[0] == 0);
    assert(dup[1] == 1);        // exact duplicate after normalization
    assert(dup[2] == 0);
    assert(dup[3] == 0);

    assert(jp_arena_bytes() > 0);
    jp_arena_reset();
    assert(jp_arena_bytes() == 0);

    std::printf("native tests ok (%s)\n", jp_version());
    return 0;
}
