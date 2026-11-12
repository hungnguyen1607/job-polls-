#ifndef JOBPOLLS_H
#define JOBPOLLS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define JP_OK 0
#define JP_ERR -1

/* Library version string (static storage). */
const char *jp_version(void);

/*
 * Normalize a UTF-8 string into `out`: ASCII-lowercase, punctuation -> space,
 * collapse whitespace, trim. Returns the output length (excluding NUL), or
 * (size_t)-1 if the buffer is too small.
 */
size_t jp_normalize(const char *in, char *out, size_t out_cap);

/* 64-bit SimHash over normalized word tokens. */
uint64_t jp_simhash64(const char *text);

/* Hamming distance between two 64-bit SimHashes. */
uint32_t jp_hamming64(uint64_t a, uint64_t b);

/* Compute SimHash for n strings into caller-owned out_hashes[n]. */
int jp_simhash_batch(const char *const *texts, size_t n, uint64_t *out_hashes);

/*
 * Batch near-duplicate detection.
 *   out_dup[i] = 1 when item i is within `threshold` Hamming bits of an
 *                earlier item, else 0.
 *   out_hashes may be NULL (computed internally in the arena).
 * Returns JP_OK / JP_ERR.
 */
int jp_dedup_batch(const char *const *texts, size_t n, uint32_t threshold,
                   uint8_t *out_dup, uint64_t *out_hashes);

/* Bytes currently reserved by the per-thread batch arena. */
uint64_t jp_arena_bytes(void);

/* Release the per-thread batch arena. */
void jp_arena_reset(void);

#ifdef __cplusplus
}
#endif

#endif /* JOBPOLLS_H */
