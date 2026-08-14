/**
 * BloomFilter — probabilistic set membership check (DESIGN.md Section 4.2/4.4).
 *
 * Used as a fast pre-check in front of SSTable's index+seek lookup: if
 * mightContain(key) is false, the key is DEFINITELY not in the SSTable,
 * so the caller can skip the file entirely with no disk I/O. If it
 * returns true, the key MIGHT be present — the caller must still do the
 * real lookup. Bloom filters never produce false negatives, only false
 * positives, so this check is always safe.
 *
 * Sized from an expected key count and target false-positive rate using
 * the standard formulas:
 *   m (bits)          = ceil(-(n * ln(p)) / (ln(2)^2))
 *   k (hash functions) = round((m / n) * ln(2))
 *
 * Rather than implementing k independent hash functions, this uses the
 * Kirsch-Mitzenmacher double-hashing technique: two independent base
 * hashes (FNV-1a and djb2) are combined as
 *   h_i(x) = (h1(x) + i * h2(x)) mod m
 * to derive all k hash values from just two real hash computations per
 * key. This is the standard approach used in real bloom filter
 * implementations.
 *
 * Bit storage is a true bit-packed Uint32Array (not persisted — Section
 * 4.4 notes the filter is rebuilt in-memory whenever an SSTable is
 * loaded, not stored separately on disk).
 */

const DEFAULT_FALSE_POSITIVE_RATE = 0.01;

/** 32-bit FNV-1a hash. */
function fnv1aHash(str: string): number {
  let hash = 0x811c9dc5; // FNV offset basis
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    // hash *= 16777619 (FNV prime), done via shifts to stay in 32-bit int math
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0; // unsigned 32-bit
}

/** djb2 hash. */
function djb2Hash(str: string): number {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = Math.imul(hash, 33) ^ str.charCodeAt(i);
  }
  return hash >>> 0; // unsigned 32-bit
}

export class BloomFilter {
  private readonly words: Uint32Array;
  private readonly numBits: number;
  private readonly numHashes: number;

  private constructor(numBits: number, numHashes: number) {
    this.numBits = numBits;
    this.numHashes = numHashes;
    this.words = new Uint32Array(Math.ceil(numBits / 32));
  }

  /**
   * Creates a filter sized for `expectedKeys` entries at the given
   * target false-positive rate (default 1%, per project convention).
   */
  static create(expectedKeys: number, falsePositiveRate: number = DEFAULT_FALSE_POSITIVE_RATE): BloomFilter {
    // Guard against degenerate n=0 (e.g. an empty SSTable) — a 1-bit,
    // 1-hash filter is harmless and mightContain() will just always
    // fall through to false for every real key, which is correct: an
    // empty SSTable contains nothing.
    const n = Math.max(expectedKeys, 1);
    const p = falsePositiveRate;

    const m = Math.max(1, Math.ceil(-(n * Math.log(p)) / (Math.LN2 * Math.LN2)));
    const k = Math.max(1, Math.round((m / n) * Math.LN2));

    return new BloomFilter(m, k);
  }

  private bitPositions(key: string): number[] {
    const h1 = fnv1aHash(key);
    const h2 = djb2Hash(key);
    const positions = new Array<number>(this.numHashes);
    for (let i = 0; i < this.numHashes; i++) {
      // Combine via Kirsch-Mitzenmacher, keep unsigned before mod so
      // the result is always a valid non-negative bit index.
      const combined = (h1 + i * h2) >>> 0;
      positions[i] = combined % this.numBits;
    }
    return positions;
  }

  private setBit(i: number): void {
    this.words[i >>> 5]! |= 1 << (i & 31);
  }

  private testBit(i: number): boolean {
    return (this.words[i >>> 5]! & (1 << (i & 31))) !== 0;
  }

  /** Records `key` as present. */
  add(key: string): void {
    for (const pos of this.bitPositions(key)) {
      this.setBit(pos);
    }
  }

  /**
   * Returns false if `key` is DEFINITELY not present (no false
   * negatives — safe to skip the underlying SSTable entirely). Returns
   * true if `key` MIGHT be present (false positives are possible;
   * caller must still do the real lookup).
   */
  mightContain(key: string): boolean {
    for (const pos of this.bitPositions(key)) {
      if (!this.testBit(pos)) {
        return false;
      }
    }
    return true;
  }
}