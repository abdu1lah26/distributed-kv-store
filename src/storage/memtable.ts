/**
 * In-memory key-value store (DESIGN.md Section 4.1/4.2).
 *
 * Milestone 2, Step 1: deletes are represented as tombstones, not
 * removals, because a flushed SSTable can hold a stale value for a
 * deleted key — the tombstone is what tells a read "stop here, key is
 * deleted" instead of "keep checking older SSTables." (Section 4.4.2)
 *
 * Superseded Milestone 1 behavior (plain Map.delete(), no tombstones)
 * is kept only as a historical note in DESIGN.md, not in this file.
 *
 * KNOWN CONSEQUENCE: Store.ts's get() call sites are built against the
 * old `string | undefined` contract and will not compile against this
 * change until Milestone 2, Step 4 (Store wiring) updates them. This is
 * expected and intentionally deferred — do not fix it as part of this step.
 */

/** Unique sentinel marking a key as deleted, distinct from any real value. */
export const TOMBSTONE = Symbol('TOMBSTONE');
export type Tombstone = typeof TOMBSTONE;

export type GetResult =
  | { found: false }
  | { found: true; tombstone: true }
  | { found: true; tombstone: false; value: string };

export class MemTable {
  private readonly map: Map<string, string | Tombstone>;

  constructor() {
    this.map = new Map();
  }

  /**
   * Sets or overwrites the value for a key with a live value. Overwrites
   * a prior tombstone for the same key, if any.
   */
  put(key: string, value: string): void {
    this.map.set(key, value);
  }

  /**
   * Marks a key as deleted by writing a tombstone. The key is NOT removed
   * from the underlying Map — the entry must persist so reads can tell
   * "deleted" apart from "never present" once older SSTables exist.
   */
  delete(key: string): void {
    this.map.set(key, TOMBSTONE);
  }

  /**
   * Returns a discriminated result:
   * - { found: false } — no entry at all; caller should check SSTables next.
   * - { found: true, tombstone: true } — definitively deleted, stop here.
   * - { found: true, tombstone: false, value } — live value.
   */
  get(key: string): GetResult {
    if (!this.map.has(key)) {
      return { found: false };
    }

    const stored = this.map.get(key) as string | Tombstone;

    if (stored === TOMBSTONE) {
      return { found: true, tombstone: true };
    }

    return { found: true, tombstone: false, value: stored };
  }

  /**
   * Returns whether the key has any entry in the MemTable — live value
   * OR tombstone. A tombstoned key still "has" an entry; it's just marked
   * deleted rather than absent.
   */
  has(key: string): boolean {
    return this.map.has(key);
  }

  /**
   * Current entry count, including tombstoned keys (the tombstone still
   * occupies a Map entry). Will back the flush-threshold check in a later
   * Milestone 2 step (Section 4.1, step 3).
   */
  size(): number {
    return this.map.size;
  }

  /**
   * Returns all entries (live values and tombstones) sorted by key,
   * computed at call time rather than continuously maintained. An array
   * (not a lazy iterator) is returned deliberately, so SSTable flush logic
   * can pass over entries more than once without a half-consumed-iterator
   * bug.
   */
  entries(): Array<[string, string | Tombstone]> {
    return Array.from(this.map.entries()).sort(([keyA], [keyB]) =>
      keyA < keyB ? -1 : keyA > keyB ? 1 : 0,
    );
  }

  /**
   * Removes all entries (live values and tombstones alike). Used after
   * a successful SSTable flush (Milestone 2, Step 2) — once entries()
   * has been written out to disk, the MemTable is reset to empty so
   * new writes start accumulating toward the next flush threshold.
   */
  clear(): void {
    this.map.clear();
  }
}