/**
 * In-memory key-value store (DESIGN.md Section 4.1/4.2).
 *
 * Backed by a plain Map, not a sorted structure. "Sorted map" in Section 4.1
 * matters for SSTable flush (sorted JSON lines, per Section 4.4) — Milestone 1
 * has no SSTable flush, so sortedness is deferred to whichever Milestone 2
 * step implements the flush.
 *
 * This class has no knowledge of the WAL. Write-ordering (WAL append before
 * MemTable mutation) is enforced by the Store coordinator, not here.
 */
export class MemTable {
  private readonly map: Map<string, string>;

  constructor() {
    this.map = new Map();
  }

  /**
   * Sets or overwrites the value for a key.
   */
  put(key: string, value: string): void {
    this.map.set(key, value);
  }

  /**
   * Removes a key. Plain removal for Milestone 1 — no tombstone semantics,
   * since there's no older SSTable layer a delete would need to shadow yet.
   */
  delete(key: string): void {
    this.map.delete(key);
  }

  /**
   * Returns the current value for a key, or undefined if absent.
   */
  get(key: string): string | undefined {
    return this.map.get(key);
  }

  /**
   * Returns whether a key is currently present.
   */
  has(key: string): boolean {
    return this.map.has(key);
  }

  /**
   * Current key count. Unused for now — will back the flush-threshold
   * check in Milestone 2 (Section 4.1, step 3).
   */
  size(): number {
    return this.map.size;
  }
}