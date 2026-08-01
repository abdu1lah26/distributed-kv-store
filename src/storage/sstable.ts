/**
 * SSTable — immutable, sorted, on-disk key-value file (DESIGN.md Section 4.4).
 *
 * Milestone 2, Step 2 scope: single-file write + single-file read
 * (index + seek). Explicitly NOT handled here:
 *  - Bloom filter fast-path (Step 3) — get() always does the index
 *    binary search directly, no pre-check yet.
 *  - Multi-SSTable traversal / newest-to-oldest ordering (Step 4, Store).
 *  - Flush-threshold triggering or wiring MemTable.entries() in
 *    automatically (Step 4, Store) — this module just writes whatever
 *    entries array it's handed.
 *  - Compaction/merging (non-goal, DESIGN.md Section 2).
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';
import type { GetResult, Tombstone } from './memtable.js';
import { TOMBSTONE } from './memtable.js';

/** One key's location within the SSTable file, used for seek-based reads. */
interface IndexEntry {
  key: string;
  /** Byte offset of the start of this line's content. */
  offset: number;
  /** Byte length of this line's content, NOT including the trailing newline. */
  length: number;
}

export interface SSTableHandle {
  readonly filepath: string;
  get(key: string): Promise<GetResult>;
}

type SSTableLine = { key: string; value: string } | { key: string; tombstone: true };

/**
 * Writes `entries` (already sorted — as returned by MemTable.entries())
 * to a new SSTable file in `dir`. Filename is `sstable_<timestamp>.dat`
 * per Section 4.4.
 *
 * Uses write-to-temp-then-fsync-then-rename: a file existing under its
 * final name is therefore always complete. A crash mid-write leaves only
 * a `<name>.dat.tmp` file, which loadSSTable() is never pointed at.
 *
 * Returns the final file path.
 */
export async function writeSSTable(
  dir: string,
  entries: Array<[string, string | Tombstone]>,
): Promise<string> {
  const filename = `sstable_${Date.now()}.dat`;
  const finalPath = path.join(dir, filename);
  const tmpPath = `${finalPath}.tmp`;

  const lines = entries.map(([key, value]) => {
    const line: SSTableLine = value === TOMBSTONE ? { key, tombstone: true } : { key, value };
    return JSON.stringify(line);
  });
  const content = lines.length > 0 ? lines.join('\n') + '\n' : '';

  const handle = await fs.open(tmpPath, 'w');
  try {
    await handle.writeFile(content, { encoding: 'utf8' });
    await handle.sync();
  } finally {
    await handle.close();
  }

  await fs.rename(tmpPath, finalPath);
  return finalPath;
}

/**
 * Loads an existing SSTable file, scanning it once to build an in-memory
 * index of key -> byte offset/length (values are NOT loaded into memory —
 * they stay on disk). Returns a handle whose get() binary-searches the
 * index, then seeks to read only the one matched line.
 *
 * Throws if any line fails to parse as valid JSON — an SSTable file
 * found under its real (non-.tmp) name is expected to always be intact,
 * because writeSSTable()'s temp-then-rename means a torn file should
 * never exist under that name. A parse failure here indicates the file
 * was tampered with or corrupted some other way, not an expected
 * recoverable case (unlike WAL torn-write tolerance).
 */
export async function loadSSTable(filepath: string): Promise<SSTableHandle> {
  const buffer = await fs.readFile(filepath);
  const index: IndexEntry[] = [];

  let lineStart = 0;
  for (let i = 0; i <= buffer.length; i++) {
    const atEnd = i === buffer.length;
    if (atEnd || buffer[i] === 0x0a /* '\n' */) {
      const lineLength = i - lineStart;
      if (lineLength > 0) {
        const lineBuf = buffer.subarray(lineStart, i);
        let parsed: SSTableLine;
        try {
          parsed = JSON.parse(lineBuf.toString('utf8')) as SSTableLine;
        } catch (err) {
          throw new Error(
            `loadSSTable: failed to parse line at byte offset ${lineStart} in ${filepath}: ${
              (err as Error).message
            }`,
          );
        }
        index.push({ key: parsed.key, offset: lineStart, length: lineLength });
      }
      lineStart = i + 1;
    }
  }

  // index is already in file order, which is sorted by key — writeSSTable()
  // only ever receives pre-sorted entries (from MemTable.entries()), so no
  // re-sort is needed here.

  async function get(key: string): Promise<GetResult> {
    let lo = 0;
    let hi = index.length - 1;

    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const entry = index[mid]!;

      if (entry.key === key) {
        const fh = await fs.open(filepath, 'r');
        try {
          const lineBuf = Buffer.alloc(entry.length);
          await fh.read(lineBuf, 0, entry.length, entry.offset);
          const parsed = JSON.parse(lineBuf.toString('utf8')) as SSTableLine;
          if ('tombstone' in parsed && parsed.tombstone) {
            return { found: true, tombstone: true };
          }
          return { found: true, tombstone: false, value: (parsed as { key: string; value: string }).value };
        } finally {
          await fh.close();
        }
      } else if (entry.key < key) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    return { found: false };
  }

  return { filepath, get };
}