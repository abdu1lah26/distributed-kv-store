import { readdir, mkdir } from "node:fs/promises";
import path from "node:path";
import { WriteAheadLog } from "./wal.js";
import { MemTable } from "./memtable.js";
import { writeSSTable, loadSSTable, type SSTableHandle } from "./sstable.js";

/**
 * Coordinates the WAL, MemTable, and SSTables to implement the write/read
 * path (DESIGN.md Section 4.1, 4.2, 4.3).
 *
 * NOTE: correctness here relies on single-writer, strict-WAL-order
 * replay — no concurrency yet. Revisit once Raft / concurrent writes
 * enter in a later milestone.
 *
 * KNOWN CONSEQUENCE (see DESIGN.md Section 4.4.1): because the WAL is
 * never truncated, open()'s replay reconstructs the MemTable from the
 * ENTIRE WAL history, not just writes since the last flush. Still
 * correct (Map = latest value per key) but not minimal — this is the
 * scenario Milestone 2, Step 5's durability tests verify explicitly.
 */

const DEFAULT_FLUSH_THRESHOLD_KEYS = 1000;
const DEFAULT_FLUSH_THRESHOLD_BYTES = 1024 * 1024; // 1MB

const SSTABLE_FILENAME_PATTERN = /^sstable_(\d+)\.dat$/;

/**
 * Optional override for the flush thresholds (Section 4.1: 1000 keys OR
 * 1MB, whichever first). Defaults to the real production values — this
 * exists purely so tests can trigger a flush deterministically and
 * quickly (e.g. maxKeys: 10) without waiting on the real 1000-key
 * threshold. All existing call sites using the two-arg constructor are
 * unaffected.
 */
export interface FlushThresholds {
  maxKeys?: number;
  maxBytes?: number;
}

export class Store {
  private readonly wal: WriteAheadLog;
  private readonly memtable: MemTable;
  private readonly sstableDir: string;
  private readonly maxKeys: number;
  private readonly maxBytes: number;
  private sstables: SSTableHandle[];
  private approxMemtableBytes: number;
  private opened: boolean;

  constructor(walPath: string, sstableDir: string, thresholds: FlushThresholds = {}) {
    this.wal = new WriteAheadLog(walPath);
    this.memtable = new MemTable();
    this.sstableDir = sstableDir;
    this.maxKeys = thresholds.maxKeys ?? DEFAULT_FLUSH_THRESHOLD_KEYS;
    this.maxBytes = thresholds.maxBytes ?? DEFAULT_FLUSH_THRESHOLD_BYTES;
    this.sstables = [];
    this.approxMemtableBytes = 0;
    this.opened = false;
  }

  async open(): Promise<void> {
    await this.wal.open();

    for await (const entry of this.wal.replay()) {
      if (entry.op === "PUT") {
        if (entry.value !== undefined) {
          this.memtable.put(entry.key, entry.value);
        }
      } else if (entry.op === "DELETE") {
        this.memtable.delete(entry.key);
      }
    }

    await mkdir(this.sstableDir, { recursive: true });
    this.sstables = await this.discoverSSTables();

    this.opened = true;
  }

  private async discoverSSTables(): Promise<SSTableHandle[]> {
    const filenames = await readdir(this.sstableDir);

    const matches = filenames
      .map((filename) => {
        const match = SSTABLE_FILENAME_PATTERN.exec(filename);
        return match ? { filename, timestamp: Number(match[1]) } : null;
      })
      .filter((m): m is { filename: string; timestamp: number } => m !== null)
      .sort((a, b) => b.timestamp - a.timestamp);

    const handles: SSTableHandle[] = [];
    for (const { filename } of matches) {
      handles.push(await loadSSTable(path.join(this.sstableDir, filename)));
    }
    return handles;
  }

  async put(key: string, value: string): Promise<void> {
    this.assertOpened();
    await this.wal.append({ op: "PUT", key, value });
    this.memtable.put(key, value);
    this.approxMemtableBytes += byteLength(key) + byteLength(value);
    await this.maybeFlush();
  }

  async delete(key: string): Promise<void> {
    this.assertOpened();
    await this.wal.append({ op: "DELETE", key });
    this.memtable.delete(key);
    this.approxMemtableBytes += byteLength(key);
    await this.maybeFlush();
  }

  async get(key: string): Promise<string | undefined> {
    this.assertOpened();

    const memResult = this.memtable.get(key);
    if (memResult.found) {
      return memResult.tombstone ? undefined : memResult.value;
    }

    for (const table of this.sstables) {
      const result = await table.get(key);
      if (result.found) {
        return result.tombstone ? undefined : result.value;
      }
    }

    return undefined;
  }

  private async maybeFlush(): Promise<void> {
    const overKeyThreshold = this.memtable.size() >= this.maxKeys;
    const overByteThreshold = this.approxMemtableBytes >= this.maxBytes;

    if (!overKeyThreshold && !overByteThreshold) {
      return;
    }

    const entries = this.memtable.entries();
    const filepath = await writeSSTable(this.sstableDir, entries);
    const handle = await loadSSTable(filepath);

    this.sstables.unshift(handle);
    this.memtable.clear();
    this.approxMemtableBytes = 0;
  }

  async close(): Promise<void> {
    await this.wal.close();
  }

  private assertOpened(): void {
    if (!this.opened) {
      throw new Error("Store.open() must be called before put/delete/get");
    }
  }
}

function byteLength(s: string): number {
  return Buffer.byteLength(s, "utf8");
}