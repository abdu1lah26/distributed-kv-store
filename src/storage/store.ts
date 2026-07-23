import { WriteAheadLog } from './wal.js';
import { MemTable } from './memtable.js';

/**
 * Coordinates the WAL and MemTable to implement the Milestone 1 write/read
 * path (DESIGN.md Section 4.1, 4.2, 4.3).
 *
 * Write ordering: WAL append (fsynced) always completes before the
 * corresponding MemTable mutation. Reads never touch the WAL.
 *
 * NOTE: correctness here relies on single-writer, strict-WAL-order
 * replay (agreed for Milestone 1 — no concurrency yet). This assumption
 * will need revisiting once Raft / concurrent writes enter in a later
 * milestone — do not assume it still holds at that point.
 */
export class Store {
  private readonly wal: WriteAheadLog;
  private readonly memtable: MemTable;
  private opened: boolean;

  constructor(walPath: string) {
    this.wal = new WriteAheadLog(walPath);
    this.memtable = new MemTable();
    this.opened = false;
  }

  /**
   * Opens the WAL and replays it into the MemTable to rebuild pre-crash
   * state (Section 4.3). Must be called once before put/delete/get.
   */
  async open(): Promise<void> {
    await this.wal.open();

    for await (const entry of this.wal.replay()) {
      if (entry.op === 'PUT') {
        // value is expected to be present for PUT entries written by append();
        // guard defensively in case of unexpected/corrupt data.
        if (entry.value !== undefined) {
          this.memtable.put(entry.key, entry.value);
        }
      } else if (entry.op === 'DELETE') {
        this.memtable.delete(entry.key);
      }
    }

    this.opened = true;
  }

  /**
   * Appends the write to the WAL (fsynced), then applies it to the MemTable.
   */
  async put(key: string, value: string): Promise<void> {
    this.assertOpened();
    await this.wal.append({ op: 'PUT', key, value });
    this.memtable.put(key, value);
  }

  /**
   * Appends the delete to the WAL (fsynced), then applies it to the MemTable.
   */
  async delete(key: string): Promise<void> {
    this.assertOpened();
    await this.wal.append({ op: 'DELETE', key });
    this.memtable.delete(key);
  }

  /**
   * Reads are served from the MemTable only (Section 4.2) — no WAL access.
   */
  get(key: string): string | undefined {
    this.assertOpened();
    return this.memtable.get(key);
  }

  /**
   * Closes the underlying WAL file handle. Intended for clean shutdown in tests.
   */
  async close(): Promise<void> {
    await this.wal.close();
  }

  private assertOpened(): void {
    if (!this.opened) {
      throw new Error('Store.open() must be called before put/delete/get');
    }
  }
}