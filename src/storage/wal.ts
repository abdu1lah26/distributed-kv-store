import { open, type FileHandle } from "node:fs/promises";

export type WalOp = "PUT" | "DELETE";

export interface WalEntry {
  op: WalOp;
  key: string;
  value?: string;
  timestamp: number;
}

export type WalAppendInput =
  | { op: "PUT"; key: string; value: string }
  | { op: "DELETE"; key: string };

const LENGTH_PREFIX_BYTES = 4;

/**
 * Append-only Write-Ahead Log.
 *
 * On-disk record format (per DESIGN.md Section 4.4):
 *   [4-byte big-endian length][JSON payload]\n
 *
 * No truncation/compaction in Milestone 1 (DESIGN.md Section 2 non-goals) —
 * the file only ever grows via append().
 */
export class WriteAheadLog {
  private readonly filePath: string;
  private fileHandle: FileHandle | undefined;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  /**
   * Opens the WAL file in append mode, creating it if it doesn't exist.
   * Must be called before append().
   */
  async open(): Promise<void> {
    this.fileHandle = await open(this.filePath, "a+");
  }

  /**
   * Serializes the entry, writes it as a length-prefixed record, and
   * fsyncs before resolving. The write is not considered durable until
   * this promise resolves (DESIGN.md Section 4.1).
   */
  async append(entry: WalAppendInput): Promise<void> {
    if (!this.fileHandle) {
      throw new Error("WriteAheadLog.open() must be called before append()");
    }

    const fullEntry: WalEntry = {
      ...entry,
      timestamp: Date.now(),
    };

    const json = JSON.stringify(fullEntry);
    const jsonBuffer = Buffer.from(json, "utf8");

    const lengthBuffer = Buffer.alloc(LENGTH_PREFIX_BYTES);
    lengthBuffer.writeUInt32BE(jsonBuffer.byteLength, 0);

    const newlineBuffer = Buffer.from("\n", "utf8");
    const record = Buffer.concat([lengthBuffer, jsonBuffer, newlineBuffer]);

    await this.fileHandle.write(record);
    await this.fileHandle.sync(); // fsync — required before the write counts as durable
  }

  /**
   * Reads the WAL from the start and yields entries in the order they
   * were written. Used on startup to rebuild the MemTable (Section 4.3).
   *
   * Opens its own read handle so it doesn't disturb the append handle's
   * file position.
   */
  async *replay(): AsyncGenerator<WalEntry> {
    const readHandle = await open(this.filePath, "r");
    try {
      const { size } = await readHandle.stat();
      let offset = 0;

      while (offset < size) {
        const lengthBuffer = Buffer.alloc(LENGTH_PREFIX_BYTES);
        const { bytesRead: lengthBytesRead } = await readHandle.read(
          lengthBuffer,
          0,
          LENGTH_PREFIX_BYTES,
          offset,
        );
        if (lengthBytesRead < LENGTH_PREFIX_BYTES) {
          // Incomplete trailing record (e.g. crash mid-write) — stop replay.
          break;
        }
        offset += LENGTH_PREFIX_BYTES;

        const payloadLength = lengthBuffer.readUInt32BE(0);
        const payloadBuffer = Buffer.alloc(payloadLength);
        const { bytesRead: payloadBytesRead } = await readHandle.read(
          payloadBuffer,
          0,
          payloadLength,
          offset,
        );
        if (payloadBytesRead < payloadLength) {
          // Incomplete trailing record — stop replay.
          break;
        }
        offset += payloadLength;

        // Skip the trailing '\n'.
        offset += 1;

        const entry = JSON.parse(payloadBuffer.toString("utf8")) as WalEntry;
        yield entry;
      }
    } finally {
      await readHandle.close();
    }
  }

  /**
   * Closes the append file handle. Safe to call even if open() was never called.
   */
  async close(): Promise<void> {
    if (this.fileHandle) {
      await this.fileHandle.close();
      this.fileHandle = undefined;
    }
  }
}
