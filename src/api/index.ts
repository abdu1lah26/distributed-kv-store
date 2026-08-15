import path from "node:path";
import { Store } from "../storage/store.js";
import { createApp } from "../api/server.js";

/**
 * Real entrypoint for Milestone 3 (single-node REST API, no Raft yet —
 * DESIGN.md Section 7 item 3). Owns the Store lifecycle (open before
 * serving, close on shutdown) and the port binding — createApp() itself
 * stays free of both so it can be tested without a real socket
 * (see test/api.test.ts).
 *
 * Data directory defaults to ./data relative to cwd (not specified in
 * DESIGN.md — this is a runtime default, not a design/scope decision,
 * so it's not something that needs a DESIGN.md update to change).
 * Override via WAL_PATH / SSTABLE_DIR / PORT env vars if needed.
 */

const PORT = Number(process.env.PORT ?? 3000);
const DATA_DIR = path.resolve(process.cwd(), "data");
const WAL_PATH = process.env.WAL_PATH ?? path.join(DATA_DIR, "wal.log");
const SSTABLE_DIR = process.env.SSTABLE_DIR ?? path.join(DATA_DIR, "sstables");

async function main(): Promise<void> {
  const store = new Store(WAL_PATH, SSTABLE_DIR);
  await store.open();

  const app = createApp(store);

  const server = app.listen(PORT, () => {
    console.log(`KV store listening on http://localhost:${PORT}`);
    console.log(`  WAL:      ${WAL_PATH}`);
    console.log(`  SSTables: ${SSTABLE_DIR}`);
  });

  // Best-effort graceful shutdown: stop accepting new connections, then
  // close the WAL file handle. Not a full drain of in-flight requests —
  // fine for a learning-scale single-node demo; revisit if this ever
  // needs to be production-grade.
  async function shutdown(signal: string): Promise<void> {
    console.log(`\nReceived ${signal}, shutting down...`);
    server.close(() => {
      console.log("HTTP server closed.");
    });
    await store.close();
    process.exit(0);
  }

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

main().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});