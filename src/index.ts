import path from "node:path";
import { Store } from "./storage/store.js";
import { createApp, type ClusterContext } from "./api/server.js";

/**
 * Standalone single-node entrypoint (Milestone 3), predating Raft
 * entirely. createApp() now requires a ClusterContext as of Milestone 5,
 * Step 6.5 — this file satisfies it with a trivial always-leader,
 * direct-to-Store context below, preserving this entrypoint's original
 * single-node behavior exactly rather than dragging in a real Raft
 * cluster it was never designed to have. Superseded by Milestone 5,
 * Step 6.6's real combined entrypoint (src/raft/main.ts extended with a
 * real RaftNode-backed ClusterContext) — this file is kept working in
 * the meantime rather than left broken, since it's still a legitimate
 * way to run a single unreplicated node.
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

  // Trivial always-leader context: this node is the only node, so it's
  // always "leader" and every write commits by definition (writes go
  // straight to the local Store, no replication, no real Raft log —
  // there's nothing to replicate to). index is not a real Raft log
  // index in this mode (there is no Raft log here at all); it's a
  // meaningless placeholder that a real ClusterContext (Step 6.6) would
  // never actually produce this way.
  const soloContext: ClusterContext = {
    isLeader: () => true,
    leaderRedirectHint: () => null,
    write: async (command) => {
      if (command.op === "put") {
        await store.put(command.key, command.value);
      } else {
        await store.delete(command.key);
      }
      return { status: "committed", index: -1 };
    },
  };

  const app = createApp(store, soloContext);

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