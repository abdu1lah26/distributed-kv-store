/**
 * API integration tests (Milestone 3, Step 5; updated Milestone 5, Step
 * 6.5 for Raft-aware createApp) — run with:
 *   npx tsx test/api.test.ts
 *
 * Lives in top-level /test, not src/test/, per DESIGN.md Section 9's
 * intentional split: this wires the API layer to a real Store, so it's
 * an integration test, not a single-module unit test.
 *
 * Uses supertest against createApp(store, cluster) directly — no real
 * socket is bound, per the design note's Step 5 approach (createApp()
 * itself doesn't call store.open() or .listen(), so it's testable
 * without one).
 *
 * Milestone 5, Step 6.5: createApp() now requires a second ClusterContext
 * argument. All of Milestone 3's original tests below are updated to
 * pass a FakeCluster (a lightweight always-leader-by-default fake
 * satisfying ClusterContext, writing straight to the real Store — same
 * structural-interface-fake pattern already used for ApplyLoop's
 * CommitSource/ApplyTarget in src/test/applyLoop.test.ts) so the
 * existing single-node-behavior coverage keeps its value without
 * needing a real Raft cluster spun up just to test JSON body validation
 * or Store wiring. New tests below cover the actual Step 6.5 behavior:
 * 421 NOT_LEADER (both the up-front isLeader() check and a mid-write
 * "not-leader"/"stepped-down" WriteResult), 503 on write timeout, the
 * leaderRedirectHint() body mapping, GET staying leader-independent, and
 * — explicitly, given this file's own history with the malformed-JSON
 * 500-vs-400 bug — that body validation still wins over a 421 when both
 * conditions are true, rather than assuming that ordering silently held.
 */
import { strict as assert } from "node:assert";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import request from "supertest";
import { Store } from "../src/storage/store.js";
import { createApp, type ClusterContext } from "../src/api/server.js";
import type { LogCommand } from "../src/raft/log.js";
import type { WriteResult } from "../src/raft/writePath.js";

function test(name: string, fn: () => Promise<void>): Promise<void> {
  return fn()
    .then(() => console.log(`  ✓ ${name}`))
    .catch((err) => {
      console.error(`  ✗ ${name}`);
      throw err;
    });
}

async function withTmpStore<T>(fn: (store: Store) => Promise<T>): Promise<T> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "api-test-"));
  try {
    const store = new Store(path.join(dir, "wal.log"), path.join(dir, "sstables"));
    await store.open();
    try {
      return await fn(store);
    } finally {
      await store.close();
    }
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

/**
 * A fake ClusterContext for tests that don't care about real Raft
 * behavior: leader by default (so Milestone 3's original tests, which
 * predate any leader concept, keep working unchanged), real writes go
 * straight to the given Store (so GET-after-PUT round-trips still work
 * for real), and every knob is overridable per-test for the new Step
 * 6.5 coverage below.
 */
class FakeCluster implements ClusterContext {
  leader = true;
  redirectHint: { leaderId: string; url: string } | null = null;
  /** Overridable per-test: if set, write() returns this instead of actually writing. */
  forcedWriteResult: WriteResult | null = null;
  writeCallCount = 0;
  private nextIndex = 1;

  constructor(private readonly store: Store) {}

  isLeader(): boolean {
    return this.leader;
  }

  leaderRedirectHint(): { leaderId: string; url: string } | null {
    return this.redirectHint;
  }

  async write(command: LogCommand): Promise<WriteResult> {
    this.writeCallCount += 1;

    if (this.forcedWriteResult !== null) {
      return this.forcedWriteResult;
    }

    if (command.op === "put") {
      await this.store.put(command.key, command.value);
    } else {
      await this.store.delete(command.key);
    }
    return { status: "committed", index: this.nextIndex++ };
  }
}

async function main(): Promise<void> {
  console.log("API integration tests");

  // ---- Milestone 3 original behavior, preserved (via FakeCluster) ----

  await test("PUT then GET round-trip returns 200 with the stored value", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const putRes = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(putRes.status, 200);
      assert.deepEqual(putRes.body, { key: "foo", value: "bar" });

      const getRes = await request(app).get("/kv/foo");
      assert.equal(getRes.status, 200);
      assert.deepEqual(getRes.body, { key: "foo", value: "bar" });
    });
  });

  await test("GET on a key that was never written returns 404", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app).get("/kv/never-existed");
      assert.equal(res.status, 404);
      assert.deepEqual(res.body, { error: "not found" });
    });
  });

  await test("DELETE then GET returns 404 (tombstone shadows the value)", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      await request(app).put("/kv/foo").send({ value: "bar" });

      const delRes = await request(app).delete("/kv/foo");
      assert.equal(delRes.status, 200);
      assert.deepEqual(delRes.body, { key: "foo", deleted: true });

      const getRes = await request(app).get("/kv/foo");
      assert.equal(getRes.status, 404);
      assert.deepEqual(getRes.body, { error: "not found" });
    });
  });

  await test("DELETE on a key that was never written is idempotent (200, not 404)", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      // Per the approved design: delete is unconditional, no pre-check
      // GET, consistent with Store's tombstone-always semantics.
      const res = await request(app).delete("/kv/never-existed");
      assert.equal(res.status, 200);
      assert.deepEqual(res.body, { key: "never-existed", deleted: true });
    });
  });

  await test("PUT with a missing 'value' field returns 400", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app).put("/kv/bad").send({ notValue: 123 });
      assert.equal(res.status, 400);
      assert.equal(typeof res.body.error, "string");

      // Confirm the rejected write never actually landed.
      const getRes = await request(app).get("/kv/bad");
      assert.equal(getRes.status, 404);
    });
  });

  await test("PUT with a non-string 'value' field returns 400", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app).put("/kv/bad").send({ value: 123 });
      assert.equal(res.status, 400);
    });
  });

  await test("PUT with a malformed JSON body returns 400", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app)
        .put("/kv/bad")
        .set("Content-Type", "application/json")
        .send("{not valid json");
      assert.equal(res.status, 400);
    });
  });

  await test("an unknown route returns a JSON 404, not Express's default HTML page", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app).get("/nope");
      assert.equal(res.status, 404);
      assert.deepEqual(res.body, { error: "not found" });
    });
  });

  await test("an overwritten value returns the new value, not the old one", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store, new FakeCluster(store));

      await request(app).put("/kv/k").send({ value: "first" });
      await request(app).put("/kv/k").send({ value: "second" });

      const res = await request(app).get("/kv/k");
      assert.equal(res.status, 200);
      assert.deepEqual(res.body, { key: "k", value: "second" });
    });
  });

  await test("an error thrown by Store (e.g. not opened) is caught and returns JSON 500", async () => {
    // Deliberately does NOT use withTmpStore — constructs a Store that
    // is never open()'d, so Store.assertOpened() throws, exercising the
    // central error-handling middleware rather than the happy path.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "api-test-500-"));
    try {
      const store = new Store(path.join(dir, "wal.log"), path.join(dir, "sstables"));
      const app = createApp(store, new FakeCluster(store));

      const res = await request(app).get("/kv/anything");
      assert.equal(res.status, 500);
      assert.equal(typeof res.body.error, "string");
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  // ---- Milestone 5, Step 6.5: leader-only writes, 421 NOT_LEADER ----

  await test("PUT on a non-leader returns 421 NOT_LEADER without writing", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = false;
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(res.status, 421);
      assert.deepEqual(res.body, { error: "NOT_LEADER", leaderId: null, leaderHint: null });
      assert.equal(cluster.writeCallCount, 0, "write() must not be called at all when isLeader() is false");

      const getRes = await request(app).get("/kv/foo");
      assert.equal(getRes.status, 404, "the rejected write never actually landed in the store");
    });
  });

  await test("DELETE on a non-leader returns 421 NOT_LEADER without writing", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = false;
      const app = createApp(store, cluster);

      const res = await request(app).delete("/kv/foo");
      assert.equal(res.status, 421);
      assert.deepEqual(res.body, { error: "NOT_LEADER", leaderId: null, leaderHint: null });
      assert.equal(cluster.writeCallCount, 0, "write() must not be called at all when isLeader() is false");
    });
  });

  await test("a 421 response includes a real leaderId/leaderHint when the cluster context knows one", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = false;
      cluster.redirectHint = { leaderId: "node-b", url: "http://localhost:5002" };
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(res.status, 421);
      assert.deepEqual(res.body, {
        error: "NOT_LEADER",
        leaderId: "node-b",
        leaderHint: "http://localhost:5002",
      });
    });
  });

  await test('PUT whose write() resolves "not-leader" (a race discovered mid-write) returns 421', async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = true; // passes the up-front check...
      cluster.forcedWriteResult = { status: "not-leader" }; // ...but the write itself finds otherwise
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(res.status, 421);
      assert.equal(res.body.error, "NOT_LEADER");
      assert.equal(cluster.writeCallCount, 1, "write() WAS called this time (the race is discovered inside it, not before)");
    });
  });

  await test('PUT whose write() resolves "stepped-down" also returns 421 (same client-facing situation)', async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = true;
      cluster.forcedWriteResult = { status: "stepped-down", index: 7 };
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(res.status, 421);
      assert.equal(res.body.error, "NOT_LEADER");
    });
  });

  await test('DELETE whose write() resolves "stepped-down" also returns 421', async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.forcedWriteResult = { status: "stepped-down", index: 3 };
      const app = createApp(store, cluster);

      const res = await request(app).delete("/kv/foo");
      assert.equal(res.status, 421);
      assert.equal(res.body.error, "NOT_LEADER");
    });
  });

  // ---- Milestone 5, Step 6.5: 503 on write timeout ----

  await test('PUT whose write() resolves "timeout" returns 503 WRITE_TIMEOUT, not 421 or 500', async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.forcedWriteResult = { status: "timeout", index: 5 };
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/foo").send({ value: "bar" });
      assert.equal(res.status, 503);
      assert.deepEqual(res.body, { error: "WRITE_TIMEOUT", index: 5 });
    });
  });

  await test('DELETE whose write() resolves "timeout" returns 503 WRITE_TIMEOUT', async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.forcedWriteResult = { status: "timeout", index: 9 };
      const app = createApp(store, cluster);

      const res = await request(app).delete("/kv/foo");
      assert.equal(res.status, 503);
      assert.deepEqual(res.body, { error: "WRITE_TIMEOUT", index: 9 });
    });
  });

  // ---- Milestone 5, Step 6.5: GET stays leader-independent ----

  await test("GET works on a non-leader (local reads, per Section 3's documented eventual-consistency tradeoff)", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      const app = createApp(store, cluster);

      // Seed a value while still (the default) leader...
      await request(app).put("/kv/foo").send({ value: "bar" });

      // ...then flip to follower and confirm GET still works against
      // the SAME app/store — GET must not be gated behind isLeader() at
      // all, unlike PUT/DELETE.
      cluster.leader = false;

      const res = await request(app).get("/kv/foo");
      assert.equal(res.status, 200);
      assert.deepEqual(res.body, { key: "foo", value: "bar" });
    });
  });

  // ---- Explicitly required: body validation still wins over 421 ----
  // Given this file's own history with the malformed-JSON 500-vs-400
  // bug, this is tested directly rather than assumed to hold from the
  // handler's written order.

  await test("PUT with a malformed JSON body returns 400 even on a non-leader (validation precedes the leader check)", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = false;
      const app = createApp(store, cluster);

      const res = await request(app)
        .put("/kv/bad")
        .set("Content-Type", "application/json")
        .send("{not valid json");
      assert.equal(res.status, 400, "malformed JSON must surface as 400, not 421, regardless of leadership");
      assert.equal(cluster.writeCallCount, 0);
    });
  });

  await test("PUT with a missing 'value' field returns 400 even on a non-leader", async () => {
    await withTmpStore(async (store) => {
      const cluster = new FakeCluster(store);
      cluster.leader = false;
      const app = createApp(store, cluster);

      const res = await request(app).put("/kv/bad").send({ notValue: 123 });
      assert.equal(res.status, 400, "missing 'value' must surface as 400, not 421, regardless of leadership");
      assert.equal(cluster.writeCallCount, 0);
    });
  });

  console.log("\nAll API integration tests passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});