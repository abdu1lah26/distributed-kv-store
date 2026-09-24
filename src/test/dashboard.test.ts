// test/dashboard.test.ts
//
// Milestone 9 (DESIGN.md §7.9): isolated tests for the /status and
// /debug/kill routes added to createApp(). Same structural-fake
// pattern as test/api.test.ts's FakeCluster (Milestone 5, Step 6.5) —
// exercises the real Express app against a fake StatusSource, no real
// RaftNode/ApplyLoop needed. main.ts's real wiring is a separate,
// later step (not covered here) once this passes.

import assert from "node:assert";
import request from "supertest";
import { Store } from "../storage/store.js";
import { createApp, type ClusterContext, type StatusSource } from "../api/server.js";
import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";

// Trivial always-leader fake, same shape as api.test.ts's FakeCluster —
// /status and /debug/kill don't touch leadership at all, so this only
// needs to exist to satisfy createApp's required second param.
const fakeCluster: ClusterContext = {
  isLeader: () => true,
  leaderRedirectHint: () => null,
  write: async () => ({ status: "committed", index: -1 }),
};

// Mutable so individual tests can change state (term, commitIndex, etc)
// mid-test without rebuilding the app — mirrors how a real StatusSource
// backed by a live RaftNode/ApplyLoop would behave (values change
// between polls without the app being rebuilt).
function makeFakeStatus(overrides: Partial<StatusSource> = {}): StatusSource {
  return {
    nodeId: "node-a",
    state: "leader",
    currentTerm: 3,
    commitIndex: 7,
    lastApplied: 7,
    currentLeaderId: "node-a",
    ...overrides,
  };
}

async function withTempStore<T>(fn: (store: Store) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "dashboard-test-"));
  const store = new Store(path.join(dir, "wal.log"), path.join(dir, "sstables"));
  await store.open();
  try {
    return await fn(store);
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
}

async function run(): Promise<void> {
  let passed = 0;
  let failed = 0;

  async function test(name: string, fn: () => Promise<void>): Promise<void> {
    try {
      await fn();
      passed++;
      console.log(`  ok - ${name}`);
    } catch (err) {
      failed++;
      console.error(`  FAIL - ${name}`);
      console.error(err);
    }
  }

  // ---- /status: absent unless features.status is provided ----

  await test("GET /status returns 404 when features is omitted entirely", async () => {
    await withTempStore(async (store) => {
      const app = createApp(store, fakeCluster);
      const res = await request(app).get("/status");
      assert.strictEqual(res.status, 404);
    });
  });

  await test("GET /status returns 404 when features.status is undefined", async () => {
    await withTempStore(async (store) => {
      const app = createApp(store, fakeCluster, {});
      const res = await request(app).get("/status");
      assert.strictEqual(res.status, 404);
    });
  });

  // ---- /status: present and correct when provided ----

  await test("GET /status returns 200 with the exact StatusSource shape", async () => {
    await withTempStore(async (store) => {
      const status = makeFakeStatus();
      const app = createApp(store, fakeCluster, { status });
      const res = await request(app).get("/status");
      assert.strictEqual(res.status, 200);
      assert.deepStrictEqual(res.body, {
        nodeId: "node-a",
        state: "leader",
        currentTerm: 3,
        commitIndex: 7,
        lastApplied: 7,
        currentLeaderId: "node-a",
      });
    });
  });

  await test("GET /status reflects live reads, not a startup snapshot", async () => {
    await withTempStore(async (store) => {
      // A plain mutable object satisfies StatusSource structurally
      // without needing getters — mirrors how a live RaftNode-backed
      // StatusSource would report DIFFERENT values across two polls
      // without the app being rebuilt.
      const status: StatusSource = makeFakeStatus({ currentTerm: 1, commitIndex: 0 });
      const app = createApp(store, fakeCluster, { status });

      const first = await request(app).get("/status");
      assert.strictEqual(first.body.currentTerm, 1);
      assert.strictEqual(first.body.commitIndex, 0);

      // Mutate the underlying object the route closure holds a
      // reference to — simulates a real term/commit advancing between
      // polls.
      (status as { currentTerm: number }).currentTerm = 2;
      (status as { commitIndex: number }).commitIndex = 5;

      const second = await request(app).get("/status");
      assert.strictEqual(second.body.currentTerm, 2);
      assert.strictEqual(second.body.commitIndex, 5);
    });
  });

  await test("GET /status includes currentLeaderId: null when leader is unknown", async () => {
    await withTempStore(async (store) => {
      const status = makeFakeStatus({ currentLeaderId: null, state: "candidate" });
      const app = createApp(store, fakeCluster, { status });
      const res = await request(app).get("/status");
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.body.currentLeaderId, null);
      assert.strictEqual(res.body.state, "candidate");
    });
  });

  await test("GET /status sets Access-Control-Allow-Origin, scoped to this route only", async () => {
    await withTempStore(async (store) => {
      const status = makeFakeStatus();
      const app = createApp(store, fakeCluster, { status });

      const statusRes = await request(app).get("/status");
      assert.strictEqual(statusRes.headers["access-control-allow-origin"], "*");

      // Not a general CORS opening — /kv/:key must NOT carry the header.
      const kvRes = await request(app).get("/kv/some-key");
      assert.strictEqual(kvRes.headers["access-control-allow-origin"], undefined);
    });
  });

  // ---- /debug/kill: absent unless features.enableKill === true ----

  await test("POST /debug/kill returns 404 when features is omitted entirely", async () => {
    await withTempStore(async (store) => {
      const app = createApp(store, fakeCluster);
      const res = await request(app).post("/debug/kill");
      assert.strictEqual(res.status, 404);
    });
  });

  await test("POST /debug/kill returns 404 when enableKill is false", async () => {
    await withTempStore(async (store) => {
      const app = createApp(store, fakeCluster, { enableKill: false });
      const res = await request(app).post("/debug/kill");
      assert.strictEqual(res.status, 404);
    });
  });

  await test("POST /debug/kill returns 404 when only status is provided (independent gates)", async () => {
    // Explicitly checks the two features gate independently of each
    // other — enabling /status must not accidentally enable /debug/kill.
    await withTempStore(async (store) => {
      const app = createApp(store, fakeCluster, { status: makeFakeStatus() });
      const res = await request(app).post("/debug/kill");
      assert.strictEqual(res.status, 404);
    });
  });

  // ---- /debug/kill: present and calls process.exit(1) after real flush ----
  //
  // Can't safely call the real route in-process (it would exit this
  // test runner's own process). Instead, spawn a real child process
  // running a tiny fixture server with enableKill:true, hit it over a
  // real socket, and confirm both (a) a genuine 200 {killed:true} body
  // is received BEFORE the connection drops, and (b) the process
  // actually exits with code 1 — proving res.on('finish', ...) really
  // did wait for the flush rather than racing it. This is the only way
  // to honestly test this route without letting a bad implementation
  // (e.g. exiting before flush) pass by accident.

  await test("POST /debug/kill: real process, response flushes before exit, exit code 1", async () => {
    const { spawn } = await import("node:child_process");
    const dir = await mkdtemp(path.join(os.tmpdir(), "dashboard-kill-test-"));
    const fixturePath = path.join(dir, "kill-fixture.ts");
    const { writeFile } = await import("node:fs/promises");

    await writeFile(
      fixturePath,
      `
import path from "node:path";
import { Store } from "${path.resolve("src/storage/store.js").replace(/\\/g, "/")}";
import { createApp } from "${path.resolve("src/api/server.js").replace(/\\/g, "/")}";

async function main() {
  const store = new Store(path.join(process.argv[2], "wal.log"), path.join(process.argv[2], "sstables"));
  await store.open();
  const app = createApp(store, {
    isLeader: () => true,
    leaderRedirectHint: () => null,
    write: async () => ({ status: "committed", index: -1 }),
  }, { enableKill: true });
  app.listen(Number(process.argv[3]), () => {
    console.log("FIXTURE_READY");
  });
}
main();
`,
      "utf8",
    );

    const dataDir = await mkdtemp(path.join(os.tmpdir(), "dashboard-kill-data-"));
    const port = 41000 + Math.floor(Math.random() * 1000);

    // Direct node -> resolved tsx CLI, not npx, per the §4.4.5 orphan-
    // process precedent already established in this project.
    const tsxCli = path.resolve("node_modules/tsx/dist/cli.mjs");
    const child = spawn(process.execPath, [tsxCli, fixturePath, dataDir, String(port)], {
      windowsHide: true,
    });

    let ready = false;
    child.stdout?.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("FIXTURE_READY")) ready = true;
    });

    const start = Date.now();
    while (!ready && Date.now() - start < 5000) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.ok(ready, "fixture server did not report ready in time");

    const res = await fetch(`http://localhost:${port}/debug/kill`, { method: "POST" });
    assert.strictEqual(res.status, 200);
    const body = await res.json();
    assert.deepStrictEqual(body, { killed: true });

    const exitResult = await new Promise<{ code: number | null }>((resolve) => {
      child.on("exit", (code) => resolve({ code }));
    });
    assert.strictEqual(exitResult.code, 1);

    await rm(dir, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) process.exit(1);
}

run();