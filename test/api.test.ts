/**
 * API integration tests (Milestone 3, Step 5) — run with:
 *   npx tsx test/api.test.ts
 *
 * Lives in top-level /test, not src/test/, per DESIGN.md Section 9's
 * intentional split: this wires the API layer to a real Store, so it's
 * an integration test, not a single-module unit test.
 *
 * Uses supertest against createApp(store) directly — no real socket is
 * bound, per the design note's Step 5 approach (createApp() itself
 * doesn't call store.open() or .listen(), so it's testable without one).
 */
import { strict as assert } from "node:assert";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import request from "supertest";
import { Store } from "../src/storage/store.js";
import { createApp } from "../src/api/server.js";

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

async function main(): Promise<void> {
  console.log("API integration tests");

  await test("PUT then GET round-trip returns 200 with the stored value", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

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
      const app = createApp(store);

      const res = await request(app).get("/kv/never-existed");
      assert.equal(res.status, 404);
      assert.deepEqual(res.body, { error: "not found" });
    });
  });

  await test("DELETE then GET returns 404 (tombstone shadows the value)", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

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
      const app = createApp(store);

      // Per the approved design: delete is unconditional, no pre-check
      // GET, consistent with Store's tombstone-always semantics.
      const res = await request(app).delete("/kv/never-existed");
      assert.equal(res.status, 200);
      assert.deepEqual(res.body, { key: "never-existed", deleted: true });
    });
  });

  await test("PUT with a missing 'value' field returns 400", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

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
      const app = createApp(store);

      const res = await request(app).put("/kv/bad").send({ value: 123 });
      assert.equal(res.status, 400);
    });
  });

  await test("PUT with a malformed JSON body returns 400", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

      const res = await request(app)
        .put("/kv/bad")
        .set("Content-Type", "application/json")
        .send("{not valid json");
      assert.equal(res.status, 400);
    });
  });

  await test("an unknown route returns a JSON 404, not Express's default HTML page", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

      const res = await request(app).get("/nope");
      assert.equal(res.status, 404);
      assert.deepEqual(res.body, { error: "not found" });
    });
  });

  await test("an overwritten value returns the new value, not the old one", async () => {
    await withTmpStore(async (store) => {
      const app = createApp(store);

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
      const app = createApp(store);

      const res = await request(app).get("/kv/anything");
      assert.equal(res.status, 500);
      assert.equal(typeof res.body.error, "string");
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  console.log("\nAll API integration tests passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});