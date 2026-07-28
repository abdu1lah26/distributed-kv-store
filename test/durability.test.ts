import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Store } from "../src/storage/store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.join(__dirname, "fixtures", "write-and-die.ts");

async function makeTempWalPath(): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "kv-durability-"));
  return path.join(dir, "wal.log");
}

async function cleanupTempWal(walPath: string): Promise<void> {
  await rm(path.dirname(walPath), { recursive: true, force: true });
}

/**
 * Test (a): clean exit after writes, no explicit close().
 * Verifies the basic crash-then-replay path.
 */
async function testCleanExitReplay(): Promise<void> {
  const walPath = await makeTempWalPath();

  const child = spawn("npx", ["tsx", fixturePath, walPath, "fixed"], {
    stdio: "ignore",
  });

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`fixture exited with code ${code}`));
    });
    child.on("error", reject);
  });

  const store = new Store(walPath);
  await store.open();

  // Fixture wrote: put a=1, put b=2, delete a, put c=3
  assert.equal(store.get("a"), undefined, "a was deleted, should be absent");
  assert.equal(store.get("b"), "2", "b should survive replay");
  assert.equal(store.get("c"), "3", "c should survive replay");

  await store.close();
  await cleanupTempWal(walPath);
  console.log("PASS: testCleanExitReplay");
}

/**
 * Test (b): true SIGKILL mid-write. Verifies every write that had
 * *resolved* (fsync completed, logged by the fixture) before the kill is
 * recovered, nothing beyond that, and that replay doesn't throw.
 */
async function testSigkillMidWriteReplay(): Promise<void> {
  const walPath = await makeTempWalPath();

  const child = spawn("npx", ["tsx", fixturePath, walPath, "loop"], {
    stdio: ["ignore", "pipe", "inherit"],
  });

  const resolvedWrites: Array<{ key: string; value: string }> = [];
  let stdoutBuffer = "";
  child.stdout.on("data", (chunk: Buffer) => {
    stdoutBuffer += chunk.toString("utf8");
    let newlineIndex = stdoutBuffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = stdoutBuffer.slice(0, newlineIndex);
      stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);
      const match = /^WROTE (\S+)=(\S+)$/.exec(line);
      if (match && match[1] && match[2]) {
        resolvedWrites.push({ key: match[1], value: match[2] });
      }
      newlineIndex = stdoutBuffer.indexOf("\n");
    }
  });

  // Let the child accumulate a handful of writes, then hard-kill it.
  await new Promise((resolve) => setTimeout(resolve, 150));
  child.kill("SIGKILL");

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (_code, signal) => {
      if (signal === "SIGKILL") resolve();
      else reject(new Error(`expected SIGKILL, got signal=${signal}`));
    });
    child.on("error", reject);
  });

  assert.ok(
    resolvedWrites.length > 0,
    "expected at least one write to resolve before the kill",
  );

  const store = new Store(walPath);
  await store.open(); // must not throw

  for (const { key, value } of resolvedWrites) {
    assert.equal(
      store.get(key),
      value,
      `resolved write ${key}=${value} should survive replay`,
    );
  }

  await store.close();
  await cleanupTempWal(walPath);
  console.log(
    `PASS: testSigkillMidWriteReplay (${resolvedWrites.length} writes verified)`,
  );
}

/**
 * Extra test: manually truncate the WAL mid-record to simulate a torn
 * write, and verify replay() stops cleanly without throwing, per
 * Section 4.3. Not naturally exercised by (a)/(b) since fsync completes
 * before append() ever resolves.
 */
async function testTornWriteReplayDoesNotThrow(): Promise<void> {
  const walPath = await makeTempWalPath();

  const store = new Store(walPath);
  await store.open();
  await store.put("clean", "record");
  await store.close();

  // Manually append a torn record: a length prefix claiming a payload
  // larger than what actually follows on disk.
  const lengthBuffer = Buffer.alloc(4);
  lengthBuffer.writeUInt32BE(1000, 0); // claims 1000 bytes
  const partialPayload = Buffer.from('{"op":"PUT","key":"torn"', "utf8"); // truncated JSON

  const existing = await readFile(walPath);
  await writeFile(
    walPath,
    Buffer.concat([existing, lengthBuffer, partialPayload]),
  );

  const recoveredStore = new Store(walPath);
  await recoveredStore.open(); // must not throw

  assert.equal(
    recoveredStore.get("clean"),
    "record",
    "the clean record before the torn one should still be recovered",
  );
  assert.equal(
    recoveredStore.get("torn"),
    undefined,
    "the torn record must not be applied",
  );

  await recoveredStore.close();
  await cleanupTempWal(walPath);
  console.log("PASS: testTornWriteReplayDoesNotThrow");
}

async function main(): Promise<void> {
  await testCleanExitReplay();
  await testSigkillMidWriteReplay();
  await testTornWriteReplayDoesNotThrow();
  console.log("\nAll durability tests passed.");
}

main().catch((err) => {
  console.error("FAIL:", err);
  process.exitCode = 1;
});
