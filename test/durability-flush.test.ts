import { strict as assert } from "node:assert";
import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, rm, readdir } from "node:fs/promises";
import type { ChildProcess } from 'child_process';
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { Store } from "../src/storage/store.js";

/**
 * Milestone 2, Step 5: durability tests with a flush involved
 * (DESIGN.md Section 7, Milestone 2, Step 5).
 *
 * Two complementary tests, deliberately not one:
 *
 *  - Test A is the literal Section 7 requirement: a real kill -9 AFTER a
 *    flush has genuinely happened, confirming recovery correctly
 *    combines replayed WAL/MemTable state with existing on-disk SSTable
 *    state. Uses loop-with-flush.ts (a new fixture, NOT write-and-die.ts
 *    — write-and-die.ts stays untouched, already verified stable across
 *    Steps 2-4) with a small configurable flush threshold so this
 *    doesn't need to wait on the real 1000-key threshold.
 *
 *  - Test B is a deterministic delete/overwrite-across-the-flush-
 *    boundary correctness test: a key flushed to an SSTable, then
 *    overwritten or deleted AFTER that flush, must not resurface its
 *    stale flushed value. This is NOT a kill -9 test (see
 *    flush-then-mutate.ts's docstring for why) — it's a clean-but-
 *    uncommitted exit chosen for an exact, repeatable sequence. Test A
 *    covers the literal SIGKILL requirement; Test B covers a
 *    correctness case Test A can't reach deterministically.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const require = createRequire(import.meta.url);
const tsxCliPath = require.resolve("tsx/cli");

const isWindows = process.platform === "win32";
const platformSpawnOptions = isWindows ? { windowsHide: true } : { detached: true };

function spawnFixture(fixtureFile: string, args: string[], captureStdout: boolean) {
  const fixturePath = path.join(__dirname, "fixtures", fixtureFile);
  return spawn(process.execPath, [tsxCliPath, fixturePath, ...args], {
    stdio: captureStdout ? ["ignore", "pipe", "inherit"] : "ignore",
    ...platformSpawnOptions,
  });
}

/**
 * Same cross-platform whole-process-tree kill as durability.test.ts.
 *
 * Parameter typed as `{ pid: number | undefined }`, not `{ pid?: number }`
 * — under this project's `exactOptionalPropertyTypes: true`, those are NOT
 * the same thing. `ChildProcess.pid` is declared as `number | undefined`
 * (always present as a property, but its value may be undefined), whereas
 * `pid?: number` means the property itself may be entirely absent. Node's
 * real ChildProcess type only satisfies the former.
 */
function killProcessGroup(child: ChildProcess) {
  if (child.pid === undefined) return;
  if (isWindows) {
    spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"]);
  } else {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      // Group may already be gone — fine.
    }
  }
}

interface StorePaths {
  walPath: string;
  sstableDir: string;
}

async function makeTempStorePaths(): Promise<StorePaths> {
  const dir = await mkdtemp(path.join(tmpdir(), "kv-durability-flush-"));
  return {
    walPath: path.join(dir, "wal.log"),
    sstableDir: path.join(dir, "sstables"),
  };
}

async function cleanupTempStorePaths({ walPath }: StorePaths): Promise<void> {
  await rm(path.dirname(walPath), { recursive: true, force: true });
}

async function listSSTableFiles(sstableDir: string): Promise<string[]> {
  try {
    const files = await readdir(sstableDir);
    return files.filter((f) => f.startsWith("sstable_") && f.endsWith(".dat"));
  } catch {
    return [];
  }
}

/**
 * Test A: real kill -9 after a flush has genuinely happened.
 */
async function testSigkillAfterRealFlush(): Promise<void> {
  const { walPath, sstableDir } = await makeTempStorePaths();
  const MAX_KEYS = 10; // small, deterministic-ish threshold for this test

  const child = spawnFixture("loop-with-flush.ts", [walPath, String(MAX_KEYS)], true);

  const resolvedWrites: Array<{ key: string; value: string }> = [];
  let stdoutBuffer = "";

  // child.stdout is typed as `Readable | null` by Node's types (it's
  // null only if stdio wasn't configured to pipe it) — we always pass
  // captureStdout=true here, so it's always present in practice, but
  // TypeScript can't know that from the call site alone. Guard explicitly
  // rather than asserting non-null with `!`, so a real misconfiguration
  // would fail loudly instead of crashing on a null dereference later.
  if (!child.stdout) {
    throw new Error("expected child.stdout to be piped, but it was null — check spawnFixture's stdio config");
  }

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

  // Poll until we've seen comfortably more writes than MAX_KEYS, so at
  // least one real flush must already have happened, plus some writes
  // in the fresh post-flush MemTable — same poll-until-evidence pattern
  // as durability.test.ts, for the same cross-machine timing reasons.
  const MIN_WRITES_BEFORE_KILL = MAX_KEYS * 2 + 5;
  const MAX_WAIT_MS = 15_000;
  const POLL_INTERVAL_MS = 20;

  await new Promise<void>((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (resolvedWrites.length >= MIN_WRITES_BEFORE_KILL) {
        resolve();
      } else if (Date.now() - start > MAX_WAIT_MS) {
        reject(
          new Error(
            `timed out waiting for ${MIN_WRITES_BEFORE_KILL} writes (only saw ${resolvedWrites.length})`,
          ),
        );
      } else {
        setTimeout(check, POLL_INTERVAL_MS);
      }
    };
    check();
  });

  killProcessGroup(child);

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code, signal) => {
      if (isWindows) {
        if (code !== 0) resolve();
        else reject(new Error("expected a forced termination on Windows, got clean exit code 0"));
      } else {
        if (signal === "SIGKILL") resolve();
        else reject(new Error(`expected SIGKILL, got signal=${signal}`));
      }
    });
    child.on("error", reject);
  });

  // Confirm a flush genuinely happened — the real point of this test,
  // not just trusting the write count.
  const sstableFilesBeforeRestart = await listSSTableFiles(sstableDir);
  assert.ok(
    sstableFilesBeforeRestart.length >= 1,
    `expected at least one SSTable to exist before restart, found ${sstableFilesBeforeRestart.length}`,
  );

  // Restart: replay WAL + discover SSTables, then verify every resolved
  // write — both pre-flush (served from the SSTable) and post-flush
  // (served from WAL-replayed MemTable) — comes back correctly.
  const store = new Store(walPath, sstableDir);
  await store.open();

  for (const { key, value } of resolvedWrites) {
    assert.equal(
      await store.get(key),
      value,
      `resolved write ${key}=${value} should survive replay, combining SSTable + WAL state`,
    );
  }

  await store.close();
  await cleanupTempStorePaths({ walPath, sstableDir });
  console.log(
    `PASS: testSigkillAfterRealFlush (${resolvedWrites.length} writes verified, ${sstableFilesBeforeRestart.length} SSTable(s) flushed before kill)`,
  );
}

/**
 * Test B: deterministic delete/overwrite-across-a-flush-boundary
 * correctness. NOT a kill -9 test — see flush-then-mutate.ts.
 */
async function testFlushThenMutateAcrossBoundary(): Promise<void> {
  const { walPath, sstableDir } = await makeTempStorePaths();
  const MAX_KEYS = 10;

  const child = spawnFixture("flush-then-mutate.ts", [walPath, String(MAX_KEYS)], false);

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`fixture exited with code ${code}`));
    });
    child.on("error", reject);
  });

  // Confirm the fixture's own precondition: exactly one flush happened
  // (target + doomed + fillers = MAX_KEYS distinct keys).
  const sstableFiles = await listSSTableFiles(sstableDir);
  assert.equal(
    sstableFiles.length,
    1,
    `expected exactly one SSTable from the fixture's setup phase, found ${sstableFiles.length}`,
  );

  const store = new Store(walPath, sstableDir);
  await store.open();

  assert.equal(
    await store.get("target"),
    "updated_target_value",
    "target was overwritten after the flush — must return the NEW value, not the stale flushed one",
  );
  assert.equal(
    await store.get("doomed"),
    undefined,
    "doomed was deleted after the flush — the tombstone must shadow the stale flushed value",
  );
  assert.equal(
    await store.get("fresh"),
    "fresh_value",
    "fresh was written after the flush and never flushed itself — must be recovered via WAL replay alone",
  );
  assert.equal(
    await store.get("filler_0000"),
    "filler_value_0",
    "an untouched filler key must still be served correctly straight from the SSTable",
  );

  await store.close();
  await cleanupTempStorePaths({ walPath, sstableDir });
  console.log("PASS: testFlushThenMutateAcrossBoundary");
}

async function main(): Promise<void> {
  await testSigkillAfterRealFlush();
  await testFlushThenMutateAcrossBoundary();
  console.log("\nAll flush-durability tests passed.");
}

main().catch((err) => {
  console.error("FAIL:", err);
  process.exitCode = 1;
});