import { strict as assert } from "node:assert";
import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { Store } from "../src/storage/store.js";
import type { ChildProcess } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.join(__dirname, "fixtures", "write-and-die.ts");

// Resolve tsx's CLI entry and invoke it directly via `node <tsx-cli> <file>`
// instead of `spawn("npx", ["tsx", ...])`. npx forks an extra process on top
// of the real one it runs, and a kill sent to that outer npx process does
// NOT propagate to the actual fixture process underneath it — the fixture
// survives as an orphan. That was invisible in Milestone 1 (an orphan just
// kept appending harmlessly to a WAL file about to be deleted anyway), but
// in Milestone 2 an orphan can live long enough to cross the flush
// threshold and crash once its temp directory has already been cleaned up.
// Spawning tsx's CLI directly under node is a single process, so kill()
// actually reaches it.
const require = createRequire(import.meta.url);
const tsxCliPath = require.resolve("tsx/cli");

const isWindows = process.platform === "win32";

// On POSIX, `detached: true` puts the spawned process in its own process
// group, which is what lets killProcessGroup() below reach every process
// tsx spawns internally (tsx's CLI forks a further child of its own to
// actually run the script — confirmed by inspecting tsx/dist/cli.mjs), not
// just the top one. Windows has no equivalent of POSIX process groups, and
// `detached: true` there instead makes the child spawn its own console
// window unless windowsHide is set — so on Windows we skip detached
// entirely and rely on `taskkill /T` (whole tree) in killProcessGroup()
// instead, which walks the process tree via the OS's own bookkeeping
// rather than a process group.
const platformSpawnOptions = isWindows ? { windowsHide: true } : { detached: true };

function spawnFixtureIgnoreStdio(walPath: string, mode: string) {
  return spawn(process.execPath, [tsxCliPath, fixturePath, walPath, mode], {
    stdio: "ignore",
    ...platformSpawnOptions,
  });
}

function spawnFixtureCaptureStdout(walPath: string, mode: string) {
  return spawn(process.execPath, [tsxCliPath, fixturePath, walPath, mode], {
    stdio: ["ignore", "pipe", "inherit"],
    ...platformSpawnOptions,
  });
}

/**
 * Force-kills every process in `child`'s process tree, not just `child`
 * itself.
 *
 * POSIX: SIGKILL sent to the negative pid targets the whole process group
 * (requires detached: true above, which makes `child` the group leader).
 *
 * Windows: no process groups or real signals exist. `taskkill /T /F`
 * forcibly terminates (TerminateProcess) the given pid and every process
 * in its tree, which is the closest available equivalent.
 */
function killProcessGroup(child: ChildProcess) {
  if (child.pid === undefined) return;

  if (isWindows) {
    spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"]);
  } else {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      // Group may already be gone (e.g. process exited on its own) — fine.
    }
  }
}

interface StorePaths {
  walPath: string;
  sstableDir: string;
}

async function makeTempStorePaths(): Promise<StorePaths> {
  const dir = await mkdtemp(path.join(tmpdir(), "kv-durability-"));
  return {
    walPath: path.join(dir, "wal.log"),
    sstableDir: path.join(dir, "sstables"),
  };
}

async function cleanupTempStorePaths({ walPath }: StorePaths): Promise<void> {
  await rm(path.dirname(walPath), { recursive: true, force: true });
}

/**
 * Test (a): clean exit after writes, no explicit close().
 * Verifies the basic crash-then-replay path.
 */
async function testCleanExitReplay(): Promise<void> {
  const { walPath, sstableDir } = await makeTempStorePaths();

  const child = spawnFixtureIgnoreStdio(walPath, "fixed");

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`fixture exited with code ${code}`));
    });
    child.on("error", reject);
  });

  const store = new Store(walPath, sstableDir);
  await store.open();

  // Fixture wrote: put a=1, put b=2, delete a, put c=3
  assert.equal(await store.get("a"), undefined, "a was deleted, should be absent");
  assert.equal(await store.get("b"), "2", "b should survive replay");
  assert.equal(await store.get("c"), "3", "c should survive replay");

  await store.close();
  await cleanupTempStorePaths({ walPath, sstableDir });
  console.log("PASS: testCleanExitReplay");
}

/**
 * Test (b): true SIGKILL mid-write. Verifies every write that had
 * *resolved* (fsync completed, logged by the fixture) before the kill is
 * recovered, nothing beyond that, and that replay doesn't throw.
 */
async function testSigkillMidWriteReplay(): Promise<void> {
  const { walPath, sstableDir } = await makeTempStorePaths();

  const child = spawnFixtureCaptureStdout(walPath, "loop");

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

  // Wait until we've SEEN evidence of real writes resolving, rather than
  // guessing a fixed delay before killing. A fixed wait (e.g. 150ms) is
  // fragile across machines — cold-start time for spawning tsx varies a
  // lot (fast on some Linux/Mac setups, much slower observed on Windows /
  // Git Bash, and slower still in some sandboxed environments), so a
  // constant tuned for one machine can easily be too short on another.
  // Polling for actual output sidesteps that entirely: we kill as soon as
  // we have real evidence of resolved writes, on whatever machine this
  // runs on.
  const MIN_WRITES_BEFORE_KILL = 3;
  const MAX_WAIT_MS = 10_000;
  const POLL_INTERVAL_MS = 20;

  await new Promise<void>((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (resolvedWrites.length >= MIN_WRITES_BEFORE_KILL) {
        resolve();
      } else if (Date.now() - start > MAX_WAIT_MS) {
        reject(
          new Error(
            `timed out after ${MAX_WAIT_MS}ms waiting for ${MIN_WRITES_BEFORE_KILL} writes to resolve (only saw ${resolvedWrites.length}) — the fixture process may not be starting at all`,
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
      // The fixture's "loop" mode is an infinite loop with no code path
      // that exits on its own — the ONLY way this process ends is by
      // being forcibly terminated (by us, above) or by crashing on an
      // unexpected error. So "did it fail to reach a clean/normal exit"
      // is the meaningful cross-platform check here, not the exact
      // signal — POSIX reports signal:'SIGKILL', code:null; Windows'
      // taskkill /F reports code:1 (via TerminateProcess), signal:null,
      // since Windows has no real POSIX signals to report.
      if (isWindows) {
        if (code !== 0) resolve();
        else reject(new Error("expected a forced termination on Windows, but process exited cleanly with code 0"));
      } else {
        if (signal === "SIGKILL") resolve();
        else reject(new Error(`expected SIGKILL, got signal=${signal}`));
      }
    });
    child.on("error", reject);
  });

  assert.ok(
    resolvedWrites.length > 0,
    "expected at least one write to resolve before the kill",
  );

  const store = new Store(walPath, sstableDir);
  await store.open(); // must not throw

  for (const { key, value } of resolvedWrites) {
    assert.equal(
      await store.get(key),
      value,
      `resolved write ${key}=${value} should survive replay`,
    );
  }

  await store.close();
  await cleanupTempStorePaths({ walPath, sstableDir });
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
  const { walPath, sstableDir } = await makeTempStorePaths();

  const store = new Store(walPath, sstableDir);
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

  const recoveredStore = new Store(walPath, sstableDir);
  await recoveredStore.open(); // must not throw

  assert.equal(
    await recoveredStore.get("clean"),
    "record",
    "the clean record before the torn one should still be recovered",
  );
  assert.equal(
    await recoveredStore.get("torn"),
    undefined,
    "the torn record must not be applied",
  );

  await recoveredStore.close();
  await cleanupTempStorePaths({ walPath, sstableDir });
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