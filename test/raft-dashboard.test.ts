// test/raft-dashboard.test.ts
//
// Milestone 9 (DESIGN.md §7.9): integration test for main.ts's real
// dashboard wiring — a genuine 3-process cluster with
// ENABLE_STATUS_ENDPOINT and ENABLE_KILL_ENDPOINT both on, verifying
// what was manually confirmed across the §7.9.7/§7.9.8 investigation:
// real /status accuracy (election, term, commitIndex/lastApplied
// convergence) and a real /debug/kill triggering genuine re-election
// against a live 2-node quorum — not the isolated route-mechanics
// coverage test/dashboard.test.ts already provides against a fake
// ClusterContext.
//
// Separate file from test/dashboard.test.ts (unit-level, fake
// StatusSource) and from test/raft-election.test.ts /
// test/raft-write-path.test.ts (real cluster, but no dashboard
// features enabled) — same separation-of-concerns instinct as every
// other module/test split in this project (§9's two-test-locations
// note).
//
// Uses mkdtemp-isolated DATA_DIR per node from the start, per the
// tracked cleanup item in §6.2 ("should be retrofitted to the other
// [election/write-path] tests the next time any test needs to make a
// real assertion about on-disk state") — this file needs exactly that
// guarantee (§7.9.8's original incident was about a specific node's
// on-disk/process state), so it starts clean rather than adding to
// the backlog.

import assert from "node:assert";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

interface NodeHandle {
  id: string;
  clientPort: number;
  raftPort: number;
  process: ChildProcess;
  dataDir: string;
}

interface StatusPayload {
  nodeId: string;
  state: string;
  currentTerm: number;
  commitIndex: number;
  lastApplied: number;
  currentLeaderId: string | null;
}

const TSX_CLI = path.resolve("node_modules/tsx/dist/cli.mjs");
const MAIN_ENTRY = path.resolve("src/raft/main.ts");

/**
 * Polls a predicate until it returns truthy or a timeout elapses. Wraps
 * the predicate in try/catch and treats any thrown error (e.g. fetch's
 * ECONNREFUSED against a not-yet-listening or just-killed node) as
 * "not ready yet, keep polling" rather than a hard failure — the exact
 * bug found and fixed in §6.1 across raft-follower-crash.test.ts and
 * raft-write-path.test.ts. This file starts with the fix already in
 * place rather than risking reintroducing it.
 */
async function pollUntil<T>(
  predicate: () => Promise<T | undefined | null | false>,
  { timeoutMs = 10000, intervalMs = 100 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const result = await predicate();
      if (result) return result;
    } catch {
      // Not ready yet — keep polling, per §6.1.
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`pollUntil: timed out after ${timeoutMs}ms`);
}

async function getStatus(port: number): Promise<StatusPayload> {
  const res = await fetch(`http://localhost:${port}/status`);
  if (!res.ok) throw new Error(`status ${res.status}`);
  return (await res.json()) as StatusPayload;
}

/**
 * Cross-platform process kill, matching §4.4.5's fixture exactly:
 * process group SIGKILL on POSIX, taskkill /T /F on Windows (real
 * process groups/signals don't exist there).
 */
async function killNode(handle: NodeHandle): Promise<void> {
  const pid = handle.process.pid;
  if (pid === undefined) return;

  const exitPromise = new Promise<void>((resolve) => {
    handle.process.on("exit", () => resolve());
  });

  if (process.platform === "win32") {
    await new Promise<void>((resolve) => {
      const killer = spawn("taskkill", ["/pid", String(pid), "/T", "/F"]);
      killer.on("exit", () => resolve());
    });
  } else {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      handle.process.kill("SIGKILL");
    }
  }

  await exitPromise;
}

function spawnNode(id: string, index: number, dataDir: string, peers: NodeHandle[]): NodeHandle {
  const raftPort = 4101 + index;
  const clientPort = 5101 + index;

  const peersEnv = peers.map((p) => `${p.id}=localhost:${p.raftPort}:${p.clientPort}`).join(",");

  const child = spawn(
    process.execPath,
    [TSX_CLI, MAIN_ENTRY],
    {
      env: {
        ...process.env,
        NODE_ID: id,
        PORT: String(raftPort),
        CLIENT_PORT: String(clientPort),
        PEERS: peersEnv,
        DATA_DIR: dataDir,
        ENABLE_STATUS_ENDPOINT: "true",
        ENABLE_KILL_ENDPOINT: "true",
      },
      // Own process group on POSIX so killNode's -pid group-kill can
      // reach it (and the real tsx process underneath, not an npx
      // wrapper — spawned directly via node against the resolved tsx
      // CLI path, per §4.4.5).
      detached: process.platform !== "win32",
      windowsHide: true,
    },
  );

  child.stdout?.on("data", () => {});
  child.stderr?.on("data", (chunk: Buffer) => {
    // Surface anything unexpected immediately rather than only on
    // failure — this file exists partly because §7.9.8's original
    // incident lost exactly this kind of output.
    process.stderr.write(`[${id} stderr] ${chunk}`);
  });

  return { id, clientPort, raftPort, process: child, dataDir };
}

function peersFor(all: NodeHandle[], self: NodeHandle): NodeHandle[] {
  return all.filter((n) => n.id !== self.id);
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

  await test("real 3-node cluster: election, /status agreement, write propagation, /debug/kill re-election", async () => {
    const baseDir = await mkdtemp(path.join(os.tmpdir(), "raft-dashboard-test-"));
    const ids = ["node-a", "node-b", "node-c"];
    const handles: NodeHandle[] = [];

    try {
      // First pass: construct handles with correct peer lists. Peers
      // config needs every OTHER node's ports known up front, so build
      // all 3 dataDirs/ports first, then spawn.
      const dataDirs = await Promise.all(ids.map((id) => mkdtemp(path.join(baseDir, `${id}-`))));
      const provisional = ids.map((id, i) => ({
        id,
        raftPort: 4101 + i,
        clientPort: 5101 + i,
      }));

      for (let i = 0; i < ids.length; i++) {
        const peers = provisional
          .filter((p) => p.id !== ids[i])
          .map((p) => ({ id: p.id, raftPort: p.raftPort, clientPort: p.clientPort }));
        const peersEnv = peers.map((p) => `${p.id}=localhost:${p.raftPort}:${p.clientPort}`).join(",");

        const self = provisional[i]!;
        const child = spawn(process.execPath, [TSX_CLI, MAIN_ENTRY], {
          env: {
            ...process.env,
            NODE_ID: self.id,
            PORT: String(self.raftPort),
            CLIENT_PORT: String(self.clientPort),
            PEERS: peersEnv,
            DATA_DIR: dataDirs[i]!,
            ENABLE_STATUS_ENDPOINT: "true",
            ENABLE_KILL_ENDPOINT: "true",
          },
          detached: process.platform !== "win32",
          windowsHide: true,
        });
        child.stderr?.on("data", (chunk: Buffer) => {
          process.stderr.write(`[${self.id} stderr] ${chunk}`);
        });
        handles.push({ id: self.id, clientPort: self.clientPort, raftPort: self.raftPort, process: child, dataDir: dataDirs[i]! });
      }

      // ---- Wait for a real election to settle ----
      const initialLeader = await pollUntil(async () => {
        const statuses = await Promise.all(handles.map((h) => getStatus(h.clientPort)));
        const leaders = statuses.filter((s) => s.state === "leader");
        if (leaders.length !== 1) return undefined;
        const term = leaders[0]!.currentTerm;
        const allAgree = statuses.every((s) => s.currentTerm === term && s.currentLeaderId === leaders[0]!.nodeId);
        return allAgree ? { leaderId: leaders[0]!.nodeId, term } : undefined;
      }, { timeoutMs: 10000 });

      assert.ok(initialLeader.leaderId);
      const leaderHandle = handles.find((h) => h.id === initialLeader.leaderId)!;

      // ---- Real write via the real leader, real convergence ----
      const putRes = await fetch(`http://localhost:${leaderHandle.clientPort}/kv/dashboard-key`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: "dashboard-value" }),
      });
      assert.strictEqual(putRes.status, 200);

      await pollUntil(async () => {
        const statuses = await Promise.all(handles.map((h) => getStatus(h.clientPort)));
        return statuses.every((s) => s.commitIndex >= 1 && s.lastApplied >= 1) ? true : undefined;
      }, { timeoutMs: 5000 });

      const converged = await Promise.all(handles.map((h) => getStatus(h.clientPort)));
      for (const s of converged) {
        assert.strictEqual(s.commitIndex, s.lastApplied, `${s.nodeId}: commitIndex should equal lastApplied once settled`);
      }

      // ---- Real /debug/kill against the real leader ----
      const killRes = await fetch(`http://localhost:${leaderHandle.clientPort}/debug/kill`, { method: "POST" });
      assert.strictEqual(killRes.status, 200);
      assert.deepStrictEqual(await killRes.json(), { killed: true });

      await new Promise<void>((resolve) => {
        leaderHandle.process.on("exit", () => resolve());
      });

      const survivors = handles.filter((h) => h.id !== leaderHandle.id);

      // ---- Real re-election among the 2 survivors ----
      const newLeader = await pollUntil(async () => {
        const statuses = await Promise.all(survivors.map((h) => getStatus(h.clientPort)));
        const leaders = statuses.filter((s) => s.state === "leader");
        if (leaders.length !== 1) return undefined;
        const term = leaders[0]!.currentTerm;
        const allAgree = statuses.every((s) => s.currentTerm === term && s.currentLeaderId === leaders[0]!.nodeId);
        return allAgree ? { leaderId: leaders[0]!.nodeId, term } : undefined;
      }, { timeoutMs: 10000 });

      assert.notStrictEqual(newLeader.leaderId, initialLeader.leaderId, "a genuinely different node must be elected — the killed node can't be its own successor");
      assert.ok(newLeader.term > initialLeader.term, "re-election must produce a strictly higher term");

      // Kill the 2 survivors for cleanup.
      for (const h of survivors) {
        await killNode(h);
      }
    } finally {
      for (const h of handles) {
        if (h.process.exitCode === null && h.process.signalCode === null) {
          await killNode(h).catch(() => {});
        }
      }
      await rm(baseDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) process.exit(1);
}

run();