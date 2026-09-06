// Integration test: Section 6 item #3 — network partition (minority
// side). Top-level /test, same tier as the other multi-process
// integration tests.
//
// Run via: npx tsx test/raft-partition.test.ts
//
// SCOPE (DESIGN.md §6.3): "Network partition (minority side) → minority
// side cannot elect a leader or accept writes (no split-brain)."
//
// WHY THIS IS A DIFFERENT TEST FROM §6.2 PART A (partial process
// crash), NOT A VARIANT OF IT:
//
// §6.2 Part A killed 2 of 3 processes outright — process DEATH, not a
// network PARTITION. A killed process is gone entirely: no election
// timer running, unable to respond to anything, not even a stale
// self-belief about being leader. A partitioned process is ALIVE and
// running — its Raft state machine keeps ticking, it can still respond
// to a real client (only inter-node links are cut, not client access),
// and critically, if it was the LEADER when the partition happened, it
// has no built-in way to notice — standard Raft only steps a leader
// down on discovering a higher term via a real RPC, never from mere
// silence. §6.2 proved "a 1-of-3 minority can't elect itself leader."
// It never tested the genuinely distinct, and more interesting,
// question this test is actually about: if the LEADER itself becomes
// isolated, can it be tricked into telling a client a write succeeded
// when it structurally cannot have gotten majority commit? That is the
// real split-brain risk, and §6.2 never touched it.
//
// HOW A REAL PARTITION IS SIMULATED WITHOUT DEBUG HOOKS, ADMIN-LEVEL
// FIREWALL RULES, OR ANY CHANGE TO SHIPPED CODE:
//
// A thin TCP passthrough proxy (plain node:net, no HTTP-awareness
// needed) sits in front of every directed inter-node Raft-RPC link. Six
// proxies total — one per (from, to) ordered pair among the 3 nodes,
// since each direction is a logically separate set of outgoing HTTP
// calls (A calling B and B calling A are different connections). Each
// node's PEERS config is built to route its RAFT PORT entry for a given
// peer through that peer's inbound proxy, rather than the peer's real
// Raft port directly. The CLIENT port entries in PEERS are left as the
// real client ports, unchanged — only inter-node Raft RPC needs
// isolating; a real external client was never partitioned in a real
// partition scenario either, so this test's own client calls also go
// straight to each node's real client port throughout.
//
// Partitioning a node = closing the 4 proxies that touch it (2 outbound
// + 2 inbound; the 2 proxies for the OTHER pair, not touching the
// isolated node, are left open). Closing a proxy both stops it from
// accepting new connections AND forcibly destroys any already-live
// piped sockets — necessary because an HTTP keep-alive connection
// established before the partition could otherwise silently keep
// working through an already-open pipe, which would be a fake
// partition, not a real one. This produces a clean, deterministic,
// immediate connection-refused failure mode for any new or in-flight
// inter-node call — not a flaky timeout-based approximation.
//
// WHO GETS ISOLATED IS DECIDED DYNAMICALLY, NOT ASSUMED: this test
// cannot control which of the 3 nodes wins the initial election, and
// retrying spawns until a specific node happens to win would make the
// test's reliability depend on election luck — exactly the kind of
// hidden flakiness this project has avoided everywhere else. Instead,
// all 6 directed proxies are built up front (covering every node as a
// possible isolation target), and only the 4 relevant to whichever node
// actually becomes leader are closed once the real leader is known.
//
// BLACK-BOX CONSTRAINT, UNCHANGED, NO EXCEPTIONS: no debug endpoint
// asking the isolated node "do you still think you're leader" — its
// state is inferred purely from what a real external client could
// observe (what it does or doesn't return over its real client API),
// same as every other test in this project.
//
// DIAGNOSTIC TRANSPARENCY: the hard assertion for the isolated leader's
// write attempt is only "it must not falsely return 200" — but HOW it
// fails (timeout, non-200 status, connection-level error) is logged
// regardless, same diagnostic-transparency habit as
// test/raft-leader-crash.test.ts's ack/no-ack split logging. Knowing
// the failure mode is useful even though the precise mode isn't asserted
// on, since the isolated node's client API itself is never cut off —
// per §6.5, an isolated leader unable to reach majority is expected to
// eventually return its own real 503 WRITE_TIMEOUT after proposeWrite's
// 1000ms internal timeout, not a raw connection error, since only the
// inter-node RPC ports are partitioned.
//
// EXPLICITLY OUT OF SCOPE: partition healing / rejoin behavior (closer
// to test #2's territory; Section 6 item #3's literal wording is about
// the minority being unable to elect/write, not recovery); any
// partition shape other than "isolate one of the 3 nodes" (the only
// meaningfully different shape with a 3-node cluster).

import { spawn, ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string): void {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`FAIL: ${message}`);
  }
}

// ---- Cross-platform process spawn (§4.4.5 pattern, mirrored from the other integration tests) ----

const tsxCliPath = require.resolve("tsx/cli");
const mainTsPath = path.join(__dirname, "..", "src", "raft", "main.ts");

interface NodeProcess {
  nodeId: string;
  port: number;
  clientPort: number;
  child: ChildProcess;
  stdoutBuffer: string;
}

function spawnNode(nodeId: string, port: number, clientPort: number, peers: string): NodeProcess {
  const child = spawn(process.execPath, [tsxCliPath, mainTsPath], {
    env: {
      ...process.env,
      NODE_ID: nodeId,
      PORT: String(port),
      CLIENT_PORT: String(clientPort),
      PEERS: peers,
    },
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });

  const proc: NodeProcess = { nodeId, port, clientPort, child, stdoutBuffer: "" };

  child.stdout?.on("data", (chunk: Buffer) => {
    proc.stdoutBuffer += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    process.stderr.write(`[${nodeId} stderr] ${chunk.toString()}`);
  });

  return proc;
}

async function killNode(proc: NodeProcess): Promise<void> {
  const pid = proc.child.pid;
  if (pid === undefined) {
    return;
  }

  const exited = new Promise<void>((resolve) => {
    proc.child.once("exit", () => resolve());
  });

  if (process.platform === "win32") {
    spawn("taskkill", ["/pid", String(pid), "/T", "/F"]);
  } else {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      // Process may have already exited.
    }
  }

  await Promise.race([exited, new Promise((resolve) => setTimeout(resolve, 3000))]);
}

// ---- Stdout polling for leader election (mirrored from the other integration tests) ----

const LEADER_LINE = /state=leader/;

function nodeIdFromLeaderLine(buffer: string, candidateIds: string[]): string | null {
  const lines = buffer.split("\n");
  for (const line of lines) {
    if (LEADER_LINE.test(line)) {
      const match = candidateIds.find((id) => line.startsWith(`[${id}]`));
      if (match) {
        return match;
      }
    }
  }
  return null;
}

async function waitForLeader(nodes: NodeProcess[], timeoutMs: number): Promise<NodeProcess | null> {
  const candidateIds = nodes.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of nodes) {
      const leaderId = nodeIdFromLeaderLine(node.stdoutBuffer, candidateIds);
      if (leaderId !== null) {
        return nodes.find((n) => n.nodeId === leaderId) ?? null;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
}

async function pollForLeaderAmong(
  nodes: NodeProcess[],
  bufferOffsets: Map<string, number>,
  timeoutMs: number,
): Promise<NodeProcess | null> {
  const candidateIds = nodes.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of nodes) {
      const offset = bufferOffsets.get(node.nodeId) ?? 0;
      const newOutput = node.stdoutBuffer.slice(offset);
      const leaderId = nodeIdFromLeaderLine(newOutput, candidateIds);
      if (leaderId !== null) {
        return nodes.find((n) => n.nodeId === leaderId) ?? null;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
}

// ---- Generic condition polling (same ECONNREFUSED-safe fix as the other integration tests) ----

async function pollUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    let result: boolean;
    try {
      result = await predicate();
    } catch {
      result = false;
    }
    if (result) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

function toRecordOrNull(value: unknown): Record<string, unknown> | null {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

async function parseJsonBody(res: Response): Promise<Record<string, unknown> | null> {
  let parsed: unknown;
  try {
    parsed = await res.json();
  } catch {
    return null;
  }
  return toRecordOrNull(parsed);
}

interface KvResponse {
  status: number;
  body: Record<string, unknown> | null;
}

async function getKv(clientPort: number, key: string): Promise<KvResponse> {
  try {
    const res = await fetch(`http://localhost:${clientPort}/kv/${key}`);
    const body = await parseJsonBody(res);
    return { status: res.status, body };
  } catch {
    return { status: 0, body: null };
  }
}

async function putKv(clientPort: number, key: string, value: string): Promise<KvResponse> {
  try {
    const res = await fetch(`http://localhost:${clientPort}/kv/${key}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    });
    const body = await parseJsonBody(res);
    return { status: res.status, body };
  } catch {
    return { status: 0, body: null };
  }
}

// ---- TCP passthrough proxy (the partition mechanism itself) ----

interface ProxyHandle {
  server: net.Server;
  port: number;
  activeSockets: Set<net.Socket>;
}

function startProxy(targetPort: number): Promise<ProxyHandle> {
  return new Promise((resolve, reject) => {
    const activeSockets = new Set<net.Socket>();

    const server = net.createServer((clientSocket) => {
      const upstream = net.connect(targetPort, "127.0.0.1");
      activeSockets.add(clientSocket);
      activeSockets.add(upstream);

      clientSocket.pipe(upstream);
      upstream.pipe(clientSocket);

      const cleanup = (): void => {
        activeSockets.delete(clientSocket);
        activeSockets.delete(upstream);
        clientSocket.destroy();
        upstream.destroy();
      };
      clientSocket.on("error", cleanup);
      upstream.on("error", cleanup);
      clientSocket.on("close", cleanup);
      upstream.on("close", cleanup);
    });

    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      resolve({ server, port, activeSockets });
    });
  });
}

/**
 * Closes a proxy AND forcibly destroys any already-live piped sockets —
 * not just stopping new connections. Without the forced destroy, an
 * HTTP keep-alive connection established before the partition could
 * keep working straight through an already-open pipe, which would make
 * this a fake partition (new calls blocked, old ones silently fine)
 * rather than a real one.
 */
function closeProxy(handle: ProxyHandle): Promise<void> {
  return new Promise((resolve) => {
    for (const socket of handle.activeSockets) {
      socket.destroy();
    }
    handle.activeSockets.clear();
    handle.server.close(() => resolve());
  });
}

// ---- Test ----

type ShortId = "a" | "b" | "c";
const SHORT_IDS: ShortId[] = ["a", "b", "c"];

function fullNodeId(id: ShortId): string {
  return `node-${id}`;
}

async function main(): Promise<void> {
  const PORT: Record<ShortId, number> = { a: 6401, b: 6402, c: 6403 };
  const CLIENT_PORT: Record<ShortId, number> = { a: 6501, b: 6502, c: 6503 };

  // ---- Build all 6 directed proxy links up front — covers isolating
  // any of the 3 nodes, since we don't get to choose who wins the
  // initial election (see header comment). Keyed "from-to": the proxy
  // used when node `from` calls node `to`.
  const directedPairs: Array<[ShortId, ShortId]> = [];
  for (const from of SHORT_IDS) {
    for (const to of SHORT_IDS) {
      if (from !== to) {
        directedPairs.push([from, to]);
      }
    }
  }

  const proxies = new Map<string, ProxyHandle>();
  await Promise.all(
    directedPairs.map(async ([from, to]) => {
      const handle = await startProxy(PORT[to]);
      proxies.set(`${from}-${to}`, handle);
    }),
  );

  async function isolate(nodeId: ShortId): Promise<void> {
    const toClose = [...proxies.entries()].filter(([key]) => {
      const [from, to] = key.split("-") as [ShortId, ShortId];
      return from === nodeId || to === nodeId;
    });
    await Promise.all(toClose.map(([, handle]) => closeProxy(handle)));
  }

  async function closeAllProxies(): Promise<void> {
    await Promise.all([...proxies.values()].map((handle) => closeProxy(handle)));
  }

  const peersFor = (self: ShortId): string => {
    return SHORT_IDS.filter((id) => id !== self)
      .map((peerId) => {
        // Raft-RPC port routes through the proxy for this direction —
        // the mechanism that makes partitioning possible later. Client
        // port stays real/direct, since only inter-node RPC is ever
        // isolated (see header comment).
        const proxyPort = proxies.get(`${self}-${peerId}`)!.port;
        return `${fullNodeId(peerId)}=127.0.0.1:${proxyPort}:${CLIENT_PORT[peerId]}`;
      })
      .join(",");
  };

  const nodeA = spawnNode(fullNodeId("a"), PORT.a, CLIENT_PORT.a, peersFor("a"));
  const nodeB = spawnNode(fullNodeId("b"), PORT.b, CLIENT_PORT.b, peersFor("b"));
  const nodeC = spawnNode(fullNodeId("c"), PORT.c, CLIENT_PORT.c, peersFor("c"));
  const nodesByShortId: Record<ShortId, NodeProcess> = { a: nodeA, b: nodeB, c: nodeC };
  const allNodes = [nodeA, nodeB, nodeC];

  try {
    // ---- Setup: wait for a real leader, through the proxy layer (normal passthrough so far) ----
    const leader = await waitForLeader(allNodes, 15_000);
    assert(leader !== null, "a leader emerges among the 3 nodes (routed through the proxy layer) within 15s");
    if (leader === null) {
      return;
    }

    const leaderShortId = (SHORT_IDS.find((id) => fullNodeId(id) === leader.nodeId)) as ShortId;
    const majorityShortIds = SHORT_IDS.filter((id) => id !== leaderShortId);
    const majorityNodes = majorityShortIds.map((id) => nodesByShortId[id]);

    console.log(`[diagnostic] leader elected: ${leader.nodeId} — this node will be the one isolated.`);

    // ---- Baseline: ordinary writes converge before any partition ----
    const baselineWrites: Array<[string, string]> = [
      ["p1", "vp1"],
      ["p2", "vp2"],
    ];
    for (const [key, value] of baselineWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `baseline PUT /kv/${key} returns 200`);
    }
    for (const node of allNodes) {
      for (const [key, value] of baselineWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 3000);
        assert(ok, `${node.nodeId} converges on baseline key ${key} before the partition`);
      }
    }

    const bufferOffsets = new Map(majorityNodes.map((n) => [n.nodeId, n.stdoutBuffer.length]));

    // ---- Partition: isolate the leader (still alive, just cut off from peers) ----
    await isolate(leaderShortId);
    console.log(`[diagnostic] partition applied: ${leader.nodeId} is now isolated from both peers (still running).`);

    // ---- Attempt a write directly against the isolated (former) leader ----
    const isolatedWriteKey = "p-during-partition";
    const isolatedWriteValue = "should-not-commit";
    const isolatedWriteRes = await putKv(leader.clientPort, isolatedWriteKey, isolatedWriteValue);

    // Diagnostic — HOW it failed, logged regardless of what it turns
    // out to be, per standing project habit (same as the leader-crash
    // ack/no-ack split). The isolated node's client API is NOT cut off
    // (only inter-node RPC is), so per §6.5 the expected shape is a
    // real 503 WRITE_TIMEOUT once proposeWrite's own 1000ms internal
    // timeout elapses — not a raw connection error.
    console.log(
      `[diagnostic] isolated leader's write attempt returned status=${isolatedWriteRes.status}, ` +
        `body=${JSON.stringify(isolatedWriteRes.body)}`,
    );

    // ---- THE CORE SAFETY ASSERTION: it must never falsely succeed ----
    assert(
      isolatedWriteRes.status !== 200,
      `isolated (former) leader does NOT return 200 for a write it cannot get majority commit on (got ${isolatedWriteRes.status})`,
    );

    // ---- Majority side notices the silence and elects a new leader ----
    const newLeader = await pollForLeaderAmong(majorityNodes, bufferOffsets, 15_000);
    assert(newLeader !== null, "the 2-node majority side elects a new leader within 15s of the partition");

    // ---- Majority side continues accepting and converging writes ----
    if (newLeader !== null) {
      const postPartitionWrites: Array<[string, string]> = [["p3", "vp3"]];
      for (const [key, value] of postPartitionWrites) {
        const res = await putKv(newLeader.clientPort, key, value);
        assert(res.status === 200, `PUT /kv/${key} via the new majority-side leader returns 200`);
      }
      for (const node of majorityNodes) {
        for (const [key, value] of postPartitionWrites) {
          const ok = await pollUntil(async () => {
            const r = await getKv(node.clientPort, key);
            return r.status === 200 && r.body?.value === value;
          }, 3000);
          assert(ok, `${node.nodeId} converges on post-partition key ${key}`);
        }
      }

      // ---- Isolated node never receives what the majority committed while it was cut off ----
      const isolatedMissingPostPartitionWrite = await getKv(leader.clientPort, "p3");
      assert(
        !(isolatedMissingPostPartitionWrite.status === 200 && isolatedMissingPostPartitionWrite.body?.value === "vp3"),
        "the isolated node does NOT have the write the majority committed while it was cut off",
      );
    }

    // ---- The write attempted against the isolated leader never committed anywhere ----
    const isolatedWriteAbsentOnIsolated = await getKv(leader.clientPort, isolatedWriteKey);
    assert(
      !(isolatedWriteAbsentOnIsolated.status === 200 && isolatedWriteAbsentOnIsolated.body?.value === isolatedWriteValue),
      "the failed write is absent even on the isolated node's own local state (never appended/never committed)",
    );
    for (const node of majorityNodes) {
      const r = await getKv(node.clientPort, isolatedWriteKey);
      assert(
        !(r.status === 200 && r.body?.value === isolatedWriteValue),
        `${node.nodeId} does NOT have the write attempted against the isolated leader (never committed anywhere)`,
      );
    }
  } finally {
    await Promise.all(allNodes.map((n) => killNode(n)));
    await closeAllProxies();
  }
}

main()
  .then(() => {
    console.log(`\n${passed} passed, ${failed} failed`);
    if (failed > 0) {
      process.exit(1);
    }
  })
  .catch((err) => {
    console.error("Integration test crashed:", err);
    process.exit(1);
  });