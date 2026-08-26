// Unit tests for src/cluster/config.ts (Milestone 4, Step 2; updated
// Milestone 5, Step 6.1 for the dual-port PEERS format).
// Plain Node script, run via `npx tsx src/test/config.test.ts` — no test
// framework, per this project's established convention (§9).

import { parseClusterConfig } from "../cluster/config.js";

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

function assertEqual<T>(actual: T, expected: T, message: string): void {
  assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    `${message} (expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)})`,
  );
}

function assertThrows(fn: () => void, expectedSubstring: string, message: string): void {
  try {
    fn();
    failed++;
    console.error(`FAIL: ${message} (expected to throw, but did not)`);
  } catch (err) {
    const text = err instanceof Error ? err.message : String(err);
    assert(
      text.includes(expectedSubstring),
      `${message} (expected error to include "${expectedSubstring}", got "${text}")`,
    );
  }
}

function baseEnv(overrides: Partial<NodeJS.ProcessEnv> = {}): NodeJS.ProcessEnv {
  return {
    NODE_ID: "node-a",
    PORT: "4001",
    CLIENT_PORT: "5001",
    PEERS: "node-b=4002:5002,node-c=4003:5003",
    ...overrides,
  } as NodeJS.ProcessEnv;
}

// ---- Valid parse: "id=port:clientPort" form (host defaults to localhost) ----
{
  const config = parseClusterConfig(baseEnv());
  assertEqual(config.nodeId, "node-a", "parses NODE_ID");
  assertEqual(config.port, 4001, "parses PORT as a number");
  assertEqual(config.clientPort, 5001, "parses CLIENT_PORT as a number");
  assertEqual(
    config.peers,
    [
      { id: "node-b", host: "localhost", port: 4002, clientPort: 5002 },
      { id: "node-c", host: "localhost", port: 4003, clientPort: 5003 },
    ],
    "parses id=port:clientPort entries with host defaulted to localhost",
  );
}

// ---- Valid parse: "id=host:port:clientPort" form ----
{
  const config = parseClusterConfig(
    baseEnv({ PEERS: "node-b=192.168.1.5:4002:5002,node-c=other-host:4003:5003" }),
  );
  assertEqual(
    config.peers,
    [
      { id: "node-b", host: "192.168.1.5", port: 4002, clientPort: 5002 },
      { id: "node-c", host: "other-host", port: 4003, clientPort: 5003 },
    ],
    "parses id=host:port:clientPort entries with explicit host",
  );
}

// ---- Valid parse: mixed forms in one PEERS string ----
{
  const config = parseClusterConfig(
    baseEnv({ PEERS: "node-b=4002:5002,node-c=other-host:4003:5003" }),
  );
  assertEqual(
    config.peers,
    [
      { id: "node-b", host: "localhost", port: 4002, clientPort: 5002 },
      { id: "node-c", host: "other-host", port: 4003, clientPort: 5003 },
    ],
    "parses a mix of id=port:clientPort and id=host:port:clientPort entries in the same PEERS string",
  );
}

// ---- Whitespace tolerance ----
{
  const config = parseClusterConfig(
    baseEnv({ PEERS: " node-b = 4002 : 5002 , node-c = other-host : 4003 : 5003 " }),
  );
  assertEqual(
    config.peers,
    [
      { id: "node-b", host: "localhost", port: 4002, clientPort: 5002 },
      { id: "node-c", host: "other-host", port: 4003, clientPort: 5003 },
    ],
    "tolerates surrounding whitespace around ids, hosts, and both ports",
  );
}

// ---- Missing required env vars ----
{
  const env = baseEnv({ NODE_ID: undefined });
  assertThrows(
    () => parseClusterConfig(env),
    "Missing required environment variable: NODE_ID",
    "throws a specific error when NODE_ID is missing",
  );
}

{
  const env = baseEnv({ PORT: undefined });
  assertThrows(
    () => parseClusterConfig(env),
    "Missing required environment variable: PORT",
    "throws a specific error when PORT is missing",
  );
}

{
  const env = baseEnv({ CLIENT_PORT: undefined });
  assertThrows(
    () => parseClusterConfig(env),
    "Missing required environment variable: CLIENT_PORT",
    "throws a specific error when CLIENT_PORT is missing",
  );
}

{
  const env = baseEnv({ PEERS: undefined });
  assertThrows(
    () => parseClusterConfig(env),
    "Missing required environment variable: PEERS",
    "throws a specific error when PEERS is missing",
  );
}

{
  const env = baseEnv({ NODE_ID: "   " });
  assertThrows(
    () => parseClusterConfig(env),
    "Missing required environment variable: NODE_ID",
    "treats a whitespace-only NODE_ID as missing",
  );
}

// ---- Invalid PORT values ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PORT: "not-a-number" })),
    'Invalid PORT: "not-a-number"',
    "rejects a non-numeric PORT",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PORT: "0" })),
    "Invalid PORT",
    "rejects PORT of 0",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PORT: "70000" })),
    "Invalid PORT",
    "rejects PORT above 65535",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PORT: "4001.5" })),
    "Invalid PORT",
    "rejects a non-integer PORT",
  );
}

// ---- Invalid CLIENT_PORT values (mirrors PORT's own validation) ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ CLIENT_PORT: "not-a-number" })),
    'Invalid CLIENT_PORT: "not-a-number"',
    "rejects a non-numeric CLIENT_PORT",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ CLIENT_PORT: "0" })),
    "Invalid CLIENT_PORT",
    "rejects CLIENT_PORT of 0",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ CLIENT_PORT: "70000" })),
    "Invalid CLIENT_PORT",
    "rejects CLIENT_PORT above 65535",
  );
}

// ---- Malformed PEERS entries: general shape ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b-no-equals-sign" })),
    'Invalid PEERS entry "node-b-no-equals-sign"',
    "rejects a peer entry missing the = separator",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "=4002:5002" })),
    "peer id is empty",
    "rejects a peer entry with an empty id",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=" })),
    "address is empty",
    "rejects a peer entry with an empty address",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=:4002:5002" })),
    "host is empty",
    "rejects a peer entry with an empty host before the ports",
  );
}

// ---- Malformed PEERS entries: dual-port requirement (Milestone 5, Step 6.1) ----
{
  // The old Milestone 4 single-port shorthand is no longer valid — this
  // is the crux of the breaking change, and the most important new
  // coverage in this file: a peer entry with only ONE port must be
  // rejected outright, not silently guessed/defaulted.
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=4002,node-c=4003:5003" })),
    "expected exactly two ports",
    "rejects a peer entry with only one port (old single-port shorthand is no longer valid)",
  );
}

{
  // "other-host:4002" is 2 segments, so it's parsed as the "port:clientPort"
  // form (not "host:port" — that shorthand no longer exists), meaning
  // "other-host" lands in the Raft-RPC port slot and fails there. Still a
  // hard, actionable rejection (no silent guess) — just via port-number
  // validation rather than the segment-count check, since 2 segments is
  // inherently ambiguous between "host:onePort" (invalid, old shorthand)
  // and "raftPort:clientPort" (valid) without a host present.
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=other-host:4002,node-c=4003:5003" })),
    "is not a valid port number",
    "rejects a 2-segment entry with a non-numeric first segment (ambiguous old host:port shorthand, now invalid)",
  );
}

{
  // Too many segments (4+) is equally a hard error, not "use the first two".
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=host:4002:5002:extra,node-c=4003:5003" })),
    "expected exactly two ports",
    "rejects a peer entry with more than three colon-separated segments",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=host:port:clientPort:host2:5002,node-c=4003:5003" })),
    "expected exactly two ports",
    "rejects a peer entry with many extra colon-separated segments",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=4002:not-a-port,node-c=4003:5003" })),
    "Invalid PEERS entry",
    "rejects a peer entry with a non-numeric client port",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=not-a-port:5002,node-c=4003:5003" })),
    "Invalid PEERS entry",
    "rejects a peer entry with a non-numeric Raft-RPC port",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=host:4002:not-a-port,node-c=4003:5003" })),
    "Invalid PEERS entry",
    "rejects a peer entry (explicit host form) with a non-numeric client port",
  );
}

// ---- Duplicate peer ids ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-b=4002:5002,node-b=4003:5003" })),
    'duplicate peer id "node-b"',
    "rejects duplicate peer ids in PEERS",
  );
}

// ---- NODE_ID appearing in PEERS ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "node-a=4002:5002,node-c=4003:5003" })),
    'NODE_ID "node-a" also appears in PEERS',
    "rejects a node listing itself as a peer",
  );
}

// ---- Empty PEERS is rejected (ADR-2: fixed 3-node cluster, no zero-peer state) ----
{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: "" })),
    "Missing required environment variable: PEERS",
    "empty string PEERS is treated as missing",
  );
}

{
  assertThrows(
    () => parseClusterConfig(baseEnv({ PEERS: " , , " })),
    "must list at least one peer",
    "PEERS containing only separators/whitespace parses to zero peers and is rejected",
  );
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}