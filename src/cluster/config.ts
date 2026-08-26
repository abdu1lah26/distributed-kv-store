// Cluster / peer config parsing (Milestone 4, Step 2; updated Milestone 5,
// Step 6.1).
//
// Peer discovery is via environment variables (locked decision, see
// DESIGN.md §5 discussion for Milestone 4):
//   NODE_ID     — this node's own identifier (e.g. "node-a")
//   PORT        — this node's own Raft-RPC listen port
//   CLIENT_PORT — this node's own client-API listen port (Milestone 5,
//                 Step 6.1 — the two-port architecture: Raft RPCs and
//                 client HTTP are separate Express servers, separate
//                 ports, per DESIGN.md's architecture diagram)
//   PEERS       — comma-separated peer list, each entry either:
//                   "id=port:clientPort"          (host defaults to "localhost")
//                   "id=host:port:clientPort"
//                 e.g. "node-b=4002:5002,node-c=localhost:4003:5003"
//
// BREAKING CHANGE from Milestone 4's format: every peer entry now
// requires BOTH ports explicitly. The old single-port "id=port" /
// "id=host:port" shorthand is no longer valid — there is deliberately
// no default/guessed clientPort. A peer entry with only one port is a
// hard parse error, not a silent guess: two colon-separated ports living
// in one field is exactly the kind of thing that's easy to get out of
// sync, so a missing one must fail loudly at startup rather than produce
// a node that's silently unreachable on its client port.
//
// Pure function taking an env-like object, rather than reading
// process.env directly, so it's unit-testable without mutating global
// state — same instinct as createApp(store) in Milestone 3.

export interface PeerConfig {
  id: string;
  host: string;
  port: number;
  clientPort: number;
}

export interface ClusterConfig {
  nodeId: string;
  port: number;
  clientPort: number;
  peers: PeerConfig[];
}

const DEFAULT_HOST = "localhost";

/**
 * Parses NODE_ID / PORT / CLIENT_PORT / PEERS from an env-like object
 * into a ClusterConfig. Throws a specific, actionable Error on any
 * malformed or missing input — this runs at process startup, so a vague
 * parse crash is not acceptable here.
 */
export function parseClusterConfig(env: NodeJS.ProcessEnv): ClusterConfig {
  const nodeId = requireEnv(env, "NODE_ID");
  const port = parsePort(requireEnv(env, "PORT"), "PORT");
  const clientPort = parsePort(requireEnv(env, "CLIENT_PORT"), "CLIENT_PORT");
  const peersRaw = requireEnv(env, "PEERS");

  const peers = parsePeers(peersRaw);

  const peerIds = new Set<string>();
  for (const peer of peers) {
    if (peerIds.has(peer.id)) {
      throw new Error(
        `Invalid PEERS: duplicate peer id "${peer.id}" — each peer id must be unique.`,
      );
    }
    peerIds.add(peer.id);
  }

  if (peerIds.has(nodeId)) {
    throw new Error(
      `Invalid PEERS: NODE_ID "${nodeId}" also appears in PEERS — a node must not list itself as a peer.`,
    );
  }

  if (peers.length === 0) {
    throw new Error(
      "Invalid PEERS: must list at least one peer (got an empty list after parsing).",
    );
  }

  return { nodeId, port, clientPort, peers };
}

function requireEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (value === undefined || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value.trim();
}

function parsePort(raw: string, contextLabel: string): number {
  const port = Number(raw);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(
      `Invalid ${contextLabel}: "${raw}" is not a valid port number (expected an integer between 1 and 65535).`,
    );
  }
  return port;
}

function parsePeers(raw: string): PeerConfig[] {
  const entries = raw
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  return entries.map((entry) => parsePeerEntry(entry));
}

function parsePeerEntry(entry: string): PeerConfig {
  const eqIndex = entry.indexOf("=");
  if (eqIndex === -1) {
    throw new Error(
      `Invalid PEERS entry "${entry}": expected format "id=port:clientPort" or "id=host:port:clientPort".`,
    );
  }

  const id = entry.slice(0, eqIndex).trim();
  const addressPart = entry.slice(eqIndex + 1).trim();

  if (id === "") {
    throw new Error(
      `Invalid PEERS entry "${entry}": peer id is empty (expected format "id=port:clientPort" or "id=host:port:clientPort").`,
    );
  }
  if (addressPart === "") {
    throw new Error(
      `Invalid PEERS entry "${entry}": address is empty (expected format "id=port:clientPort" or "id=host:port:clientPort").`,
    );
  }

  // Milestone 5, Step 6.1: always exactly 2 or 3 colon-separated segments
  // now that both ports are required — no single-port shorthand anymore,
  // and no guessed/defaulted clientPort. Any other segment count is a
  // hard error rather than a best-effort guess at what was meant.
  const segments = addressPart.split(":").map((segment) => segment.trim());

  let host: string;
  let raftPortRaw: string;
  let clientPortRaw: string;

  if (segments.length === 2) {
    // "id=port:clientPort" — host defaults to localhost. Non-null
    // assertions below are safe: the length === 2 check above guarantees
    // indices 0 and 1 exist. (TypeScript can't narrow array destructuring
    // from a length check, so this is spelled out explicitly rather than
    // cast away — indexing directly, with the guarantee stated inline,
    // keeps the safety reasoning visible at the point of use.)
    host = DEFAULT_HOST;
    raftPortRaw = segments[0]!;
    clientPortRaw = segments[1]!;
  } else if (segments.length === 3) {
    // "id=host:port:clientPort" — same reasoning: length === 3 guarantees
    // indices 0, 1, and 2 all exist.
    host = segments[0]!;
    raftPortRaw = segments[1]!;
    clientPortRaw = segments[2]!;
    if (host === "") {
      throw new Error(
        `Invalid PEERS entry "${entry}": host is empty before the ports (expected "id=host:port:clientPort", or omit the host and its colon entirely for "id=port:clientPort").`,
      );
    }
  } else {
    throw new Error(
      `Invalid PEERS entry "${entry}": expected exactly two ports — "id=port:clientPort" or "id=host:port:clientPort" — got ${segments.length} colon-separated segment(s) after the peer id. Both the Raft-RPC port and the client port are required; there is no default for either.`,
    );
  }

  const port = parsePort(raftPortRaw, `PEERS entry "${entry}" (Raft-RPC port)`);
  const clientPort = parsePort(clientPortRaw, `PEERS entry "${entry}" (client port)`);

  return { id, host, port, clientPort };
}