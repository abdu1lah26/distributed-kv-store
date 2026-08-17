// Cluster / peer config parsing (Milestone 4, Step 2).
//
// Peer discovery is via environment variables (locked decision, see
// DESIGN.md §5 discussion for Milestone 4):
//   NODE_ID — this node's own identifier (e.g. "node-a")
//   PORT    — this node's own listen port
//   PEERS   — comma-separated peer list, each entry either:
//               "id=port"          (host defaults to "localhost")
//               "id=host:port"
//             e.g. "node-b=4002,node-c=localhost:4003"
//
// Pure function taking an env-like object, rather than reading
// process.env directly, so it's unit-testable without mutating global
// state — same instinct as createApp(store) in Milestone 3.

export interface PeerConfig {
  id: string;
  host: string;
  port: number;
}

export interface ClusterConfig {
  nodeId: string;
  port: number;
  peers: PeerConfig[];
}

const DEFAULT_HOST = "localhost";

/**
 * Parses NODE_ID / PORT / PEERS from an env-like object into a
 * ClusterConfig. Throws a specific, actionable Error on any malformed
 * or missing input — this runs at process startup, so a vague parse
 * crash is not acceptable here.
 */
export function parseClusterConfig(env: NodeJS.ProcessEnv): ClusterConfig {
  const nodeId = requireEnv(env, "NODE_ID");
  const port = parsePort(requireEnv(env, "PORT"), "PORT");
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

  return { nodeId, port, peers };
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
      `Invalid PEERS entry "${entry}": expected format "id=port" or "id=host:port".`,
    );
  }

  const id = entry.slice(0, eqIndex).trim();
  const addressPart = entry.slice(eqIndex + 1).trim();

  if (id === "") {
    throw new Error(
      `Invalid PEERS entry "${entry}": peer id is empty (expected format "id=port" or "id=host:port").`,
    );
  }
  if (addressPart === "") {
    throw new Error(
      `Invalid PEERS entry "${entry}": address is empty (expected format "id=port" or "id=host:port").`,
    );
  }

  const colonIndex = addressPart.lastIndexOf(":");

  let host: string;
  let portRaw: string;

  if (colonIndex === -1) {
    // "id=port" form — host defaults to localhost.
    host = DEFAULT_HOST;
    portRaw = addressPart;
  } else {
    // "id=host:port" form.
    host = addressPart.slice(0, colonIndex).trim();
    portRaw = addressPart.slice(colonIndex + 1).trim();

    if (host === "") {
      throw new Error(
        `Invalid PEERS entry "${entry}": host is empty before the colon (expected "id=host:port", or omit the host and colon entirely for "id=port").`,
      );
    }
  }

  const port = parsePort(portRaw, `PEERS entry "${entry}"`);

  return { id, host, port };
}