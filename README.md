# distributed-kv-store

A distributed key-value store built from scratch: a custom LSM-tree
storage engine (WAL + MemTable + SSTable + Bloom filter) per node, with
a hand-rolled Raft consensus layer (leader election + log replication
+ commit index) on top. No Redis, no `raft` npm package, no ORM for
storage. See `DESIGN.md` for the full design doc, including the
Section 6 failure-model test results.

## Running the 3-node cluster

```bash
docker compose up --build
```

This starts 3 nodes (`node-a`, `node-b`, `node-c`), each running the
full stack — storage engine, Raft state machine, apply loop, and both
the Raft-RPC server and the client-facing REST API — wired together
per `src/raft/main.ts`.

Client APIs are published to the host:

| Node   | Client API (host) |
|--------|--------------------|
| node-a | http://localhost:5001 |
| node-b | http://localhost:5002 |
| node-c | http://localhost:5003 |

Inter-node Raft-RPC traffic (ports 4001-4003) stays inside the Docker
network — nothing outside the cluster needs it, matching how a real
client only ever talks to the client API.

Watch the logs to see leader election happen:

```bash
docker compose logs -f
```

Each node logs `[node-x] term=N state=follower|candidate|leader`
whenever its state changes.

## Talking to the cluster

Writes must go to the current leader — a follower responds `421
NOT_LEADER` with a redirect hint. Reads can go to any node (see
"Consistency model" below).

```bash
# Try a write against any node — if it's not the leader you'll get a
# 421 with a leaderHint pointing at the real leader's client port.
curl -X PUT http://localhost:5001/kv/foo -H "Content-Type: application/json" -d '{"value":"bar"}'

# Read from any node.
curl http://localhost:5002/kv/foo

# Delete.
curl -X DELETE http://localhost:5001/kv/foo
```

If you get a `421`, the response body includes which node is currently
believed to be leader — retry against that node's client port.

**Note on `leaderHint` from a host-machine `curl`:** the hint's URL
uses the Docker-internal service name (e.g. `http://node-b:5002`) —
correct for a client running *inside* the Docker network, but that
hostname isn't resolvable from your host machine. From your host,
translate the `leaderId` in the response to the matching `localhost`
port using the table above instead of curling the hint URL directly.
For example, if a `421` names `node-b` as leader:

```bash
# leaderHint says http://node-b:5002 — not resolvable from the host.
# Translate node-b -> localhost:5002 (see port table above) and retry:
curl -X PUT http://localhost:5002/kv/foo -H "Content-Type: application/json" -d '{"value":"bar"}'
```

## Killing a node (see it recover)

```bash
docker compose stop node-b
docker compose start node-b
```

If `node-b` was a follower, it rejoins and catches up via replication.
If it was the leader, the surviving two elect a new leader while it's
down (watch `docker compose logs -f` to see the term change).

## Consistency model — read this before assuming more than the system promises

- **Writes** are only accepted by the leader, and only return success
  after real majority commit (§5.4). Non-leader writes get `421`.
- **Reads** can go to any node, including followers, and are **not**
  guaranteed to reflect the latest committed write the instant it
  commits — a follower applies committed entries on a background
  interval (25ms, demo-tuned), not synchronously with the leader. This
  is a deliberate, documented tradeoff (§3), not a bug.

## A real, open gap — please read this before relying on this project for anything

The Raft log itself (`src/raft/log.ts`) is **in-memory only** — it is
**not** persisted to disk, unlike the KV store's own WAL. This means:

- **A node's WAL/SSTables (its actual key-value data on disk) genuinely
  survive a restart** — this is real durability, proven in
  `DESIGN.md` §6.2's failure-model test, and is why `docker-compose.yml`
  volume-mounts each node's `DATA_DIR`.
- **The Raft log itself does not survive a restart.** If all 3 nodes
  crash or are restarted at the same time, any entry that was
  committed-but-not-yet-applied, or appended-but-never-committed, at
  that moment is genuinely and provably lost — no node retains any
  record of it, at either the Raft layer or the storage layer. This was
  proven deterministically, not assumed — see `DESIGN.md` §6.2 for the
  full test and the honest account of a first attempt that gave a
  falsely optimistic result.

`docker-compose.yml` deliberately does **not** volume-mount anything
for the Raft log, so a container restart won't appear to "remember"
in-flight consensus state it never actually persisted. If you want to
resolve this gap, see `DESIGN.md` §5.3.2 for what's needed (WAL-backed
`RaftLog` persistence or equivalent) — it's tracked as open, not
silently worked around here.

## Running tests (outside Docker)

```bash
npm test
```

Runs the unit + integration suite (`tsc --noEmit` first, then storage
and API tests). The Raft failure-model integration tests
(`test/raft-*.test.ts`) spawn real local processes directly via `tsx`
and are run separately — see `DESIGN.md` §6 for how each one is
invoked and what it proves.

## Notes for Windows / Docker Desktop

Developed on Windows (Git Bash/MINGW64). A few things that matter if
you're running Docker Desktop on Windows specifically:

- Run `docker compose` commands from PowerShell, cmd, or WSL2 — Git
  Bash's path-mangling (auto-converting `/app/data/node-a`-style paths)
  can corrupt Docker CLI arguments. This repo avoids host bind-mounts
  entirely (named volumes only), which sidesteps most of the classic
  Windows volume-path quirks, but the CLI invocation itself can still
  be affected.
- If you edit any shell scripts added later in this repo, keep them
  LF-terminated — this repo's existing files are CRLF (Windows-native),
  but anything that runs *inside* the Linux container needs LF or it
  can fail with cryptic `\r`-related errors.
- Make sure Docker Desktop is set to use the WSL2 backend (default on
  modern installs) for best volume/networking performance.

## Project structure

See `DESIGN.md` §9 for the full repo layout and design rationale.