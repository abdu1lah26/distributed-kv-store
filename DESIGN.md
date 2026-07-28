# Distributed KV Store — Design Doc

**Author:** Abdullah
**Status:** Locked (v1) — scope changes require updating this file first
**Stack:** TypeScript (ESM, `module: nodenext`), Node.js, Docker Compose
**Script runner: use `tsx`, not `ts-node`.** `ts-node` (including
  `--loader ts-node/esm`) is broken with this project's `nodenext` +
  `"type": "module"` config (`TypeError: Cannot read properties of
  undefined (reading 'fileExists')`). Always run scripts with
  `npx tsx <file>`.

---

## 1. Goal

Build a distributed key-value store from scratch to demonstrate understanding of:
- Durable single-node storage engines (LSM-tree family)
- Distributed consensus (Raft: leader election + log replication)
- Failure handling in a real running system (not just theory)

This is a learning/portfolio project, not a production system. Every layer is
built manually — no Redis, no `raft` npm packages, no ORMs for storage.

---

## 2. Non-Goals (explicitly out of scope for v1)

Write code freely for anything in scope; if you're about to build one of these,
stop and come back here first.

- ❌ ML-based cache eviction (no XGBoost/ONNX layer)
- ❌ Natural language / LLM query interface
- ❌ Multi-datacenter / cross-region replication
- ❌ Log compaction / snapshotting (log grows unbounded for v1 — fine for demo scale)
- ❌ Dynamic cluster membership changes (cluster is fixed at 3 nodes, configured at boot)
- ❌ gRPC (HTTP/JSON only — see ADR-1 below)
- ❌ Authentication / TLS (local demo only)

If a stretch goal is wanted later, add it as a new numbered milestone at the
bottom — don't silently expand scope mid-build.

---

## 3. Architecture Overview

```
        ┌────────────┐
        │   Client    │  (curl / simple CLI / Postman)
        └──────┬─────┘
               │ HTTP (GET/PUT/DELETE)
               ▼
     ┌───────────────────────────────┐
     │      3-node Raft Cluster       │
     │                                 │
     │   Node A ──┐                   │
     │   Node B ──┼── Raft RPCs (HTTP)│
     │   Node C ──┘                   │
     └───────────────────────────────┘
               │
        Each node owns its own
        local storage engine
        (WAL + MemTable + SSTables)
```

- Client writes MUST go to the current leader. If a follower receives a write,
  it responds `421 NOT_LEADER` with a redirect hint to the known leader.
- Client reads can go to any node for v1 (eventually consistent reads from
  followers — document this tradeoff explicitly in the README, don't hide it).
- Each node's storage engine is fully independent — replication happens at the
  **log entry** level (Raft replicates commands, not files).

---

## 4. Component 1: Storage Engine (per node, single-node scope)

### 4.1 Write path
1. Write arrives → append to **WAL** (Write-Ahead Log) on disk, fsync.
2. Only after WAL write succeeds → apply to in-memory **MemTable** (sorted map).
3. If MemTable exceeds a size threshold (e.g. 1000 keys or 1MB) → flush to a new
   immutable **SSTable** file on disk, clear MemTable.

### 4.2 Read path
1. Check MemTable first (most recent data).
2. Check **Bloom filter** for each SSTable, newest to oldest — skip files the
   filter says definitely don't contain the key.
3. For SSTables the filter didn't rule out, binary search the sorted file.

### 4.3 Crash recovery
On startup: replay the WAL to rebuild the MemTable to the state it was in
before the crash. This is the one behavior you MUST test explicitly
(kill -9 the process, restart, verify no acknowledged write is lost).

### 4.4 File formats (lock these before coding storage)
- **WAL entry:** `[4-byte length][JSON: {op, key, value, timestamp}]\n`
- **SSTable:** sorted JSON lines, one key-value pair per line, filename =
  `sstable_<timestamp>.dat`
- **Bloom filter:** built in-memory when an SSTable is loaded/created; not
  persisted separately in v1 (rebuilt on startup from the SSTable — simpler,
  acceptable tradeoff to document).

---

### 4.4.1 WAL Implementation Decisions (locked after Milestone 1 build)

- **No truncation/compaction in Milestone 1** — WAL is append-only and grows
  unbounded, consistent with Section 2 non-goals. Revisit only if a
  compaction milestone is added later.
- **fsync on every single append**, not batched — chosen for correctness-first
  over performance, appropriate for a learning project at this stage. Batching
  fsyncs is a valid future optimization and a good interview talking point,
  but is NOT implemented — don't assume it exists.
- **Torn writes on replay:** if the WAL file ends mid-record (partial length
  prefix or partial JSON payload — the crash-mid-write scenario from Section
  4.3), `replay()` stops cleanly at that point rather than throwing. This is
  the mechanism that satisfies the Section 4.3 crash-recovery requirement.

### 4.4.2 MemTable Implementation Decisions (locked after Milestone 1 build)

- MemTable is backed by a plain `Map<string, string>` for Milestone 1, not a
  sorted structure. "Sorted map" in Section 4.1 is about SSTable flush
  needing sorted output (Section 4.4) — since Milestone 1 has no flush,
  sortedness is deferred to whichever Milestone 2 step implements it.
- Deletes are plain removals from the Map in Milestone 1 — no tombstones,
  since there's no older SSTable layer for a delete to shadow yet. Tombstone
  semantics become necessary once SSTables exist (Milestone 2).

## 5. Component 2: Raft Consensus

### 5.1 Scope for v1
- Leader election (randomized timeouts, terms, votes) ✅
- Log replication (AppendEntries, commit index advancement) ✅
- Log compaction / snapshots ❌ (non-goal above)

### 5.2 Node states
`Follower → Candidate → Leader`, exactly per the Raft paper. Do not invent
shortcuts here — this is the part interviewers will actually probe.

### 5.3 RPCs (HTTP/JSON, per ADR-1)
- `POST /raft/request-vote` — `{term, candidateId, lastLogIndex, lastLogTerm}`
- `POST /raft/append-entries` — `{term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit}`
  (empty `entries[]` = heartbeat)

### 5.4 Client-facing RPC
- `PUT /kv/:key` `{value}` — only leader accepts; must be committed (majority
  ack) before returning 200 to client.
- `GET /kv/:key` — served locally, any node (documented eventual-consistency caveat).
- `DELETE /kv/:key` — same commit path as PUT.

### 5.5 Timeouts to lock now (avoid tuning-by-guessing mid-demo)
- Election timeout: randomized 150–300ms per node
- Heartbeat interval: 50ms

---

## 6. Failure Model — what you will explicitly test and demo

Write a test (script or manual) for each of these. This list IS the rubric for
"is this a real systems project or just a demo that ran once."

1. **Leader crash mid-write** → new leader elected, no acknowledged write lost.
2. **Follower crash + rejoin** → catches up via AppendEntries, log converges.
3. **Network partition (minority side)** → minority side cannot elect a leader
   or accept writes (no split-brain).
4. **Process crash + restart (WAL recovery)** → node rebuilds correct state
   from WAL alone.
5. **Concurrent writes to leader** → correctly ordered in the log, applied in
   order on all nodes.

---

## 7. Milestones (each = a runnable, demoable state — commit at each one)

1. 1. Single-node KV store: WAL + MemTable... 
   **[IN PROGRESS — WAL module (`src/storage/wal.ts`) implemented and tested. MemTable next.]**
2. Add SSTable flush + Bloom filter — durable across full process kill/restart.
3. Wrap with REST API (`GET/PUT/DELETE /kv/:key`) — single node, no Raft yet.
4. 3-node cluster, Raft leader election only (no log replication) — prove
   election + heartbeats work, kill leader, watch re-election.
5. Add log replication (AppendEntries, commit index) — this is the hardest
   milestone, budget the most time here.
6. Wire client writes through leader-only path with `421 NOT_LEADER` redirect.
7. Run failure-model tests from Section 6, document results (including any
   that fail initially — document the fix, like your cousin did with the
   benchmark post. That's the most convincing part of a writeup).
8. Docker Compose for the 3-node cluster, README with `docker compose up`.
9. (Stretch, optional) Simple dashboard or CLI showing cluster state.

---

## 8. Architecture Decision Records (ADRs)

**ADR-1: HTTP/JSON over gRPC for all RPCs**
Chosen for debuggability (curl-able, human-readable) while learning Raft for
the first time. The hard part of this project is consensus logic, not
transport. Revisit only as a stretch goal after Milestone 7 is solid.

**ADR-2: Fixed 3-node cluster, no dynamic membership**
Dynamic membership changes are a known-hard Raft extension (Raft paper
Section 6) and explicitly out of scope to keep the project finishable.

---

## 9. Repo structure (proposed)

```
/src
  /storage      → WAL, MemTable, SSTable, BloomFilter
  /raft         → node state machine, RPC handlers, election/heartbeat timers
  /api          → REST layer (client-facing + raft RPC routes)
  /cluster      → node config, peer discovery (static list for v1)
/test           → failure-model tests from Section 6
docker-compose.yml
README.md       → setup + demo instructions
DESIGN.md       → this file
```

---

## 9.1 Tooling Lock

- `package.json`: `"type": "module"` (ESM, not CommonJS)
- `tsconfig.json`: `--module nodenext --target es2020 --strict --esModuleInterop`
- Do not switch back to CommonJS or change these flags without updating this
  section first — ESM import syntax (`import`/`export`, `.js` extensions in
  relative imports even for `.ts` files) applies to all code from here on.

## 10. How this doc gets used

- Paste this whole file at the start of any new Claude conversation about this
  project, or keep it in the repo root for Claude Code to read directly.
- Update it BEFORE changing scope, not after — if you're tempted to add
  something not listed here, add it to Section 2/7 first, then build it.
