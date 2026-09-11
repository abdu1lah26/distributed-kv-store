# One image, reused for all 3 nodes in docker-compose.yml — each
# service just sets different NODE_ID/PORT/CLIENT_PORT/PEERS/DATA_DIR
# env vars at `docker compose up` time (see main.ts's parseClusterConfig).
#
# Full Debian-based node:22, not -alpine: safer default for native
# deps (e.g. anything requiring glibc), matching your stated preference.
FROM node:22

WORKDIR /app

# Copy manifest first so `npm install` is cached across rebuilds unless
# dependencies actually change.
COPY package.json package-lock.json* ./

# Real (not --omit=dev) install: this project runs directly via `tsx`
# with no build/compile step (per the Tooling Lock, §9.1), and `tsx`
# and `typescript` both live in devDependencies — omitting dev deps
# would leave the container unable to start at all.
RUN npm install

# Now copy the rest of the source.
COPY . .

# Both ports are set via env at compose time; EXPOSE here is
# documentation only (Docker doesn't enforce it), matching the actual
# values used in docker-compose.yml.
EXPOSE 4001 5001

# Entry point: the combined Raft node process (Milestone 5, Step 6.6).
# Runs via tsx directly — no ts-node, per the Tooling Lock.
CMD ["npx", "tsx", "src/raft/main.ts"]