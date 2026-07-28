import { rm } from "node:fs/promises";
import { WriteAheadLog } from "../storage/wal.js";

const WAL_PATH = "./test.wal";

async function main() {
  // Start from a clean file.
  await rm(WAL_PATH, { force: true });

  const wal = new WriteAheadLog(WAL_PATH);

  await wal.open();

  await wal.append({
    op: "PUT",
    key: "name",
    value: "Alice",
  });

  await wal.append({
    op: "PUT",
    key: "city",
    value: "London",
  });

  await wal.append({
    op: "DELETE",
    key: "name",
  });

  await wal.close();

  console.log("Replaying WAL...");

  const replayWal = new WriteAheadLog(WAL_PATH);

  for await (const entry of replayWal.replay()) {
    console.log(entry);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});