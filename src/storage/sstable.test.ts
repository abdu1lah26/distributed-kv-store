/**
 * SSTable tests — run with: npx tsx src/storage/sstable.test.ts
 */
import { strict as assert } from "node:assert";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeSSTable, loadSSTable } from "./sstable.js";
import { TOMBSTONE, type Tombstone } from "./memtable.js";

function test(name: string, fn: () => Promise<void>): Promise<void> {
  return fn()
    .then(() => console.log(`  ✓ ${name}`))
    .catch((err) => {
      console.error(`  ✗ ${name}`);
      throw err;
    });
}

async function withTmpDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "sstable-test-"));
  try {
    return await fn(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  console.log("SSTable tests");

  await test("round-trip: write then get returns live values and tombstones correctly", async () => {
    await withTmpDir(async (dir) => {
      const entries: Array<[string, string | Tombstone]> = [
        ["apple", "red"],
        ["banana", TOMBSTONE],
        ["cherry", "dark red"],
      ];
      const filepath = await writeSSTable(dir, entries);
      const table = await loadSSTable(filepath);

      assert.deepEqual(await table.get("apple"), {
        found: true,
        tombstone: false,
        value: "red",
      });
      assert.deepEqual(await table.get("banana"), {
        found: true,
        tombstone: true,
      });
      assert.deepEqual(await table.get("cherry"), {
        found: true,
        tombstone: false,
        value: "dark red",
      });
    });
  });

  await test("get() on a key never written returns found:false", async () => {
    await withTmpDir(async (dir) => {
      const filepath = await writeSSTable(dir, [["apple", "red"]]);
      const table = await loadSSTable(filepath);
      assert.deepEqual(await table.get("missing"), { found: false });
    });
  });

  await test("filename follows sstable_<timestamp>.dat convention", async () => {
    await withTmpDir(async (dir) => {
      const filepath = await writeSSTable(dir, [["a", "1"]]);
      const filename = path.basename(filepath);
      assert.match(filename, /^sstable_\d+\.dat$/);
    });
  });

  await test("on-disk file is sorted JSON lines matching the input order", async () => {
    await withTmpDir(async (dir) => {
      const entries: Array<[string, string | Tombstone]> = [
        ["apple", "red"],
        ["banana", TOMBSTONE],
        ["cherry", "dark red"],
      ];
      const filepath = await writeSSTable(dir, entries);
      const raw = await fs.readFile(filepath, "utf8");
      const lines = raw
        .trim()
        .split("\n")
        .map((l) => JSON.parse(l));

      assert.deepEqual(lines, [
        { key: "apple", value: "red" },
        { key: "banana", tombstone: true },
        { key: "cherry", value: "dark red" },
      ]);
    });
  });

  await test("write-then-rename leaves no .tmp file behind", async () => {
    await withTmpDir(async (dir) => {
      await writeSSTable(dir, [["a", "1"]]);
      const filesInDir = await fs.readdir(dir);
      const tmpFiles = filesInDir.filter((f) => f.endsWith(".tmp"));
      assert.deepEqual(tmpFiles, []);
    });
  });

  await test("index+seek path holds up over many entries (exercises binary search, not just linear scan)", async () => {
    await withTmpDir(async (dir) => {
      const count = 500;
      const entries: Array<[string, string | Tombstone]> = [];
      for (let i = 0; i < count; i++) {
        // zero-padded so lexicographic sort matches numeric order
        const key = `key_${String(i).padStart(5, "0")}`;
        entries.push([key, `value_${i}`]);
      }
      const filepath = await writeSSTable(dir, entries);
      const table = await loadSSTable(filepath);

      // spot-check first, middle, last, and a handful of random-ish indices
      const checkIndices = [0, 1, 250, 251, 498, 499];
      for (const i of checkIndices) {
        const key = `key_${String(i).padStart(5, "0")}`;
        const result = await table.get(key);
        assert.deepEqual(result, {
          found: true,
          tombstone: false,
          value: `value_${i}`,
        });
      }

      assert.deepEqual(await table.get("key_99999"), { found: false });
    });
  });

  await test("an empty entries array produces a valid, loadable, empty SSTable", async () => {
    await withTmpDir(async (dir) => {
      const filepath = await writeSSTable(dir, []);
      const table = await loadSSTable(filepath);
      assert.deepEqual(await table.get("anything"), { found: false });
    });
  });

  await test("loadSSTable throws on a corrupted/truncated file rather than silently misreading", async () => {
    await withTmpDir(async (dir) => {
      const filepath = path.join(dir, "sstable_corrupt.dat");
      // Deliberately malformed: valid line followed by a torn JSON fragment,
      // simulating a file that was somehow damaged after being written
      // (writeSSTable()'s temp+rename is what prevents this in practice —
      // this test targets loadSSTable()'s own defensiveness directly).
      await fs.writeFile(
        filepath,
        '{"key":"a","value":"1"}\n{"key":"b","valu\n',
      );

      await assert.rejects(() => loadSSTable(filepath), /failed to parse line/);
    });
  });

  console.log("\nAll SSTable tests passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
