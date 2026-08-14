/**
 * BloomFilter tests — run with: npx tsx src/storage/bloomfilter.test.ts
 */
import { strict as assert } from 'node:assert';
import { BloomFilter } from '../storage/bloomfilter.js';

function test(name: string, fn: () => void): void {
  try {
    fn();
    console.log(`  ✓ ${name}`);
  } catch (err) {
    console.error(`  ✗ ${name}`);
    throw err;
  }
}

console.log('BloomFilter tests');

test('no false negatives: every added key returns mightContain() true', () => {
  const filter = BloomFilter.create(1000);
  const keys = Array.from({ length: 1000 }, (_, i) => `key_${i}`);
  for (const key of keys) {
    filter.add(key);
  }
  for (const key of keys) {
    assert.equal(filter.mightContain(key), true, `expected mightContain('${key}') to be true`);
  }
});

test('a key never added to an otherwise-populated filter is usually absent', () => {
  const filter = BloomFilter.create(1000);
  for (let i = 0; i < 1000; i++) {
    filter.add(`key_${i}`);
  }
  const neverAdded = Array.from({ length: 1000 }, (_, i) => `absent_${i}`);
  const falsePositives = neverAdded.filter((k) => filter.mightContain(k));
  assert.ok(
    falsePositives.length < neverAdded.length,
    'filter should not report every unseen key as possibly present',
  );
});

test('empirical false-positive rate is in the right ballpark for the 1% target', () => {
  const n = 5000;
  const filter = BloomFilter.create(n, 0.01);
  for (let i = 0; i < n; i++) {
    filter.add(`present_${i}`);
  }

  const trials = 20000;
  let falsePositives = 0;
  for (let i = 0; i < trials; i++) {
    if (filter.mightContain(`absent_${i}`)) {
      falsePositives += 1;
    }
  }
  const observedRate = falsePositives / trials;

  assert.ok(
    observedRate < 0.03,
    `observed false-positive rate ${observedRate} too high for 1% target`,
  );
});

test('create() sizes a larger bit array for a larger expected key count', () => {
  const small = BloomFilter.create(10);
  const large = BloomFilter.create(10000);

  for (let i = 0; i < 5000; i++) {
    small.add(`k_${i}`);
  }
  for (let i = 0; i < 5000; i++) {
    large.add(`k_${i}`);
  }

  let smallFalsePositives = 0;
  let largeFalsePositives = 0;
  const trials = 2000;
  for (let i = 0; i < trials; i++) {
    if (small.mightContain(`absent_${i}`)) smallFalsePositives += 1;
    if (large.mightContain(`absent_${i}`)) largeFalsePositives += 1;
  }

  assert.ok(
    smallFalsePositives > largeFalsePositives,
    'an overloaded small filter should have a visibly worse false-positive rate than a correctly-sized large filter',
  );
});

test('create() handles an expected key count of 0 without throwing', () => {
  const filter = BloomFilter.create(0);
  assert.equal(filter.mightContain('anything'), false);
});

console.log('\nAll BloomFilter tests passed.');