import { test } from "node:test";
import assert from "node:assert/strict";
import { normalQuantile, sequentialRho, usersPerVariant, usersPerVariantForMean } from "./power.ts";

test("normal quantile matches tables", () => {
  assert.ok(Math.abs(normalQuantile(0.975) - 1.959964) < 1e-6);
  assert.ok(Math.abs(normalQuantile(0.8) - 0.841621) < 1e-6);
  assert.ok(Math.abs(normalQuantile(0.001) + 3.090232) < 1e-6);
});

test("conversion sample size matches the textbook formula", () => {
  // 10% base, 10% relative lift, alpha 0.05, 80% power: 2·(1.96+0.8416)²·0.09 / 0.01² ≈ 14,128.
  const n = usersPerVariant(0.1, 0.1);
  assert.ok(Math.abs(n - 14128) <= 2, String(n));
  assert.equal(usersPerVariant(0, 0.1), Infinity);
  assert.equal(usersPerVariant(0.1, 0), Infinity);
});

test("more variants, more power and sequential all need more users", () => {
  const base = usersPerVariant(0.1, 0.1);
  assert.ok(usersPerVariant(0.1, 0.1, { variants: 3 }) > base);
  assert.ok(usersPerVariant(0.1, 0.1, { power: 0.9 }) > base);
  const seq = usersPerVariant(0.1, 0.1, { sequential: true });
  assert.ok(seq > base && seq < base * 3, `${seq} vs ${base}`);
});

test("mean metrics use the standard deviation", () => {
  // mean 50, sd 20, 5% lift: delta 2.5, 2·400·7.849 / 6.25 ≈ 1,005.
  const n = usersPerVariantForMean(50, 20, 0.05);
  assert.ok(Math.abs(n - 1005) <= 2, String(n));
});

test("sequential rho matches the server formula", () => {
  // l = -2 ln 0.05 ≈ 5.9915; rho = sqrt((l + ln(l + 1)) / 8000) ≈ 0.031496
  assert.ok(Math.abs(sequentialRho(0.05, 8000) - 0.031496) < 1e-5, String(sequentialRho(0.05, 8000)));
});
