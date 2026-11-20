import { test } from "node:test";
import assert from "node:assert/strict";
import { cardInterval, chartKindBlocker, isAdditive, niceTicks, pctChange, stackBases } from "./chart.ts";

test("ticks land on round numbers and cover the max", () => {
  assert.deepEqual(niceTicks(3851), [0, 1000, 2000, 3000, 4000]);
  assert.deepEqual(niceTicks(10), [0, 2.5, 5, 7.5, 10]);
  assert.deepEqual(niceTicks(0), [0, 1]);
  const t = niceTicks(0.37);
  assert.ok(t[t.length - 1] >= 0.37);
});

test("stacking and pie only when series add up to a whole", () => {
  assert.equal(isAdditive(["uniques"], true, false), true); // users split by city
  assert.equal(isAdditive(["uniques", "uniques"], false, false), false); // overlapping users
  assert.equal(isAdditive(["totals", "sum"], false, false), true);
  assert.equal(isAdditive(["avg"], true, false), false);
  assert.equal(isAdditive(["totals"], true, true), false); // formula
  assert.equal(chartKindBlocker("pie", { seriesCount: 1, additive: true }), "Needs a breakdown or a second metric");
  assert.ok(chartKindBlocker("stacked-area", { seriesCount: 3, additive: false }));
  assert.equal(chartKindBlocker("stacked-column", { seriesCount: 3, additive: true }), null);
  assert.equal(chartKindBlocker("line", { seriesCount: 1, additive: false }), null);
});

test("percent change skips zero and missing baselines", () => {
  assert.equal(pctChange(4, 5), 25);
  assert.equal(pctChange(0, 5), null);
  assert.equal(pctChange(null, 5), null);
});

test("stack bases accumulate earlier series and ignore gaps", () => {
  assert.deepEqual(stackBases([[1, 2], [3, null], [5, 6]]), [[0, 0], [1, 2], [4, 2]]);
});

test("dashboard cards keep the report's interval unless it gives too many points", () => {
  assert.equal(cardInterval("week", 7), "week");
  assert.equal(cardInterval("month", 365), "month");
  assert.equal(cardInterval("day", 30), "day");
  assert.equal(cardInterval("day", 180), "week");
  assert.equal(cardInterval("hour", 1), "hour");
  assert.equal(cardInterval("hour", 14), "day");
  assert.equal(cardInterval(undefined, 14), "day"); // older reports: from the range, as before
  assert.equal(cardInterval(undefined, 90), "week");
});
