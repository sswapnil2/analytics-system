import { test } from "node:test";
import assert from "node:assert/strict";
import { formatTime, propertyOptions, segmentName, setProjectTimeZone, withUnit } from "./format.ts";

test("times render on the project's clock", () => {
  setProjectTimeZone("Asia/Kolkata");
  assert.match(formatTime(0), /05:30/); // 00:00 UTC
  setProjectTimeZone("America/New_York");
  assert.match(formatTime(0), /07:00|19:00/); // 1969-12-31 19:00 EST
});

test("an unknown zone falls back to the browser's instead of throwing", () => {
  setProjectTimeZone("Mars/Olympus");
  assert.doesNotThrow(() => formatTime(0));
});

test("profile properties are listed after event properties, without the user. prefix", () => {
  const opts = propertyOptions(["user.plan", "country", "user.total_revenue", "page_url"]);
  assert.deepEqual(opts.map((o) => o.value), ["country", "page_url", "user.plan", "user.total_revenue"]);
  assert.deepEqual(opts.map((o) => o.label), ["country", "page url", "plan", "total revenue"]);
  assert.deepEqual(opts.map((o) => o.group), ["Event properties", "Event properties", "User profile", "User profile"]);
});

test("cohort split keys read as words", () => {
  assert.equal(segmentName("in_cohort"), "In cohort");
  assert.equal(segmentName("not_in_cohort"), "Not in cohort");
  assert.equal(segmentName("Mumbai"), "Mumbai");
});

test("units: symbols lead after the sign, words follow", () => {
  assert.equal(withUnit("1,200", "₹"), "₹1,200");
  assert.equal(withUnit("+12.5", "$"), "+$12.5");
  assert.equal(withUnit("−3", "€"), "−€3");
  assert.equal(withUnit("3.2", "items"), "3.2 items");
  assert.equal(withUnit("3.2", "  "), "3.2");
  assert.equal(withUnit("3.2", null), "3.2");
});
