import { test } from "node:test";
import assert from "node:assert/strict";
import { formatTime, setProjectTimeZone } from "./format.ts";

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
