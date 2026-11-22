import { test } from "node:test";
import assert from "node:assert/strict";
import { matchCommands } from "./palette.ts";

const cmds = [
  { id: "a", label: "Trends", group: "Go to", run() {} },
  { id: "b", label: "AI agents: Traces", group: "Go to", keywords: "runs spans", run() {} },
  { id: "c", label: "Switch to dark theme", group: "Actions", run() {} },
];

test("matchCommands: every word must match, across label, group and keywords", () => {
  assert.deepEqual(matchCommands(cmds, "").length, 3);
  assert.deepEqual(matchCommands(cmds, "runs").map((c) => c.id), ["b"]);
  assert.deepEqual(matchCommands(cmds, "go tr").map((c) => c.id), ["a", "b"]);
  assert.deepEqual(matchCommands(cmds, "DARK").map((c) => c.id), ["c"]);
  assert.deepEqual(matchCommands(cmds, "agents theme"), []);
});
