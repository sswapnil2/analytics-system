import { test } from "node:test";
import assert from "node:assert/strict";
import { formatScore, formatUsd, orderSpanTree, toChatMessages } from "./agentTrace.ts";

const sp = (span_id: string, parent_span_id: string | null, start_ts_ms: number) => ({
  span_id,
  parent_span_id,
  start_ts_ms,
});

test("spans come out parent-first, children by start time, with depth", () => {
  const rows = orderSpanTree([sp("tool", "root", 30), sp("llm", "root", 10), sp("root", null, 0), sp("sub", "llm", 12)]);
  assert.deepEqual(
    rows.map((r) => `${r.span.span_id}@${r.depth}`),
    ["root@0", "llm@1", "sub@2", "tool@1"],
  );
});

test("orphans and parent cycles still show every span once", () => {
  const rows = orderSpanTree([sp("a", "b", 0), sp("b", "a", 1), sp("lost", "missing", 2), sp("self", "self", 3)]);
  assert.deepEqual(rows.map((r) => r.span.span_id).sort(), ["a", "b", "lost", "self"]);
  assert.equal(rows.length, 4);
});

test("money and scores format for small, zero and missing values", () => {
  assert.equal(formatUsd(0.00042), "$0.0004");
  assert.equal(formatUsd(12.3), "$12.30");
  assert.equal(formatUsd(0), "$0");
  assert.equal(formatUsd(null), "–");
  assert.equal(formatScore(0.866), "87%");
  assert.equal(formatScore(undefined), "–");
});

test("chat messages are read from the common LLM payload shapes", () => {
  assert.deepEqual(toChatMessages({ messages: [{ role: "system", content: "Be brief" }, { role: "user", content: "Hi" }] }), [
    { role: "system", text: "Be brief" },
    { role: "user", text: "Hi" },
  ]);
  assert.deepEqual(toChatMessages({ choices: [{ message: { role: "assistant", content: "Hello" } }] }), [
    { role: "assistant", text: "Hello" },
  ]);
  assert.deepEqual(
    toChatMessages({ role: "assistant", content: [{ type: "text", text: "Sure" }, { type: "tool_use", name: "x" }] }),
    [{ role: "assistant", text: "Sure\n[tool_use]" }],
  );
  assert.deepEqual(
    toChatMessages([{ role: "assistant", content: null, tool_calls: [{ function: { name: "lookup" } }] }]),
    [{ role: "assistant", text: "→ calls lookup" }],
  );
});

test("non-chat payloads stay raw", () => {
  assert.equal(toChatMessages({ question: "How?" }), null);
  assert.equal(toChatMessages("plain text"), null);
  assert.equal(toChatMessages([{ role: "user", content: "ok" }, { foo: 1 }]), null);
  assert.equal(toChatMessages([]), null);
});
