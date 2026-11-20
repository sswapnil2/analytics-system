import { test } from "node:test";
import assert from "node:assert/strict";
import { cohortDefinition, condsFromDefinition, type Cond } from "./cohort.ts";

const did: Cond = { type: "did", event: "Signed_Up", minCount: 2, days: 30, prop: "", value: "" };
const prop: Cond = { type: "prop", event: "", minCount: 1, days: 0, prop: "plan", value: "pro" };

test("builder conditions survive a save and reload", () => {
  for (const [conds, match] of [[[did], "and"], [[did, prop], "or"]] as const) {
    const back = condsFromDefinition(cohortDefinition([...conds], match));
    assert.deepEqual(back, { conds, match: conds.length > 1 ? match : "and" });
  }
});

test("definitions the builder can't show are not editable", () => {
  assert.equal(condsFromDefinition({ kind: "users", canonical_uids: [1, 2] }), null);
  assert.equal(condsFromDefinition({ kind: "and", clauses: [{ kind: "cohort_ref", cohort_id: "c1" }] }), null);
  assert.equal(condsFromDefinition({ kind: "performed_event", event_name: "x", min_count: 1, where_clauses: [{}] }), null);
});
