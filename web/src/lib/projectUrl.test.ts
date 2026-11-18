import assert from "node:assert/strict";
import { test } from "node:test";
import { projectFromHash, withProject } from "./projectUrl.ts";

const ids = ["funnels", "dashboards"];

test("a report link keeps its query and gains the project", () => {
  const h = withProject("#/funnels?q=abc&range=7", "proj_acme", ids);
  assert.equal(h, "#/funnels?q=abc&range=7&p=proj_acme");
  assert.equal(projectFromHash(h!), "proj_acme");
});

test("dashboard links carry the project", () => {
  assert.equal(withProject("#/dashboards/dash_1", "proj_acme", ids), "#/dashboards/dash_1?p=proj_acme");
});

test("switching project replaces p; same project is a no-op", () => {
  assert.equal(withProject("#/funnels?p=a&q=x", "b", ids), "#/funnels?p=b&q=x");
  assert.equal(withProject("#/funnels?p=b", "b", ids), null);
});

test("auth screens are left alone", () => {
  assert.equal(withProject("#/signin", "b", ids), null);
  assert.equal(withProject("#/invite/abc", "b", ids), null);
  assert.equal(projectFromHash("#/funnels"), "");
});
