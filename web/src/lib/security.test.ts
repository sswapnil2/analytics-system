import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { isAbortError, outranks, projectPath, roleAtLeast } from "../api/client.ts";
import { displayUserId, errorMessage, formatDuration, formatPercent, formatProbability, formatPValue, isNotFound } from "./format.ts";

describe("API URL encoding & RBAC helpers", () => {
  it("encodes projectId to prevent path traversal in API URLs", () => {
    assert.equal(projectPath("../audit-logs", "/users"), "/v1/projects/..%2Faudit-logs/users");
    assert.equal(projectPath("proj/1?admin=1#x", "/export"), "/v1/projects/proj%2F1%3Fadmin%3D1%23x/export");
    assert.equal(projectPath("default", "/deletions"), "/v1/projects/default/deletions");
  });

  it("enforces role hierarchy accurately", () => {
    assert.equal(roleAtLeast("viewer", "viewer"), true);
    assert.equal(roleAtLeast("viewer", "member"), false);
    assert.equal(roleAtLeast("member", "member"), true);
    assert.equal(roleAtLeast("member", "admin"), false);
    assert.equal(roleAtLeast("admin", "admin"), true);
    assert.equal(roleAtLeast("owner", "admin"), true);
    assert.equal(roleAtLeast(undefined, "member"), false);

    assert.equal(outranks("owner", "owner"), true);
    assert.equal(outranks("admin", "member"), true);
    assert.equal(outranks("admin", "admin"), false);
    assert.equal(outranks("admin", "owner"), false);
  });

  it("identifies AbortError instances cleanly", () => {
    const domErr = new DOMException("Aborted", "AbortError");
    assert.equal(isAbortError(domErr), true);
    const err = new Error("Aborted");
    err.name = "AbortError";
    assert.equal(isAbortError(err), true);
    assert.equal(isAbortError(new Error("Network error")), false);
  });
});

describe("PII masking & formatting helpers", () => {
  it("masks email addresses and phone numbers while preserving opaque IDs", () => {
    assert.equal(displayUserId("alice@company.com"), "a•••@company.com");
    assert.equal(displayUserId("+91 98765 43210"), "+91 •••• 210");
    assert.equal(displayUserId("anon_9f8e7d6c"), "anon_9f8e7d6c");
  });

  it("extracts readable JSON error messages and detects 404s", () => {
    assert.equal(errorMessage(new Error('{"message":"Project not found"}')), "Project not found");
    assert.equal(errorMessage(new Error("Plain error")), "Plain error");
    assert.equal(isNotFound(new Error("HTTP 404")), true);
    assert.equal(isNotFound(new Error("Not found")), true);
    assert.equal(isNotFound(new Error("HTTP 500")), false);
  });

  it("formats durations, percentages, and statistical bounds safely", () => {
    assert.equal(formatDuration(240), "240 ms");
    assert.equal(formatDuration(4200), "4.2 s");
    assert.equal(formatDuration(-5), "–");
    assert.equal(formatPercent(0.2814), "28.1%");
    assert.equal(formatPercent(Number.NaN), "–");
    assert.equal(formatProbability(0.9999), ">99.9%");
    assert.equal(formatProbability(0.0001), "<0.1%");
    assert.equal(formatPValue(0.0002), "<0.001");
  });
});
