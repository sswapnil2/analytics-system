import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildCsv, formatCsvCell } from "./csv.ts";

describe("CSV serialization & formula injection protection", () => {
  it("neutralizes spreadsheet formula prefixes (=, +, -, @, TAB, CR, LF, and leading spaces)", () => {
    assert.equal(formatCsvCell("=SUM(A1:A10)"), "\"'=SUM(A1:A10)\"");
    assert.equal(formatCsvCell("+cmd|' /C calc'!A0"), "\"'+cmd|' /C calc'!A0\"");
    assert.equal(formatCsvCell("-2+3"), "\"'-2+3\"");
    assert.equal(formatCsvCell("@SUM(1+1)"), "\"'@SUM(1+1)\"");
    assert.equal(formatCsvCell("\t=1+1"), "\"'\t=1+1\"");
    assert.equal(formatCsvCell("\r=1+1"), "\"'\r=1+1\"");
    assert.equal(formatCsvCell("\n=1+1"), "\"'\n=1+1\"");
    assert.equal(formatCsvCell("  =cmd|' /C calc'!A0"), "\"'  =cmd|' /C calc'!A0\"");
  });

  it("leaves safe strings and numbers unprefixed while quoting per RFC 4180", () => {
    assert.equal(formatCsvCell("pro"), '"pro"');
    assert.equal(formatCsvCell(42), '"42"');
    assert.equal(formatCsvCell("a, b"), '"a, b"');
    assert.equal(formatCsvCell('say "hi"'), '"say ""hi"""');
    assert.equal(formatCsvCell(null), '""');
    assert.equal(formatCsvCell(undefined), '""');
  });

  it("builds multi-row CSV with escaped headers and cells", () => {
    const csv = buildCsv([
      ["distinct_id", "plan", "note"],
      ["user_1", "pro", "=HYPERLINK(\"http://evil.example\")"],
      ["user_2", "free", "normal, value"],
    ]);
    assert.equal(
      csv,
      [
        '"distinct_id","plan","note"',
        '"user_1","pro","\'=HYPERLINK(""http://evil.example"")"',
        '"user_2","free","normal, value"',
      ].join("\n"),
    );
  });
});
