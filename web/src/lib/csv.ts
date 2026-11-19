// Safe CSV serialization with OWASP CSV/Formula Injection neutralization.

const FORMULA_PREFIX = /^[\t\r\n ]*[=+\-@]|^[\t\r\n]/;

/**
 * Escapes a single CSV cell value per RFC 4180 and neutralizes spreadsheet
 * formula injection by prefixing values starting with =, +, -, @, TAB, or CR
 * with a single quote.
 */
export function formatCsvCell(value: unknown): string {
  const raw = value === null || value === undefined ? "" : String(value);
  const safe = FORMULA_PREFIX.test(raw) ? `'${raw}` : raw;
  return `"${safe.replace(/"/g, '""')}"`;
}

/** Builds an RFC 4180 CSV string with formula-injection protection. */
export function buildCsv(rows: ReadonlyArray<ReadonlyArray<unknown>>): string {
  return rows.map((row) => row.map(formatCsvCell).join(",")).join("\n");
}
