// Display formatting shared by every workspace.

/**
 * Masks user identifiers that look like personal data (phone numbers, emails)
 * before they are shown on screen. Opaque ids (anon_…, uuids) pass through.
 * The raw id still travels to the API; this only governs what is rendered.
 */
export function displayUserId(id: string): string {
  const raw = id.trim();
  const email = raw.match(/^([^@\s]{1,64})@([^@\s]+)$/);
  if (email) {
    const [, local, domain] = email;
    return `${local.slice(0, 1)}•••@${domain}`;
  }
  const digits = raw.replace(/[^\d]/g, "");
  if (digits.length >= 8 && /^[+\d][\d\s()-]+$/.test(raw)) {
    const prefix = raw.startsWith("+") ? `+${digits.slice(0, 2)} ` : "";
    return `${prefix}•••• ${digits.slice(-3)}`;
  }
  return raw;
}

export function formatCount(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "–";
  return Math.round(n).toLocaleString();
}

/** 0.2814 -> "28.1%"; accepts fractions (0..1). */
export function formatPercent(fraction: number | null | undefined, digits = 1): string {
  if (fraction === null || fraction === undefined || !Number.isFinite(fraction)) return "–";
  return `${(fraction * 100).toFixed(digits)}%`;
}

/** Probabilities never render as a certain 0% or 100%. */
export function formatProbability(p: number | null | undefined): string {
  if (p === null || p === undefined || !Number.isFinite(p)) return "–";
  if (p >= 0.999) return ">99.9%";
  if (p <= 0.001) return "<0.1%";
  return `${(p * 100).toFixed(1)}%`;
}

export function formatPValue(p: number | null | undefined): string {
  if (p === null || p === undefined || !Number.isFinite(p)) return "–";
  if (p < 0.001) return "<0.001";
  return p.toFixed(3);
}

/** Milliseconds -> "240 ms", "4.2 s", "3m 12s", "2h 05m". */
export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) return "–";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(s < 10 ? 1 : 0)} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(Math.round(s % 60)).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  return `${Math.round(h / 24)}d`;
}

/** Engine timing reported in microseconds -> "Ran in 18 ms". */
export function formatRunTime(us: number | null | undefined): string {
  if (!us || us <= 0) return "";
  if (us < 1000) return "Ran in under 1 ms";
  if (us < 1_000_000) return `Ran in ${Math.round(us / 1000)} ms`;
  return `Ran in ${(us / 1_000_000).toFixed(1)} s`;
}

/** Event names arrive as Snake_Case identifiers; show them as words. */
export function humanizeEvent(name: string): string {
  if (name.startsWith("$")) return name;
  return name.replace(/_/g, " ");
}

export function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    try {
      const parsed = JSON.parse(err.message) as { message?: string };
      if (parsed.message) return parsed.message;
    } catch {
      // not JSON
    }
    return err.message;
  }
  return String(err);
}

/** True for a 404 thrown by the API client (`HTTP 404` or a not-found JSON body). */
export function isNotFound(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return /\b404\b|not found/i.test(err.message);
}

/** Trigger a browser download for a generated CSV, NDJSON, or JSON text payload. */
export function downloadTextFile(
  filename: string,
  content: string,
  mimeType = "text/plain;charset=utf-8",
): void {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

/** Format epoch milliseconds into a readable local date/time string. */
export function formatTimestamp(ms: number | null | undefined): string {
  if (!ms || ms <= 0) return "—";
  return new Date(ms).toLocaleString(undefined, { timeZone: projectTimeZone, timeZoneName: "short" });
}

// ponytail: module-level, set by App when the active project changes; pass it as a prop if two projects ever render at once.
let projectTimeZone: string | undefined;

/** Timestamps render on the project's clock (the one reports bucket days in), not the browser's. */
export function setProjectTimeZone(tz: string | undefined): void {
  try {
    projectTimeZone = tz ? new Intl.DateTimeFormat(undefined, { timeZone: tz }).resolvedOptions().timeZone : undefined;
  } catch {
    projectTimeZone = undefined;
  }
}

export function formatDateTime(ts: number, opts: Intl.DateTimeFormatOptions = {}): string {
  return new Date(ts).toLocaleString(undefined, { ...opts, timeZone: projectTimeZone });
}

export function formatDate(ts: number, opts: Intl.DateTimeFormatOptions = {}): string {
  return new Date(ts).toLocaleDateString(undefined, { ...opts, timeZone: projectTimeZone });
}

/** "14:05 IST": the zone is shown so the clock is never ambiguous. */
export function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: projectTimeZone,
    timeZoneName: "short",
  });
}

/** A cohort split comes back as in_cohort / not_in_cohort; say it in words. */
export const segmentName = (k: string): string =>
  k === "in_cohort" ? "In cohort" : k === "not_in_cohort" ? "Not in cohort" : k;
