// Pure chart helpers shared by the trend chart and its tests.

export type ChartKind = "line" | "area" | "stacked-area" | "column" | "stacked-column" | "pie" | "number";

export const CHART_KINDS: { kind: ChartKind; label: string; group: "time" | "share" | "total" }[] = [
  { kind: "line", label: "Line", group: "time" },
  { kind: "area", label: "Area", group: "time" },
  { kind: "stacked-area", label: "Stacked area", group: "time" },
  { kind: "column", label: "Column", group: "time" },
  { kind: "stacked-column", label: "Stacked column", group: "time" },
  { kind: "pie", label: "Pie", group: "share" },
  { kind: "number", label: "Number", group: "total" },
];

/**
 * Why a chart kind can't show this result, or null if it can.
 * Stacking and pie add series together, so they need two or more series whose values
 * are parts of one whole: counts split by a breakdown, or event/session totals.
 * Unique users of different events overlap, and averages, percentiles, rolling
 * actives and formulas don't add up, so those stay line, area, column or number.
 */
export function chartKindBlocker(
  kind: ChartKind,
  ctx: { seriesCount: number; additive: boolean },
): string | null {
  if (kind !== "stacked-area" && kind !== "stacked-column" && kind !== "pie") return null;
  if (ctx.seriesCount < 2) return "Needs a breakdown or a second metric";
  if (!ctx.additive) return "These values don't add up to a whole";
  return null;
}

/** Metrics whose segment values partition a total. */
const SUMMABLE = new Set(["totals", "sessions", "sum"]);

/** Whether a trend's series can be stacked or shown as shares of a whole. */
export function isAdditive(metricKinds: string[], hasBreakdown: boolean, hasFormula: boolean): boolean {
  if (hasFormula || metricKinds.length === 0) return false;
  if (metricKinds.every((k) => SUMMABLE.has(k))) return true;
  // Unique users split by one property: each segment is a slice of the same event's users.
  return metricKinds.length === 1 && metricKinds[0] === "uniques" && hasBreakdown;
}

/** Round axis ticks from 0 up to at least `max`: steps of 1, 2, 2.5 or 5 × 10^n. */
export function niceTicks(max: number, target = 4): number[] {
  if (!Number.isFinite(max) || max <= 0) return [0, 1];
  const raw = max / target;
  const p = 10 ** Math.floor(Math.log10(raw));
  const m = raw / p;
  const step = (m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10) * p;
  const n = Math.ceil(max / step - 1e-9);
  return Array.from({ length: n + 1 }, (_, i) => +(i * step).toPrecision(12));
}

/** Change from a to b as a percentage; null when it can't be computed. */
export function pctChange(prev: number | null | undefined, cur: number | null | undefined): number | null {
  if (prev === null || prev === undefined || cur === null || cur === undefined || prev === 0) return null;
  const v = ((cur - prev) / Math.abs(prev)) * 100;
  return Number.isFinite(v) ? v : null;
}

/** Running totals per bucket for stacking: base[s][i] is where series s starts at bucket i. */
export function stackBases(values: (number | null)[][]): number[][] {
  const n = Math.max(0, ...values.map((v) => v.length));
  const acc = new Array<number>(n).fill(0);
  return values.map((v) => {
    const base = acc.slice();
    for (let i = 0; i < n; i++) acc[i] += Math.max(0, v[i] ?? 0);
    return base;
  });
}

/** SVG arc path for a donut slice; angles in radians, 0 at 12 o'clock, clockwise. */
export function donutSlice(cx: number, cy: number, r: number, inner: number, a0: number, a1: number): string {
  // A full circle can't be drawn as one arc; stop just short of it.
  const end = a1 - a0 >= Math.PI * 2 ? a0 + Math.PI * 2 - 1e-4 : a1;
  const pt = (rad: number, a: number) => `${(cx + rad * Math.sin(a)).toFixed(2)},${(cy - rad * Math.cos(a)).toFixed(2)}`;
  const large = end - a0 > Math.PI ? 1 : 0;
  return `M${pt(r, a0)} A${r},${r} 0 ${large} 1 ${pt(r, end)} L${pt(inner, end)} A${inner},${inner} 0 ${large} 0 ${pt(inner, a0)} Z`;
}

/**
 * Interval for a trend on a dashboard card: the report's own when it has one, stepped down only
 * where a small card would get too many points (hourly past 7 days, daily past 90).
 */
export function cardInterval(saved: unknown, rangeDays: number): "hour" | "day" | "week" | "month" {
  if (saved === "hour") return rangeDays > 7 ? "day" : "hour";
  if (saved === "day") return rangeDays > 90 ? "week" : "day";
  if (saved === "week" || saved === "month") return saved;
  return rangeDays > 30 ? "week" : "day";
}
