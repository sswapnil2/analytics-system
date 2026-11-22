import React from "react";
import { ExperimentResults } from "../api/client";
import { niceRange } from "../lib/chart";
import { formatDate } from "../lib/format";

const W = 640;
const H = 200;
const P = { left: 44, right: W - 12, top: 10, bottom: H - 24 };

/**
 * Cumulative relative lift per treatment by exposure day, with its interval as a band.
 * Separate from TrendChart because lift goes below zero and needs a zero line.
 */
// ponytail: fixed viewBox scaled to width, so labels shrink on phones; measure like TrendChart if that matters.
export const LiftChart: React.FC<{ series: ExperimentResults["timeseries"]; confidence: number }> = ({ series, confidence }) => {
  if (series.length < 2) return null;
  const keys = series[0].variants.filter((v) => !v.is_control).map((v) => v.variant_key);
  // The first days' intervals are huge and would flatten the rest, so scale to day 3 onward
  // (and clip the early band); a lift can't go below -100%.
  const all = series.slice(series.length > 3 ? 2 : 0).flatMap((p) => p.variants.filter((v) => !v.is_control));
  const ticks = niceRange(
    Math.min(0, ...all.map((v) => Math.max(-100, v.ci_low_pct))),
    Math.max(0, ...all.map((v) => v.ci_high_pct)),
  );
  const lo = ticks[0];
  const hi = ticks[ticks.length - 1];
  const xAt = (i: number) => P.left + (i / (series.length - 1)) * (P.right - P.left);
  const yAt = (v: number) => P.bottom - ((Math.min(hi, Math.max(lo, v)) - lo) / (hi - lo)) * (P.bottom - P.top);
  const labelEvery = Math.ceil(series.length / 6);

  return (
    <figure className="space-y-2">
      <figcaption className="text-xs text-muted">
        Lift so far, by day of first exposure · shaded: {Math.round(confidence * 100)}% range
      </figcaption>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label="Cumulative lift over time">
        {ticks.map((t) => (
          <g key={t}>
            <line
              x1={P.left}
              x2={P.right}
              y1={yAt(t)}
              y2={yAt(t)}
              stroke={t === 0 ? "rgb(var(--line-strong))" : "rgb(var(--line))"}
              strokeWidth={t === 0 ? 1.5 : 1}
            />
            <text x={P.left - 8} y={yAt(t) + 4} textAnchor="end" fill="rgb(var(--muted))" fontSize={11} className="tabular">
              {t > 0 ? "+" : ""}
              {t}%
            </text>
          </g>
        ))}
        {keys.map((k, ki) => {
          const color = `rgb(var(--chart-${(ki % 8) + 1}))`;
          const pts = series.map((p, i) => ({ i, v: p.variants.find((v) => v.variant_key === k) }));
          const band =
            pts.map(({ i, v }) => `${xAt(i)},${yAt(v?.ci_high_pct ?? 0)}`).join(" ") +
            " " +
            [...pts].reverse().map(({ i, v }) => `${xAt(i)},${yAt(v?.ci_low_pct ?? 0)}`).join(" ");
          return (
            <g key={k}>
              <polygon points={band} fill={color} opacity={0.12} />
              <polyline
                points={pts.map(({ i, v }) => `${xAt(i)},${yAt(v?.relative_lift_pct ?? 0)}`).join(" ")}
                fill="none"
                stroke={color}
                strokeWidth={2}
                strokeLinejoin="round"
              />
            </g>
          );
        })}
        {series.map((p, i) =>
          (series.length - 1 - i) % labelEvery === 0 ? (
            <text
              key={p.day_start_ms}
              x={xAt(i)}
              y={H - 6}
              textAnchor={i === 0 ? "start" : i === series.length - 1 ? "end" : "middle"}
              fill="rgb(var(--muted))"
              fontSize={11}
            >
              {formatDate(p.day_start_ms, { month: "short", day: "numeric" })}
            </text>
          ) : null,
        )}
      </svg>
      {keys.length > 1 && (
        <ul className="flex flex-wrap gap-3 text-xs text-ink-2">
          {keys.map((k, ki) => (
            <li key={k} className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full" style={{ background: `rgb(var(--chart-${(ki % 8) + 1}))` }} />
              {k}
            </li>
          ))}
        </ul>
      )}
    </figure>
  );
};
