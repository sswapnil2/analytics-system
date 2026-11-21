import React from "react";
import { BanditResults as Results, MetricType } from "../api/client";
import { formatCount, formatDate, formatPercent, formatProbability } from "../lib/format";

function formatValue(type: MetricType | undefined, v: number): string {
  if (!type || type === "conversion") return formatPercent(v, 2);
  return v.toLocaleString(undefined, { maximumFractionDigits: Math.abs(v) < 10 ? 3 : 2 });
}

/** A loss is a difference: percentage points for conversion rates, metric units otherwise. */
function formatLoss(type: MetricType | undefined, v: number): string {
  if (!type || type === "conversion") return `${(v * 100).toFixed(2)} pts`;
  return formatValue(type, v);
}

const colorOf = (i: number) => `rgb(var(--chart-${(i % 8) + 1}))`;

/** A bandit's arms (chance to be best, expected loss, current traffic) and how traffic moved. */
export const BanditResultsView: React.FC<{
  results: Results;
  metricType: MetricType | undefined;
  confidence: number;
  burnInUsers: number;
}> = ({ results, metricType, confidence, burnInUsers }) => {
  const best = results.arms.find((a) => a.variant_key === results.best_variant);
  const keys = results.arms.map((a) => a.variant_key);
  return (
    <div className="space-y-4">
      <p className="rounded-lg border border-line bg-sunken px-3.5 py-3 text-sm text-ink-2">
        {!results.burn_in_complete
          ? `Traffic stays at the starting split until every variant has ${formatCount(burnInUsers)} users. After that it moves toward the leader once a day.`
          : results.ready_to_ship && best
            ? `${best.variant_key} has a ${formatProbability(best.prob_best)} chance to be best, and shipping it now is expected to cost only ${formatLoss(metricType, best.expected_loss)} against whichever variant really is. Safe to ship.`
            : best
              ? `${best.variant_key} is leading with a ${formatProbability(best.prob_best)} chance to be best. Keep it running until its expected loss is under 1% of its value.`
              : "No users yet."}
      </p>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[680px] text-sm">
          <thead>
            <tr className="border-b border-line text-xs text-muted">
              <th className="py-2 text-left font-medium">Variant</th>
              <th className="py-2 text-right font-medium">Users</th>
              <th className="py-2 text-right font-medium">{!metricType || metricType === "conversion" ? "Conversion rate" : "Per user"}</th>
              <th className="py-2 text-right font-medium">{Math.round(confidence * 100)}% range</th>
              <th className="py-2 pl-4 text-left font-medium">Chance to be best</th>
              <th className="py-2 text-right font-medium">Expected loss</th>
              <th className="py-2 text-right font-medium">Traffic now</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-line">
            {results.arms.map((a) => (
              <tr key={a.variant_key}>
                <td className="py-3 font-medium text-ink">
                  <span className="flex items-center gap-1.5">
                    {a.variant_key}
                    {a.variant_key === results.best_variant && (
                      <span className="rounded bg-ok/15 px-1.5 py-0.5 text-xs text-ok">Leading</span>
                    )}
                  </span>
                </td>
                <td className="tabular py-3 text-right text-ink-2">{formatCount(a.users)}</td>
                <td className="tabular py-3 text-right font-medium text-ink">{formatValue(metricType, a.mean_value)}</td>
                <td className="tabular py-3 text-right text-muted">
                  {formatValue(metricType, a.ci_low)} to {formatValue(metricType, a.ci_high)}
                </td>
                <td className="py-3 pl-4">
                  <div className="flex items-center gap-2">
                    <div className="h-2 w-28 overflow-hidden rounded-full bg-sunken">
                      <div className="h-full rounded-full bg-brand" style={{ width: `${Math.max(1, a.prob_best * 100)}%` }} />
                    </div>
                    <span className="tabular text-ink">{formatProbability(a.prob_best)}</span>
                  </div>
                </td>
                <td className="tabular py-3 text-right text-ink-2">{formatLoss(metricType, a.expected_loss)}</td>
                <td className="tabular py-3 text-right text-ink">{formatPercent(a.weight_bps / 10_000, 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {results.weight_history.length > 0 && (
        <figure className="space-y-2">
          <figcaption className="text-xs text-muted">Share of traffic, by the day it took effect</figcaption>
          <ul className="space-y-1.5">
            {results.weight_history.map((s) => (
              <li key={s.effective_from_ms} className="flex items-center gap-3 text-xs">
                <span className="tabular w-16 shrink-0 text-muted">
                  {formatDate(s.effective_from_ms, { month: "short", day: "numeric" })}
                </span>
                <div className="flex h-3 flex-1 overflow-hidden rounded-full bg-sunken" role="img" aria-label={keys.map((k) => `${k} ${formatPercent((s.weights[k] ?? 0) / 10_000, 0)}`).join(", ")}>
                  {keys.map((k, i) => (
                    <div key={k} style={{ width: `${(s.weights[k] ?? 0) / 100}%`, background: colorOf(i) }} />
                  ))}
                </div>
              </li>
            ))}
          </ul>
          <ul className="flex flex-wrap gap-3 text-xs text-ink-2">
            {keys.map((k, i) => (
              <li key={k} className="flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full" style={{ background: colorOf(i) }} />
                {k}
              </li>
            ))}
          </ul>
        </figure>
      )}
    </div>
  );
};
