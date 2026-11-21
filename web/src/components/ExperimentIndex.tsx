import React, { useState } from "react";
import { Experiment } from "../api/client";
import { field } from "./ui";

const DAY_MS = 86_400_000;

const FILTERS = [
  { value: "live", label: "Running or paused", match: (s: string) => s === "running" || s === "paused" },
  { value: "draft", label: "Drafts", match: (s: string) => s === "draft" },
  { value: "concluded", label: "Stopped", match: (s: string) => s === "concluded" },
  { value: "all", label: "All", match: () => true },
] as const;

/** "Day 3 of 14": the same count everywhere (list, setup, summary). */
export function dayOfTotal(e: Experiment, now = Date.now()): { day: number; total: number } {
  const total = Math.max(1, Math.ceil((e.end_ts_ms - e.start_ts_ms) / DAY_MS));
  return { day: Math.min(Math.max(1, Math.ceil((now - e.start_ts_ms) / DAY_MS)), total), total };
}

function statusText(e: Experiment): string {
  if (e.status === "draft") return "Draft";
  if (e.status === "concluded") {
    const d = e.decision;
    if (!d) return "Stopped";
    return `Stopped · ${d.outcome === "ship" ? `shipped ${d.variant}` : d.outcome === "keep_control" ? "kept control" : "inconclusive"}`;
  }
  const { day, total } = dayOfTotal(e);
  return `${e.status === "paused" ? "Paused" : "Running"} · day ${day} of ${total}`;
}

/** Every experiment in the project, findable by name, flag or owner, with its status at a glance. */
export const ExperimentIndex: React.FC<{
  experiments: Experiment[];
  selectedId: string;
  onSelect: (id: string) => void;
}> = ({ experiments, selectedId, onSelect }) => {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]["value"]>("all");
  const [q, setQ] = useState("");
  const match = FILTERS.find((f) => f.value === filter)!.match;
  const needle = q.trim().toLowerCase();
  const rows = experiments.filter(
    (e) =>
      match(e.status) &&
      (!needle || [e.name, e.flag_key, e.owner || e.created_by || ""].some((t) => t.toLowerCase().includes(needle))),
  );
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2">
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search by name, flag or owner"
          aria-label="Search experiments"
          className={`${field} min-w-0 flex-1`}
        />
        <select aria-label="Show" value={filter} onChange={(e) => setFilter(e.target.value as typeof filter)} className={field}>
          {FILTERS.map((f) => (
            <option key={f.value} value={f.value}>
              {f.label} ({experiments.filter((e) => f.match(e.status)).length})
            </option>
          ))}
        </select>
      </div>
      <div className="max-h-72 overflow-auto">
        <table className="w-full min-w-[560px] text-sm">
          <thead className="sticky top-0 bg-panel">
            <tr className="border-b border-line text-xs text-muted">
              <th className="py-2 text-left font-medium">Experiment</th>
              <th className="py-2 text-left font-medium">Status</th>
              <th className="py-2 text-left font-medium">Flag</th>
              <th className="py-2 text-left font-medium">Owner</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-line">
            {rows.map((e) => (
              <tr key={e.id} className={e.id === selectedId ? "bg-hover" : undefined}>
                <td className="py-2">
                  <button
                    type="button"
                    onClick={() => onSelect(e.id)}
                    aria-current={e.id === selectedId ? "true" : undefined}
                    className="text-left font-medium text-ink hover:underline"
                  >
                    {e.name}
                  </button>
                </td>
                <td className="py-2 text-ink-2">{statusText(e)}</td>
                <td className="py-2 font-mono text-xs text-ink-2">{e.flag_key}</td>
                <td className="py-2 text-muted">{e.owner || e.created_by || "–"}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={4} className="py-3 text-center text-muted">
                  No experiments match.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
