import React, { useId, useState } from "react";
import { AlertTriangle, Loader2, Zap } from "lucide-react";
import type { Role, SamplingInfo, SamplingMode } from "../api/client";
import { formatRunTime } from "../lib/format";

/** Page title plus a one-line description of what the page answers. */
export const PageHeader: React.FC<{
  title: string;
  description: string;
  actions?: React.ReactNode;
}> = ({ title, description, actions }) => (
  <header className="flex flex-wrap items-end justify-between gap-3 pb-1">
    <div className="min-w-0 max-w-2xl">
      <h1 className="text-[1.5rem] font-semibold leading-tight tracking-[-0.02em] text-ink">{title}</h1>
      <p className="mt-1 text-sm text-muted">{description}</p>
    </div>
    {actions && <div className="flex min-w-0 max-w-full flex-wrap items-center gap-2">{actions}</div>}
  </header>
);

export const Panel: React.FC<{
  title?: string;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
  children: React.ReactNode;
}> = ({ title, meta, actions, className = "", children }) => (
  <section className={`rounded-xl border border-line bg-panel ${className}`}>
    {(title || actions || meta) && (
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line px-5 py-3.5">
        <div className="flex items-baseline gap-3 min-w-0">
          {title && <h2 className="text-md font-semibold text-ink">{title}</h2>}
          {meta && <span className="text-xs text-faint tabular">{meta}</span>}
        </div>
        {actions && <div className="flex min-w-0 max-w-full flex-wrap items-center gap-x-2 gap-y-1">{actions}</div>}
      </div>
    )}
    <div className="p-5">{children}</div>
  </section>
);

/** Quiet run-time readout; engine internals stay out of the main UI. */
export const RanIn: React.FC<{ us?: number | null }> = ({ us }) => {
  const text = formatRunTime(us);
  return text ? <span className="text-xs text-faint tabular">{text}</span> : null;
};

function samplingModeToKey(mode: SamplingMode): string {
  if (mode === "auto") return "auto";
  if (mode === "exact") return "exact";
  return String(mode.rate);
}

function keyToSamplingMode(key: string): SamplingMode {
  if (key === "auto") return "auto";
  if (key === "exact") return "exact";
  const rate = Number(key);
  return Number.isFinite(rate) && rate > 0 && rate < 1 ? { rate } : "auto";
}

/** Transparent sampling badge + mode selector + run-time readout for query panels. */
export const SamplingMeta: React.FC<{
  sampling?: SamplingInfo | null;
  mode: SamplingMode;
  onChangeMode: (mode: SamplingMode) => void;
  us?: number | null;
  loading?: boolean;
}> = ({ sampling, mode, onChangeMode, us, loading }) => {
  const isHll = sampling?.method === "hll_sketch";
  const pct = sampling ? +(sampling.sample_rate * 100).toFixed(1) : null;
  const errText =
    sampling?.relative_error_pct != null ? ` (±${sampling.relative_error_pct}%)` : "";

  return (
    <div className="flex flex-wrap items-center gap-2">
      {sampling?.is_sampled && (
        <span
          className="inline-flex items-center gap-1.5 rounded-full border border-warn/30 bg-warn-soft px-2.5 py-0.5 text-2xs font-medium text-warn tabular"
          title={
            isHll
              ? `Instant HyperLogLog sketch across 100% of segments (${sampling.total_buckets}/${sampling.total_buckets} buckets, zero segment I/O).`
              : `Entity-consistent deterministic sample of ${sampling.sampled_buckets}/${sampling.total_buckets} user buckets (${pct}% of users, ${sampling.scale_factor}x scale).`
          }
        >
          <Zap className="h-3 w-3 shrink-0" />
          <span>
            {isHll ? `~HLL sketch${errText}` : `~${pct}% sample${errText}`}
          </span>
          <button
            type="button"
            onClick={() => onChangeMode("exact")}
            className="ml-0.5 rounded px-1 text-2xs font-semibold text-ink underline decoration-warn/60 underline-offset-2 hover:decoration-warn"
          >
            Run exact
          </button>
        </span>
      )}
      <select
        aria-label="Query sampling mode"
        value={samplingModeToKey(mode)}
        onChange={(e) => onChangeMode(keyToSamplingMode(e.target.value))}
        className="h-6 rounded-md border border-line bg-field px-1.5 text-2xs font-medium text-muted hover:text-ink focus:border-brand focus:outline-none"
        title="Auto stays exact on fast queries and samples user buckets on very large ranges."
      >
        <option value="auto">Auto</option>
        <option value="exact">Exact (100%)</option>
        <option value="0.25">Fast (25%)</option>
        <option value="0.125">Faster (12.5%)</option>
        <option value="0.0625">Turbo (6.25%)</option>
      </select>
      {loading ? (
        <span className="text-xs text-faint">Updating…</span>
      ) : (
        <RanIn us={us} />
      )}
    </div>
  );
};

export const EmptyState: React.FC<{
  title: string;
  body?: React.ReactNode;
  action?: React.ReactNode;
  icon?: React.ComponentType<{ className?: string }>;
}> = ({ title, body, action, icon: Icon }) => (
  <div className="flex flex-col items-center justify-center gap-2 px-6 py-12 text-center">
    {Icon && <Icon className="mb-1 h-6 w-6 text-faint" />}
    <p className="text-md font-semibold text-ink">{title}</p>
    {body && <p className="max-w-sm text-sm text-muted">{body}</p>}
    {action && <div className="mt-2">{action}</div>}
  </div>
);

export const ErrorNotice: React.FC<{ message: string; onRetry?: () => void }> = ({
  message,
  onRetry,
}) => (
  <div
    role="alert"
    className="flex items-start gap-3 rounded-lg border border-danger/30 bg-danger-soft px-3.5 py-3 text-sm text-ink"
  >
    <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-danger" />
    <div className="min-w-0 flex-1 break-words">{message}</div>
    {onRetry && (
      <button type="button" onClick={onRetry} className="shrink-0 font-medium text-danger hover:underline">
        Try again
      </button>
    )}
  </div>
);

export const Spinner: React.FC<{ label?: string }> = ({ label = "Loading" }) => (
  <span className="inline-flex items-center gap-2 text-sm text-muted" role="status">
    <Loader2 className="h-4 w-4 animate-spin" />
    {label}
  </span>
);

/** Shared control styles so every workspace reads as one product. */
export const btn = {
  primary:
    "inline-flex whitespace-nowrap items-center justify-center gap-1.5 rounded-lg bg-brand px-3.5 py-2 text-sm font-semibold text-on-brand hover:bg-brand-hover disabled:opacity-50 disabled:cursor-not-allowed",
  secondary:
    "inline-flex whitespace-nowrap items-center justify-center gap-1.5 rounded-lg border border-line-strong bg-panel px-3.5 py-2 text-sm font-medium text-ink hover:bg-hover disabled:opacity-50 disabled:cursor-not-allowed",
  ghost:
    "inline-flex min-h-9 min-w-9 whitespace-nowrap items-center justify-center gap-1.5 rounded-lg px-2 py-1 text-sm text-muted hover:bg-hover hover:text-ink",
};

export const field =
  "rounded-lg border border-line-strong bg-field px-3 py-2 text-sm text-ink placeholder:text-faint focus:border-brand focus:outline-none focus:ring-2 focus:ring-brand/25 aria-[invalid=true]:border-danger";

export const fieldLabel = "text-compact font-semibold text-ink-2";

/**
 * Report page body: results on the left, the query on a sticky rail on the
&On narrow screens the query comes first.
 */
export const ReportLayout: React.FC<{ query: React.ReactNode; summary: string; children: React.ReactNode }> = ({
  query,
  summary,
  children,
}) => {
  // Below 1280px the query collapses to a one-line summary so results stay on screen.
  const [open, setOpen] = useState(false);
  const queryId = useId();
  return (
    <div className="grid grid-cols-1 items-start gap-5 xl:grid-cols-[minmax(0,1fr)_20rem]">
      <div className="xl:hidden">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          aria-controls={queryId}
          className="flex w-full items-center gap-3 rounded-xl border border-line bg-panel px-4 py-3 text-left"
        >
          <span className="min-w-0 flex-1 truncate text-sm text-ink-2">{summary}</span>
          <span className="shrink-0 text-sm font-semibold text-brand">{open ? "Done" : "Edit"}</span>
        </button>
      </div>
      <aside
        id={queryId}
        aria-label="Query"
        className={`space-y-5 xl:sticky xl:top-4 xl:order-2 xl:block ${open ? "block" : "hidden"}`}
      >
        {query}
      </aside>
      <div className="min-w-0 space-y-5 xl:order-1">{children}</div>
    </div>
  );
};

/** Grey placeholder blocks while results load; static when motion is reduced. */
export const Skeleton: React.FC<{ className?: string }> = ({ className = "" }) => (
  <div className={`animate-pulse rounded-md bg-sunken motion-reduce:animate-none ${className}`} aria-hidden="true" />
);

export const ChartSkeleton: React.FC<{ rows?: number }> = ({ rows }) => (
  <div role="status" aria-label="Loading results" className="space-y-3">
    <Skeleton className="h-6 w-2/3" />
    {rows ? (
      Array.from({ length: rows }, (_, i) => <Skeleton key={i} className="h-8" />)
    ) : (
      <Skeleton className="h-56" />
    )}
  </div>
);

/** Sort state for a table; click the same column again to flip direction. */
/** Number columns start high-to-low on first click; `textKeys` start A→Z. */
export function useSort<K extends string>(initial: K, initialDesc = true, textKeys: K[] = []) {
  const [key, setKey] = useState<K>(initial);
  const [desc, setDesc] = useState(initialDesc);
  const toggle = (k: K) => {
    if (k === key) setDesc((d) => !d);
    else {
      setKey(k);
      setDesc(!textKeys.includes(k));
    }
  };
  const sortBy = <T,>(rows: T[], get: (r: T, k: K) => number | string) =>
    [...rows].sort((a, b) => {
      const x = get(a, key);
      const y = get(b, key);
      const c =
        typeof x === "number" && typeof y === "number"
          ? x === y ? 0 : x < y ? -1 : 1 // not x - y: -Infinity - -Infinity is NaN
          : String(x).localeCompare(String(y), undefined, { numeric: true });
      return desc ? -c : c;
    });
  return { key, desc, toggle, sortBy };
}

export const SortHeader: React.FC<{
  label: string;
  active: boolean;
  desc: boolean;
  onClick: () => void;
  align?: "left" | "right";
}> = ({ label, active, desc, onClick, align = "left" }) => (
  <th
    scope="col"
    aria-sort={active ? (desc ? "descending" : "ascending") : "none"}
    className={`sticky top-0 z-10 bg-panel py-2 font-medium ${align === "right" ? "pl-3 text-right" : "pr-3 text-left"}`}
  >
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-1 hover:text-ink ${active ? "text-ink" : ""}`}
    >
      {label}
      <span aria-hidden="true" className={active ? "" : "opacity-0"}>
        {desc ? "↓" : "↑"}
      </span>
    </button>
  </th>
);

export const ListSkeleton: React.FC<{ rows?: number }> = ({ rows = 4 }) => (
  <div role="status" aria-label="Loading" className="space-y-2">
    {Array.from({ length: rows }, (_, i) => (
      <Skeleton key={i} className="h-10" />
    ))}
  </div>
);

export const ROLE_HELP: Record<Role, string> = {
  viewer: "Reads reports and dashboards",
  member: "Also creates and edits reports, dashboards, cohorts and flags",
  admin: "Also manages members, invites and API keys",
  owner: "Also renames the organisation and manages owners",
};

export const RoleBadge: React.FC<{ role: Role }> = ({ role }) => (
  <span
    title={ROLE_HELP[role]}
    className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
      role === "owner" || role === "admin" ? "bg-brand-soft text-brand" : "border border-line-strong text-ink-2"
    }`}
  >
    {role[0].toUpperCase() + role.slice(1)}
  </span>
);

/**
 * Tab strip with the standard keyboard model: one tab stop, arrows move and select,
 * Home/End jump. Pair with `<div role="tabpanel" id={panelId} aria-labelledby={tabId(id)}>`.
 */
export function Tabs<T extends string>({
  label,
  tabs,
  value,
  onChange,
  panelId,
  variant = "underline",
}: {
  label: string;
  tabs: { id: T; label: string }[];
  value: T;
  onChange: (id: T) => void;
  panelId?: string;
  variant?: "underline" | "pill";
}) {
  const onKey = (e: React.KeyboardEvent<HTMLDivElement>) => {
    const i = tabs.findIndex((t) => t.id === value);
    const n =
      e.key === "ArrowRight" ? i + 1 : e.key === "ArrowLeft" ? i - 1 : e.key === "Home" ? 0 : e.key === "End" ? tabs.length - 1 : null;
    if (n === null) return;
    e.preventDefault();
    const next = tabs[(n + tabs.length) % tabs.length];
    onChange(next.id);
    e.currentTarget.querySelector<HTMLElement>(`[data-tab="${next.id}"]`)?.focus();
  };
  const cls = (on: boolean) =>
    variant === "underline"
      ? `-mb-px whitespace-nowrap border-b-2 px-3 py-2 text-sm font-medium transition-colors duration-150 ${on ? "border-brand text-ink" : "border-transparent text-muted hover:text-ink"}`
      : `whitespace-nowrap rounded-md px-2.5 py-1 text-sm font-medium ${on ? "bg-sunken text-ink" : "text-muted hover:text-ink"}`;
  return (
    <div
      role="tablist"
      aria-label={label}
      onKeyDown={onKey}
      className={variant === "underline" ? "no-scrollbar flex gap-1 overflow-x-auto border-b border-line" : "flex gap-1"}
    >
      {tabs.map((t) => (
        <button
          key={t.id}
          type="button"
          role="tab"
          id={panelId ? `${panelId}-tab-${t.id}` : undefined}
          data-tab={t.id}
          aria-selected={value === t.id}
          aria-controls={panelId}
          tabIndex={value === t.id ? 0 : -1}
          onClick={() => onChange(t.id)}
          className={cls(value === t.id)}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

/** Segmented choice (time range, direction): the same look as the report date bar. */
export function Segmented<T extends string | number>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
}) {
  return (
    <div role="group" aria-label={label} className="no-scrollbar flex max-w-full overflow-x-auto rounded-lg border border-line bg-panel p-0.5 text-compact">
      {options.map((o) => (
        <button
          key={String(o.value)}
          type="button"
          aria-pressed={value === o.value}
          onClick={() => onChange(o.value)}
          className={`whitespace-nowrap rounded-md px-2.5 py-1.5 font-medium ${value === o.value ? "bg-brand text-on-brand" : "text-ink-2 hover:bg-hover"}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

/** Keyboard key hint, e.g. <Kbd>⌘K</Kbd>. */
export const Kbd: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <kbd className="rounded border border-line-strong bg-panel px-1 font-sans text-2xs font-medium text-muted">{children}</kbd>
);
