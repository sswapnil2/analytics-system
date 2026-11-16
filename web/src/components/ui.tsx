import React from "react";
import { AlertTriangle, Loader2 } from "lucide-react";
import { formatRunTime } from "../lib/format";

/** Page title plus a one-line description of what the page answers. */
export const PageHeader: React.FC<{
  title: string;
  description: string;
  actions?: React.ReactNode;
}> = ({ title, description, actions }) => (
  <header className="flex flex-wrap items-end justify-between gap-3 pb-5">
    <div className="min-w-0 max-w-2xl">
      <h1 className="text-[1.375rem] font-semibold leading-tight tracking-[-0.01em] text-ink">
        {title}
      </h1>
      <p className="mt-1 text-sm text-muted">{description}</p>
    </div>
    {actions && <div className="flex items-center gap-2">{actions}</div>}
  </header>
);

export const Panel: React.FC<{
  title?: string;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
  children: React.ReactNode;
}> = ({ title, meta, actions, className = "", children }) => (
  <section className={`rounded-[10px] border border-line bg-panel ${className}`}>
    {(title || actions || meta) && (
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line px-4 py-3">
        <div className="flex items-baseline gap-3 min-w-0">
          {title && <h2 className="text-sm font-semibold text-ink">{title}</h2>}
          {meta && <span className="text-xs text-faint tabular">{meta}</span>}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
    )}
    <div className="p-4">{children}</div>
  </section>
);

/** Quiet run-time readout; engine internals stay out of the main UI. */
export const RanIn: React.FC<{ us?: number | null }> = ({ us }) => {
  const text = formatRunTime(us);
  return text ? <span className="text-xs text-faint tabular">{text}</span> : null;
};

export const EmptyState: React.FC<{
  title: string;
  body?: React.ReactNode;
  action?: React.ReactNode;
  icon?: React.ComponentType<{ className?: string }>;
}> = ({ title, body, action, icon: Icon }) => (
  <div className="flex flex-col items-center justify-center gap-2 px-6 py-12 text-center">
    {Icon && <Icon className="mb-1 h-6 w-6 text-faint" />}
    <p className="text-sm font-medium text-ink">{title}</p>
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
    "inline-flex items-center justify-center gap-1.5 rounded-md bg-brand px-3 py-1.5 text-sm font-medium text-on-brand hover:bg-brand-hover disabled:opacity-50 disabled:cursor-not-allowed",
  secondary:
    "inline-flex items-center justify-center gap-1.5 rounded-md border border-line-strong bg-panel px-3 py-1.5 text-sm font-medium text-ink hover:bg-hover disabled:opacity-50 disabled:cursor-not-allowed",
  ghost:
    "inline-flex items-center justify-center gap-1.5 rounded-md px-2 py-1 text-sm text-muted hover:bg-hover hover:text-ink",
};

export const field =
  "rounded-md border border-line-strong bg-field px-2.5 py-1.5 text-sm text-ink placeholder:text-faint focus:border-brand focus:outline-none";

export const fieldLabel = "text-xs font-medium text-muted";
