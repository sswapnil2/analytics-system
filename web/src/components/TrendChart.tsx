import React, { useLayoutEffect, useRef, useState } from "react";
import { ChartArea, ChartColumn, ChartColumnStacked, ChartLine, ChartPie, ChevronDown, Hash } from "lucide-react";
import { CHART_KINDS, ChartKind, donutSlice, niceTicks, pctChange, stackBases } from "../lib/chart";
import { Menu } from "./Menu";
import { btn } from "./ui";

/** One drawn series: a metric, or one segment of a breakdown. */
export interface ChartSeries {
  key: string;
  name: string;
  /** Segment name under the metric name, e.g. a city. */
  detail?: string;
  color: string;
  points: (number | null)[];
  /** Whole-period value: legend figure and pie slice size. */
  overall: number | null;
}

interface Props {
  kind: Exclude<ChartKind, "number">;
  series: ChartSeries[];
  buckets: number[];
  /** Short x-axis label for a bucket. */
  axisLabel: (ts: number) => string;
  /** Tooltip heading for bucket i. */
  bucketTitle: (i: number) => string;
  fmt: (v: number | null | undefined, digits?: number) => string;
  /** The last bucket is still filling up (today): drawn dashed and marked "so far". */
  partialLast?: boolean;
  /** A faint dashed reference line, e.g. the previous period, on line and area charts. */
  ghost?: (number | null)[];
  /** Annotation markers at bucket indexes. */
  marks?: { key: string; i: number; title: string }[];
  compact?: boolean;
  /** Legend toggling. Without these the legend is read-only. */
  hidden?: Set<string>;
  onToggle?: (key: string) => void;
  /** Focused series, when the page shares it with something else (e.g. a table). Uncontrolled without these. */
  focus?: string | null;
  onFocus?: (key: string | null) => void;
  ariaLabel: string;
}


/**
 * The trend chart for report pages and dashboard cards: line, area, column (plain or
 * stacked) and pie from the same series, so switching kinds never refetches.
 * Hovering focuses the nearest series and dims the rest; the legend hides and shows series.
 */
export const TrendChart: React.FC<Props> = ({
  kind,
  series,
  buckets,
  axisLabel,
  bucketTitle,
  fmt,
  partialLast = false,
  marks = [],
  ghost,
  compact = false,
  hidden,
  onToggle,
  focus: focusProp,
  onFocus,
  ariaLabel,
}) => {
  const [hover, setHoverI] = useState<{ i: number } | null>(null);
  const [ownFocus, setOwnFocus] = useState<string | null>(null);
  const focus = focusProp !== undefined ? focusProp : ownFocus;
  const setFocus = onFocus ?? setOwnFocus;
  const setHover = (h: { i: number; key: string | null } | null) => {
    setHoverI(h && { i: h.i });
    setFocus(h?.key ?? null);
  };
  // Draw in real pixels so labels stay 11px and the plot fills the card at any width.
  const boxRef = useRef<HTMLDivElement>(null);
  // 0 until measured, so the first paint is already the right size and nothing animates in from a guess.
  const [boxW, setBoxW] = useState(0);
  useLayoutEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    setBoxW(Math.round(el.getBoundingClientRect().width));
    const ro = new ResizeObserver(([e]) => e.contentRect.width > 0 && setBoxW(Math.round(e.contentRect.width)));
    ro.observe(el);
    return () => ro.disconnect();
  }, [kind]);
  const shown = series.filter((s) => !hidden?.has(s.key));

  const legend = compact && series.length < 2 ? null : (
    <ul className={`flex flex-wrap gap-x-1 gap-y-1 ${compact ? "mb-1 text-xs" : "mb-3 text-sm"}`} aria-label="Series">
      {series.map((s) => {
        const off = hidden?.has(s.key) ?? false;
        const body = (
          <>
            <span
              className={`h-2.5 w-2.5 shrink-0 rounded-[3px] transition-opacity duration-150 ${off ? "opacity-30" : ""}`}
              style={{ backgroundColor: s.color }}
              aria-hidden="true"
            />
            <span className={`font-medium ${off ? "text-faint line-through" : "text-ink-2"}`}>{s.name}</span>
            {s.detail && <span className={off ? "text-faint" : "text-muted"}>{s.detail}</span>}
            {!compact && <span className="tabular text-muted">{kind === "pie" ? share(s) : fmt(s.overall)}</span>}
          </>
        );
        const common = {
          onMouseEnter: () => !off && setFocus(s.key),
          onMouseLeave: () => setFocus(null),
        };
        return (
          <li key={s.key}>
            {onToggle ? (
              <button
                type="button"
                aria-pressed={!off}
                title={off ? "Show this series" : shown.length === 1 ? "At least one series stays visible" : "Hide this series"}
                onClick={() => onToggle(s.key)}
                onFocus={common.onMouseEnter}
                onBlur={common.onMouseLeave}
                {...common}
                className="flex items-center gap-1.5 rounded-md px-1.5 py-0.5 transition-colors duration-150 hover:bg-hover"
              >
                {body}
              </button>
            ) : (
              <span className="flex items-center gap-1.5 px-1.5 py-0.5" {...common}>
                {body}
              </span>
            )}
          </li>
        );
      })}
    </ul>
  );

  function share(s: ChartSeries): string {
    const total = shown.reduce((a, x) => a + Math.max(0, x.overall ?? 0), 0);
    if (hidden?.has(s.key) || !total) return fmt(s.overall);
    return `${fmt(s.overall)} · ${((Math.max(0, s.overall ?? 0) / total) * 100).toFixed(0)}%`;
  }

  if (kind === "pie") return <>{legend}<Pie {...{ shown, fmt, compact, ariaLabel, focus, setFocus }} /></>;

  const W = boxW;
  const H = compact ? 160 : 256;
  const P = { left: compact ? 44 : 56, right: W - 15, top: 12, bottom: H - 26 };
  const plotH = P.bottom - P.top;
  const n = buckets.length;
  const bandW = (P.right - P.left) / Math.max(1, n);
  const xAt = (i: number) => P.left + (i + 0.5) * bandW;
  const stacked = kind === "stacked-area" || kind === "stacked-column";
  const bases = stacked ? stackBases(shown.map((s) => s.points)) : shown.map(() => [] as number[]);
  const tops = shown.flatMap((s, k) => s.points.map((v, i) => (bases[k][i] ?? 0) + Math.max(0, v ?? 0)));
  const showGhost = ghost && ghost.length === buckets.length && !stacked && kind !== "column";
  const ticks = niceTicks(Math.max(0, ...tops, ...(showGhost ? ghost.map((v) => v ?? 0) : [])), compact ? 3 : 4);
  const maxY = ticks[ticks.length - 1];
  const yAt = (v: number) => P.bottom - (v / maxY) * plotH;
  const labelEvery = Math.max(1, Math.ceil(n / Math.max(2, Math.floor(W / 90))));
  const lastI = n - 1;
  const dim = (key: string) => (focus && focus !== key ? 0.18 : 1);

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    // Pointer in viewBox units, whatever the rendered size.
    const ctm = e.currentTarget.getScreenCTM();
    if (!ctm) return;
    const { x, y } = new DOMPoint(e.clientX, e.clientY).matrixTransform(ctm.inverse());
    const i = Math.floor((x - P.left) / bandW);
    if (i < 0 || i >= n) return setHover(null);
    // Focus the series under the pointer: the stacked band it's in, else the nearest line within reach.
    let key: string | null = null;
    if (stacked) {
      shown.forEach((s, k) => {
        const lo = yAt(bases[k][i] ?? 0);
        const hi = yAt((bases[k][i] ?? 0) + Math.max(0, s.points[i] ?? 0));
        if (y <= lo && y >= hi) key = s.key;
      });
    } else if (kind === "column") {
      const barW = (bandW * 0.8) / Math.max(1, shown.length);
      const k = Math.floor((x - (P.left + i * bandW + bandW * 0.1)) / barW);
      if (k >= 0 && k < shown.length) key = shown[k].key;
    } else {
      let best = 22;
      for (const s of shown) {
        const v = s.points[i];
        if (v === null || v === undefined) continue;
        const d = Math.abs(yAt(v) - y);
        if (d < best) [best, key] = [d, s.key];
      }
    }
    setHover({ i, key });
  };

  const linePath = (s: ChartSeries, k: number, upTo: number) => {
    let d = "";
    let pen = false;
    s.points.forEach((v, i) => {
      if (i > upTo) return;
      if (v === null) return void (pen = false);
      d += `${pen ? "L" : "M"}${xAt(i).toFixed(1)},${yAt((bases[k][i] ?? 0) + v).toFixed(1)} `;
      pen = true;
    });
    return d;
  };
  const areaPath = (s: ChartSeries, k: number) => {
    // Gaps are drawn as zero so the band stays closed.
    const top = s.points.map((v, i) => `${xAt(i).toFixed(1)},${yAt((bases[k][i] ?? 0) + Math.max(0, v ?? 0)).toFixed(1)}`);
    const bottom = s.points.map((_, i) => `${xAt(i).toFixed(1)},${yAt(bases[k][i] ?? 0).toFixed(1)}`).reverse();
    return `M${top.join(" L")} L${bottom.join(" L")} Z`;
  };

  return (
    <>
      {legend}
      <div ref={boxRef} className={`relative w-full ${compact ? "h-40" : "h-64"}`}>
        {W > 0 && <svg
          viewBox={`0 0 ${W} ${H}`}
          className="h-full w-full overflow-visible"
          role="img"
          aria-label={ariaLabel}
          onMouseMove={onMove}
          onMouseLeave={() => setHover(null)}
        >
          {ticks.map((t) => (
            <g key={t}>
              <line x1={P.left} y1={yAt(t)} x2={P.right} y2={yAt(t)} stroke="rgb(var(--line))" vectorEffect="non-scaling-stroke" />
              <text x={P.left - 8} y={yAt(t) + 4} textAnchor="end" fill="rgb(var(--muted))" fontSize={11} className="tabular">
                {fmt(t, maxY >= 10 ? 0 : 1)}
              </text>
            </g>
          ))}
          {hover && (
            <line x1={xAt(hover.i)} x2={xAt(hover.i)} y1={P.top} y2={P.bottom} stroke="rgb(var(--line-strong))" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
          )}

          {showGhost && (
            <path
              d={ghost.map((v, i) => `${i === 0 ? "M" : "L"}${xAt(i).toFixed(1)},${yAt(v ?? 0).toFixed(1)}`).join(" ")}
              fill="none"
              stroke="rgb(var(--faint))"
              strokeWidth={1.5}
              strokeDasharray="4 4"
              vectorEffect="non-scaling-stroke"
            />
          )}
          <g key={kind} className="motion-safe:animate-fade-in">
            {kind === "column" || kind === "stacked-column"
              ? shown.map((s, k) => {
                  const barW = stacked ? bandW * 0.7 : Math.max(2, (bandW * 0.8) / shown.length);
                  return (
                    <g key={s.key} style={{ opacity: dim(s.key) }} className="transition-opacity duration-150">
                      {s.points.map((v, i) => {
                        if (v === null || v <= 0) return null;
                        const base = bases[k][i] ?? 0;
                        const x = stacked ? P.left + i * bandW + bandW * 0.15 : P.left + i * bandW + bandW * 0.1 + k * barW;
                        return (
                          <rect
                            key={buckets[i]}
                            x={x}
                            y={yAt(base + v)}
                            width={Math.max(1, barW - (stacked ? 0 : 1))}
                            height={Math.max(0, yAt(base) - yAt(base + v))}
                            fill={s.color}
                            fillOpacity={partialLast && i === lastI ? 0.55 : 1}
                            rx={stacked ? 0 : 1.5}
                            className="chart-geom"
                          />
                        );
                      })}
                    </g>
                  );
                })
              : shown.map((s, k) => {
                  const area = kind === "area" || kind === "stacked-area";
                  // The last bucket is today and still filling up, so its segment is dashed.
                  const tailOk = partialLast && lastI > 0 && s.points[lastI] !== null && s.points[lastI - 1] !== null;
                  const tail = tailOk
                    ? `M${xAt(lastI - 1)},${yAt((bases[k][lastI - 1] ?? 0) + s.points[lastI - 1]!)} L${xAt(lastI)},${yAt((bases[k][lastI] ?? 0) + s.points[lastI]!)}`
                    : "";
                  return (
                    <g key={s.key} style={{ opacity: dim(s.key) }} className="transition-opacity duration-150">
                      {area && <path d={areaPath(s, k)} fill={s.color} fillOpacity={kind === "stacked-area" ? 0.4 : 0.12} className="chart-geom" />}
                      <path
                        d={linePath(s, k, tailOk ? lastI - 1 : lastI)}
                        fill="none"
                        stroke={s.color}
                        strokeWidth={focus === s.key ? 2.75 : 2}
                        strokeLinejoin="round"
                        strokeLinecap="round"
                        vectorEffect="non-scaling-stroke"
                        className="chart-geom"
                      />
                      {tail && (
                        <path d={tail} fill="none" stroke={s.color} strokeWidth={2} strokeDasharray="3 4" strokeLinecap="round" vectorEffect="non-scaling-stroke" className="chart-geom" />
                      )}
                    </g>
                  );
                })}
          </g>

          {/* Hovered points: a ring on the focused series, small dots on the others. */}
          {hover && kind !== "column" && kind !== "stacked-column" &&
            shown.map((s, k) => {
              const v = s.points[hover.i];
              if (v === null || v === undefined) return null;
              const on = focus === s.key;
              return (
                <circle
                  key={s.key}
                  cx={xAt(hover.i)}
                  cy={yAt((bases[k][hover.i] ?? 0) + v)}
                  r={on ? 5 : 3}
                  fill={on ? "rgb(var(--panel))" : s.color}
                  stroke={s.color}
                  strokeWidth={on ? 2.5 : 0}
                  opacity={dim(s.key)}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}

          {marks.map((m) => (
            <g key={m.key}>
              <line x1={xAt(m.i)} x2={xAt(m.i)} y1={P.top} y2={P.bottom} stroke="rgb(var(--warn))" strokeWidth={1.5} strokeDasharray="4 3" vectorEffect="non-scaling-stroke" />
              <circle cx={xAt(m.i)} cy={P.top} r={4} fill="rgb(var(--warn))">
                <title>{m.title}</title>
              </circle>
            </g>
          ))}

          {buckets.map((ts, i) =>
            i % labelEvery !== 0 && i !== lastI ? null : (
              <text key={ts} x={xAt(i)} y={H - 6} textAnchor="middle" fill="rgb(var(--muted))" fontSize={11}>
                {axisLabel(ts)}
              </text>
            ),
          )}
        </svg>}
        {hover && W > 0 && (
          <Tooltip x={xAt(hover.i) / W} title={bucketTitle(hover.i)} soFar={partialLast && hover.i === lastI}>
            {shown.slice(0, 8).map((s) => {
              const v = s.points[hover.i];
              const ch = hover.i > 0 ? pctChange(s.points[hover.i - 1], v) : null;
              const on = focus === s.key;
              return (
                <li key={s.key} className={`flex items-center gap-2 ${focus && !on ? "opacity-60" : ""}`}>
                  <span className="h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: s.color }} aria-hidden="true" />
                  <span className={`min-w-0 flex-1 truncate ${on ? "font-semibold text-ink" : "text-ink-2"}`}>{s.detail || s.name}</span>
                  <span className="tabular font-semibold text-ink">{fmt(v ?? null)}</span>
                  {!compact && <Delta v={ch} />}
                </li>
              );
            })}
            {shown.length > 8 && <li className="text-xs text-muted">+{shown.length - 8} more in the table below</li>}
          </Tooltip>
        )}
      </div>
    </>
  );
};

const Delta: React.FC<{ v: number | null }> = ({ v }) =>
  v === null ? (
    <span className="w-12" />
  ) : (
    <span
      title="Change from the previous point"
      className={`tabular w-12 text-right text-xs font-semibold ${v >= 0 ? "text-ok" : "text-danger"}`}
    >
      {v >= 0 ? "+" : "−"}
      {Math.abs(v) >= 100 ? Math.abs(v).toFixed(0) : Math.abs(v).toFixed(1)}%
    </span>
  );

const Tooltip: React.FC<{ x: number; title: string; soFar?: boolean; children: React.ReactNode }> = ({ x, title, soFar, children }) => (
  <div
    className="pointer-events-none absolute top-2 z-10 min-w-[12rem] max-w-[20rem] rounded-lg border border-line-strong bg-panel px-3 py-2 text-compact shadow-pop"
    style={x > 0.6 ? { right: `${100 - x * 100 + 1.5}%` } : { left: `${x * 100 + 1.5}%` }}
  >
    <p className="mb-1.5 font-semibold text-ink">
      {title}
      {soFar && <span className="font-normal text-muted"> (so far)</span>}
    </p>
    <ul className="space-y-1">{children}</ul>
  </div>
);

const Pie: React.FC<{
  shown: ChartSeries[];
  fmt: Props["fmt"];
  compact: boolean;
  ariaLabel: string;
  focus: string | null;
  setFocus: (k: string | null) => void;
}> = ({ shown, fmt, compact, ariaLabel, focus, setFocus }) => {
  const total = shown.reduce((a, s) => a + Math.max(0, s.overall ?? 0), 0);
  const S = 200;
  let a = 0;
  const hovered = shown.find((s) => s.key === focus);
  return (
    <div className={`relative mx-auto ${compact ? "h-40 w-40" : "h-64 w-64"}`}>
      <svg viewBox={`0 0 ${S} ${S}`} className="h-full w-full overflow-visible motion-safe:animate-fade-in" role="img" aria-label={ariaLabel}>
        {total > 0 &&
          shown.map((s) => {
            const v = Math.max(0, s.overall ?? 0);
            const a0 = a;
            a += (v / total) * Math.PI * 2;
            if (v === 0) return null;
            const on = focus === s.key;
            return (
              <path
                key={s.key}
                d={donutSlice(S / 2, S / 2, on ? 98 : 94, 62, a0, a)}
                fill={s.color}
                opacity={focus && !on ? 0.35 : 1}
                stroke="rgb(var(--panel))"
                strokeWidth={1.5}
                className="chart-geom cursor-default"
                onMouseEnter={() => setFocus(s.key)}
                onMouseLeave={() => setFocus(null)}
              />
            );
          })}
      </svg>
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center text-center">
        <span className={`tabular font-semibold tracking-[-0.02em] text-ink ${compact ? "text-lg" : "text-2xl"}`}>
          {fmt(hovered ? hovered.overall : total)}
        </span>
        <span className="max-w-[8rem] truncate text-xs text-muted">
          {hovered
            ? `${hovered.detail || hovered.name} · ${total ? ((Math.max(0, hovered.overall ?? 0) / total) * 100).toFixed(1) : 0}%`
            : "Total"}
        </span>
      </div>
    </div>
  );
};

const KIND_ICON: Record<ChartKind, React.ComponentType<{ className?: string }>> = {
  line: ChartLine,
  area: ChartArea,
  "stacked-area": ChartArea,
  column: ChartColumn,
  "stacked-column": ChartColumnStacked,
  pie: ChartPie,
  number: Hash,
};

/** Chart kind dropdown. Kinds the result can't support stay listed but disabled, with the reason. */
export const ChartKindPicker: React.FC<{
  value: ChartKind;
  onChange: (k: ChartKind) => void;
  blocker: (k: ChartKind) => string | null;
}> = ({ value, onChange, blocker }) => {
  const Icon = KIND_ICON[value];
  const current = CHART_KINDS.find((c) => c.kind === value);
  return (
    <Menu
      label={`Chart type: ${current?.label}`}
      triggerClass={`${btn.secondary} py-1.5`}
      trigger={
        <>
          <Icon className="h-4 w-4" />
          {current?.label}
          <ChevronDown className="h-3.5 w-3.5 text-muted" />
        </>
      }
      items={CHART_KINDS.map((c) => {
        const why = blocker(c.kind);
        return {
          label: c.label,
          icon: KIND_ICON[c.kind],
          group: c.group,
          checked: c.kind === value,
          disabled: why !== null,
          hint: why ?? undefined,
          onSelect: () => onChange(c.kind),
        };
      })}
    />
  );
};
