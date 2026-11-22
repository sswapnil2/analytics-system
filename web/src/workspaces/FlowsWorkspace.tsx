import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RangePresets, RangeSel, rangeFor, rangePhrase } from "../dashboards/ReportCard";
import { SaveToDashboard, useSavedReport } from "../dashboards/SaveToDashboard";
import { fromGlobalFilter } from "../components/FilterPanel";
import { PropertyPicker } from "../components/PropertyPicker";
import { GitBranch, RefreshCw } from "lucide-react";
import { api, FlowsResult, isAbortError, SamplingMode } from "../api/client";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";
import {
  btn,
  EmptyState,
  ErrorNotice,
  fieldLabel,
  PageHeader,
  Panel,
  ReportLayout,
  SamplingMeta,
  ChartSkeleton,
} from "../components/ui";
import { FilterPanel, EMPTY_FILTER, FilterDraft, describeFilter, toGlobalFilter } from "../components/FilterPanel";

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  refreshToken: number;
}

// Names the engine emits for path structure; these survive "Hide system events".
const DROP_OFF = "$Drop_Off";
const STRUCTURAL = new Map([
  [DROP_OFF, "Dropped off"],
  ["$Session_Start", "Session start"],
  ["$Other", "Other events"],
]);

const nodeLabel = (name: string) => STRUCTURAL.get(name) ?? humanizeEvent(name);

const NODE_W = 18;
const NODE_GAP = 10;
const LABEL_MIN_H = 34; // two text lines; keeps small nodes from overlapping
const COL_H = 360;
const TOP = 12;
const LAST_COL_LABEL_W = 170;
const SVG_W = 920;

function truncate(text: string, maxChars: number): string {
  return text.length > maxChars ? `${text.slice(0, Math.max(1, maxChars - 1))}…` : text;
}

export const FlowsWorkspace: React.FC<Props> = ({ projectId, availableEvents, availableProps, refreshToken }) => {
  const cleanEvents = availableEvents.filter((e) => !e.startsWith("$"));
  const [anchorEvent, setAnchorEvent] = useState<string>(() => cleanEvents[0] ?? "");
  const [direction, setDirection] = useState<"forward" | "reverse">("forward");
  const [steps, setSteps] = useState<number>(3);
  const [hideSystem, setHideSystem] = useState<boolean>(true);
  const [filter, setFilter] = useState<FilterDraft>(EMPTY_FILTER);
  const [result, setResult] = useState<FlowsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  // Node or ribbon under the pointer, or the node whose table row has focus; its paths stay lit.
  const [hot, setHot] = useState<{ kind: "node" | "ribbon"; id: string } | null>(null);
  const [tip, setTip] = useState<{ x: number; y: number; below: boolean; text: string; sub: string } | null>(null);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const clean = availableEvents.filter((e) => !e.startsWith("$"));
    if (clean.length === 0) return;
    setAnchorEvent((prev) => prev || clean[0]);
  }, [availableEvents]);

  const [range, setRange] = useState<RangeSel>("14");
  const reqId = useRef(0);
  const flowAbortRef = useRef<AbortController | null>(null);
  const buildQuery = useCallback(
    () => ({ anchor_event: anchorEvent, direction, steps, global_filter: toGlobalFilter(filter) }),
    [anchorEvent, direction, steps, filter],
  );
  const { error: openError, saved: savedReport } = useSavedReport(
    projectId,
    (q) => {
      const s = q as { anchor_event?: string; direction?: "forward" | "reverse"; steps?: number; global_filter?: unknown };
      if (s.anchor_event) setAnchorEvent(s.anchor_event);
      if (s.direction) setDirection(s.direction);
      if (s.steps) setSteps(s.steps);
      setFilter(fromGlobalFilter(s.global_filter));
    },
    setRange,
    { query: buildQuery, range },
  );

  const [samplingMode, setSamplingMode] = useState<SamplingMode>("auto");

  const runFlows = useCallback(async () => {
    if (!anchorEvent) {
      setResult(null);
      return;
    }
    flowAbortRef.current?.abort();
    const ctrl = new AbortController();
    flowAbortRef.current = ctrl;
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryFlows(projectId, { time_range: rangeFor(range), ...buildQuery(), sampling: samplingMode }, ctrl.signal);
      if (id === reqId.current && !ctrl.signal.aborted) setResult(res);
    } catch (err) {
      if (!isAbortError(err) && id === reqId.current && !ctrl.signal.aborted) setError(errorMessage(err));
    } finally {
      if (id === reqId.current && !ctrl.signal.aborted) setLoading(false);
    }
  }, [projectId, anchorEvent, buildQuery, range, samplingMode]);

  useEffect(() => {
    void runFlows();
    return () => {
      flowAbortRef.current?.abort();
    };
  }, [runFlows, refreshToken]);

  const sankey = useMemo(() => {
    if (!result) return null;
    const nodes = hideSystem
      ? result.nodes.filter((n) => !n.event_name.startsWith("$") || STRUCTURAL.has(n.event_name))
      : result.nodes;
    if (nodes.length === 0) return result.nodes.length > 0 ? { svgH: 0, nodeRects: [], ribbons: [], onlySystem: true } : null;
    const byId = new Map(nodes.map((n) => [n.id, n]));
    const links = result.links.filter((l) => byId.has(l.source) && byId.has(l.target));

    // Main path: from the anchor, follow the biggest link one step outward each time.
    const mainLinks = new Set<string>();
    const mainNodes = new Set<string>();
    let cur = nodes.find((n) => n.step_offset === 0);
    while (cur) {
      mainNodes.add(cur.id);
      const from = cur;
      const next = links
        .map((l) => ({ l, other: byId.get(l.source === from.id ? l.target : l.target === from.id ? l.source : "") }))
        .filter(
          (c) =>
            c.other &&
            Math.abs(c.other.step_offset) === Math.abs(from.step_offset) + 1 &&
            !STRUCTURAL.has(c.other.event_name),
        )
        .sort((a, b) => b.l.user_count - a.l.user_count)[0];
      if (!next) break;
      mainLinks.add(`${next.l.source}->${next.l.target}`);
      cur = next.other;
    }

    const colsMap = new Map<number, typeof nodes>();
    for (const n of nodes) {
      const list = colsMap.get(n.step_offset) ?? [];
      list.push(n);
      colsMap.set(n.step_offset, list);
    }
    const sortedCols = Array.from(colsMap.keys()).sort((a, b) => a - b);
    const colGap =
      sortedCols.length > 1 ? (SVG_W - 20 - NODE_W - LAST_COL_LABEL_W) / (sortedCols.length - 1) : 0;
    const labelChars = (cIdx: number) =>
      Math.floor(((cIdx === sortedCols.length - 1 ? LAST_COL_LABEL_W : colGap - NODE_W) - 16) / 6.5);

    const nodeRects = new Map<
      string,
      { x: number; y: number; h: number; name: string; count: number; isDrop: boolean; isMain: boolean; maxChars: number; step: number; share: number }
    >();
    let svgH = 0;

    sortedCols.forEach((colOffset, cIdx) => {
      const colNodes = (colsMap.get(colOffset) ?? []).slice(0, 7);
      const colTotal = Math.max(1, colNodes.reduce((acc, n) => acc + n.user_count, 0));
      const usableH = COL_H - Math.max(0, colNodes.length - 1) * NODE_GAP;
      let curY = TOP;
      const x = 20 + cIdx * colGap;
      for (const n of colNodes) {
        const h = Math.max(4, (n.user_count / colTotal) * usableH);
        nodeRects.set(n.id, {
          x,
          y: curY,
          h,
          name: n.event_name,
          count: n.user_count,
          isDrop: n.event_name === DROP_OFF,
          isMain: mainNodes.has(n.id),
          maxChars: labelChars(cIdx),
          step: colOffset,
          share: n.user_count / colTotal,
        });
        curY += Math.max(h, LABEL_MIN_H) + NODE_GAP;
      }
      svgH = Math.max(svgH, curY);
    });

    const srcOutOffset = new Map<string, number>();
    const dstInOffset = new Map<string, number>();
    const ribbons = links
      .filter((l) => nodeRects.has(l.source) && nodeRects.has(l.target))
      .map((l) => {
        const s = nodeRects.get(l.source)!;
        const t = nodeRects.get(l.target)!;
        const sThick = Math.max(2, (l.user_count / Math.max(1, s.count)) * s.h);
        const tThick = Math.max(2, (l.user_count / Math.max(1, t.count)) * t.h);
        const sOff = srcOutOffset.get(l.source) ?? 0;
        const tOff = dstInOffset.get(l.target) ?? 0;
        srcOutOffset.set(l.source, sOff + sThick);
        dstInOffset.set(l.target, tOff + tThick);

        const x0 = s.x + NODE_W;
        const y0 = s.y + Math.min(s.h - 1, sOff + sThick / 2);
        const x1 = t.x;
        const y1 = t.y + Math.min(t.h - 1, tOff + tThick / 2);
        const cx = (x0 + x1) / 2;
        const key = `${l.source}->${l.target}`;
        return {
          key,
          source: l.source,
          target: l.target,
          share: l.user_count / Math.max(1, s.count),
          d: `M ${x0},${y0} C ${cx},${y0} ${cx},${y1} ${x1},${y1}`,
          strokeW: Math.max(2, (sThick + tThick) / 2),
          tone: (t.isDrop ? "drop" : mainLinks.has(key) ? "main" : "other") as "drop" | "main" | "other",
          count: l.user_count,
          srcLabel: nodeLabel(s.name),
          dstLabel: nodeLabel(t.name),
        };
      });

    return { svgH: svgH + 4, nodeRects: Array.from(nodeRects.entries()), ribbons, onlySystem: false };
  }, [result, hideSystem]);

  const eventOptions = Array.from(
    new Set([...availableEvents.filter((e) => !e.startsWith("$")), anchorEvent].filter(Boolean)),
  );

  const ribbonStyle = {
    main: { stroke: "rgb(var(--brand))", opacity: 0.45 },
    drop: { stroke: "rgb(var(--danger))", opacity: 0.35 },
    other: { stroke: "rgb(var(--line-strong))", opacity: 0.8 },
  } as const;

  const pct = (v: number) => `${(v * 100).toFixed(v < 0.1 ? 1 : 0)}%`;
  const stepName = (k: number) => (k === 0 ? "Key event" : k > 0 ? `Step ${k}` : `${-k} before`);
  // A ribbon is lit when it, or a node at either end, is hot.
  const lit = (rb: { key: string; source: string; target: string }) =>
    !hot || (hot.kind === "ribbon" ? hot.id === rb.key : hot.id === rb.source || hot.id === rb.target);
  const nodeLit = (id: string) =>
    !hot ||
    (hot.kind === "node"
      ? hot.id === id || !!sankey?.ribbons.some((rb) => (rb.source === hot.id && rb.target === id) || (rb.target === hot.id && rb.source === id))
      : !!sankey?.ribbons.some((rb) => rb.key === hot.id && (rb.source === id || rb.target === id)));
  /** Tooltip by the pointer, in the chart box's own pixels; kept inside the box. */
  const showTip = (e: React.MouseEvent, text: string, sub: string) => {
    const box = boxRef.current?.getBoundingClientRect();
    if (!box) return;
    // Near the top edge there's no room above, so it goes under the pointer instead.
    const y = e.clientY - box.top;
    const below = y < 72;
    const x = Math.min(box.width - 110, Math.max(110, e.clientX - box.left));
    setTip({ x, y: below ? y + 18 : y - 12, below, text, sub });
  };
  // Same target as before: keep the object so moving within it doesn't re-render the chart's state.
  const heat = (kind: "node" | "ribbon", id: string) => setHot((h) => (h?.kind === kind && h.id === id ? h : { kind, id }));
  const clearHot = () => {
    setHot(null);
    setTip(null);
  };

  const pageTitle = anchorEvent
    ? `${direction === "forward" ? "After" : "Before"} ${humanizeEvent(anchorEvent)}`
    : "Journeys";

  return (
    <div className="space-y-5">
      <PageHeader
        title={pageTitle}
        description="Journey: what users do before or after a key event."
        actions={
          <>
          <RangePresets value={range} onChange={setRange} />
          <SaveToDashboard saved={savedReport} projectId={projectId} kind="flows" defaultName={pageTitle} query={buildQuery} />
          <button type="button" onClick={() => void runFlows()} disabled={loading} className={btn.ghost} aria-label="Refresh results">
            <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
          </button>
          </>
        }
      />

      <ReportLayout
        summary={`${direction === "forward" ? "After" : "Before"} ${humanizeEvent(anchorEvent)} · ${steps} steps${
          hideSystem ? " · system events hidden" : ""
        }${describeFilter(filter) ? ` · ${describeFilter(filter)}` : ""}`}
        query={<>
      <FilterPanel projectId={projectId} availableProps={availableProps} anchorEvent={anchorEvent} value={filter} onChange={setFilter} />
      <Panel title="Query">
        <div className="flex flex-col gap-4">
          <PropertyPicker
            label="Key event"
            value={anchorEvent}
            onChange={setAnchorEvent}
            placeholder="Search events"
            recentKey={`pulse.recent-events.${projectId}`}
            options={eventOptions.map((ev) => ({ value: ev, label: humanizeEvent(ev), group: "Events" }))}
          />

          <div className="flex flex-col gap-1">
            <span className={fieldLabel} id="journey-direction">
              Show
            </span>
            <div
              className="flex rounded-lg border border-line-strong p-0.5"
              role="group"
              aria-labelledby="journey-direction"
            >
              {(
                [
                  ["forward", "After the event"],
                  ["reverse", "Before the event"],
                ] as const
              ).map(([k, text]) => (
                <button
                  key={k}
                  type="button"
                  aria-pressed={direction === k}
                  onClick={() => setDirection(k)}
                  className={`rounded px-2.5 py-1 text-sm ${
                    direction === k ? "bg-brand text-on-brand" : "text-muted hover:text-ink"
                  }`}
                >
                  {text}
                </button>
              ))}
            </div>
          </div>

          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>
              Steps: <span className="tabular text-ink">{steps}</span>
            </span>
            <input
              type="range"
              min={2}
              max={5}
              value={steps}
              onChange={(e) => setSteps(Number(e.target.value))}
              className="h-8 w-32 accent-brand"
            />
          </label>

          <label className="flex items-center gap-2 pb-1.5 text-sm text-ink-2">
            <input
              type="checkbox"
              checked={hideSystem}
              onChange={(e) => setHideSystem(e.target.checked)}
              className="accent-brand"
            />
            Hide system events
          </label>
        </div>
      </Panel>
        </>}
      >

      {openError && <ErrorNotice message={openError} />}
      {error && <ErrorNotice message={error} onRetry={() => void runFlows()} />}

      <Panel
        title="Paths"
        meta={
          <SamplingMeta
            sampling={result?.sampling}
            mode={samplingMode}
            onChangeMode={setSamplingMode}
            us={result?.exec_time_us}
            loading={loading && !!result}
          />
        }
      >
        {sankey?.onlySystem ? (
          <EmptyState
            icon={GitBranch}
            title="Only system events here"
            body={`Users did ${humanizeEvent(anchorEvent)} ${rangePhrase(range)}, but every step ${direction === "forward" ? "after" : "before"} it is a system event. Untick "Hide system events" to see them.`}
          />
        ) : sankey ? (
          <>
            <p className="mb-3 text-sm text-muted">
              Repeated events in a row count as one step. The most common path is highlighted; orange marks users who
              stopped. Point at a step or a path to follow it.
            </p>
            <div ref={boxRef} className="relative" onMouseLeave={clearHot}>
            <div className="overflow-x-auto" onScroll={clearHot}>
              <svg
                viewBox={`0 0 ${SVG_W} ${sankey.svgH}`}
                className="w-full min-w-[720px] motion-safe:animate-fade-in"
                style={{ height: "auto" }}
                role="img"
                aria-label={`User journeys ${direction === "forward" ? "after" : "before"} ${humanizeEvent(anchorEvent)}; each step is listed in the table below`}
                key={`${anchorEvent}-${direction}-${steps}`}
              >
                {sankey.ribbons.map((r) => (
                  <path
                    key={r.key}
                    d={r.d}
                    fill="none"
                    stroke={ribbonStyle[r.tone].stroke}
                    strokeOpacity={lit(r) ? ribbonStyle[r.tone].opacity * (hot ? 1.6 : 1) : 0.08}
                    strokeWidth={r.strokeW}
                    className="transition-[stroke-opacity] duration-150"
                    onMouseMove={(e) => {
                      heat("ribbon", r.key);
                      showTip(e, `${r.srcLabel} → ${r.dstLabel}`, `${formatCount(r.count)} users · ${pct(r.share)} of ${r.srcLabel}`);
                    }}
                  />
                ))}

                {sankey.nodeRects.map(([id, n]) => {
                  const label = nodeLabel(n.name);
                  return (
                    <g
                      key={id}
                      opacity={nodeLit(id) ? 1 : 0.35}
                      className="transition-opacity duration-150"
                      onMouseMove={(e) => {
                        heat("node", id);
                        showTip(e, label, `${formatCount(n.count)} users · ${pct(n.share)} of ${stepName(n.step).toLowerCase()}`);
                      }}
                    >
                      <rect
                        x={n.x}
                        y={n.y}
                        width={NODE_W}
                        height={n.h}
                        rx={3}
                        fill={
                          n.isDrop ? "rgb(var(--danger))" : n.isMain ? "rgb(var(--brand))" : "rgb(var(--muted))"
                        }
                        stroke={hot?.kind === "node" && hot.id === id ? "rgb(var(--ink))" : "none"}
                        strokeWidth={1.5}
                      />
                      <text
                        x={n.x + NODE_W + 6}
                        y={n.y + 12}
                        fill={n.isDrop ? "rgb(var(--danger))" : "rgb(var(--ink))"}
                        fontSize={12}
                        fontWeight={500}
                      >
                        {truncate(label, n.maxChars)}
                      </text>
                      <text x={n.x + NODE_W + 6} y={n.y + 27} fill="rgb(var(--muted))" fontSize={11} className="tabular">
                        {`${formatCount(n.count)} users`}
                      </text>
                    </g>
                  );
                })}
              </svg>
            </div>
              {tip && (
                <div
                  className={`pointer-events-none absolute z-10 -translate-x-1/2 ${tip.below ? "" : "-translate-y-full"} whitespace-nowrap rounded-lg border border-line-strong bg-panel px-3 py-2 text-compact shadow-pop motion-safe:animate-pop`}
                  style={{ left: tip.x, top: tip.y }}
                >
                  <p className="font-semibold text-ink">{tip.text}</p>
                  <p className="tabular text-ink-2">{tip.sub}</p>
                </div>
              )}
            </div>

            <h3 className="mb-2 mt-5 text-sm font-semibold text-ink">Step by step</h3>
            <div className="max-h-[22rem] overflow-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-line text-xs text-muted">
                    <th className="sticky top-0 bg-panel py-2 pr-3 font-medium">Step</th>
                    <th className="sticky top-0 bg-panel py-2 pr-3 font-medium">Event</th>
                    <th className="sticky top-0 bg-panel py-2 pr-3 text-right font-medium">Users</th>
                    <th className="sticky top-0 bg-panel py-2 text-right font-medium">Share</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {sankey.nodeRects.map(([id, n], i, all) => {
                    const on = hot?.kind === "node" && hot.id === id;
                    const first = i === 0 || all[i - 1][1].step !== n.step;
                    return (
                      <tr
                        key={id}
                        tabIndex={0}
                        onMouseEnter={() => setHot({ kind: "node", id })}
                        onMouseLeave={clearHot}
                        onFocus={() => setHot({ kind: "node", id })}
                        onBlur={clearHot}
                        className={`outline-none transition-colors duration-150 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand/40 ${on ? "bg-hover" : "hover:bg-hover"}`}
                      >
                        <td className="whitespace-nowrap py-1.5 pr-3 text-xs text-muted">{first ? stepName(n.step) : ""}</td>
                        <td className="py-1.5 pr-3">
                          <span className="flex items-center gap-2">
                            <span
                              className="h-2.5 w-2.5 shrink-0 rounded-[3px]"
                              style={{ backgroundColor: n.isDrop ? "rgb(var(--danger))" : n.isMain ? "rgb(var(--brand))" : "rgb(var(--muted))" }}
                              aria-hidden="true"
                            />
                            <span className={`whitespace-nowrap ${n.isDrop ? "text-danger" : "text-ink"}`}>{nodeLabel(n.name)}</span>
                            {n.isMain && n.step !== 0 && <span className="sr-only text-xs text-muted sm:not-sr-only">most common</span>}
                          </span>
                        </td>
                        <td className="tabular py-1.5 pr-3 text-right text-ink">{formatCount(n.count)}</td>
                        <td className="tabular py-1.5 text-right text-ink-2">{pct(n.share)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        ) : loading ? (
          <ChartSkeleton />
        ) : (
          <EmptyState
            icon={GitBranch}
            title="No journeys found"
            body={`Nobody did ${humanizeEvent(anchorEvent)} ${rangePhrase(range)}. Pick a different key event.`}
          />
        )}
      </Panel>
      </ReportLayout>
    </div>
  );
};
