import React, { useCallback, useEffect, useMemo, useState } from "react";
import { GitBranch } from "lucide-react";
import { api, default14DayRange, FlowsResult } from "../api/client";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";
import {
  btn,
  EmptyState,
  ErrorNotice,
  field,
  fieldLabel,
  PageHeader,
  Panel,
  RanIn,
  Spinner,
} from "../components/ui";

interface Props {
  projectId: string;
  availableEvents: string[];
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

export const FlowsWorkspace: React.FC<Props> = ({ projectId, availableEvents, refreshToken }) => {
  const [anchorEvent, setAnchorEvent] = useState<string>("KYC_PAN_Submitted");
  const [direction, setDirection] = useState<"forward" | "reverse">("forward");
  const [steps, setSteps] = useState<number>(3);
  const [hideSystem, setHideSystem] = useState<boolean>(true);
  const [result, setResult] = useState<FlowsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const runFlows = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryFlows(projectId, {
        time_range: default14DayRange(),
        anchor_event: anchorEvent,
        direction,
        steps,
      });
      setResult(res);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId, anchorEvent, direction, steps]);

  useEffect(() => {
    void runFlows();
  }, [runFlows, refreshToken]);

  const sankey = useMemo(() => {
    if (!result) return null;
    const nodes = hideSystem
      ? result.nodes.filter((n) => !n.event_name.startsWith("$") || STRUCTURAL.has(n.event_name))
      : result.nodes;
    if (nodes.length === 0) return null;
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
      { x: number; y: number; h: number; name: string; count: number; isDrop: boolean; isMain: boolean; maxChars: number }
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
          d: `M ${x0},${y0} C ${cx},${y0} ${cx},${y1} ${x1},${y1}`,
          strokeW: Math.max(2, (sThick + tThick) / 2),
          tone: (t.isDrop ? "drop" : mainLinks.has(key) ? "main" : "other") as "drop" | "main" | "other",
          count: l.user_count,
          srcLabel: nodeLabel(s.name),
          dstLabel: nodeLabel(t.name),
        };
      });

    return { svgH: svgH + 4, nodeRects: Array.from(nodeRects.entries()), ribbons };
  }, [result, hideSystem]);

  const eventOptions = [
    "App_Open",
    "OTP_Verified",
    "KYC_PAN_Submitted",
    "Video_KYC_Completed",
    "UPI_Autopay_Mandate_Approved",
    "First_Disbursal_Success",
    ...availableEvents,
  ].filter((v, i, a) => a.indexOf(v) === i);

  const ribbonStyle = {
    main: { stroke: "rgb(var(--brand))", opacity: 0.45 },
    drop: { stroke: "rgb(var(--danger))", opacity: 0.35 },
    other: { stroke: "rgb(var(--line-strong))", opacity: 0.8 },
  } as const;

  return (
    <div className="space-y-5">
      <PageHeader
        title="Journeys"
        description="What users do before or after a key event."
        actions={
          <button type="button" onClick={() => void runFlows()} disabled={loading} className={btn.primary}>
            {loading ? "Running…" : "Run"}
          </button>
        }
      />

      <Panel>
        <div className="flex flex-wrap items-end gap-4">
          <label className="flex min-w-0 flex-col gap-1">
            <span className={fieldLabel}>Key event</span>
            <select value={anchorEvent} onChange={(e) => setAnchorEvent(e.target.value)} className={field}>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {humanizeEvent(ev)}
                </option>
              ))}
            </select>
          </label>

          <div className="flex flex-col gap-1">
            <span className={fieldLabel} id="journey-direction">
              Show
            </span>
            <div
              className="flex rounded-md border border-line-strong p-0.5"
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

      {error && <ErrorNotice message={error} onRetry={() => void runFlows()} />}

      <Panel
        title="Paths"
        meta={loading && result ? "Updating…" : <RanIn us={result?.exec_time_us} />}
      >
        {sankey ? (
          <>
            <p className="mb-3 text-sm text-muted">
              Repeated events in a row count as one step. The most common path is highlighted; red marks users who
              stopped.
            </p>
            <div className="overflow-x-auto">
              <svg
                viewBox={`0 0 ${SVG_W} ${sankey.svgH}`}
                className="w-full min-w-[720px]"
                style={{ height: "auto" }}
                role="img"
                aria-label={`User journeys ${direction === "forward" ? "after" : "before"} ${humanizeEvent(anchorEvent)}`}
              >
                {sankey.ribbons.map((r) => (
                  <path
                    key={r.key}
                    d={r.d}
                    fill="none"
                    stroke={ribbonStyle[r.tone].stroke}
                    strokeOpacity={ribbonStyle[r.tone].opacity}
                    strokeWidth={r.strokeW}
                  >
                    <title>
                      {`${r.srcLabel} to ${r.dstLabel}: ${formatCount(r.count)} users`}
                    </title>
                  </path>
                ))}

                {sankey.nodeRects.map(([id, n]) => {
                  const label = nodeLabel(n.name);
                  return (
                    <g key={id}>
                      <title>{`${label}: ${formatCount(n.count)} users`}</title>
                      <rect
                        x={n.x}
                        y={n.y}
                        width={NODE_W}
                        height={n.h}
                        rx={3}
                        fill={
                          n.isDrop ? "rgb(var(--danger))" : n.isMain ? "rgb(var(--brand))" : "rgb(var(--muted))"
                        }
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
          </>
        ) : loading ? (
          <div className="flex h-64 items-center justify-center">
            <Spinner />
          </div>
        ) : (
          <EmptyState
            icon={GitBranch}
            title="No journeys found"
            body={`Nobody did ${humanizeEvent(anchorEvent)} in the last 14 days. Pick a different key event.`}
          />
        )}
      </Panel>
    </div>
  );
};
