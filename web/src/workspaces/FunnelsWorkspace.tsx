import React, { useCallback, useEffect, useState } from "react";
import { Clock, Eye, Plus, RefreshCw, Trash2, X } from "lucide-react";
import {
  api,
  default14DayRange,
  DroppedSessionSample,
  FunnelResult,
  ReplayChunk,
} from "../api/client";
import { WireframePlayer } from "../components/WireframePlayer";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

export const FunnelsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  refreshToken,
}) => {
  const [steps, setSteps] = useState<string[]>([
    "App_Open",
    "KYC_PAN_Submitted",
    "Video_KYC_Completed",
    "UPI_Autopay_Mandate_Approved",
    "First_Disbursal_Success",
  ]);
  const [orderMode, setOrderMode] = useState<"loose_order" | "strict_order" | "any_order">(
    "loose_order",
  );
  const [windowHours, setWindowHours] = useState<number>(168);
  const [excludeEvent, setExcludeEvent] = useState<string>("");
  const [holdConstant, setHoldConstant] = useState<string>("");
  const [breakdownBy, setBreakdownBy] = useState<string>("platform");
  const [result, setResult] = useState<FunnelResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  // Slide-over Drop-Off Drawer state
  const [drawerStep, setDrawerStep] = useState<number | null>(null);
  const [activeReplaySession, setActiveReplaySession] = useState<string | null>(null);
  const [replayChunks, setReplayChunks] = useState<ReplayChunk[]>([]);
  const [replayLoading, setReplayLoading] = useState<boolean>(false);

  const runFunnel = useCallback(async () => {
    setLoading(true);
    try {
      const payload = {
        time_range: default14DayRange(),
        steps: steps.map((event_name) => ({
          event_name,
          filters: [],
          first_time_ever: false,
        })),
        order_mode: orderMode,
        conversion_window_ms: windowHours * 3_600_000,
        exclusions: excludeEvent
          ? [{ event_name: excludeEvent, between_steps: [1, Math.min(2, steps.length - 1)] }]
          : [],
        hold_constant_props: holdConstant ? [holdConstant] : [],
        breakdown_by: breakdownBy || null,
      };
      const res = await api.queryFunnel(projectId, payload);
      setResult(res);
    } finally {
      setLoading(false);
    }
  }, [projectId, steps, orderMode, windowHours, excludeEvent, holdConstant, breakdownBy]);

  useEffect(() => {
    void runFunnel();
  }, [runFunnel, refreshToken]);

  const openReplayForDrop = async (sample: DroppedSessionSample) => {
    setActiveReplaySession(sample.session_id);
    setReplayLoading(true);
    try {
      const stream = await api.getReplay(projectId, sample.session_id, sample.replay_offset_kb);
      setReplayChunks(stream.chunks);
    } catch {
      // If this random dropped user didn't have a recorded replay, load one of the frustrated KYC sessions
      const frustrated = await api.searchSessions(projectId, {
        only_rage_tap: true,
        limit: 1,
      });
      if (frustrated.sessions[0]) {
        const sid = frustrated.sessions[0].session_id;
        setActiveReplaySession(sid);
        const stream = await api.getReplay(projectId, sid, 0);
        setReplayChunks(stream.chunks);
      } else {
        setReplayChunks([]);
      }
    } finally {
      setReplayLoading(false);
    }
  };

  const droppedForDrawer =
    result?.dropped_sample_sessions.filter((d) => d.dropped_before_step === drawerStep) ?? [];

  return (
    <div className="space-y-5 relative">
      {/* Funnel Step & Constraint Builder */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="text-sm font-semibold text-slate-100">
            Exact Entity-Centric Funnel Engine (Retroactive Union-Find + Cross-Bucket Stitch)
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSteps([...steps, availableEvents[0] || "Quick_Commerce_Order_Paid"])}
              className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs text-slate-200"
            >
              <Plus className="w-3.5 h-3.5" /> Add Step
            </button>
            <button
              onClick={() => void runFunnel()}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
              Compute Funnel
            </button>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {steps.map((st, idx) => (
            <div
              key={idx}
              className="flex items-center gap-1.5 bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs"
            >
              <span className="w-5 h-5 rounded-full bg-blue-600/30 border border-blue-500/40 text-blue-300 flex items-center justify-center font-mono text-[11px]">
                {idx + 1}
              </span>
              <select
                value={st}
                onChange={(e) => {
                  const next = [...steps];
                  next[idx] = e.target.value;
                  setSteps(next);
                }}
                className="bg-transparent text-slate-100 focus:outline-none"
              >
                {[
                  "App_Open",
                  "OTP_Verified",
                  "KYC_PAN_Submitted",
                  "Video_KYC_Completed",
                  "UPI_Autopay_Mandate_Approved",
                  "First_Disbursal_Success",
                  "Quick_Grocery_Browse",
                  "Quick_Commerce_Order_Paid",
                  ...availableEvents,
                ]
                  .filter((v, i, a) => a.indexOf(v) === i)
                  .map((ev) => (
                    <option key={ev} value={ev} className="bg-slate-900">
                      {ev}
                    </option>
                  ))}
              </select>
              {steps.length > 2 && (
                <button
                  onClick={() => setSteps(steps.filter((_, i) => i !== idx))}
                  className="text-slate-500 hover:text-red-400"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              )}
            </div>
          ))}
        </div>

        <div className="grid grid-cols-2 md:grid-cols-5 gap-2.5 text-xs">
          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5">
            <div className="text-[10px] text-slate-500 uppercase">Order Mode</div>
            <select
              value={orderMode}
              onChange={(e) =>
                setOrderMode(e.target.value as "loose_order" | "strict_order" | "any_order")
              }
              className="bg-transparent w-full text-slate-100 mt-0.5 focus:outline-none"
            >
              <option value="loose_order" className="bg-slate-900">
                Loose Order
              </option>
              <option value="strict_order" className="bg-slate-900">
                Strict Order
              </option>
              <option value="any_order" className="bg-slate-900">
                Any Order
              </option>
            </select>
          </div>

          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5">
            <div className="text-[10px] text-slate-500 uppercase">Window</div>
            <select
              value={windowHours}
              onChange={(e) => setWindowHours(Number(e.target.value))}
              className="bg-transparent w-full text-slate-100 mt-0.5 focus:outline-none"
            >
              <option value={1} className="bg-slate-900">
                1 Hour
              </option>
              <option value={24} className="bg-slate-900">
                24 Hours
              </option>
              <option value={168} className="bg-slate-900">
                7 Days
              </option>
              <option value={336} className="bg-slate-900">
                14 Days
              </option>
            </select>
          </div>

          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5">
            <div className="text-[10px] text-slate-500 uppercase">Exclude Step 2→3</div>
            <select
              value={excludeEvent}
              onChange={(e) => setExcludeEvent(e.target.value)}
              className="bg-transparent w-full text-slate-100 mt-0.5 focus:outline-none"
            >
              <option value="" className="bg-slate-900">
                None
              </option>
              <option value="NSDL_Gateway_504_Timeout" className="bg-slate-900">
                NSDL_Gateway_504_Timeout
              </option>
            </select>
          </div>

          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5">
            <div className="text-[10px] text-slate-500 uppercase">Hold Constant</div>
            <select
              value={holdConstant}
              onChange={(e) => setHoldConstant(e.target.value)}
              className="bg-transparent w-full text-slate-100 mt-0.5 focus:outline-none"
            >
              <option value="" className="bg-slate-900">
                None
              </option>
              <option value="payment_method" className="bg-slate-900">
                payment_method
              </option>
              <option value="platform" className="bg-slate-900">
                platform
              </option>
              <option value="city" className="bg-slate-900">
                city
              </option>
            </select>
          </div>

          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5">
            <div className="text-[10px] text-slate-500 uppercase">Breakdown By</div>
            <select
              value={breakdownBy}
              onChange={(e) => setBreakdownBy(e.target.value)}
              className="bg-transparent w-full text-slate-100 mt-0.5 focus:outline-none"
            >
              <option value="" className="bg-slate-900">
                None
              </option>
              <option value="platform" className="bg-slate-900">
                platform
              </option>
              <option value="city" className="bg-slate-900">
                city
              </option>
              <option value="payment_method" className="bg-slate-900">
                payment_method
              </option>
            </select>
          </div>
        </div>
      </div>

      {/* Funnel Conversion & Clickable Drop-Off Bars */}
      {result && (
        <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <span className="text-sm font-semibold text-slate-100">
                Overall Conversion:{" "}
                <span className="text-blue-400 font-mono">
                  {result.overall_steps[result.overall_steps.length - 1]?.conversion_from_start_pct.toFixed(
                    1,
                  )}
                  %
                </span>
              </span>
              <span className="ml-3 text-xs text-slate-400">
                Click any red Drop-Off bar to inspect dropped sessions &amp; watch binary wireframe
                replay
              </span>
            </div>
            <span className="font-mono text-xs text-emerald-400">
              Scan Latency: {(result.exec_time_us / 1000).toFixed(2)} ms
            </span>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-5 gap-4 pt-2">
            {result.overall_steps.map((st, idx) => {
              const pct = Math.max(2, st.conversion_from_start_pct);
              const dropPct =
                idx > 0
                  ? Math.max(0, 100 - st.conversion_from_prev_pct)
                  : 0;
              const prevDropped = idx > 0 ? result.overall_steps[idx - 1].dropped_users : 0;

              return (
                <div
                  key={st.step_index}
                  className="bg-[#090d16] border border-slate-800/80 rounded-xl p-3.5 flex flex-col justify-between"
                >
                  <div>
                    <div className="text-[11px] font-mono text-slate-400">STEP {idx + 1}</div>
                    <div className="text-xs font-semibold text-slate-100 truncate mt-0.5">
                      {st.event_name}
                    </div>
                  </div>

                  <div className="my-4 h-36 bg-slate-900/70 rounded-lg p-2 flex flex-col justify-end gap-1 relative">
                    {idx > 0 && prevDropped > 0 && (
                      <button
                        onClick={() => {
                          setDrawerStep(idx);
                          setActiveReplaySession(null);
                        }}
                        title="Click to view dropped sessions & watch wireframe replay"
                        style={{ height: `${Math.max(18, dropPct * 0.65)}%` }}
                        className="w-full rounded bg-red-500/25 hover:bg-red-500/45 border border-red-500/50 border-dashed flex flex-col items-center justify-center text-[10px] text-red-200 transition cursor-pointer"
                      >
                        <span className="font-mono font-bold">
                          -{prevDropped.toLocaleString()} dropped
                        </span>
                        <span className="underline text-[9px]">Watch Replay</span>
                      </button>
                    )}

                    <div
                      style={{ height: `${pct}%` }}
                      className="w-full rounded bg-gradient-to-t from-blue-700 to-blue-500 flex items-end justify-center pb-1.5 text-xs font-mono font-bold text-white shadow-inner"
                    >
                      {st.conversion_from_start_pct.toFixed(1)}%
                    </div>
                  </div>

                  <div className="flex items-center justify-between text-xs border-t border-slate-800 pt-2">
                    <span className="font-mono text-slate-200 font-semibold">
                      {st.entered_users.toLocaleString()} users
                    </span>
                    <span className="text-[11px] text-slate-400">
                      {idx === 0 ? "100% entry" : `${st.conversion_from_prev_pct.toFixed(1)}% step`}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Breakdown Table + Time-to-Convert DDSketch Card */}
      {result && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4">
            <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
              Segment Breakdown ({breakdownBy || "overall"})
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400">
                  <th className="py-2 text-left">Segment</th>
                  <th className="py-2 text-right">Entered</th>
                  <th className="py-2 text-right">Converted</th>
                  <th className="py-2 text-right">Conv %</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {result.breakdowns.map((bg) => {
                  const first = bg.steps[0]?.entered_users ?? 0;
                  const last = bg.steps[bg.steps.length - 1]?.entered_users ?? 0;
                  const rate = bg.steps[bg.steps.length - 1]?.conversion_from_start_pct ?? 0;
                  return (
                    <tr key={bg.breakdown_value}>
                      <td className="py-2 font-medium text-slate-200">{bg.breakdown_value}</td>
                      <td className="py-2 text-right font-mono text-slate-300">
                        {first.toLocaleString()}
                      </td>
                      <td className="py-2 text-right font-mono text-slate-300">
                        {last.toLocaleString()}
                      </td>
                      <td className="py-2 text-right font-mono font-semibold text-emerald-400">
                        {rate.toFixed(1)}%
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-slate-400">
                <Clock className="w-3.5 h-3.5 text-blue-400" />
                Time-to-Convert Distribution (DDSketch)
              </div>
              <div className="flex gap-3 font-mono text-xs">
                <span className="text-slate-300">
                  p50: {(result.time_to_convert.p50_ms / 1000).toFixed(1)}s
                </span>
                <span className="text-blue-300">
                  p90: {(result.time_to_convert.p90_ms / 1000).toFixed(1)}s
                </span>
                <span className="text-purple-300">
                  p99: {(result.time_to_convert.p99_ms / 1000).toFixed(1)}s
                </span>
              </div>
            </div>

            <div className="flex items-end gap-2 h-36 pt-4">
              {result.time_to_convert.histogram_bins.map(([lo, hi, cnt], i) => {
                const maxBin = Math.max(
                  1,
                  ...result.time_to_convert.histogram_bins.map((b) => b[2]),
                );
                const hPct = Math.max(4, (cnt / maxBin) * 100);
                return (
                  <div key={i} className="flex-1 flex flex-col items-center gap-1 h-full justify-end">
                    <span className="text-[10px] font-mono text-slate-400">{cnt}</span>
                    <div
                      style={{ height: `${hPct}%` }}
                      className="w-full bg-indigo-500/70 border border-indigo-400/50 rounded-t"
                      title={`${(lo / 1000).toFixed(0)}s - ${(hi / 1000).toFixed(0)}s: ${cnt} users`}
                    />
                    <span className="text-[9px] font-mono text-slate-500">
                      {(lo / 1000).toFixed(0)}s
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* Slide-Over Drawer: Dropped Sessions & Wireframe Replay */}
      {drawerStep !== null && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex justify-end">
          <div className="w-full max-w-4xl bg-[#090d16] border-l border-slate-800 h-full overflow-y-auto p-6 space-y-5">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div>
                <h3 className="text-base font-semibold text-white">
                  Dropped Sessions Before Step {drawerStep + 1} ({steps[drawerStep]})
                </h3>
                <p className="text-xs text-slate-400">
                  Direct jump via byte-range <code className="text-blue-300">replay_offset_kb</code>{" "}
                  into <code className="text-blue-300">.plsr</code> wireframe object store
                </p>
              </div>
              <button
                onClick={() => {
                  setDrawerStep(null);
                  setActiveReplaySession(null);
                }}
                className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5 max-h-56 overflow-y-auto">
              {droppedForDrawer.map((d, idx) => (
                <div
                  key={`${d.session_id}-${idx}`}
                  className="flex items-center justify-between bg-[#0e1422] border border-slate-800 rounded-lg px-3 py-2 text-xs"
                >
                  <div>
                    <div className="font-mono text-slate-200">{d.distinct_id}</div>
                    <div className="font-mono text-[11px] text-slate-500">
                      {d.session_id} • offset {d.replay_offset_kb}KB
                    </div>
                  </div>
                  <button
                    onClick={() => void openReplayForDrop(d)}
                    className="flex items-center gap-1 px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white font-medium"
                  >
                    <Eye className="w-3.5 h-3.5" /> Watch Replay
                  </button>
                </div>
              ))}
            </div>

            {activeReplaySession && (
              <div className="border-t border-slate-800 pt-4">
                {replayLoading ? (
                  <div className="text-xs text-slate-400 py-8 text-center">
                    Streaming byte-range wireframe chunks...
                  </div>
                ) : replayChunks.length > 0 ? (
                  <WireframePlayer sessionId={activeReplaySession} chunks={replayChunks} />
                ) : (
                  <div className="text-xs text-slate-400">No wireframe chunks recorded.</div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
