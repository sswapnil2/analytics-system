import React, { useCallback, useEffect, useState } from "react";
import { AlertTriangle, Cpu, RefreshCw, Zap } from "lucide-react";
import { api, ReplayChunk, SessionSearchResult, SessionSummaryItem } from "../api/client";
import { WireframePlayer } from "../components/WireframePlayer";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

export const SessionsReplayWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  refreshToken,
}) => {
  const [eventFilter, setEventFilter] = useState<string>("NSDL_Gateway_504_Timeout");
  const [onlyRage, setOnlyRage] = useState<boolean>(true);
  const [onlyErrors, setOnlyErrors] = useState<boolean>(false);
  const [searchRes, setSearchRes] = useState<SessionSearchResult | null>(null);
  const [selected, setSelected] = useState<SessionSummaryItem | null>(null);
  const [chunks, setChunks] = useState<ReplayChunk[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  const loadReplay = useCallback(
    async (sess: SessionSummaryItem) => {
      setSelected(sess);
      try {
        const res = await api.getReplay(projectId, sess.session_id, 0);
        setChunks(res.chunks);
      } catch {
        setChunks([]);
      }
    },
    [projectId],
  );

  const runSearch = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.searchSessions(projectId, {
        event_name: eventFilter || null,
        only_rage_tap: onlyRage,
        only_errors: onlyErrors,
        limit: 30,
      });
      setSearchRes(res);
      if (res.sessions.length > 0) {
        await loadReplay(res.sessions[0]);
      } else {
        setSelected(null);
        setChunks([]);
      }
    } finally {
      setLoading(false);
    }
  }, [projectId, eventFilter, onlyRage, onlyErrors, loadReplay]);

  useEffect(() => {
    void runSearch();
  }, [runSearch, refreshToken]);

  const prunePct =
    searchRes && searchRes.total_sessions_inspected > 0
      ? ((searchRes.sessions_pruned_by_bloom / searchRes.total_sessions_inspected) * 100).toFixed(1)
      : "0.0";

  return (
    <div className="space-y-5">
      {/* Filter Bar + 64-bit SessionIndex Bloom Pruning Telemetry */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 flex flex-wrap items-center justify-between gap-3 text-xs">
        <div className="flex flex-wrap items-center gap-3">
          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <span className="text-slate-400 mr-2">Contains Event:</span>
            <select
              value={eventFilter}
              onChange={(e) => setEventFilter(e.target.value)}
              className="bg-transparent text-slate-100 font-medium focus:outline-none"
            >
              <option value="" className="bg-slate-900">
                Any Event
              </option>
              {[
                "NSDL_Gateway_504_Timeout",
                "KYC_PAN_Submitted",
                "UPI_Autopay_Mandate_Approved",
                "First_Disbursal_Success",
                ...availableEvents,
              ]
                .filter((v, i, a) => a.indexOf(v) === i)
                .map((ev) => (
                  <option key={ev} value={ev} className="bg-slate-900">
                    {ev}
                  </option>
                ))}
            </select>
          </div>

          <button
            onClick={() => setOnlyRage((v) => !v)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border transition ${
              onlyRage
                ? "bg-amber-500/20 border-amber-500/60 text-amber-200"
                : "bg-[#090d16] border-slate-800 text-slate-400"
            }`}
          >
            <Zap className="w-3.5 h-3.5" /> Rage Tap Sessions
          </button>

          <button
            onClick={() => setOnlyErrors((v) => !v)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border transition ${
              onlyErrors
                ? "bg-red-500/20 border-red-500/60 text-red-200"
                : "bg-[#090d16] border-slate-800 text-slate-400"
            }`}
          >
            <AlertTriangle className="w-3.5 h-3.5" /> 5xx / Timeout Errors
          </button>
        </div>

        <div className="flex items-center gap-3">
          {searchRes && (
            <div className="flex items-center gap-2 bg-indigo-950/50 border border-indigo-800/60 text-indigo-200 px-3 py-1.5 rounded-lg font-mono">
              <Cpu className="w-3.5 h-3.5 text-indigo-400" />
              <span>
                64-bit SessionIndex Pruned:{" "}
                <strong className="text-white">
                  {searchRes.sessions_pruned_by_bloom.toLocaleString()}
                </strong>{" "}
                / {searchRes.total_sessions_inspected.toLocaleString()} ({prunePct}%)
              </span>
            </div>
          )}

          <button
            onClick={() => void runSearch()}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
            Filter
          </button>
        </div>
      </div>

      {/* Split Session List + Interactive Phone Wireframe Player */}
      <div className="grid grid-cols-1 xl:grid-cols-12 gap-5">
        <div className="xl:col-span-4 bg-[#0e1422] border border-slate-800/80 rounded-xl p-3 space-y-2 max-h-[680px] overflow-y-auto">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 px-1 pb-1">
            Matched Sessions ({searchRes?.sessions.length ?? 0})
          </div>
          {searchRes?.sessions.map((s) => {
            const isSel = selected?.session_id === s.session_id;
            return (
              <button
                key={s.session_id}
                onClick={() => void loadReplay(s)}
                className={`w-full text-left p-3 rounded-lg border transition space-y-1.5 ${
                  isSel
                    ? "bg-blue-950/40 border-blue-500/70"
                    : "bg-[#090d16] border-slate-800/70 hover:border-slate-700"
                }`}
              >
                <div className="flex items-center justify-between text-xs">
                  <span className="font-mono font-semibold text-slate-100 truncate">
                    {s.distinct_id}
                  </span>
                  <div className="flex items-center gap-1">
                    {s.has_rage_tap && (
                      <span className="px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-300 text-[10px] font-bold">
                        RAGE
                      </span>
                    )}
                    {s.has_error && (
                      <span className="px-1.5 py-0.5 rounded bg-red-500/20 text-red-300 text-[10px] font-bold">
                        504
                      </span>
                    )}
                  </div>
                </div>
                <div className="font-mono text-[11px] text-slate-400 truncate">{s.session_id}</div>
                <div className="flex items-center justify-between text-[11px] text-slate-500">
                  <span>{s.event_count} events</span>
                  <span>{(s.duration_ms / 1000).toFixed(1)}s duration</span>
                </div>
              </button>
            );
          })}
        </div>

        <div className="xl:col-span-8 space-y-4">
          {selected && chunks.length > 0 ? (
            <WireframePlayer sessionId={selected.session_id} chunks={chunks} />
          ) : (
            <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-12 text-center text-xs text-slate-400">
              Select a session with recorded wireframe chunks (or click &ldquo;Simulate Traffic
              Burst&rdquo; in the topbar) to watch the binary wireframe replay.
            </div>
          )}

          {selected && (
            <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
                Chronological Analytical Event Timeline ({selected.events.length})
              </div>
              <div className="flex flex-wrap gap-2">
                {selected.events.map(([ts, evName, rkb], idx) => (
                  <div
                    key={idx}
                    className={`px-2.5 py-1 rounded-lg border text-xs font-mono ${
                      evName.includes("504") || evName.includes("Timeout")
                        ? "bg-red-950/40 border-red-800/60 text-red-200"
                        : "bg-[#090d16] border-slate-800 text-slate-300"
                    }`}
                  >
                    {evName}{" "}
                    <span className="text-slate-500 text-[10px]">
                      ({new Date(ts).toISOString().slice(11, 19)} • {rkb}KB)
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
