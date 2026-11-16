import React, { useCallback, useEffect, useState } from "react";
import { CheckCircle2, FlaskConical, Plus, RefreshCw, Sliders } from "lucide-react";
import { api, Experiment, ExperimentResults, FeatureFlag } from "../api/client";

interface Props {
  projectId: string;
  refreshToken: number;
}

export const ExperimentsFlagsWorkspace: React.FC<Props> = ({ projectId, refreshToken }) => {
  const [flags, setFlags] = useState<FeatureFlag[]>([]);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [selectedExpId, setSelectedExpId] = useState<string>("");
  const [expResults, setExpResults] = useState<ExperimentResults | null>(null);
  const [newFlagKey, setNewFlagKey] = useState<string>("");
  const [newFlagDesc, setNewFlagDesc] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const [fList, eList] = await Promise.all([
        api.getFlags(projectId),
        api.getExperiments(projectId),
      ]);
      setFlags(fList);
      setExperiments(eList);
      const targetExp = selectedExpId || eList[0]?.id || "";
      if (targetExp) {
        setSelectedExpId(targetExp);
        const res = await api.getExperimentResults(projectId, targetExp);
        setExpResults(res);
      }
    } finally {
      setLoading(false);
    }
  }, [projectId, selectedExpId]);

  useEffect(() => {
    void loadAll();
  }, [loadAll, refreshToken]);

  const updateRollout = async (flag: FeatureFlag, rolloutPct: number) => {
    const updated = await api.patchFlag(projectId, flag.id, { rollout_pct: rolloutPct });
    setFlags((prev) => prev.map((f) => (f.id === flag.id ? updated : f)));
  };

  const toggleFlag = async (flag: FeatureFlag) => {
    const updated = await api.patchFlag(projectId, flag.id, { enabled: !flag.enabled });
    setFlags((prev) => prev.map((f) => (f.id === flag.id ? updated : f)));
  };

  const createFlag = async () => {
    if (!newFlagKey.trim()) return;
    await api.createFlag(projectId, {
      key: newFlagKey.trim(),
      description: newFlagDesc.trim() || "Edge evaluated feature flag",
      rollout_pct: 50,
    });
    setNewFlagKey("");
    setNewFlagDesc("");
    await loadAll();
  };

  return (
    <div className="space-y-5">
      {/* Phase 2 Statistical Experimentation Lift Engine */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <FlaskConical className="w-4 h-4 text-purple-400" />
            <span className="text-sm font-semibold text-white">
              Experimentation Engine (Frequentist Z-Test &amp; 20,000-Draw Bayesian Monte Carlo)
            </span>
          </div>

          <div className="flex items-center gap-2 text-xs">
            <select
              value={selectedExpId}
              onChange={(e) => setSelectedExpId(e.target.value)}
              className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-slate-100"
            >
              {experiments.map((ex) => (
                <option key={ex.id} value={ex.id}>
                  {ex.name} ({ex.status})
                </option>
              ))}
            </select>
            <button
              onClick={() => void loadAll()}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
              Recompute Lift
            </button>
          </div>
        </div>

        {expResults && (
          <>
            <div className="bg-[#090d16] border border-slate-800/80 rounded-lg p-3 text-xs flex flex-wrap items-center justify-between gap-2">
              <div className="text-slate-300">
                <strong className="text-slate-100">Hypothesis:</strong> {expResults.hypothesis}
              </div>
              <span className="font-mono text-emerald-400">
                Computed in {(expResults.exec_time_us / 1000).toFixed(2)} ms
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-800 text-slate-400">
                    <th className="py-2 text-left">Variant</th>
                    <th className="py-2 text-right">Exposed Users (n)</th>
                    <th className="py-2 text-right">Conversions</th>
                    <th className="py-2 text-right">Conv Rate</th>
                    <th className="py-2 text-right">Relative Lift</th>
                    <th className="py-2 px-4 text-center">95% Confidence Interval</th>
                    <th className="py-2 text-right">Z-Test p-value</th>
                    <th className="py-2 text-right">Bayesian P(Beat Control)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {expResults.variants.map((v) => {
                    const probPct = v.bayesian_prob_to_beat_control * 100;
                    const ciMin = Math.max(-30, Math.min(30, v.ci95_low_pct));
                    const ciMax = Math.max(-30, Math.min(30, v.ci95_high_pct));
                    const leftPct = ((ciMin + 30) / 60) * 100;
                    const widthPct = Math.max(3, ((ciMax - ciMin) / 60) * 100);

                    return (
                      <tr key={v.variant_key} className="hover:bg-slate-900/40">
                        <td className="py-3 font-mono font-semibold text-slate-100 flex items-center gap-1.5">
                          {v.variant_key}
                          {v.is_control && (
                            <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">
                              CONTROL
                            </span>
                          )}
                          {v.statistically_significant && (
                            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                          )}
                        </td>
                        <td className="py-3 text-right font-mono text-slate-300">
                          {v.sample_size.toLocaleString()}
                        </td>
                        <td className="py-3 text-right font-mono text-slate-300">
                          {v.conversions.toLocaleString()}
                        </td>
                        <td className="py-3 text-right font-mono font-semibold text-white">
                          {(v.conversion_rate * 100).toFixed(2)}%
                        </td>
                        <td
                          className={`py-3 text-right font-mono font-bold ${
                            v.is_control
                              ? "text-slate-500"
                              : v.relative_lift_pct >= 0
                                ? "text-emerald-400"
                                : "text-red-400"
                          }`}
                        >
                          {v.is_control
                            ? "—"
                            : `${v.relative_lift_pct >= 0 ? "+" : ""}${v.relative_lift_pct.toFixed(2)}%`}
                        </td>
                        <td className="py-3 px-4">
                          {v.is_control ? (
                            <div className="text-center text-slate-600 font-mono">baseline</div>
                          ) : (
                            <div className="space-y-1">
                              <div className="relative h-2.5 bg-slate-900 rounded-full overflow-hidden border border-slate-800">
                                <div className="absolute left-1/2 top-0 bottom-0 w-px bg-slate-600" />
                                <div
                                  style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
                                  className={`absolute top-0.5 bottom-0.5 rounded-full ${
                                    v.ci95_low_pct > 0 ? "bg-emerald-500" : "bg-amber-500"
                                  }`}
                                />
                              </div>
                              <div className="text-[10px] font-mono text-center text-slate-400">
                                [{v.ci95_low_pct.toFixed(1)}%, +{v.ci95_high_pct.toFixed(1)}%]
                              </div>
                            </div>
                          )}
                        </td>
                        <td className="py-3 text-right font-mono text-slate-300">
                          {v.is_control ? "—" : v.p_value.toExponential(2)}
                        </td>
                        <td className="py-3 text-right font-mono font-bold text-purple-300">
                          {v.is_control ? "50.0%" : `${probPct.toFixed(1)}%`}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {expResults.guardrails.length > 0 && (
              <div className="border-t border-slate-800 pt-3">
                <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-400 mb-2">
                  Guardrail Metric Health ({expResults.guardrails[0].metric_name})
                </div>
                <div className="flex flex-wrap gap-4 text-xs">
                  {expResults.guardrails[0].variants.map((gv) => (
                    <div
                      key={gv.variant_key}
                      className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-2 font-mono"
                    >
                      <span className="text-slate-300">{gv.variant_key}: </span>
                      <span className="text-slate-100">
                        {(gv.conversion_rate * 100).toFixed(2)}% ({gv.conversions}/{gv.sample_size})
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* Edge Feature Flags & Deterministic xxh3_64 Rollout Sliders */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <Sliders className="w-4 h-4 text-blue-400" />
            Edge Feature Flags (ETag Cached • Deterministic xxh3_64 Rollout)
          </div>
        </div>

        <div className="space-y-3">
          {flags.map((f) => (
            <div
              key={f.id}
              className="bg-[#090d16] border border-slate-800 rounded-xl p-4 flex flex-wrap items-center justify-between gap-4 text-xs"
            >
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <span className="font-mono font-bold text-blue-300 text-sm">{f.key}</span>
                  <button
                    onClick={() => void toggleFlag(f)}
                    className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                      f.enabled
                        ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                        : "bg-slate-800 text-slate-400"
                    }`}
                  >
                    {f.enabled ? "ENABLED" : "DISABLED"}
                  </button>
                </div>
                <div className="text-slate-400">{f.description}</div>
              </div>

              <div className="flex items-center gap-3">
                <span className="text-slate-400">Rollout:</span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  value={Math.round(f.rollout_pct)}
                  onChange={(e) => void updateRollout(f, Number(e.target.value))}
                  className="w-36 accent-blue-500"
                />
                <span className="font-mono font-semibold text-white w-12 text-right">
                  {Math.round(f.rollout_pct)}%
                </span>
              </div>
            </div>
          ))}
        </div>

        <div className="flex flex-wrap items-center gap-2 pt-2 border-t border-slate-800 text-xs">
          <input
            value={newFlagKey}
            onChange={(e) => setNewFlagKey(e.target.value)}
            placeholder="new_flag_key"
            className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 font-mono text-slate-100"
          />
          <input
            value={newFlagDesc}
            onChange={(e) => setNewFlagDesc(e.target.value)}
            placeholder="Description..."
            className="flex-1 bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-slate-200"
          />
          <button
            onClick={() => void createFlag()}
            className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-white font-medium"
          >
            <Plus className="w-3.5 h-3.5" /> Create Flag
          </button>
        </div>
      </div>
    </div>
  );
};
