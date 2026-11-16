import React, { useCallback, useEffect, useState } from "react";
import { AlertTriangle, FlaskConical, Plus, RefreshCw, ToggleLeft } from "lucide-react";
import { api, Experiment, ExperimentResults, FeatureFlag } from "../api/client";
import { EmptyState, ErrorNotice, PageHeader, Panel, RanIn, Spinner, btn, field, fieldLabel } from "../components/ui";
import { errorMessage, formatCount, formatPValue, formatPercent, formatProbability } from "../lib/format";

interface Props {
  projectId: string;
  refreshToken: number;
}

const FLAG_KEY_PATTERN = /^[a-z0-9_]+$/;

/** Lift arrives in percentage points (12.3 = +12.3%). */
function formatLift(pct: number): string {
  const text = formatPercent(pct / 100);
  return pct > 0 ? `+${text}` : text;
}

function validateFlagKey(key: string): string | null {
  if (!key.trim()) return "Enter a flag key.";
  if (!FLAG_KEY_PATTERN.test(key.trim())) return "Use lowercase letters, numbers and underscores only.";
  return null;
}

export const ExperimentsFlagsWorkspace: React.FC<Props> = ({ projectId, refreshToken }) => {
  const [flags, setFlags] = useState<FeatureFlag[]>([]);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [selectedExpId, setSelectedExpId] = useState<string>("");
  const [expResults, setExpResults] = useState<ExperimentResults | null>(null);
  const [newFlagKey, setNewFlagKey] = useState<string>("");
  const [newFlagDesc, setNewFlagDesc] = useState<string>("");
  const [keyError, setKeyError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [loaded, setLoaded] = useState<boolean>(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [flagError, setFlagError] = useState<string | null>(null);
  const [creating, setCreating] = useState<boolean>(false);

  const loadAll = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
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
    } catch (err) {
      setLoadError(errorMessage(err));
    } finally {
      setLoading(false);
      setLoaded(true);
    }
  }, [projectId, selectedExpId]);

  useEffect(() => {
    void loadAll();
  }, [loadAll, refreshToken]);

  const patchFlag = async (flag: FeatureFlag, body: { enabled?: boolean; rollout_pct?: number }) => {
    setFlagError(null);
    try {
      const updated = await api.patchFlag(projectId, flag.id, body);
      setFlags((prev) => prev.map((f) => (f.id === flag.id ? updated : f)));
    } catch (err) {
      setFlagError(`Couldn't update ${flag.key}: ${errorMessage(err)}`);
    }
  };

  const createFlag = async () => {
    const err = validateFlagKey(newFlagKey);
    setKeyError(err);
    if (err) return;
    setCreating(true);
    setFlagError(null);
    try {
      await api.createFlag(projectId, {
        key: newFlagKey.trim(),
        description: newFlagDesc.trim(),
        rollout_pct: 50,
      });
      setNewFlagKey("");
      setNewFlagDesc("");
      await loadAll();
    } catch (e) {
      setFlagError(`Couldn't create flag: ${errorMessage(e)}`);
    } finally {
      setCreating(false);
    }
  };

  const selectedExp = experiments.find((e) => e.id === selectedExpId);
  const selectedFlag = selectedExp
    ? flags.find((f) => f.id === selectedExp.flag_id || f.key === selectedExp.flag_key)
    : undefined;
  const rolloutBreaksTest =
    selectedExp?.status.toLowerCase() === "running" && selectedFlag !== undefined && selectedFlag.rollout_pct >= 100;

  return (
    <div className="space-y-5">
      <PageHeader
        title="Experiments"
        description="Feature flags and the A/B tests that measure them."
        actions={
          <button type="button" onClick={() => void loadAll()} disabled={loading} className={btn.primary}>
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
            Update results
          </button>
        }
      />

      {loadError && <ErrorNotice message={loadError} onRetry={() => void loadAll()} />}

      <Panel
        title="Results"
        meta={expResults ? <RanIn us={expResults.exec_time_us} /> : undefined}
        actions={
          experiments.length > 0 ? (
            <>
              <label htmlFor="experiment-select" className="sr-only">
                Experiment
              </label>
              <select
                id="experiment-select"
                value={selectedExpId}
                onChange={(e) => setSelectedExpId(e.target.value)}
                className={field}
              >
                {experiments.map((ex) => (
                  <option key={ex.id} value={ex.id}>
                    {ex.name} ({ex.status})
                  </option>
                ))}
              </select>
            </>
          ) : undefined
        }
      >
        {!loaded ? (
          <Spinner />
        ) : experiments.length === 0 ? (
          <EmptyState
            icon={FlaskConical}
            title="No experiments yet"
            body="Experiments appear here once one is attached to a feature flag below."
          />
        ) : !expResults ? (
          loading ? <Spinner /> : <EmptyState title="No results for this experiment yet" />
        ) : (
          <div className="space-y-4">
            {rolloutBreaksTest && selectedFlag && (
              <div
                role="alert"
                className="flex items-start gap-3 rounded-lg border border-warn/40 bg-warn-soft px-3.5 py-3 text-sm text-ink"
              >
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warn" />
                <p>
                  This experiment is running, but <code className="font-mono">{selectedFlag.key}</code> is rolled out
                  to 100% of users. Everyone gets the same variant, so the test is no longer randomized. Lower the
                  rollout or stop the experiment.
                </p>
              </div>
            )}

            <p className="text-sm text-ink-2">
              <span className="font-medium text-ink">Hypothesis:</span> {expResults.hypothesis}
            </p>

            <div className="overflow-x-auto">
              <table className="w-full min-w-[820px] text-sm">
                <thead>
                  <tr className="border-b border-line text-xs text-muted">
                    <th className="py-2 text-left font-medium">Variant</th>
                    <th className="py-2 text-right font-medium">Users</th>
                    <th className="py-2 text-right font-medium">Conversions</th>
                    <th className="py-2 text-right font-medium">Conversion rate</th>
                    <th className="py-2 text-right font-medium">Lift</th>
                    <th className="px-4 py-2 text-center font-medium">95% range of lift</th>
                    <th className="py-2 text-right font-medium">p-value</th>
                    <th className="py-2 text-right font-medium">Chance to beat control</th>
                    <th className="py-2 pl-4 text-left font-medium">Result</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {expResults.variants.map((v) => {
                    const ciMin = Math.max(-30, Math.min(30, v.ci95_low_pct));
                    const ciMax = Math.max(-30, Math.min(30, v.ci95_high_pct));
                    const leftPct = ((ciMin + 30) / 60) * 100;
                    const widthPct = Math.max(3, ((ciMax - ciMin) / 60) * 100);

                    return (
                      <tr key={v.variant_key}>
                        <td className="py-3 font-medium text-ink">
                          <span className="flex items-center gap-1.5">
                            {v.variant_key}
                            {v.is_control && (
                              <span className="rounded bg-hover px-1.5 py-0.5 text-xs text-muted">Control</span>
                            )}
                          </span>
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                        <td className="tabular py-3 text-right text-ink-2">{formatCount(v.conversions)}</td>
                        <td className="tabular py-3 text-right font-medium text-ink">
                          {formatPercent(v.conversion_rate, 2)}
                        </td>
                        <td
                          className={`tabular py-3 text-right font-semibold ${
                            v.is_control ? "text-faint" : v.relative_lift_pct >= 0 ? "text-ok" : "text-danger"
                          }`}
                        >
                          {v.is_control ? "–" : formatLift(v.relative_lift_pct)}
                        </td>
                        <td className="px-4 py-3">
                          {v.is_control ? (
                            <div className="text-center text-faint">Baseline</div>
                          ) : (
                            <div className="space-y-1">
                              <div className="relative h-2.5 overflow-hidden rounded-full border border-line bg-sunken">
                                <div className="absolute bottom-0 left-1/2 top-0 w-px bg-line-strong" />
                                <div
                                  style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
                                  className={`absolute bottom-0.5 top-0.5 rounded-full ${
                                    v.ci95_low_pct > 0 ? "bg-ok" : "bg-warn"
                                  }`}
                                />
                              </div>
                              <div className="tabular text-center text-xs text-muted">
                                {formatLift(v.ci95_low_pct)} to {formatLift(v.ci95_high_pct)}
                              </div>
                            </div>
                          )}
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">
                          {v.is_control ? "–" : formatPValue(v.p_value)}
                        </td>
                        <td className="tabular py-3 text-right font-semibold text-ink">
                          {v.is_control ? "–" : formatProbability(v.bayesian_prob_to_beat_control)}
                        </td>
                        <td className="py-3 pl-4">
                          {v.is_control ? (
                            <span className="text-faint">–</span>
                          ) : v.statistically_significant ? (
                            <span className="inline-flex items-center gap-1.5 text-ok">
                              <span className="h-2 w-2 rounded-full bg-ok" aria-hidden />
                              Significant
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 text-muted">
                              <span className="h-2 w-2 rounded-full bg-line-strong" aria-hidden />
                              Not yet significant
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {expResults.guardrails.length > 0 && (
              <div className="border-t border-line pt-4">
                <h3 className="mb-2 text-sm font-medium text-ink">Guardrails</h3>
                <div className="space-y-3">
                  {expResults.guardrails.map((g) => (
                    <div key={g.metric_name}>
                      <p className="mb-1.5 text-xs text-muted">{g.metric_name}</p>
                      <div className="flex flex-wrap gap-2">
                        {g.variants.map((gv) => (
                          <div
                            key={gv.variant_key}
                            className="rounded-lg border border-line bg-sunken px-3 py-2 text-sm"
                          >
                            <span className="text-ink-2">{gv.variant_key}: </span>
                            <span className="tabular text-ink">
                              {formatPercent(gv.conversion_rate, 2)} ({formatCount(gv.conversions)} of{" "}
                              {formatCount(gv.sample_size)})
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <p className="text-xs text-faint">Bayesian and frequentist statistics, 20,000 simulations</p>
          </div>
        )}
      </Panel>

      <Panel title="Feature flags">
        <div className="space-y-4">
          {flagError && <ErrorNotice message={flagError} />}

          {!loaded ? (
            <Spinner />
          ) : flags.length === 0 ? (
            <EmptyState
              icon={ToggleLeft}
              title="No feature flags yet"
              body="Create a flag below to roll a feature out to a share of users."
            />
          ) : (
            <ul className="space-y-3">
              {flags.map((f) => {
                const pct = Math.round(f.rollout_pct);
                const sliderId = `rollout-${f.id}`;
                return (
                  <li
                    key={f.id}
                    className="flex flex-wrap items-center justify-between gap-4 rounded-lg border border-line bg-sunken p-4"
                  >
                    <div className="min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-sm font-semibold text-ink">{f.key}</span>
                        <button
                          type="button"
                          role="switch"
                          aria-checked={f.enabled}
                          aria-label={`${f.key} enabled`}
                          onClick={() => void patchFlag(f, { enabled: !f.enabled })}
                          className={`rounded px-2 py-0.5 text-xs font-medium ${
                            f.enabled ? "border border-ok/40 bg-ok/15 text-ok" : "border border-line bg-hover text-muted"
                          }`}
                        >
                          {f.enabled ? "On" : "Off"}
                        </button>
                      </div>
                      {f.description && <div className="text-sm text-muted">{f.description}</div>}
                    </div>

                    <div className="flex w-full items-center gap-3 sm:w-auto">
                      <label htmlFor={sliderId} className={fieldLabel}>
                        Rollout
                      </label>
                      <input
                        id={sliderId}
                        type="range"
                        min={0}
                        max={100}
                        value={pct}
                        aria-valuetext={`${pct}% of users`}
                        onChange={(e) => void patchFlag(f, { rollout_pct: Number(e.target.value) })}
                        className="w-full accent-brand sm:w-40"
                      />
                      <span className="tabular w-20 text-right text-sm font-medium text-ink">{pct}% of users</span>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}

          <form
            noValidate
            onSubmit={(e) => {
              e.preventDefault();
              void createFlag();
            }}
            className="grid grid-cols-1 items-start gap-3 border-t border-line pt-4 sm:grid-cols-[minmax(0,14rem)_1fr_auto]"
          >
            <div className="space-y-1">
              <label htmlFor="new-flag-key" className={fieldLabel}>
                Flag key
              </label>
              <input
                id="new-flag-key"
                value={newFlagKey}
                onChange={(e) => {
                  setNewFlagKey(e.target.value);
                  if (keyError) setKeyError(validateFlagKey(e.target.value));
                }}
                placeholder="new_checkout_flow"
                aria-invalid={keyError ? true : undefined}
                aria-describedby={keyError ? "new-flag-key-error" : undefined}
                className={`${field} w-full font-mono ${keyError ? "border-danger" : ""}`}
              />
              {keyError && (
                <p id="new-flag-key-error" className="text-xs text-danger">
                  {keyError}
                </p>
              )}
            </div>
            <div className="space-y-1">
              <label htmlFor="new-flag-desc" className={fieldLabel}>
                Description
              </label>
              <input
                id="new-flag-desc"
                value={newFlagDesc}
                onChange={(e) => setNewFlagDesc(e.target.value)}
                placeholder="What this flag controls"
                className={`${field} w-full`}
              />
            </div>
            <button type="submit" disabled={creating} className={`${btn.secondary} sm:mt-5`}>
              <Plus className="h-3.5 w-3.5" />
              {creating ? "Creating…" : "Create flag"}
            </button>
          </form>
        </div>
      </Panel>
    </div>
  );
};
