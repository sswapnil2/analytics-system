import React, { useCallback, useEffect, useState } from "react";
import { CheckCircle2, Upload, Users } from "lucide-react";
import { api } from "../api/client";
import { EmptyState, ErrorNotice, PageHeader, Panel, RanIn, Spinner, btn, field, fieldLabel } from "../components/ui";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";

interface Props {
  projectId: string;
  availableEvents: string[];
  onCatalogChanged: () => void;
}

type Feedback = { kind: "ok" | "error"; text: string } | null;

const FeedbackLine: React.FC<{ feedback: Feedback }> = ({ feedback }) => {
  if (!feedback) return null;
  if (feedback.kind === "error") return <ErrorNotice message={feedback.text} />;
  return (
    <p role="status" className="flex items-center gap-1.5 text-sm text-ok">
      <CheckCircle2 className="h-4 w-4" />
      {feedback.text}
    </p>
  );
};

/** Runs a save action and reports success or failure inline. */
async function withFeedback(
  setFeedback: (f: Feedback) => void,
  setBusy: (b: boolean) => void,
  okText: string,
  action: () => Promise<unknown>,
) {
  setBusy(true);
  setFeedback(null);
  try {
    await action();
    setFeedback({ kind: "ok", text: okText });
  } catch (err) {
    setFeedback({ kind: "error", text: errorMessage(err) });
  } finally {
    setBusy(false);
  }
}

export const CohortsLookupsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  onCatalogChanged,
}) => {
  const [cohortName, setCohortName] = useState<string>("Power quick-commerce buyers");
  const [cohortEvent, setCohortEvent] = useState<string>("Quick_Commerce_Order_Paid");
  const [minCount, setMinCount] = useState<number>(2);
  const [preview, setPreview] = useState<{ count: number; us: number } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [cohorts, setCohorts] = useState<
    Array<{ id: string; name: string; description: string; created_at_ms: number }>
  >([]);

  const [lookupName, setLookupName] = useState<string>("sku_margin_catalog");
  const [joinProp, setJoinProp] = useState<string>("merchant_id");
  const [csvText, setCsvText] = useState<string>(
    "merchant_id,vertical,commission_pct\nm_zepto,Grocery_10m,19.0\nm_swiggy,Food_Instant,23.5\nm_croma,Electronics,11.0",
  );

  const [customPropName, setCustomPropName] = useState<string>("gst_18_pct");
  const [customSql, setCustomSql] = useState<string>("amount * 0.18");

  const [extensions, setExtensions] = useState<{
    custom_events: Array<{ name: string; source_events: string[] }>;
    custom_properties: Array<{ name: string; sql_expr: string }>;
    lookup_tables: Array<{
      id: string;
      name: string;
      join_prop: string;
      row_count: number;
      sample_columns: string[];
    }>;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [cohortFeedback, setCohortFeedback] = useState<Feedback>(null);
  const [lookupFeedback, setLookupFeedback] = useState<Feedback>(null);
  const [propFeedback, setPropFeedback] = useState<Feedback>(null);
  const [savingCohort, setSavingCohort] = useState(false);
  const [uploadingLookup, setUploadingLookup] = useState(false);
  const [savingProp, setSavingProp] = useState(false);

  const loadAll = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const [cList, ext] = await Promise.all([
        api.getCohorts(projectId),
        api.getSchemaExtensions(projectId),
      ]);
      setCohorts(cList);
      setExtensions(ext);
    } catch (err) {
      setLoadError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const runPreview = useCallback(async () => {
    const def = {
      kind: "performed_event",
      event_name: cohortEvent,
      min_count: minCount,
      where_clauses: [],
    };
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const res = await api.previewCohort(projectId, def);
      setPreview({ count: res.user_count, us: res.exec_time_us });
    } catch (err) {
      setPreviewError(errorMessage(err));
    } finally {
      setPreviewLoading(false);
    }
  }, [projectId, cohortEvent, minCount]);

  useEffect(() => {
    void runPreview();
  }, [runPreview]);

  const saveCohort = () =>
    withFeedback(setCohortFeedback, setSavingCohort, `Saved "${cohortName.trim()}".`, async () => {
      if (!cohortName.trim()) throw new Error("Give the cohort a name.");
      await api.createCohort(projectId, {
        name: cohortName.trim(),
        description: `Did ${humanizeEvent(cohortEvent)} at least ${minCount} times`,
        definition: {
          kind: "performed_event",
          event_name: cohortEvent,
          min_count: minCount,
          where_clauses: [],
        },
      });
      await loadAll();
      onCatalogChanged();
    });

  const uploadLookup = () =>
    withFeedback(setLookupFeedback, setUploadingLookup, `Uploaded ${lookupName.trim()}.`, async () => {
      if (!lookupName.trim() || !joinProp.trim()) throw new Error("Add a table name and a join key.");
      await api.uploadLookupTable(projectId, {
        name: lookupName.trim(),
        join_prop: joinProp.trim(),
        csv_text: csvText,
      });
      await loadAll();
      onCatalogChanged();
    });

  const saveCustomProp = () =>
    withFeedback(setPropFeedback, setSavingProp, `Saved ${customPropName.trim()}.`, async () => {
      if (!customPropName.trim() || !customSql.trim()) throw new Error("Add a name and a formula.");
      await api.createCustomProperty(projectId, {
        name: customPropName.trim(),
        sql_expr: customSql.trim(),
      });
      await loadAll();
      onCatalogChanged();
    });

  const eventOptions = [
    "Quick_Commerce_Order_Paid",
    "UPI_Autopay_Mandate_Approved",
    "First_Disbursal_Success",
    "NSDL_Gateway_504_Timeout",
    ...availableEvents,
  ].filter((v, i, a) => a.indexOf(v) === i);

  return (
    <div>
      <PageHeader
        title="Cohorts"
        description="Groups of users defined by what they did. Includes reference data you upload and calculated properties."
      />

      {loadError && (
        <div className="mb-5">
          <ErrorNotice message={loadError} onRetry={() => void loadAll()} />
        </div>
      )}

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <Panel title="Cohorts" meta={preview && !previewLoading ? <RanIn us={preview.us} /> : undefined}>
          <div className="space-y-4">
            <div className="space-y-1">
              <label htmlFor="cohort-name" className={fieldLabel}>
                Cohort name
              </label>
              <input
                id="cohort-name"
                value={cohortName}
                onChange={(e) => setCohortName(e.target.value)}
                className={`${field} w-full`}
              />
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-[1fr_auto]">
              <div className="space-y-1">
                <label htmlFor="cohort-event" className={fieldLabel}>
                  Users who did
                </label>
                <select
                  id="cohort-event"
                  value={cohortEvent}
                  onChange={(e) => setCohortEvent(e.target.value)}
                  className={`${field} w-full`}
                >
                  {eventOptions.map((ev) => (
                    <option key={ev} value={ev}>
                      {humanizeEvent(ev)}
                    </option>
                  ))}
                </select>
              </div>
              <div className="space-y-1">
                <label htmlFor="cohort-min" className={fieldLabel}>
                  At least (times)
                </label>
                <input
                  id="cohort-min"
                  type="number"
                  min={1}
                  max={50}
                  value={minCount}
                  onChange={(e) => setMinCount(Math.max(1, Number(e.target.value) || 1))}
                  className={`${field} tabular w-full sm:w-24`}
                />
              </div>
            </div>

            <div className="flex min-h-[44px] items-center rounded-lg border border-line bg-sunken px-3.5 py-2.5">
              {previewError ? (
                <div className="w-full">
                  <ErrorNotice message={previewError} onRetry={() => void runPreview()} />
                </div>
              ) : previewLoading && !preview ? (
                <Spinner label="Counting users" />
              ) : preview ? (
                <p className={`text-sm text-ink-2 ${previewLoading ? "opacity-60" : ""}`}>
                  <strong className="tabular text-base font-semibold text-ink">
                    {formatCount(preview.count)}
                  </strong>{" "}
                  {preview.count === 1 ? "user matches" : "users match"}
                </p>
              ) : null}
            </div>

            <button type="button" onClick={() => void saveCohort()} disabled={savingCohort} className={`${btn.primary} w-full`}>
              {savingCohort ? "Saving…" : "Save cohort"}
            </button>
            <FeedbackLine feedback={cohortFeedback} />
          </div>

          <div className="mt-5 border-t border-line pt-4">
            <h3 className="mb-2 text-sm font-medium text-ink">
              Saved cohorts <span className="tabular text-muted">({formatCount(cohorts.length)})</span>
            </h3>
            {loading && cohorts.length === 0 ? (
              <Spinner />
            ) : cohorts.length === 0 ? (
              <EmptyState
                icon={Users}
                title="No saved cohorts yet"
                body="Pick an event and a minimum count above, then save the group to reuse it in other reports."
              />
            ) : (
              <ul className="space-y-2">
                {cohorts.map((c) => (
                  <li key={c.id} className="rounded-lg border border-line bg-sunken px-3 py-2">
                    <div className="text-sm font-medium text-ink">{c.name}</div>
                    <div className="text-xs text-muted">{c.description}</div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </Panel>

        <div className="space-y-5">
          <Panel title="Reference data">
            <div className="space-y-3">
              <p className="text-sm text-muted">
                Upload a CSV to add columns like merchant category to your events, matched on a shared key.
              </p>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <div className="space-y-1">
                  <label htmlFor="lookup-name" className={fieldLabel}>
                    Table name
                  </label>
                  <input
                    id="lookup-name"
                    value={lookupName}
                    onChange={(e) => setLookupName(e.target.value)}
                    className={`${field} w-full font-mono`}
                  />
                </div>
                <div className="space-y-1">
                  <label htmlFor="lookup-key" className={fieldLabel}>
                    Match on property
                  </label>
                  <input
                    id="lookup-key"
                    value={joinProp}
                    onChange={(e) => setJoinProp(e.target.value)}
                    placeholder="merchant_id"
                    className={`${field} w-full font-mono`}
                  />
                </div>
              </div>
              <div className="space-y-1">
                <label htmlFor="lookup-csv" className={fieldLabel}>
                  CSV (first row is the header)
                </label>
                <textarea
                  id="lookup-csv"
                  rows={4}
                  value={csvText}
                  onChange={(e) => setCsvText(e.target.value)}
                  className={`${field} w-full font-mono text-xs`}
                />
              </div>
              <button type="button" onClick={() => void uploadLookup()} disabled={uploadingLookup} className={btn.primary}>
                <Upload className="h-3.5 w-3.5" />
                {uploadingLookup ? "Uploading…" : "Upload table"}
              </button>
              <FeedbackLine feedback={lookupFeedback} />

              {extensions && extensions.lookup_tables.length > 0 && (
                <ul className="space-y-1.5 pt-2">
                  {extensions.lookup_tables.map((lt) => (
                    <li key={lt.id} className="rounded-lg border border-line bg-sunken px-3 py-2 text-sm">
                      <div className="font-mono text-ink">{lt.name}</div>
                      <div className="text-xs text-muted">
                        Matched on <code className="font-mono text-ink-2">{lt.join_prop}</code>. Adds{" "}
                        {lt.sample_columns.join(", ")} ({formatCount(lt.row_count)} rows)
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </Panel>

          <Panel title="Calculated properties">
            <div className="space-y-3">
              <p className="text-sm text-muted">
                A formula over event properties, for example <code className="font-mono text-ink-2">amount * 0.18</code>.
                It can be used like any other property.
              </p>
              <div className="space-y-1">
                <label htmlFor="prop-name" className={fieldLabel}>
                  Property name
                </label>
                <input
                  id="prop-name"
                  value={customPropName}
                  onChange={(e) => setCustomPropName(e.target.value)}
                  className={`${field} w-full font-mono`}
                />
              </div>
              <div className="space-y-1">
                <label htmlFor="prop-formula" className={fieldLabel}>
                  Formula
                </label>
                <input
                  id="prop-formula"
                  value={customSql}
                  onChange={(e) => setCustomSql(e.target.value)}
                  placeholder="CASE WHEN amount > 5000 THEN 'Prime' ELSE 'Standard' END"
                  className={`${field} w-full font-mono`}
                />
              </div>
              <button type="button" onClick={() => void saveCustomProp()} disabled={savingProp} className={btn.primary}>
                {savingProp ? "Saving…" : "Save property"}
              </button>
              <FeedbackLine feedback={propFeedback} />

              {extensions && extensions.custom_properties.length > 0 && (
                <ul className="space-y-1 pt-1">
                  {extensions.custom_properties.map((cp) => (
                    <li key={cp.name} className="rounded-lg border border-line bg-sunken px-3 py-1.5 text-xs">
                      <span className="font-mono text-ink">{cp.name}</span>
                      <span className="text-muted"> = </span>
                      <span className="font-mono text-ink-2">{cp.sql_expr}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
};
