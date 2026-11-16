import React, { useCallback, useEffect, useState } from "react";
import { Database, Sparkles, Upload, Users } from "lucide-react";
import { api } from "../api/client";

interface Props {
  projectId: string;
  availableEvents: string[];
  onCatalogChanged: () => void;
}

export const CohortsLookupsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  onCatalogChanged,
}) => {
  const [cohortName, setCohortName] = useState<string>("Power Quick-Commerce Buyers");
  const [cohortEvent, setCohortEvent] = useState<string>("Quick_Commerce_Order_Paid");
  const [minCount, setMinCount] = useState<number>(2);
  const [preview, setPreview] = useState<{ count: number; us: number } | null>(null);
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

  const loadAll = useCallback(async () => {
    const [cList, ext] = await Promise.all([
      api.getCohorts(projectId),
      api.getSchemaExtensions(projectId),
    ]);
    setCohorts(cList);
    setExtensions(ext);
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
    const res = await api.previewCohort(projectId, def);
    setPreview({ count: res.user_count, us: res.exec_time_us });
  }, [projectId, cohortEvent, minCount]);

  useEffect(() => {
    void runPreview();
  }, [runPreview]);

  const saveCohort = async () => {
    const def = {
      kind: "performed_event",
      event_name: cohortEvent,
      min_count: minCount,
      where_clauses: [],
    };
    await api.createCohort(projectId, {
      name: cohortName,
      description: `Performed ${cohortEvent} >= ${minCount} times`,
      definition: def,
    });
    await loadAll();
    onCatalogChanged();
  };

  const uploadLookup = async () => {
    await api.uploadLookupTable(projectId, {
      name: lookupName,
      join_prop: joinProp,
      csv_text: csvText,
    });
    await loadAll();
    onCatalogChanged();
  };

  const saveCustomProp = async () => {
    await api.createCustomProperty(projectId, {
      name: customPropName,
      sql_expr: customSql,
    });
    await loadAll();
    onCatalogChanged();
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
      {/* Behavioral Cohort Builder */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-4">
        <div className="flex items-center gap-2 text-sm font-semibold text-white">
          <Users className="w-4 h-4 text-blue-400" />
          Behavioral Cohort Builder (Exact RoaringBitmap)
        </div>

        <div className="space-y-3 text-xs">
          <div>
            <label className="text-slate-400 block mb-1">Cohort Name</label>
            <input
              value={cohortName}
              onChange={(e) => setCohortName(e.target.value)}
              className="w-full bg-[#090d16] border border-slate-800 rounded-lg px-3 py-2 text-slate-100"
            />
          </div>

          <div className="flex items-center gap-2 bg-[#090d16] border border-slate-800 rounded-lg p-3">
            <span className="text-slate-400">Users who performed</span>
            <select
              value={cohortEvent}
              onChange={(e) => setCohortEvent(e.target.value)}
              className="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
            >
              {[
                "Quick_Commerce_Order_Paid",
                "UPI_Autopay_Mandate_Approved",
                "First_Disbursal_Success",
                "NSDL_Gateway_504_Timeout",
                ...availableEvents,
              ]
                .filter((v, i, a) => a.indexOf(v) === i)
                .map((ev) => (
                  <option key={ev} value={ev}>
                    {ev}
                  </option>
                ))}
            </select>
            <span className="text-slate-400">≥</span>
            <input
              type="number"
              min={1}
              max={50}
              value={minCount}
              onChange={(e) => setMinCount(Math.max(1, Number(e.target.value)))}
              className="w-16 bg-slate-900 border border-slate-700 rounded px-2 py-1 font-mono text-slate-100"
            />
            <span className="text-slate-400">times</span>
          </div>

          {preview && (
            <div className="flex items-center justify-between bg-blue-950/40 border border-blue-800/50 rounded-lg px-3.5 py-2.5">
              <div>
                <span className="text-slate-300">Live Matching Users: </span>
                <strong className="font-mono text-base text-blue-300">
                  {preview.count.toLocaleString()}
                </strong>
              </div>
              <span className="font-mono text-[11px] text-emerald-400">
                {preview.us} µs RoaringBitmap scan
              </span>
            </div>
          )}

          <button
            onClick={() => void saveCohort()}
            className="w-full py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium"
          >
            Save Behavioral Cohort
          </button>
        </div>

        <div className="border-t border-slate-800 pt-3 space-y-2">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Saved Cohorts ({cohorts.length})
          </div>
          {cohorts.map((c) => (
            <div
              key={c.id}
              className="bg-[#090d16] border border-slate-800/70 rounded-lg px-3 py-2 text-xs flex items-center justify-between"
            >
              <div>
                <div className="font-medium text-slate-100">{c.name}</div>
                <div className="text-slate-400 text-[11px]">{c.description}</div>
              </div>
              <span className="font-mono text-[11px] text-slate-500">{c.id}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Lookup Tables & DataFusion Custom Properties */}
      <div className="space-y-5">
        <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-3 text-xs">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <Database className="w-4 h-4 text-emerald-400" />
            CSV / Dictionary Lookup Table Joiner
          </div>

          <div className="grid grid-cols-2 gap-2">
            <input
              value={lookupName}
              onChange={(e) => setLookupName(e.target.value)}
              placeholder="Catalog Name"
              className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-slate-100 font-mono"
            />
            <input
              value={joinProp}
              onChange={(e) => setJoinProp(e.target.value)}
              placeholder="Join Property Key (e.g. merchant_id)"
              className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-slate-100 font-mono"
            />
          </div>

          <textarea
            rows={4}
            value={csvText}
            onChange={(e) => setCsvText(e.target.value)}
            className="w-full bg-[#090d16] border border-slate-800 rounded-lg p-2.5 font-mono text-[11px] text-slate-200"
          />

          <button
            onClick={() => void uploadLookup()}
            className="flex items-center justify-center gap-1.5 w-full py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium"
          >
            <Upload className="w-3.5 h-3.5" /> Upload &amp; Register Lookup Table
          </button>

          {extensions && extensions.lookup_tables.length > 0 && (
            <div className="pt-2 space-y-1.5">
              {extensions.lookup_tables.map((lt) => (
                <div
                  key={lt.id}
                  className="flex items-center justify-between bg-[#090d16] border border-slate-800 rounded px-3 py-1.5"
                >
                  <span className="font-mono text-emerald-300">{lt.name}</span>
                  <span className="text-slate-400">
                    join on <code className="text-slate-200">{lt.join_prop}</code> →{" "}
                    {lt.sample_columns.join(", ")} ({lt.row_count} rows)
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-3 text-xs">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <Sparkles className="w-4 h-4 text-purple-400" />
            DataFusion Virtual Custom Property Expression
          </div>

          <input
            value={customPropName}
            onChange={(e) => setCustomPropName(e.target.value)}
            placeholder="Virtual Property Name"
            className="w-full bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 font-mono text-slate-100"
          />
          <input
            value={customSql}
            onChange={(e) => setCustomSql(e.target.value)}
            placeholder="SQL Expr e.g. CASE WHEN amount > 5000 THEN 'Prime' ELSE 'Standard' END"
            className="w-full bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 font-mono text-purple-200"
          />
          <button
            onClick={() => void saveCustomProp()}
            className="w-full py-2 rounded-lg bg-purple-600 hover:bg-purple-500 text-white font-medium"
          >
            Save Virtual Property
          </button>

          {extensions && extensions.custom_properties.length > 0 && (
            <div className="pt-1 space-y-1">
              {extensions.custom_properties.map((cp) => (
                <div
                  key={cp.name}
                  className="bg-[#090d16] border border-slate-800 rounded px-3 py-1.5 font-mono text-[11px]"
                >
                  <span className="text-purple-300">{cp.name}</span> ={" "}
                  <span className="text-slate-300">{cp.sql_expr}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
