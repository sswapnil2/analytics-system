import React, { useState } from "react";
import { CheckCircle2, Download, Upload } from "lucide-react";
import { api, BatchImportResponse } from "../api/client";
import {
  ErrorNotice,
  Panel,
  Spinner,
  btn,
  field,
  fieldLabel,
} from "../components/ui";
import { downloadTextFile, errorMessage, formatCount } from "../lib/format";

type ImportContentType = "application/x-ndjson" | "application/json" | "text/csv";

const PLACEHOLDERS: Record<ImportContentType, string> = {
  "application/x-ndjson":
    '{"event":"Subscription_Renewed","distinct_id":"usr_demo_01","properties":{"plan":"pro","amount":999}}\n{"event":"Signed_Up","distinct_id":"usr_demo_02","properties":{"plan":"free"}}',
  "application/json":
    '[{"event":"Subscription_Renewed","distinct_id":"usr_demo_01","properties":{"plan":"pro","amount":999}}]',
  "text/csv":
    "event_name,distinct_id,session_id,event_ts_ms,plan,amount\nSubscription_Renewed,usr_demo_01,sess_01,1731715200000,pro,999",
};

export const BatchImportExportPanel: React.FC<{ projectId: string }> = ({
  projectId,
}) => {
  const [contentType, setContentType] =
    useState<ImportContentType>("application/x-ndjson");
  const [payload, setPayload] = useState("");
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<BatchImportResponse | null>(
    null,
  );

  const [exportingFormat, setExportingFormat] = useState<"ndjson" | "csv" | null>(
    null,
  );
  const [exportNotice, setExportNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleImport = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!payload.trim()) return;
    setImporting(true);
    setError(null);
    setImportResult(null);
    try {
      const res = await api.importEvents(projectId, payload, contentType);
      setImportResult(res);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setImporting(false);
    }
  };

  const handleExport = async (format: "ndjson" | "csv") => {
    setExportingFormat(format);
    setError(null);
    setExportNotice(null);
    try {
      const text = await api.exportRawEvents(projectId, format, 2000);
      const mime =
        format === "csv"
          ? "text/csv;charset=utf-8"
          : "application/x-ndjson;charset=utf-8";
      const filename = `${projectId}-events.${format}`;
      downloadTextFile(filename, text, mime);
      const rows = text ? text.trim().split("\n").length : 0;
      setExportNotice(`Downloaded ${filename} (${formatCount(rows)} lines).`);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setExportingFormat(null);
    }
  };

  return (
    <Panel
      title="Batch Import & Warehouse Export"
      meta="JSON, NDJSON, CSV"
      actions={
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            disabled={exportingFormat !== null}
            onClick={() => handleExport("ndjson")}
            className={btn.secondary}
          >
            {exportingFormat === "ndjson" ? (
              <Spinner label="Exporting" />
            ) : (
              <>
                <Download className="h-3.5 w-3.5" />
                Export NDJSON
              </>
            )}
          </button>
          <button
            type="button"
            disabled={exportingFormat !== null}
            onClick={() => handleExport("csv")}
            className={btn.secondary}
          >
            {exportingFormat === "csv" ? (
              <Spinner label="Exporting" />
            ) : (
              <>
                <Download className="h-3.5 w-3.5" />
                Export CSV
              </>
            )}
          </button>
        </div>
      }
    >
      <div className="space-y-4">
        <p className="text-xs text-muted">
          Backfill historical events from JSON, NDJSON, or CSV, and export raw event batches for external warehouses.
        </p>
        {error && <ErrorNotice message={error} />}

        {exportNotice && (
          <div
            className="flex items-center gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-xs text-ink"
            role="status"
          >
            <CheckCircle2 className="h-4 w-4 shrink-0 text-ok" />
            <span>{exportNotice}</span>
          </div>
        )}

        <form onSubmit={handleImport} className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <label htmlFor="batch-import-format" className={fieldLabel}>
              Payload format
            </label>
            <select
              id="batch-import-format"
              value={contentType}
              onChange={(e) => {
                setContentType(e.target.value as ImportContentType);
                setImportResult(null);
              }}
              className={field}
            >
              <option value="application/x-ndjson">NDJSON (application/x-ndjson)</option>
              <option value="application/json">JSON Array / Batch (application/json)</option>
              <option value="text/csv">CSV (text/csv)</option>
            </select>
          </div>

          <textarea
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            rows={5}
            placeholder={PLACEHOLDERS[contentType]}
            className={`${field} w-full font-mono text-xs`}
            aria-label="Batch import events payload"
          />

          <div className="flex flex-wrap items-center justify-end gap-3">
            <button
              type="submit"
              disabled={importing || !payload.trim()}
              className={btn.primary}
            >
              {importing ? (
                <Spinner label="Importing" />
              ) : (
                <>
                  <Upload className="h-3.5 w-3.5" />
                  Import Events
                </>
              )}
            </button>
          </div>
        </form>

        {importResult && (
          <div
            className="flex items-center gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-xs text-ink"
            role="status"
          >
            <CheckCircle2 className="h-4 w-4 shrink-0 text-ok" />
            <span>
              Batch {importResult.status}:{" "}
              <strong>{formatCount(importResult.accepted)}</strong> accepted,{" "}
              {formatCount(importResult.deduped)} deduped,{" "}
              {formatCount(importResult.dropped)} dropped.
            </span>
          </div>
        )}
      </div>
    </Panel>
  );
};
