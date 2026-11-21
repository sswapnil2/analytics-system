import React, { useCallback, useEffect, useState } from "react";
import {
  CheckCircle2,
  Database,
  Download,
  KeyRound,
  Lock,
  Sparkles,
  Trash2,
} from "lucide-react";
import {
  api,
  ByokCredentialItem,
  CopilotResponse,
  EncryptionStatusResponse,
  EnterpriseAuditRecord,
} from "../api/client";
import {
  ErrorNotice,
  PageHeader,
  Panel,
  Spinner,
  btn,
  field,
  fieldLabel,
} from "../components/ui";
import {
  errorMessage,
  formatCount,
  formatTimestamp,
} from "../lib/format";

type CategoryMeta = {
  category: "llm" | "storage" | "webhook";
  title: string;
  subtitle: string;
  providers: { value: string; label: string }[];
  placeholder: string;
};

const CATEGORIES: CategoryMeta[] = [
  {
    category: "llm",
    title: "AI provider key",
    subtitle: "Powers AI Copilot using your own OpenAI, Anthropic, or Gemini API key.",
    providers: [
      { value: "openai", label: "OpenAI (gpt-4o-mini)" },
      { value: "anthropic", label: "Anthropic (claude-sonnet-4-5)" },
      { value: "gemini", label: "Google Gemini (gemini-2.5-flash)" },
    ],
    placeholder: "sk-proj-... or AIza...",
  },
  {
    category: "storage",
    title: "Data export storage",
    subtitle: "Connect your own S3, Cloudflare R2 or MinIO bucket.",
    providers: [
      { value: "s3_compatible", label: "AWS S3 / MinIO / R2 (s3://bucket)" },
    ],
    placeholder: "Secret access key",
  },
  {
    category: "webhook",
    title: "Alert webhook",
    subtitle: "Webhook endpoint; requests are HMAC-SHA256 signed with your signing secret.",
    providers: [
      { value: "slack_webhook", label: "Slack / Teams Incoming Webhook" },
      { value: "custom_webhook", label: "HTTPS Webhook" },
    ],
    placeholder: "https://hooks.slack.com/services/...",
  },
];

const COPILOT_PRESETS = [
  "Diagnose our biggest funnel drop-off and cite replay sessions",
  "Summarize errors and rage taps affecting conversion",
];

export const SecurityPanel: React.FC<{ projectId: string }> = ({
  projectId,
}) => {
  const [encryption, setEncryption] = useState<EncryptionStatusResponse | null>(null);
  const [auditLogs, setAuditLogs] = useState<EnterpriseAuditRecord[]>([]);
  // Why the audit log couldn't load: it's a licensed feature, or a plain error.
  const [auditProblem, setAuditProblem] = useState<string | null>(null);
  const [credentials, setCredentials] = useState<ByokCredentialItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  // Key rotation state
  const [newKeyInput, setNewKeyInput] = useState("");
  const [rotateBusy, setRotateBusy] = useState(false);

  // Warehouse export state
  const [exportBusy, setExportBusy] = useState(false);

  const [draftProvider, setDraftProvider] = useState<Record<string, string>>({
    llm: "openai",
    storage: "s3_compatible",
    webhook: "slack_webhook",
  });
  const [draftSecret, setDraftSecret] = useState<Record<string, string>>({});
  const [draftStorageUrl, setDraftStorageUrl] = useState("");
  const [draftStorageEndpoint, setDraftStorageEndpoint] = useState("");
  const [draftStorageRegion, setDraftStorageRegion] = useState("");
  const [draftSigningSecret, setDraftSigningSecret] = useState("");
  const [draftStorageAccessKey, setDraftStorageAccessKey] = useState("");
  const [busyCategory, setBusyCategory] = useState<string | null>(null);

  const [copilotPrompt, setCopilotPrompt] = useState("");
  const [copilotLoading, setCopilotLoading] = useState(false);
  const [copilotRes, setCopilotRes] = useState<CopilotResponse | null>(null);

  const loadState = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [enc, creds, logs] = await Promise.all([
        api.getEncryptionStatus().catch(() => null),
        api.listByokCredentials(projectId),
        api.listAuditLogs(20).then(
          (l) => ({ l, problem: null as string | null }),
          (err) => {
            const msg = errorMessage(err);
            return {
              l: [] as EnterpriseAuditRecord[],
              problem: msg.includes("commercial_license_required")
                ? "The audit log needs a commercial licence. Set PULSE_LICENSE_KEY on the node to turn it on."
                : `Couldn't load the audit log: ${msg}`,
            };
          },
        ),
      ]);
      setEncryption(enc);
      setCredentials(creds);
      setAuditLogs(logs.l);
      setAuditProblem(logs.problem);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    setCopilotRes(null);
    void loadState();
  }, [projectId, loadState]);

  const handleRotateKey = async (disableWrites = false) => {
    setRotateBusy(true);
    setError(null);
    setNotice(null);
    try {
      const res = await api.rotateEncryptionKey(
        disableWrites
          ? { disable_new_writes: true }
          : { secret: newKeyInput.trim() },
      );
      setEncryption(res);
      setNewKeyInput("");
      setNotice(
        disableWrites
          ? "New data is no longer encrypted. Files encrypted earlier still open."
          : `Encryption key changed to ${res.status.active_key_id}. ${res.status.keyring_size} keys kept for older files.`,
      );
      await loadState();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRotateBusy(false);
    }
  };

  const handleSaveCred = async (cat: CategoryMeta) => {
    const secret = (draftSecret[cat.category] ?? "").trim();
    if (!secret) return;
    setBusyCategory(cat.category);
    setError(null);
    setNotice(null);
    try {
      const provider = draftProvider[cat.category] ?? cat.providers[0].value;
      const config_json: Record<string, unknown> = {};
      if (cat.category === "storage") {
        config_json.url = draftStorageUrl.trim();
        if (draftStorageAccessKey.trim()) config_json.access_key = draftStorageAccessKey.trim();
        if (draftStorageRegion.trim()) config_json.region = draftStorageRegion.trim();
        if (draftStorageEndpoint.trim()) config_json.endpoint = draftStorageEndpoint.trim();
      }
      if (cat.category === "webhook") config_json.signing_secret = draftSigningSecret.trim();
      await api.upsertByokCredential(projectId, {
        category: cat.category,
        provider,
        secret,
        config_json,
      });
      setDraftSecret((prev) => ({ ...prev, [cat.category]: "" }));
      if (cat.category === "webhook") setDraftSigningSecret("");
      if (cat.category === "storage") setDraftStorageAccessKey("");
      await loadState();
      setNotice(`Saved ${cat.title}.`);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusyCategory(null);
    }
  };

  const handleVerifyCred = async (category: string) => {
    setBusyCategory(category);
    setError(null);
    setNotice(null);
    try {
      const res = await api.verifyByokCredential(projectId, category);
      await loadState();
      setNotice(res.details);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusyCategory(null);
    }
  };

  const handleDeleteCred = async (category: string) => {
    setBusyCategory(category);
    setError(null);
    setNotice(null);
    try {
      await api.deleteByokCredential(projectId, category);
      await loadState();
      setNotice(`Removed the ${category} credential.`);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusyCategory(null);
    }
  };

  const handleExportWarehouse = async () => {
    setExportBusy(true);
    setError(null);
    setNotice(null);
    try {
      const res = await api.exportWarehouse(projectId, { limit: 10000 });
      setNotice(`Exported ${formatCount(res.events_exported)} events (${formatCount(res.bytes_written)} bytes) to ${res.object_key}`);
      await loadState();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setExportBusy(false);
    }
  };

  const handleAskCopilot = async (e?: React.FormEvent, overridePrompt?: string) => {
    if (e) e.preventDefault();
    const q = (overridePrompt ?? copilotPrompt).trim();
    if (!q) return;
    setCopilotLoading(true);
    setError(null);
    try {
      const res = await api.askByokCopilot(projectId, q);
      setCopilotRes(res);
      await loadState();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setCopilotLoading(false);
    }
  };

  const credByCat = new Map(credentials.map((c) => [c.category, c]));
  const hasLlmKey = credByCat.has("llm");
  const hasStorageKey = credByCat.has("storage");

  return (
    <div className="space-y-6">
      <PageHeader
        title="Security and integrations"
        description="Encryption at rest, your own AI provider key, data export, and the audit log."
      />
      {error && <ErrorNotice message={error} onRetry={loadState} />}
      {notice && (
        <div
          className="flex items-center gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-xs text-ink"
          role="status"
        >
          <CheckCircle2 className="h-4 w-4 shrink-0 text-ok" />
          <span>{notice}</span>
        </div>
      )}
      {/* 1. Encryption at rest */}
      <Panel
        title="Encryption at rest"
        meta={loading ? "Loading…" : undefined}
        actions={
          <span
            className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${
              encryption?.status.enabled
                ? "border-ok/30 bg-ok-soft text-ok"
                : "border-line bg-sunken text-muted"
            }`}
          >
            <Lock className="h-3.5 w-3.5" />
            {encryption?.status.enabled ? "On" : "Off"}
          </span>
        }
      >
        <div className="space-y-4">
          <p className="text-xs text-muted">
            {encryption?.status.enabled
              ? "Everything Pulse writes to disk (events, session replays, logs and user data) is encrypted with AES-256-GCM. "
              : "New data is written to disk unencrypted. Set a key to encrypt everything Pulse writes from now on (AES-256-GCM). "}
            Earlier keys are kept, so files written before a key change still open.
          </p>

          <div className="grid grid-cols-1 gap-4 rounded-xl border border-line bg-sunken p-4 sm:grid-cols-3">
            <div>
              <div className="text-2xs font-semibold uppercase tracking-wide text-muted">Current key</div>
              <div className="font-mono text-xs font-medium text-ink">
                {encryption?.status.active_key_id ?? "None"}
              </div>
            </div>
            <div>
              <div className="text-2xs font-semibold uppercase tracking-wide text-muted">Keys kept</div>
              <div className="text-xs font-medium text-ink">
                {encryption?.status.keyring_size ?? 0}
              </div>
            </div>
            <div>
              <div className="text-2xs font-semibold uppercase tracking-wide text-muted">Since start</div>
              <div className="text-xs font-medium text-ink">
                {formatCount(encryption?.status.ops_encrypted ?? 0)} encrypted · {formatCount(encryption?.status.ops_decrypted ?? 0)} decrypted
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 pt-1">
            <input
              type="password"
              value={newKeyInput}
              onChange={(e) => setNewKeyInput(e.target.value)}
              placeholder="64 hex characters, or a passphrase of 16+ characters"
              className={`${field} min-w-0 flex-1 basis-64 font-mono text-xs`}
              aria-label="New encryption key or passphrase"
            />
            <button
              type="button"
              disabled={rotateBusy || !newKeyInput.trim()}
              onClick={() => void handleRotateKey(false)}
              className={btn.primary}
            >
              {rotateBusy ? <Spinner label="Saving the key" /> : encryption?.status.enabled ? "Change key" : "Turn on encryption"}
            </button>
            {encryption?.status.enabled && (
              <button
                type="button"
                disabled={rotateBusy}
                onClick={() => {
                  if (window.confirm("Stop encrypting new data? Data written from now on is stored unencrypted. Files already encrypted stay encrypted and readable."))
                    void handleRotateKey(true);
                }}
                className={btn.secondary}
              >
                Stop encrypting new data
              </button>
            )}
          </div>
        </div>
      </Panel>

      {/* 2. Provider credentials */}
      <Panel
        title="Provider credentials"
        meta="AES-256-GCM Sealed Secrets"
      >
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            {CATEGORIES.map((cat) => {
              const saved = credByCat.get(cat.category);
              const busy = busyCategory === cat.category;
              return (
                <div
                  key={cat.category}
                  className="flex flex-col justify-between gap-3 rounded-xl border border-line bg-panel p-4"
                >
                  <div className="space-y-1">
                    <div className="flex items-center justify-between gap-2">
                      <span className="flex items-center gap-1.5 text-sm font-semibold text-ink">
                        <KeyRound className="h-4 w-4 text-brand" />
                        {cat.title}
                      </span>
                      {saved && (
                        <span
                          className={`rounded border px-2 py-0.5 font-mono text-2xs ${
                            saved.status === "verified"
                              ? "border-ok/30 bg-ok-soft text-ok"
                              : "border-line bg-sunken text-ink-2"
                          }`}
                        >
                          {saved.key_prefix_masked} · {saved.status}
                        </span>
                      )}
                    </div>
                    <p className="text-xs text-muted">{cat.subtitle}</p>
                  </div>

                  <div className="space-y-2">
                    <select
                      aria-label={`${cat.title} provider`}
                      value={draftProvider[cat.category] ?? cat.providers[0].value}
                      disabled={busy}
                      onChange={(e) =>
                        setDraftProvider((p) => ({
                          ...p,
                          [cat.category]: e.target.value,
                        }))
                      }
                      className={`${field} w-full text-xs`}
                    >
                      {cat.providers.map((p) => (
                        <option key={p.value} value={p.value}>
                          {p.label}
                        </option>
                      ))}
                    </select>

                    {cat.category === "storage" && (
                      <div className="space-y-1.5">
                        <input
                          type="text"
                          value={draftStorageUrl}
                          disabled={busy}
                          onChange={(e) => setDraftStorageUrl(e.target.value)}
                          placeholder="s3://your-analytics-bucket"
                          className={`${field} w-full font-mono text-xs`}
                          aria-label="Export storage destination URL"
                        />
                        <div className="grid grid-cols-2 gap-1.5">
                          <input
                            type="text"
                            value={draftStorageRegion}
                            disabled={busy}
                            onChange={(e) => setDraftStorageRegion(e.target.value)}
                            placeholder="Region (ap-south-1)"
                            aria-label="Storage region"
                            className={`${field} font-mono text-xs`}
                          />
                          <input
                            type="text"
                            value={draftStorageAccessKey}
                            disabled={busy}
                            onChange={(e) => setDraftStorageAccessKey(e.target.value)}
                            placeholder="Access key ID"
                            aria-label="Storage access key ID"
                            className={`${field} font-mono text-xs`}
                          />
                        </div>
                        <input
                          type="text"
                          value={draftStorageEndpoint}
                          disabled={busy}
                          onChange={(e) => setDraftStorageEndpoint(e.target.value)}
                          placeholder="Custom endpoint (optional, e.g. MinIO/R2)"
                          aria-label="Custom storage endpoint (optional)"
                          className={`${field} w-full font-mono text-xs`}
                        />
                      </div>
                    )}

                    {cat.category === "webhook" && (
                      <input
                        type="password"
                        autoComplete="off"
                        value={draftSigningSecret}
                        disabled={busy}
                        onChange={(e) => setDraftSigningSecret(e.target.value)}
                        placeholder="Signing secret (at least 16 characters)"
                        className={`${field} w-full font-mono text-xs`}
                        aria-label="Webhook signing secret"
                      />
                    )}

                    <div className="flex items-center gap-2">
                      <input
                        type="password"
                        autoComplete="off"
                        value={draftSecret[cat.category] ?? ""}
                        disabled={busy}
                        onChange={(e) =>
                          setDraftSecret((p) => ({
                            ...p,
                            [cat.category]: e.target.value,
                          }))
                        }
                        placeholder={cat.placeholder}
                        className={`${field} flex-1 font-mono text-xs`}
                        aria-label={`${cat.title} secret`}
                      />
                      <button
                        type="button"
                        disabled={busy || !(draftSecret[cat.category] ?? "").trim()}
                        onClick={() => void handleSaveCred(cat)}
                        className={btn.primary}
                      >
                        {busy ? <Spinner label="Saving" /> : "Save"}
                      </button>
                    </div>

                    {saved && (
                      <div className="flex items-center justify-between pt-1">
                        <span className="text-2xs text-muted">
                          {saved.last_verified_at_ms
                            ? `Verified ${formatTimestamp(saved.last_verified_at_ms)}`
                            : "Not verified yet"}
                        </span>
                        <div className="flex items-center gap-1.5">
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void handleVerifyCred(cat.category)}
                            className={`${btn.secondary} px-2.5 py-1 text-xs`}
                          >
                            Verify
                          </button>
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void handleDeleteCred(cat.category)}
                            className="inline-flex items-center gap-1 rounded-lg border border-destructive/30 bg-destructive-soft px-2 py-1 text-xs font-medium text-destructive hover:opacity-90"
                            aria-label={`Remove ${cat.title}`}
                          >
                            <Trash2 className="h-3 w-3" />
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Warehouse Export Trigger */}
          {hasStorageKey && (
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-line bg-sunken p-4">
              <div>
                <div className="flex items-center gap-2 text-sm font-semibold text-ink">
                  <Database className="h-4 w-4 text-brand" />
                  Data export (NDJSON)
                </div>
                <p className="text-xs text-muted">
                  Exports raw event batches honoring DPDP erasure tombstones and Lexicon PII rules directly to your bucket.
                </p>
              </div>
              <button
                type="button"
                disabled={exportBusy}
                onClick={() => void handleExportWarehouse()}
                className={btn.primary}
              >
                {exportBusy ? <Spinner label="Syncing" /> : (
                  <>
                    <Download className="h-3.5 w-3.5" />
                    Sync Events to Warehouse
                  </>
                )}
              </button>
            </div>
          )}
        </div>
      </Panel>

      {/* 3. Audit log */}
      <Panel title="Audit log" meta="Latest 20 security and admin actions">
          {auditProblem ? (
            <p className="text-sm text-muted">{auditProblem}</p>
          ) : auditLogs.length === 0 ? (
            <p className="text-sm text-muted">
              Nothing recorded yet. Key changes, provider credentials, invites and role changes appear here.
            </p>
          ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="border-b border-line text-muted">
                  <th className="py-2 pr-3">Time</th>
                  <th className="py-2 pr-3">Actor</th>
                  <th className="py-2 pr-3">Action</th>
                  <th className="py-2 pr-3">Resource</th>
                  <th className="py-2 text-right">Details</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {auditLogs.map((log) => (
                  <tr key={log.id}>
                    <td className="py-2 pr-3 text-muted">{formatTimestamp(log.created_at_ms)}</td>
                    <td className="py-2 pr-3 font-mono text-ink">{log.actor}</td>
                    <td className="py-2 pr-3 font-semibold text-ink">{log.action}</td>
                    <td className="py-2 pr-3 font-mono text-ink-2">{log.resource}</td>
                    <td className="py-2 text-right font-mono text-2xs text-muted">
                      {JSON.stringify(log.details_json)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          )}
      </Panel>

      {/* 4. AI copilot */}
      <Panel
        title="AI copilot"
        meta={hasLlmKey ? `Using ${credByCat.get("llm")?.provider} key` : "Requires an AI provider key"}
      >
        <div className="space-y-4">
          <div className="rounded-lg border border-line bg-sunken px-3 py-2 text-xs text-muted">
            <strong className="text-ink">What is sent:</strong> Questions, aggregate metric summaries, and cited session IDs are sent directly to your configured provider. Pulse never trains on or stores your queries.
          </div>

          <div className="flex flex-wrap gap-2">
            {COPILOT_PRESETS.map((preset) => (
              <button
                key={preset}
                type="button"
                disabled={!hasLlmKey || copilotLoading}
                onClick={() => {
                  setCopilotPrompt(preset);
                  void handleAskCopilot(undefined, preset);
                }}
                className={`${btn.secondary} h-auto max-w-full !whitespace-normal px-2.5 py-1 text-left text-xs`}
              >
                {preset}
              </button>
            ))}
          </div>

          <form onSubmit={(e) => void handleAskCopilot(e)} className="flex flex-wrap gap-2">
            <label htmlFor="byok-copilot-input" className={`${fieldLabel} sr-only`}>
              Ask the AI copilot
            </label>
            <input
              id="byok-copilot-input"
              type="text"
              value={copilotPrompt}
              disabled={!hasLlmKey || copilotLoading}
              onChange={(e) => setCopilotPrompt(e.target.value)}
              placeholder={
                hasLlmKey
                  ? "Ask why users drop off or which replay sessions failed..."
                  : "Save an AI provider key above first..."
              }
              className={`${field} min-w-[16rem] flex-1`}
            />
            <button
              type="submit"
              disabled={!hasLlmKey || copilotLoading || !copilotPrompt.trim()}
              className={btn.primary}
            >
              {copilotLoading ? (
                <Spinner label="Analyzing" />
              ) : (
                <>
                  <Sparkles className="h-3.5 w-3.5" />
                  Analyze drop-offs
                </>
              )}
            </button>
          </form>

          {copilotRes && (
            <div className="space-y-3 rounded-xl border border-line bg-sunken p-4">
              <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
                <span>
                  <strong className="text-ink">{copilotRes.provider}</strong> ({copilotRes.model}) ·{" "}
                  <code className="font-mono">Outbound</code>
                </span>
                <span>Ran in {copilotRes.exec_time_ms} ms</span>
              </div>

              {copilotRes.root_cause_step && (
                <div className="rounded-lg border border-warn/30 bg-warn-soft px-3 py-2 text-xs text-ink">
                  Biggest Drop-Off: <strong>{copilotRes.root_cause_step}</strong> (
                  {copilotRes.dropoff_rate_pct?.toFixed(1)}% drop)
                </div>
              )}

              <pre className="whitespace-pre-wrap font-sans text-xs leading-relaxed text-ink">
                {copilotRes.answer_markdown}
              </pre>

              {copilotRes.cited_sessions.length > 0 && (
                <div className="flex flex-wrap items-center gap-1.5 pt-1">
                  <span className="text-xs font-medium text-muted">Cited sessions:</span>
                  {copilotRes.cited_sessions.map((sid) => (
                    <span
                      key={sid}
                      className="rounded border border-line bg-panel px-2 py-0.5 font-mono text-2xs text-ink"
                    >
                      {sid}
                    </span>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </Panel>
    </div>
  );
};
