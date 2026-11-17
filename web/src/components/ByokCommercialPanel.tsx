import React, { useCallback, useEffect, useState } from "react";
import {
  CheckCircle2,
  Database,
  Download,
  KeyRound,
  Lock,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Trash2,
} from "lucide-react";
import {
  api,
  ByokCredentialItem,
  CopilotResponse,
  EncryptionStatusResponse,
  EnterpriseAuditRecord,
  LicenseStatus,
} from "../api/client";
import {
  ErrorNotice,
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
    title: "BYOK AI Provider Key",
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
    title: "BYOC Object Storage & Warehouse Export",
    subtitle: "Connect customer-owned S3, Cloudflare R2, MinIO, or local archive buckets.",
    providers: [
      { value: "s3_compatible", label: "AWS S3 / MinIO / R2 (s3://bucket)" },
      { value: "memory_archive", label: "Named Memory Bucket (memory://bucket)" },
    ],
    placeholder: "Secret access key (or local secret)",
  },
  {
    category: "webhook",
    title: "Alert Webhook Destination",
    subtitle: "HMAC-SHA256 signed webhook endpoint for automated anomaly alerts.",
    providers: [
      { value: "slack_webhook", label: "Slack / Teams Incoming Webhook" },
      { value: "custom_webhook", label: "HTTPS Webhook" },
    ],
    placeholder: "https://hooks.slack.com/services/...",
  },
];

const COPILOT_PRESETS = [
  "Diagnose our biggest onboarding funnel drop-off and cite replay sessions",
  "Summarize payment & KYC errors affecting revenue conversion",
];

export const ByokCommercialPanel: React.FC<{ projectId: string }> = ({
  projectId,
}) => {
  const [license, setLicense] = useState<LicenseStatus | null>(null);
  const [encryption, setEncryption] = useState<EncryptionStatusResponse | null>(null);
  const [auditLogs, setAuditLogs] = useState<EnterpriseAuditRecord[]>([]);
  const [credentials, setCredentials] = useState<ByokCredentialItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [licenseInput, setLicenseInput] = useState("");
  const [licBusy, setLicBusy] = useState(false);
  const [confirmDeactivate, setConfirmDeactivate] = useState(false);

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
  const [draftStorageUrl, setDraftStorageUrl] = useState("s3://");
  const [draftStorageEndpoint, setDraftStorageEndpoint] = useState("");
  const [draftStorageRegion, setDraftStorageRegion] = useState("ap-south-1");
  const [draftStorageAccessKey, setDraftStorageAccessKey] = useState("");
  const [busyCategory, setBusyCategory] = useState<string | null>(null);

  const [copilotPrompt, setCopilotPrompt] = useState(COPILOT_PRESETS[0]);
  const [copilotLoading, setCopilotLoading] = useState(false);
  const [copilotRes, setCopilotRes] = useState<CopilotResponse | null>(null);

  const loadState = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [lic, enc, creds] = await Promise.all([
        api.getLicense(),
        api.getEncryptionStatus().catch(() => null),
        api.listByokCredentials(projectId),
      ]);
      setLicense(lic);
      setEncryption(enc);
      setCredentials(creds);

      if (lic.is_commercial && lic.features.includes("enterprise_audit_log")) {
        const logs = await api.listAuditLogs(20).catch(() => []);
        setAuditLogs(logs);
      } else {
        setAuditLogs([]);
      }
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    setCopilotRes(null);
    setConfirmDeactivate(false);
    void loadState();
  }, [projectId, loadState]);

  const handleActivateKey = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!licenseInput.trim()) return;
    setLicBusy(true);
    setError(null);
    setNotice(null);
    try {
      const res = await api.activateLicense({
        license_key: licenseInput.trim(),
      });
      setLicense(res);
      setLicenseInput("");
      setNotice(`Activated Commercial BYOK license for ${res.customer_org ?? "org"}.`);
      await loadState();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLicBusy(false);
    }
  };

  const handleDeactivateLicense = async () => {
    if (!confirmDeactivate) {
      setConfirmDeactivate(true);
      return;
    }
    setLicBusy(true);
    setError(null);
    setNotice(null);
    try {
      const res = await api.deactivateLicense();
      setLicense(res);
      setConfirmDeactivate(false);
      setNotice("Deactivated commercial license. Existing encrypted data remains readable via cluster keyring.");
      await loadState();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLicBusy(false);
    }
  };

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
          ? "Disabled encryption for new writes. Historical objects remain decryptable."
          : `Active encryption key rotated to ${res.status.active_key_id}. Keyring holds ${res.status.keyring_size} keys.`,
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
      await api.upsertByokCredential(projectId, {
        category: cat.category,
        provider,
        secret,
        config_json,
      });
      setDraftSecret((prev) => ({ ...prev, [cat.category]: "" }));
      await loadState();
      setNotice(`Saved sealed BYOK ${cat.category.toUpperCase()} credential.`);
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
      setNotice(`Removed BYOK ${category.toUpperCase()} credential.`);
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
  const isCommercial = license?.is_commercial ?? false;
  const inGrace = license?.in_grace_period ?? false;
  const hasLlmKey = credByCat.has("llm");
  const hasStorageKey = credByCat.has("storage");

  return (
    <div className="space-y-6">
      {/* 1. Open-Source Cluster Encryption at Rest */}
      <Panel
        title="Cluster Encryption at Rest (AES-256-GCM)"
        meta={loading ? "Loading…" : "Open-Source (Apache-2.0)"}
        actions={
          <span
            className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${
              encryption?.status.enabled
                ? "border-ok/30 bg-ok-soft text-ok"
                : "border-line bg-sunken text-muted"
            }`}
          >
            <Lock className="h-3.5 w-3.5" />
            {encryption?.status.enabled ? "Active Envelope Encryption" : "Plaintext Storage"}
          </span>
        }
      >
        <div className="space-y-4">
          <p className="text-xs text-muted">
            All analytics segments (<code className="font-mono">.pvtx</code>), wireframe replays (<code className="font-mono">.plsr</code>),
            WAL journals, manifests, and identity stores are encrypted with AES-256-GCM chunked envelope keys.
            Keyring history is preserved indefinitely across restarts and rotations so historical files always decrypt.
          </p>

          <div className="grid grid-cols-1 gap-4 rounded-xl border border-line bg-sunken p-4 sm:grid-cols-3">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-wide text-muted">Active Key</div>
              <div className="font-mono text-xs font-medium text-ink">
                {encryption?.status.active_key_id ?? "None (writes unencrypted)"}
              </div>
            </div>
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-wide text-muted">Keyring Size</div>
              <div className="text-xs font-medium text-ink">
                {encryption?.status.keyring_size ?? 0} keys loaded
              </div>
            </div>
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-wide text-muted">Encrypted I/O</div>
              <div className="text-xs font-medium text-ink">
                {formatCount(encryption?.status.ops_encrypted ?? 0)} enc / {formatCount(encryption?.status.ops_decrypted ?? 0)} dec ops
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 pt-1">
            <input
              type="password"
              value={newKeyInput}
              onChange={(e) => setNewKeyInput(e.target.value)}
              placeholder="Rotate to new 64-hex key or >=16-char passphrase"
              className={`${field} min-w-[18rem] flex-1 font-mono text-xs`}
              aria-label="New master key or passphrase"
            />
            <button
              type="button"
              disabled={rotateBusy || !newKeyInput.trim()}
              onClick={() => void handleRotateKey(false)}
              className={btn.primary}
            >
              {rotateBusy ? <Spinner label="Rotating" /> : "Rotate Master Key"}
            </button>
            {encryption?.status.enabled && (
              <button
                type="button"
                disabled={rotateBusy}
                onClick={() => void handleRotateKey(true)}
                className={btn.secondary}
              >
                Disable New Writes Encryption
              </button>
            )}
          </div>
        </div>
      </Panel>

      {/* 2. Commercial BYOK License & Verification */}
      <Panel
        title="Enterprise BYOK License"
        meta={isCommercial ? (inGrace ? "14-Day Grace Period" : "Active Commercial") : "Community OSS Edition"}
        actions={
          license && (
            <span
              className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${
                isCommercial
                  ? inGrace
                    ? "border-warn/30 bg-warn-soft text-warn"
                    : "border-ok/30 bg-ok-soft text-ok"
                  : "border-line bg-sunken text-muted"
              }`}
            >
              <ShieldCheck className="h-3.5 w-3.5" />
              {isCommercial ? (inGrace ? "Grace Period Active" : "Commercial License Active") : "Community Edition"}
            </span>
          )
        }
      >
        <div className="space-y-4">
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

          {isCommercial ? (
            <div className="space-y-3 rounded-xl border border-line bg-sunken p-4">
              <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                <div>
                  Licensed to <strong className="text-ink">{license?.customer_org ?? "Enterprise Customer"}</strong>
                  {license?.license_id && <span className="ml-2 font-mono text-muted">({license.license_id})</span>}
                </div>
                <div className="text-muted">
                  Expires {formatTimestamp(license?.expires_at_ms)}
                  {inGrace && (
                    <span className="ml-2 text-warn font-semibold">
                      (Grace ends {formatTimestamp(license?.grace_ends_at_ms)})
                    </span>
                  )}
                </div>
              </div>

              <div className="flex flex-wrap gap-1.5">
                {(license?.features ?? []).map((f) => (
                  <span
                    key={f}
                    className="rounded border border-line bg-panel px-2 py-0.5 font-mono text-[11px] text-ink"
                  >
                    {f}
                  </span>
                ))}
              </div>

              <div className="pt-2">
                {confirmDeactivate ? (
                  <div className="flex items-center gap-2 rounded-lg border border-warn/30 bg-warn-soft p-3 text-xs text-ink">
                    <ShieldAlert className="h-4 w-4 shrink-0 text-warn" />
                    <span className="flex-1">
                      Are you sure? Paid features (Copilot, Warehouse Sync, Audit Log) will pause. Decryption of all historical data is never affected.
                    </span>
                    <button
                      type="button"
                      disabled={licBusy}
                      onClick={() => void handleDeactivateLicense()}
                      className="inline-flex items-center gap-1 rounded-lg border border-destructive/30 bg-destructive px-3 py-1 font-medium text-white hover:opacity-90"
                    >
                      Confirm Deactivate
                    </button>
                    <button
                      type="button"
                      onClick={() => setConfirmDeactivate(false)}
                      className={btn.secondary}
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <button
                    type="button"
                    disabled={licBusy}
                    onClick={() => setConfirmDeactivate(true)}
                    className={btn.secondary}
                  >
                    Deactivate License
                  </button>
                )}
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="text-xs text-muted">
                Core query engines, session replays, custom properties, cohorts, and dashboards are free forever under Apache-2.0.
                To unlock BYOK AI Copilot, Cloud Warehouse Sync, Enterprise Audit Logs, and Signed Webhook Alerts, paste an offline Ed25519-signed commercial license.
              </p>
              <form onSubmit={handleActivateKey} className="flex flex-wrap items-center gap-2">
                <input
                  type="text"
                  value={licenseInput}
                  onChange={(e) => setLicenseInput(e.target.value)}
                  placeholder="pulse_lic_v1.<payload_b64>.<sig_hex>"
                  className={`${field} min-w-[20rem] flex-1 font-mono text-xs`}
                  aria-label="Offline Ed25519 license key"
                />
                <button
                  type="submit"
                  disabled={licBusy || !licenseInput.trim()}
                  className={btn.primary}
                >
                  {licBusy ? <Spinner label="Verifying" /> : "Activate License"}
                </button>
              </form>
            </div>
          )}
        </div>
      </Panel>

      {/* 3. Commercial BYOK Vault */}
      <Panel
        title="Commercial BYOK Vault"
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
                          className={`rounded border px-2 py-0.5 font-mono text-[11px] ${
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
                      disabled={!isCommercial || busy}
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
                          disabled={!isCommercial || busy}
                          onChange={(e) => setDraftStorageUrl(e.target.value)}
                          placeholder="s3://customer-analytics-bucket or memory://bucket"
                          className={`${field} w-full font-mono text-xs`}
                          aria-label="BYOC storage destination URL"
                        />
                        <div className="grid grid-cols-2 gap-1.5">
                          <input
                            type="text"
                            value={draftStorageRegion}
                            disabled={!isCommercial || busy}
                            onChange={(e) => setDraftStorageRegion(e.target.value)}
                            placeholder="Region (ap-south-1)"
                            className={`${field} font-mono text-xs`}
                          />
                          <input
                            type="text"
                            value={draftStorageAccessKey}
                            disabled={!isCommercial || busy}
                            onChange={(e) => setDraftStorageAccessKey(e.target.value)}
                            placeholder="Access key ID"
                            className={`${field} font-mono text-xs`}
                          />
                        </div>
                        <input
                          type="text"
                          value={draftStorageEndpoint}
                          disabled={!isCommercial || busy}
                          onChange={(e) => setDraftStorageEndpoint(e.target.value)}
                          placeholder="Custom endpoint (optional, e.g. MinIO/R2)"
                          className={`${field} w-full font-mono text-xs`}
                        />
                      </div>
                    )}

                    <div className="flex items-center gap-2">
                      <input
                        type="password"
                        value={draftSecret[cat.category] ?? ""}
                        disabled={!isCommercial || busy}
                        onChange={(e) =>
                          setDraftSecret((p) => ({
                            ...p,
                            [cat.category]: e.target.value,
                          }))
                        }
                        placeholder={
                          isCommercial ? cat.placeholder : "Commercial BYOK license required"
                        }
                        className={`${field} flex-1 font-mono text-xs`}
                        aria-label={`${cat.title} secret`}
                      />
                      <button
                        type="button"
                        disabled={!isCommercial || busy || !(draftSecret[cat.category] ?? "").trim()}
                        onClick={() => void handleSaveCred(cat)}
                        className={btn.primary}
                      >
                        {busy ? <Spinner label="Saving" /> : "Save"}
                      </button>
                    </div>

                    {saved && (
                      <div className="flex items-center justify-between pt-1">
                        <span className="text-[11px] text-muted">
                          {saved.last_verified_at_ms
                            ? `Verified ${formatTimestamp(saved.last_verified_at_ms)}`
                            : "Not verified yet"}
                        </span>
                        <div className="flex items-center gap-1.5">
                          <button
                            type="button"
                            disabled={!isCommercial || busy}
                            onClick={() => void handleVerifyCred(cat.category)}
                            className={`${btn.secondary} px-2.5 py-1 text-xs`}
                          >
                            Verify
                          </button>
                          <button
                            type="button"
                            disabled={!isCommercial || busy}
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
                  Cloud Data Warehouse Sync (NDJSON)
                </div>
                <p className="text-xs text-muted">
                  Exports raw event batches honoring DPDP erasure tombstones and Lexicon PII rules directly to your BYOC bucket.
                </p>
              </div>
              <button
                type="button"
                disabled={!isCommercial || exportBusy}
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

      {/* 4. Enterprise Audit Log */}
      {isCommercial && auditLogs.length > 0 && (
        <Panel title="Enterprise Audit Log" meta="Cryptographically tracked security actions">
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
                    <td className="py-2 text-right font-mono text-[11px] text-muted">
                      {JSON.stringify(log.details_json)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {/* 5. BYOK AI Root-Cause Copilot */}
      <Panel
        title="BYOK AI Root-Cause Copilot"
        meta={hasLlmKey ? `Using ${credByCat.get("llm")?.provider} key` : "Requires BYOK LLM key"}
      >
        <div className="space-y-4">
          <div className="rounded-lg border border-line bg-sunken px-3 py-2 text-xs text-muted">
            <strong className="text-ink">Data Flow Disclosure:</strong> Questions, aggregate metric summaries, and cited session IDs are sent directly to your configured provider. Pulse never trains on or stores your queries.
          </div>

          <div className="flex flex-wrap gap-2">
            {COPILOT_PRESETS.map((preset) => (
              <button
                key={preset}
                type="button"
                disabled={!isCommercial || !hasLlmKey || copilotLoading}
                onClick={() => {
                  setCopilotPrompt(preset);
                  void handleAskCopilot(undefined, preset);
                }}
                className={`${btn.secondary} px-2.5 py-1 text-xs`}
              >
                {preset}
              </button>
            ))}
          </div>

          <form onSubmit={(e) => void handleAskCopilot(e)} className="flex flex-wrap gap-2">
            <label htmlFor="byok-copilot-input" className={`${fieldLabel} sr-only`}>
              Ask BYOK Copilot
            </label>
            <input
              id="byok-copilot-input"
              type="text"
              value={copilotPrompt}
              disabled={!isCommercial || !hasLlmKey || copilotLoading}
              onChange={(e) => setCopilotPrompt(e.target.value)}
              placeholder={
                isCommercial
                  ? hasLlmKey
                    ? "Ask why users drop off or which replay sessions failed..."
                    : "Save a BYOK AI Provider Key above first..."
                  : "Activate Commercial BYOK license above to unlock Copilot"
              }
              className={`${field} min-w-[16rem] flex-1`}
            />
            <button
              type="submit"
              disabled={!isCommercial || !hasLlmKey || copilotLoading || !copilotPrompt.trim()}
              className={btn.primary}
            >
              {copilotLoading ? (
                <Spinner label="Analyzing" />
              ) : (
                <>
                  <Sparkles className="h-3.5 w-3.5" />
                  Analyze Drop-Offs
                </>
              )}
            </button>
          </form>

          {copilotRes && (
            <div className="space-y-3 rounded-xl border border-line bg-sunken p-4">
              <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
                <span>
                  <strong className="text-ink">{copilotRes.provider}</strong> ({copilotRes.model}) ·{" "}
                  <code className="font-mono">BYOK Outbound</code>
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
                      className="rounded border border-line bg-panel px-2 py-0.5 font-mono text-[11px] text-ink"
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
