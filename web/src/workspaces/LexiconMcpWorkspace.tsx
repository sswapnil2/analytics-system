import React, { useCallback, useEffect, useState } from "react";
import { Bot, Play } from "lucide-react";
import { api, LexiconEvent, LexiconProperty } from "../api/client";
import { EmptyState, ErrorNotice, PageHeader, Panel, Spinner, btn, field, fieldLabel, ListSkeleton } from "../components/ui";
import { displayUserId, errorMessage, formatCount, humanizeEvent } from "../lib/format";

interface Props {
  projectId: string;
  onLexiconChanged: () => void;
}

const EVENT_STATUS_OPTIONS = [
  { value: "active", label: "Keep" },
  { value: "hidden", label: "Hide from reports" },
  { value: "dropped", label: "Drop at ingest" },
];

const MCP_PRESETS = [
  {
    title: "Sessions with rage taps in KYC",
    tool: "inspect_frustrated_sessions",
    args: { only_rage_tap: true, limit: 5 },
  },
  {
    title: "Onboarding to disbursal funnel",
    tool: "query_funnel",
    args: {
      steps: [
        "App_Open",
        "OTP_Verified",
        "KYC_PAN_Submitted",
        "UPI_Autopay_Mandate_Approved",
        "First_Disbursal_Success",
      ],
      days: 14,
    },
  },
  {
    title: "App opens by city",
    tool: "query_insights",
    args: { event_name: "App_Open", metric: "uniques", breakdown_by: "city", days: 14 },
  },
  {
    title: "Grocery 7-day retention",
    tool: "query_retention",
    args: {
      start_event: "App_Open",
      return_event: "Quick_Grocery_Browse",
      mode: "n_day",
      num_periods: 7,
    },
  },
  {
    title: "Event and property list",
    tool: "get_lexicon_schema",
    args: {},
  },
];

const USER_ID_KEYS = new Set(["distinct_id", "user_id", "canonical_uid"]);

/**
 * Masks user ids anywhere in a tool result so phone numbers never render.
 * MCP wraps payloads as JSON text inside `content[].text`, so JSON-looking
 * strings are parsed and walked too.
 */
function maskUserIds(value: unknown, key?: string): unknown {
  if (typeof value === "string") {
    if (key && USER_ID_KEYS.has(key)) return displayUserId(value);
    const t = value.trim();
    if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))) {
      try {
        return maskUserIds(JSON.parse(t));
      } catch {
        return value;
      }
    }
    return value;
  }
  if (Array.isArray(value)) return value.map((v) => maskUserIds(v, key));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, maskUserIds(v, k)]));
  }
  return value;
}

export const LexiconMcpWorkspace: React.FC<Props> = ({ projectId, onLexiconChanged }) => {
  const [events, setEvents] = useState<LexiconEvent[]>([]);
  const [props, setProps] = useState<LexiconProperty[]>([]);
  const [lexLoading, setLexLoading] = useState(true);
  const [lexError, setLexError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);

  const [selectedTool, setSelectedTool] = useState<string>("inspect_frustrated_sessions");
  const [argsJson, setArgsJson] = useState<string>(
    JSON.stringify({ only_rage_tap: true, limit: 5 }, null, 2),
  );
  const [mcpOutput, setMcpOutput] = useState<string>("");
  const [mcpError, setMcpError] = useState<string | null>(null);
  const [mcpLoading, setMcpLoading] = useState<boolean>(false);

  const loadLexicon = useCallback(async () => {
    setLexLoading(true);
    setLexError(null);
    try {
      const res = await api.getLexicon(projectId);
      setEvents(res.events);
      setProps(res.properties);
    } catch (err) {
      setLexError(errorMessage(err));
    } finally {
      setLexLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    void loadLexicon();
  }, [loadLexicon]);

  const save = async (action: () => Promise<unknown>) => {
    setSaveError(null);
    try {
      await action();
      await loadLexicon();
      onLexiconChanged();
    } catch (err) {
      setSaveError(errorMessage(err));
    }
  };

  const setEventStatus = (ev: LexiconEvent, status: string) =>
    save(() =>
      api.updateLexiconEvent(projectId, ev.name, {
        status,
        description: ev.description,
        owner: ev.owner,
      }),
    );

  const setPiiMask = (p: LexiconProperty, masked: boolean) =>
    save(() =>
      api.updateLexiconProp(projectId, p.name, {
        pii_masked: masked,
        status: p.status,
      }),
    );

  const runMcpTool = async (tool = selectedTool, rawArgs = argsJson) => {
    let parsedArgs: Record<string, unknown>;
    try {
      parsedArgs = rawArgs.trim() ? JSON.parse(rawArgs) : {};
    } catch {
      setMcpError("Arguments must be valid JSON.");
      return;
    }
    setMcpLoading(true);
    setMcpError(null);
    try {
      const res = await api.callMcp("tools/call", {
        name: tool,
        arguments: { project_id: projectId, ...parsedArgs },
      });
      if (res.error) setMcpError(res.error.message);
      setMcpOutput(JSON.stringify(maskUserIds(res.result ?? res), null, 2));
    } catch (err) {
      setMcpError(errorMessage(err));
      setMcpOutput("");
    } finally {
      setMcpLoading(false);
    }
  };

  useEffect(() => {
    void runMcpTool("inspect_frustrated_sessions", JSON.stringify({ only_rage_tap: true, limit: 4 }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  const tableState = (count: number, emptyTitle: string, emptyBody: string) =>
    lexLoading && count === 0 ? (
      <ListSkeleton rows={6} />
    ) : lexError ? (
      <ErrorNotice message={lexError} onRetry={() => void loadLexicon()} />
    ) : count === 0 ? (
      <EmptyState title={emptyTitle} body={emptyBody} />
    ) : null;

  return (
    <div>
      <PageHeader
        title="Events and privacy"
        description="Control which events are kept and which properties are masked. Includes the AI tools endpoint."
      />

      {saveError && (
        <div className="mb-5">
          <ErrorNotice message={`Couldn't save the change: ${saveError}`} />
        </div>
      )}

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-12">
        <div className="space-y-5 xl:col-span-7">
          <Panel title="Events" meta={events.length > 0 ? `${formatCount(events.length)} events` : undefined}>
            {tableState(events.length, "No events yet", "Events appear here once your app starts sending them.") ?? (
              <div className="max-h-80 overflow-auto">
                <table className="w-full min-w-[420px] text-sm">
                  <thead>
                    <tr className="border-b border-line text-xs text-muted">
                      <th className="py-2 text-left font-medium">Event</th>
                      <th className="py-2 text-right font-medium">Volume (24h)</th>
                      <th className="py-2 pl-4 text-right font-medium">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-line">
                    {events.map((ev) => (
                      <tr key={ev.name}>
                        <td className="py-2 text-ink">{humanizeEvent(ev.name)}</td>
                        <td className="tabular py-2 text-right text-ink-2">{formatCount(ev.volume_24h)}</td>
                        <td className="py-2 pl-4 text-right">
                          <select
                            aria-label={`Status for ${humanizeEvent(ev.name)}`}
                            value={ev.status}
                            onChange={(e) => void setEventStatus(ev, e.target.value)}
                            className={`${field} py-1 ${
                              ev.status === "dropped" ? "text-danger" : ev.status === "hidden" ? "text-warn" : ""
                            }`}
                          >
                            {EVENT_STATUS_OPTIONS.map((o) => (
                              <option key={o.value} value={o.value}>
                                {o.label}
                              </option>
                            ))}
                          </select>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>

          <Panel title="Properties">
            {tableState(props.length, "No properties yet", "Properties appear here as events arrive with them.") ?? (
              <div className="max-h-72 overflow-auto">
                <table className="w-full min-w-[420px] text-sm">
                  <thead>
                    <tr className="border-b border-line text-xs text-muted">
                      <th className="py-2 text-left font-medium">Property</th>
                      <th className="py-2 text-left font-medium">Type</th>
                      <th className="py-2 pl-4 text-right font-medium">Privacy</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-line">
                    {props.map((p) => (
                      <tr key={p.name}>
                        <td className="py-2 font-mono text-ink">{p.name}</td>
                        <td className="py-2 text-muted">{p.inferred_types}</td>
                        <td className="py-2 pl-4 text-right">
                          <select
                            aria-label={`Privacy for ${p.name}`}
                            value={p.pii_masked ? "masked" : "shown"}
                            onChange={(e) => void setPiiMask(p, e.target.value === "masked")}
                            className={`${field} py-1 ${p.pii_masked ? "text-warn" : ""}`}
                          >
                            <option value="shown">Shown</option>
                            <option value="masked">Masked</option>
                          </select>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </div>

        <Panel title="AI tools" className="xl:col-span-5">
          <div className="space-y-4">
            <p className="text-sm text-muted">
              Connect Claude or another MCP client to <code className="font-mono text-ink-2">/v1/mcp</code> to query
              this project. Try a tool below.
            </p>

            <div className="flex flex-wrap gap-1.5">
              {MCP_PRESETS.map((preset) => (
                <button
                  key={preset.title}
                  type="button"
                  onClick={() => {
                    setSelectedTool(preset.tool);
                    const formatted = JSON.stringify(preset.args, null, 2);
                    setArgsJson(formatted);
                    void runMcpTool(preset.tool, formatted);
                  }}
                  className={`${btn.secondary} px-2.5 py-1 text-xs`}
                >
                  {preset.title}
                </button>
              ))}
            </div>

            <div className="space-y-1">
              <label htmlFor="mcp-tool" className={fieldLabel}>
                Tool
              </label>
              <select
                id="mcp-tool"
                value={selectedTool}
                onChange={(e) => setSelectedTool(e.target.value)}
                className={`${field} w-full font-mono`}
              >
                <option value="get_lexicon_schema">get_lexicon_schema</option>
                <option value="query_insights">query_insights</option>
                <option value="query_funnel">query_funnel</option>
                <option value="query_retention">query_retention</option>
                <option value="inspect_frustrated_sessions">inspect_frustrated_sessions</option>
              </select>
            </div>

            <div className="space-y-1">
              <label htmlFor="mcp-args" className={fieldLabel}>
                Arguments (JSON)
              </label>
              <textarea
                id="mcp-args"
                rows={5}
                value={argsJson}
                onChange={(e) => setArgsJson(e.target.value)}
                className={`${field} w-full font-mono text-xs`}
              />
            </div>

            <button type="button" onClick={() => void runMcpTool()} disabled={mcpLoading} className={`${btn.primary} w-full`}>
              <Play className="h-3.5 w-3.5" />
              {mcpLoading ? "Running…" : "Run tool"}
            </button>

            {mcpError && <ErrorNotice message={mcpError} />}

            {mcpLoading && !mcpOutput ? (
              <Spinner label="Running tool" />
            ) : mcpOutput ? (
              <pre
                aria-label="Tool result"
                className={`max-h-96 overflow-auto rounded-lg border border-line bg-sunken p-3 font-mono text-xs text-ink-2 ${
                  mcpLoading ? "opacity-60" : ""
                }`}
              >
                {mcpOutput}
              </pre>
            ) : (
              !mcpError && <EmptyState icon={Bot} title="No result yet" body="Pick a tool above to see what it returns." />
            )}
          </div>
        </Panel>
      </div>
    </div>
  );
};
