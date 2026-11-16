import React, { useCallback, useEffect, useState } from "react";
import { Bot, Lock, Play, ShieldAlert } from "lucide-react";
import { api, LexiconEvent, LexiconProperty } from "../api/client";

interface Props {
  projectId: string;
  onLexiconChanged: () => void;
}

const MCP_PRESETS = [
  {
    title: "Inspect Frustrated Rage-Tap KYC Sessions",
    tool: "inspect_frustrated_sessions",
    args: { only_rage_tap: true, limit: 5 },
  },
  {
    title: "5-Step Onboarding → Disbursal Funnel",
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
    title: "City-wise App_Open Uniques (Insights)",
    tool: "query_insights",
    args: { event_name: "App_Open", metric: "uniques", breakdown_by: "city", days: 14 },
  },
  {
    title: "Quick-Grocery 7-Day Retention",
    tool: "query_retention",
    args: {
      start_event: "App_Open",
      return_event: "Quick_Grocery_Browse",
      mode: "n_day",
      num_periods: 7,
    },
  },
  {
    title: "Fetch Governed Lexicon Schema",
    tool: "get_lexicon_schema",
    args: {},
  },
];

export const LexiconMcpWorkspace: React.FC<Props> = ({ projectId, onLexiconChanged }) => {
  const [events, setEvents] = useState<LexiconEvent[]>([]);
  const [props, setProps] = useState<LexiconProperty[]>([]);
  const [selectedTool, setSelectedTool] = useState<string>("inspect_frustrated_sessions");
  const [argsJson, setArgsJson] = useState<string>(
    JSON.stringify({ only_rage_tap: true, limit: 5 }, null, 2),
  );
  const [mcpOutput, setMcpOutput] = useState<string>("");
  const [mcpLoading, setMcpLoading] = useState<boolean>(false);

  const loadLexicon = useCallback(async () => {
    const res = await api.getLexicon(projectId);
    setEvents(res.events);
    setProps(res.properties);
  }, [projectId]);

  useEffect(() => {
    void loadLexicon();
  }, [loadLexicon]);

  const setEventStatus = async (ev: LexiconEvent, status: string) => {
    await api.updateLexiconEvent(projectId, ev.name, {
      status,
      description: ev.description,
      owner: ev.owner,
    });
    await loadLexicon();
    onLexiconChanged();
  };

  const togglePiiMask = async (p: LexiconProperty) => {
    await api.updateLexiconProp(projectId, p.name, {
      pii_masked: !p.pii_masked,
      status: p.status,
    });
    await loadLexicon();
    onLexiconChanged();
  };

  const runMcpTool = async (tool = selectedTool, rawArgs = argsJson) => {
    setMcpLoading(true);
    try {
      const parsedArgs = JSON.parse(rawArgs);
      const res = await api.callMcp("tools/call", {
        name: tool,
        arguments: { project_id: projectId, ...parsedArgs },
      });
      setMcpOutput(JSON.stringify(res, null, 2));
    } catch (err) {
      setMcpOutput(String(err));
    } finally {
      setMcpLoading(false);
    }
  };

  useEffect(() => {
    void runMcpTool("inspect_frustrated_sessions", JSON.stringify({ only_rage_tap: true, limit: 4 }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  return (
    <div className="grid grid-cols-1 xl:grid-cols-12 gap-5">
      {/* Left: Lexicon Governance Schema Table */}
      <div className="xl:col-span-7 space-y-5">
        <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm font-semibold text-white">
              <ShieldAlert className="w-4 h-4 text-amber-400" />
              Lexicon Event Governance (Zero-Latency Edge Drop Filter)
            </div>
            <span className="text-xs text-slate-400">{events.length} discovered events</span>
          </div>

          <div className="overflow-x-auto max-h-72 overflow-y-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400">
                  <th className="py-2 text-left">Event Name</th>
                  <th className="py-2 text-right">Ingested Volume</th>
                  <th className="py-2 text-right">Governance Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {events.map((ev) => (
                  <tr key={ev.name}>
                    <td className="py-2 font-mono text-slate-100">{ev.name}</td>
                    <td className="py-2 text-right font-mono text-slate-300">
                      {ev.volume_24h.toLocaleString()}
                    </td>
                    <td className="py-2 text-right">
                      <select
                        value={ev.status}
                        onChange={(e) => void setEventStatus(ev, e.target.value)}
                        className={`rounded px-2 py-0.5 text-[11px] font-semibold border bg-[#090d16] ${
                          ev.status === "dropped"
                            ? "border-red-500/60 text-red-300"
                            : ev.status === "hidden"
                              ? "border-amber-500/60 text-amber-300"
                              : "border-emerald-500/50 text-emerald-300"
                        }`}
                      >
                        <option value="active">Active</option>
                        <option value="hidden">Hidden</option>
                        <option value="dropped">Dropped at Edge</option>
                      </select>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-3">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <Lock className="w-4 h-4 text-emerald-400" />
            Property Schema &amp; Live PII Masking (`████`)
          </div>

          <div className="overflow-x-auto max-h-64 overflow-y-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400">
                  <th className="py-2 text-left">Property Key</th>
                  <th className="py-2 text-left">Inferred Type</th>
                  <th className="py-2 text-right">PII Redaction</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {props.map((p) => (
                  <tr key={p.name}>
                    <td className="py-2 font-mono text-slate-200">{p.name}</td>
                    <td className="py-2 font-mono text-slate-400">{p.inferred_types}</td>
                    <td className="py-2 text-right">
                      <button
                        onClick={() => void togglePiiMask(p)}
                        className={`px-2.5 py-0.5 rounded text-[11px] font-mono border ${
                          p.pii_masked
                            ? "bg-amber-500/20 border-amber-500/60 text-amber-200"
                            : "bg-[#090d16] border-slate-800 text-slate-400"
                        }`}
                      >
                        {p.pii_masked ? "MASKED (████)" : "Unmasked"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Right: Ask Pulse (Model Context Protocol JSON-RPC 2.0 Playground) */}
      <div className="xl:col-span-5 bg-[#0e1422] border border-slate-800/80 rounded-xl p-5 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <Bot className="w-4 h-4 text-blue-400" />
            Ask Pulse — Live MCP Server (`/v1/mcp`)
          </div>
          <span className="text-[11px] font-mono text-slate-400">JSON-RPC 2.0</span>
        </div>

        <div className="flex flex-wrap gap-1.5">
          {MCP_PRESETS.map((preset) => (
            <button
              key={preset.title}
              onClick={() => {
                setSelectedTool(preset.tool);
                const formatted = JSON.stringify(preset.args, null, 2);
                setArgsJson(formatted);
                void runMcpTool(preset.tool, formatted);
              }}
              className="px-2.5 py-1 rounded-lg bg-[#090d16] hover:bg-slate-800 border border-slate-800 text-[11px] text-blue-300 transition"
            >
              {preset.title}
            </button>
          ))}
        </div>

        <div className="space-y-2 text-xs">
          <select
            value={selectedTool}
            onChange={(e) => setSelectedTool(e.target.value)}
            className="w-full bg-[#090d16] border border-slate-800 rounded-lg px-3 py-2 font-mono text-slate-100"
          >
            <option value="get_lexicon_schema">get_lexicon_schema</option>
            <option value="query_insights">query_insights</option>
            <option value="query_funnel">query_funnel</option>
            <option value="query_retention">query_retention</option>
            <option value="inspect_frustrated_sessions">inspect_frustrated_sessions</option>
          </select>

          <textarea
            rows={5}
            value={argsJson}
            onChange={(e) => setArgsJson(e.target.value)}
            className="w-full bg-[#090d16] border border-slate-800 rounded-lg p-2.5 font-mono text-[11px] text-slate-200"
          />

          <button
            onClick={() => void runMcpTool()}
            disabled={mcpLoading}
            className="flex items-center justify-center gap-1.5 w-full py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium"
          >
            <Play className="w-3.5 h-3.5" />
            {mcpLoading ? "Executing MCP Tool..." : "Invoke MCP Tool"}
          </button>
        </div>

        <pre className="bg-[#070a12] border border-slate-800/80 rounded-lg p-3 font-mono text-[11px] text-emerald-300 overflow-x-auto max-h-80 overflow-y-auto">
          {mcpOutput || "// MCP JSON-RPC 2.0 response will appear here"}
        </pre>
      </div>
    </div>
  );
};
