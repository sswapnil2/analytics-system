export interface ProjectInfo {
  id: string;
  org_id: string;
  name: string;
  timezone: string;
  created_at_ms: number;
}

export interface NodeStatus {
  status: string;
  default_project_id: string;
  default_api_key: string;
  active_project_id: string;
  live_eps: number;
  accepted_events_total: number;
  deduped_events_total: number;
  dropped_events_total: number;
  last_query_latency_us: number;
  queries_executed: number;
  snapshot_epoch: number;
  l0_rows: number;
  l1_rows: number;
  segment_count: number;
  interned_users: number;
  projects: ProjectInfo[];
}

export interface TimeRange {
  start_ts_ms: number;
  end_ts_ms: number;
}

export interface TimePoint {
  bucket_ts_ms: number;
  value: number | null;
}

export interface SeriesOutput {
  label: string;
  breakdown_key: string;
  overall_value: number | null;
  points: TimePoint[];
}

export interface InsightsResult {
  snapshot_epoch: number;
  exec_time_us: number;
  buckets_ts_ms: number[];
  series: SeriesOutput[];
  previous_series: SeriesOutput[];
  formula_series: SeriesOutput[];
}

export interface FunnelStepCount {
  step_index: number;
  event_name: string;
  entered_users: number;
  dropped_users: number;
  conversion_from_start_pct: number;
  conversion_from_prev_pct: number;
}

export interface FunnelBreakdownGroup {
  breakdown_value: string;
  steps: FunnelStepCount[];
}

export interface DroppedSessionSample {
  dropped_before_step: number;
  session_id: string;
  canonical_uid: number;
  distinct_id: string;
  drop_ts_ms: number;
  replay_offset_kb: number;
}

export interface FunnelResult {
  snapshot_epoch: number;
  exec_time_us: number;
  overall_steps: FunnelStepCount[];
  breakdowns: FunnelBreakdownGroup[];
  time_to_convert: {
    converted_count: number;
    avg_ms: number;
    p50_ms: number;
    p90_ms: number;
    p99_ms: number;
    histogram_bins: [number, number, number][];
  };
  dropped_sample_sessions: DroppedSessionSample[];
}

export interface FlowNode {
  id: string;
  step_offset: number;
  event_name: string;
  user_count: number;
}

export interface FlowLink {
  source: string;
  target: string;
  user_count: number;
}

export interface FlowsResult {
  snapshot_epoch: number;
  exec_time_us: number;
  nodes: FlowNode[];
  links: FlowLink[];
}

export interface RetentionCohortRow {
  cohort_start_ts_ms: number;
  cohort_label: string;
  cohort_size: number;
  retained_counts: number[];
  retained_pcts: number[];
}

export interface RetentionResult {
  snapshot_epoch: number;
  exec_time_us: number;
  mode: string;
  cohorts: RetentionCohortRow[];
}

export interface SessionSummaryItem {
  session_id: string;
  canonical_uid: number;
  distinct_id: string;
  start_ts_ms: number;
  end_ts_ms: number;
  duration_ms: number;
  event_count: number;
  has_rage_tap: boolean;
  has_dead_tap: boolean;
  has_error: boolean;
  events: [number, string, number][];
}

export interface SessionSearchResult {
  snapshot_epoch: number;
  exec_time_us: number;
  total_sessions_inspected: number;
  sessions_pruned_by_bloom: number;
  sessions: SessionSummaryItem[];
}

export interface WireNode {
  node_id: number;
  parent_id: number;
  kind:
    | "screen"
    | "container"
    | "button"
    | "masked_input"
    | "text"
    | "image_placeholder"
    | "error_banner";
  x: number;
  y: number;
  w: number;
  h: number;
  bg_rgba: number;
  masked_label: string;
}

export type WireframeOp =
  | { op: "full_snapshot"; width: number; height: number; nodes: WireNode[] }
  | ({ op: "node_upsert" } & WireNode)
  | { op: "node_remove"; node_id: number }
  | {
      op: "pointer_interaction";
      ts_offset_ms: number;
      x: number;
      y: number;
      kind:
        | { type: "tap" }
        | { type: "rage_tap"; count: number }
        | { type: "dead_tap" }
        | { type: "scroll" };
    }
  | {
      op: "network_breadcrumb";
      ts_offset_ms: number;
      method: string;
      path: string;
      status_code: number;
      duration_ms: number;
    };

export interface ReplayChunk {
  project_id: string;
  session_id: string;
  chunk_start_ts_ms: number;
  ops: WireframeOp[];
}

export interface ReplayStreamResponse {
  project_id: string;
  session_id: string;
  start_byte_offset: number;
  chunks: ReplayChunk[];
}

export interface LexiconEvent {
  project_id: string;
  name: string;
  display_name: string;
  description: string;
  status: string;
  owner: string;
  volume_24h: number;
  updated_at_ms: number;
}

export interface LexiconProperty {
  project_id: string;
  name: string;
  prop_scope: string;
  inferred_types: string;
  status: string;
  pii_masked: boolean;
  updated_at_ms: number;
}

export interface FeatureFlag {
  id: string;
  project_id: string;
  key: string;
  description: string;
  enabled: boolean;
  rollout_pct: number;
  targeting_cohort_id: string | null;
  variants: { key: string; weight_bps: number; payload?: unknown }[];
  updated_at_ms: number;
}

export interface Experiment {
  id: string;
  project_id: string;
  name: string;
  flag_id: string;
  flag_key: string;
  hypothesis: string;
  status: string;
  start_ts_ms: number;
  end_ts_ms: number;
  primary_metric: {
    name: string;
    event_name: string;
    metric_type: string;
    conversion_window_ms: number;
  };
  guardrail_metrics: {
    name: string;
    event_name: string;
  }[];
}

export interface VariantLiftRow {
  variant_key: string;
  is_control: boolean;
  sample_size: number;
  conversions: number;
  conversion_rate: number;
  mean_value: number;
  variance: number;
  relative_lift_pct: number;
  ci95_low_pct: number;
  ci95_high_pct: number;
  p_value: number;
  bayesian_prob_to_beat_control: number;
  statistically_significant: boolean;
}

export interface ExperimentResults {
  experiment_id: string;
  experiment_name: string;
  flag_key: string;
  status: string;
  hypothesis: string;
  metric_name: string;
  exec_time_us: number;
  variants: VariantLiftRow[];
  guardrails: {
    metric_name: string;
    event_name: string;
    variants: VariantLiftRow[];
  }[];
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export function default14DayRange(): TimeRange {
  const now = Date.now();
  return {
    start_ts_ms: now - 14 * 86_400_000,
    end_ts_ms: now + 3_600_000,
  };
}

export const api = {
  getStatus: (projectId?: string) =>
    fetchJson<NodeStatus>(
      projectId ? `/v1/status?project_id=${encodeURIComponent(projectId)}` : "/v1/status",
    ),

  simulateBurst: (projectId: string, count = 1500) =>
    fetchJson<{
      accepted: number;
      deduped: number;
      dropped: number;
      frustrated_session_id: string;
    }>("/v1/admin/simulate-burst", {
      method: "POST",
      body: JSON.stringify({ project_id: projectId, count }),
    }),

  queryInsights: (projectId: string, body: unknown) =>
    fetchJson<InsightsResult>(`/v1/projects/${encodeURIComponent(projectId)}/query/insights`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  queryFunnel: (projectId: string, body: unknown) =>
    fetchJson<FunnelResult>(`/v1/projects/${encodeURIComponent(projectId)}/query/funnel`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  queryFlows: (projectId: string, body: unknown) =>
    fetchJson<FlowsResult>(`/v1/projects/${encodeURIComponent(projectId)}/query/flows`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  queryRetention: (projectId: string, body: unknown) =>
    fetchJson<RetentionResult>(`/v1/projects/${encodeURIComponent(projectId)}/query/retention`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  searchSessions: (projectId: string, body: unknown) =>
    fetchJson<SessionSearchResult>(`/v1/projects/${encodeURIComponent(projectId)}/sessions`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getReplay: (projectId: string, sessionId: string, offsetKb = 0) =>
    fetchJson<ReplayStreamResponse>(
      `/v1/projects/${encodeURIComponent(projectId)}/replay/${encodeURIComponent(sessionId)}?offset_kb=${offsetKb}`,
    ),

  getLexicon: (projectId: string) =>
    fetchJson<{ events: LexiconEvent[]; properties: LexiconProperty[] }>(
      `/v1/projects/${encodeURIComponent(projectId)}/lexicon`,
    ),

  updateLexiconEvent: (
    projectId: string,
    eventName: string,
    body: { status: string; description?: string; owner?: string },
  ) =>
    fetchJson<LexiconEvent>(
      `/v1/projects/${encodeURIComponent(projectId)}/lexicon/events/${encodeURIComponent(eventName)}`,
      {
        method: "PUT",
        body: JSON.stringify(body),
      },
    ),

  updateLexiconProp: (
    projectId: string,
    propName: string,
    body: { pii_masked: boolean; status?: string },
  ) =>
    fetchJson<LexiconProperty>(
      `/v1/projects/${encodeURIComponent(projectId)}/lexicon/properties/${encodeURIComponent(propName)}`,
      {
        method: "PUT",
        body: JSON.stringify(body),
      },
    ),

  getCohorts: (projectId: string) =>
    fetchJson<
      Array<{
        id: string;
        name: string;
        description: string;
        definition: unknown;
        created_at_ms: number;
      }>
    >(`/v1/projects/${encodeURIComponent(projectId)}/cohorts`),

  previewCohort: (projectId: string, definition: unknown) =>
    fetchJson<{ user_count: number; exec_time_us: number }>(
      `/v1/projects/${encodeURIComponent(projectId)}/cohorts/preview`,
      {
        method: "POST",
        body: JSON.stringify({ definition }),
      },
    ),

  createCohort: (projectId: string, body: { name: string; description: string; definition: unknown }) =>
    fetchJson(`/v1/projects/${encodeURIComponent(projectId)}/cohorts`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getSchemaExtensions: (projectId: string) =>
    fetchJson<{
      custom_events: Array<{ name: string; source_events: string[] }>;
      custom_properties: Array<{ name: string; sql_expr: string }>;
      lookup_tables: Array<{
        id: string;
        name: string;
        join_prop: string;
        row_count: number;
        sample_columns: string[];
      }>;
    }>(`/v1/projects/${encodeURIComponent(projectId)}/schema-extensions`),

  uploadLookupTable: (
    projectId: string,
    body: { name: string; join_prop: string; csv_text?: string; entries?: unknown },
  ) =>
    fetchJson(`/v1/projects/${encodeURIComponent(projectId)}/lookup-tables`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  createCustomProperty: (projectId: string, body: { name: string; sql_expr: string }) =>
    fetchJson(`/v1/projects/${encodeURIComponent(projectId)}/custom-properties`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getFlags: (projectId: string) =>
    fetchJson<FeatureFlag[]>(`/v1/projects/${encodeURIComponent(projectId)}/flags`),

  patchFlag: (
    projectId: string,
    flagId: string,
    body: { enabled?: boolean; rollout_pct?: number },
  ) =>
    fetchJson<FeatureFlag>(
      `/v1/projects/${encodeURIComponent(projectId)}/flags/${encodeURIComponent(flagId)}`,
      {
        method: "PATCH",
        body: JSON.stringify(body),
      },
    ),

  createFlag: (
    projectId: string,
    body: { key: string; description: string; rollout_pct: number },
  ) =>
    fetchJson<FeatureFlag>(`/v1/projects/${encodeURIComponent(projectId)}/flags`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getExperiments: (projectId: string) =>
    fetchJson<Experiment[]>(`/v1/projects/${encodeURIComponent(projectId)}/experiments`),

  getExperimentResults: (projectId: string, expId: string) =>
    fetchJson<ExperimentResults>(
      `/v1/projects/${encodeURIComponent(projectId)}/experiments/${encodeURIComponent(expId)}/results`,
    ),

  callMcp: (method: string, params: unknown = {}) =>
    fetchJson<{
      jsonrpc: string;
      id: number;
      result?: unknown;
      error?: { code: number; message: string };
    }>("/v1/mcp", {
      method: "POST",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: Date.now(),
        method,
        params,
      }),
    }),
};
