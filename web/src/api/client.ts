export interface ProjectInfo {
  id: string;
  org_id: string;
  name: string;
  timezone: string;
  created_at_ms: number;
}

export interface Annotation {
  id: string;
  project_id: string;
  ts_ms: number;
  text: string;
  created_by: string | null;
  created_at_ms: number;
}

export interface ApiKeyRecord {
  id: string;
  key_prefix: string;
  project_id: string;
  label: string;
  role: string;
  created_at_ms: number;
  revoked: boolean;
}

export interface CreatedApiKeyResponse {
  record: ApiKeyRecord;
  plaintext_key: string;
}

export interface NodeStatus {
  status: string;
  default_project_id: string;
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

export type SamplingMode = "auto" | "exact" | { rate: number };

export interface SamplingInfo {
  is_sampled: boolean;
  sample_rate: number;
  sampled_buckets: number;
  total_buckets: number;
  scale_factor: number;
  estimated_total_rows: number;
  relative_error_pct?: number;
  method?: "bucket_sample" | "hll_sketch" | string;
}

export interface InsightsResult {
  snapshot_epoch: number;
  exec_time_us: number;
  buckets_ts_ms: number[];
  series: SeriesOutput[];
  previous_series: SeriesOutput[];
  formula_series: SeriesOutput[];
  sampling?: SamplingInfo;
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
  has_replay?: boolean;
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
  sampling?: SamplingInfo;
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
  sampling?: SamplingInfo;
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
  sampling?: SamplingInfo;
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
  has_replay?: boolean;
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
  | { op: "full_snapshot"; ts_offset_ms?: number; width: number; height: number; nodes: WireNode[] }
  | ({ op: "node_upsert"; ts_offset_ms?: number } & WireNode)
  | { op: "node_remove"; ts_offset_ms?: number; node_id: number }
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

export interface FlagPatch {
  enabled?: boolean;
  rollout_pct?: number;
  /** A cohort id, or "" for everyone. */
  targeting_cohort_id?: string;
  variants?: { key: string; weight_bps: number }[];
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
  primary_metric: ExperimentMetric;
  guardrail_metrics: ExperimentMetric[];
  analysis?: ExperimentAnalysis;
  /** Set when Pulse stopped the experiment itself, e.g. a guardrail got worse. */
  stop_reason?: string | null;
  /** "ab" (fixed split, p-values) or "bandit" (traffic moves daily to the best variant). */
  kind?: ExperimentKind;
  bandit?: BanditConfig | null;
}

export type ExperimentKind = "ab" | "bandit";

export interface BanditConfig {
  /** No variant drops below this share (basis points). */
  min_weight_bps: number;
  /** Weights stay put until every variant has this many users. */
  burn_in_users: number;
}

export interface BanditResults {
  arms: {
    variant_key: string;
    users: number;
    mean_value: number;
    ci_low: number;
    ci_high: number;
    prob_best: number;
    /** Expected shortfall versus the best variant, in metric units. */
    expected_loss: number;
    weight_bps: number;
  }[];
  weight_history: { effective_from_ms: number; weights: Record<string, number> }[];
  best_variant: string | null;
  burn_in_complete: boolean;
  ready_to_ship: boolean;
}

export type MetricDirection = "decrease_is_bad" | "increase_is_bad";

export type MetricType = "conversion" | "count" | "sum" | "ratio";

export interface ExperimentMetric {
  name: string;
  event_name: string;
  metric_type?: MetricType | "continuous";
  /** Summed per user for `sum`; the numerator total for `ratio` (else a count). */
  numeric_prop?: string | null;
  /** Required for `ratio`: a count of `event_name`, or the total of `numeric_prop`. */
  denominator?: { event_name: string; numeric_prop?: string | null } | null;
  conversion_window_ms: number;
  /** Which way is bad, for guardrails. */
  direction?: MetricDirection;
  /** Guardrails: roll everyone back to control when a treatment makes this significantly worse. */
  auto_stop?: boolean;
}

export interface ExperimentAnalysis {
  /** Always-valid results, safe to check at any time. */
  sequential: boolean;
  alpha: number;
  planned_users_per_variant?: number | null;
  /** Adjust for each user's pre-exposure value of the metric (CUPED). */
  cuped?: boolean;
  cuped_pre_window_ms?: number;
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
  abs_diff?: number;
  abs_ci_low?: number;
  abs_ci_high?: number;
  sequential?: boolean;
}

export interface ExperimentResults {
  experiment_id: string;
  experiment_name: string;
  flag_key: string;
  status: string;
  hypothesis: string;
  metric_name: string;
  exec_time_us: number;
  srm_p_value: number;
  /** The traffic split doesn't match the flag's weights, so results can't be trusted. */
  sample_ratio_mismatch: boolean;
  statistical_methodology_note: string;
  sequential: boolean;
  /** Confidence level of every interval, e.g. 0.95. */
  confidence_level: number;
  variants: VariantLiftRow[];
  guardrails: {
    metric_name: string;
    event_name: string;
    direction: MetricDirection;
    regressed_variants: string[];
    metric_type?: MetricType;
    variants: VariantLiftRow[];
  }[];
  metric_type: MetricType;
  cuped_applied: boolean;
  variance_reduction_pct: number;
  /** Users who saw more than one variant, left out of the results. */
  multi_variant_users: number;
  segment_by: string | null;
  /** Exploratory: not corrected across segments. */
  segments: { segment: string; variants: VariantLiftRow[] }[];
  /** Cumulative lift by exposure day. */
  timeseries: {
    day_start_ms: number;
    variants: {
      variant_key: string;
      is_control: boolean;
      users: number;
      mean_value: number;
      relative_lift_pct: number;
      ci_low_pct: number;
      ci_high_pct: number;
    }[];
  }[];
  kind?: ExperimentKind;
  /** Bandits only; their `variants` and `guardrails` are empty. */
  bandit?: BanditResults | null;
}

export const ADMIN_TOKEN_STORAGE_KEY = "pulse_admin_bearer_token";
/** Non-secret hint that an HttpOnly `pulse_session` cookie is active (used for boot & cross-tab sync). */
export const SESSION_HINT_STORAGE_KEY = "pulse_has_session";

// User sessions (`ps_` tokens) are held exclusively in the server-set HttpOnly `pulse_session` cookie
// (never persisted to JS-readable localStorage). Pasted admin tokens stay in sessionStorage and die with the tab.
function storages(): Storage[] {
  try {
    return [window.sessionStorage, window.localStorage];
  } catch {
    return [];
  }
}

export function getStoredAdminToken(): string {
  for (const st of storages()) {
    try {
      const v = st.getItem(ADMIN_TOKEN_STORAGE_KEY);
      if (v && !v.startsWith("ps_")) return v;
    } catch {
      // storage blocked
    }
  }
  return "";
}

export function hasCookieSessionHint(): boolean {
  try {
    return window.localStorage.getItem(SESSION_HINT_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

export function hasStoredSession(): boolean {
  return Boolean(getStoredAdminToken()) || hasCookieSessionHint();
}

export function setStoredAdminToken(token: string): void {
  const value = token.trim();
  for (const st of storages()) {
    try {
      st.removeItem(ADMIN_TOKEN_STORAGE_KEY);
    } catch {
      // storage blocked
    }
  }
  if (!value) {
    try {
      window.localStorage.removeItem(SESSION_HINT_STORAGE_KEY);
    } catch {
      // storage blocked
    }
    return;
  }
  if (value.startsWith("ps_")) {
    // Secret session token lives in HttpOnly `pulse_session` cookie; only store a non-secret boolean hint.
    try {
      window.localStorage.setItem(SESSION_HINT_STORAGE_KEY, "1");
    } catch {
      // Ignore storage quota / sandbox restrictions
    }
    return;
  }
  try {
    window.localStorage.removeItem(SESSION_HINT_STORAGE_KEY);
    window.sessionStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, value);
  } catch {
    // Ignore storage quota / sandbox restrictions
  }
}

export type ReportKind = "funnel" | "insights" | "retention" | "flows";

export interface SavedReport {
  id: string;
  project_id: string;
  name: string;
  report_kind: ReportKind;
  query_json: Record<string, unknown>;
  created_at_ms: number;
  updated_at_ms?: number | null;
}

export interface DashboardCard {
  id?: string;
  kind: "report" | "text";
  report_id?: string;
  text?: string;
  width: 1 | 2;
}

export interface Dashboard {
  id: string;
  project_id: string;
  name: string;
  description: string | null;
  cards: DashboardCard[];
  created_by: string | null;
  created_at_ms: number;
  updated_at_ms: number | null;
}

export interface DashboardInput {
  name: string;
  description?: string | null;
  cards: DashboardCard[];
}

export interface AuthUser {
  id: string;
  email: string;
  name: string | null;
  has_password: boolean;
  has_google: boolean;
  role?: Role;
  org_id?: string;
}

export type Role = "viewer" | "member" | "admin" | "owner";
export const ROLES: Role[] = ["viewer", "member", "admin", "owner"];
/** Mirrors the server: each role can do what the ones before it can. */
export function roleAtLeast(role: Role | undefined, min: Role): boolean {
  return ROLES.indexOf(role ?? "viewer") >= ROLES.indexOf(min);
}
/** Mirrors `Role::outranks`: owners manage anyone, others only lower roles. */
export function outranks(me: Role | undefined, other: Role): boolean {
  return me === "owner" || ROLES.indexOf(other) < ROLES.indexOf(me ?? "viewer");
}

export interface OrgInfo {
  id: string;
  name: string | null;
  role: Role;
}

export interface OrgMember extends AuthUser {
  role: Role;
  created_at_ms: number;
}

export interface OrgInvite {
  id: string;
  email: string;
  role: Role;
  invited_by: string;
  created_at_ms: number;
  expires_at_ms: number;
  invite_url?: string;
}

export interface ActiveSession {
  id: string;
  user_agent: string | null;
  created_at_ms: number;
  last_seen_ms: number;
  expires_at_ms: number;
  current: boolean;
}

export interface AuthConfig {
  password: boolean;
  google: boolean;
  signup_open: boolean;
  /** No accounts yet: the first sign-up must present the root admin token. */
  setup_code_required?: boolean;
  /** Accounts exist: a new one names its organisation and gets its own projects. */
  org_name_required?: boolean;
}

export interface AuthSession {
  token: string;
  user: AuthUser;
}

export class ApiAuthError extends Error {
  public readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export interface UserProfileItem {
  project_id: string;
  distinct_id: string;
  distinct_id_masked: string;
  canonical_uid: number;
  props: Record<string, unknown>;
  total_revenue: number;
  updated_at_ms: number;
}

export interface UserEventItem {
  event_ts_ms: number;
  event_name: string;
  distinct_id: string;
  canonical_uid: number;
  session_id: string;
  props: Record<string, unknown>;
}

export interface UserDetailResponse {
  distinct_id: string;
  distinct_id_masked: string;
  canonical_uid: number;
  profile: UserProfileItem | null;
  events: UserEventItem[];
  replay_sessions: string[];
}

export interface DeletionAuditRecord {
  id: string;
  project_id: string;
  distinct_id_masked: string;
  canonical_uid: number;
  events_purged: number;
  replays_purged: number;
  requested_by: string;
  deleted_at_ms: number;
}

export interface BatchImportResponse {
  status: string;
  accepted: number;
  deduped: number;
  dropped: number;
  server_ts_ms: number;
}

export interface ByokEncryptionStatus {
  enabled: boolean;
  algorithm: string;
  active_key_id: string | null;
  key_fingerprint: string | null;
  keyring_size: number;
  encrypted_objects_count: number;
  ops_encrypted: number;
  ops_decrypted: number;
}

export interface SystemKeyringRecord {
  key_id: string;
  salt_hex: string;
  is_active: boolean;
  created_at_ms: number;
}

export interface EncryptionStatusResponse {
  status: ByokEncryptionStatus;
  keyring: SystemKeyringRecord[];
  has_durable_kek: boolean;
}

export interface EnterpriseAuditRecord {
  id: string;
  actor: string;
  action: string;
  resource: string;
  details_json: Record<string, unknown>;
  created_at_ms: number;
}

export interface ByokCredentialItem {
  id: string;
  project_id: string;
  category: "llm" | "storage" | "webhook" | string;
  provider: string;
  label: string;
  key_prefix_masked: string;
  config_json: Record<string, unknown>;
  status: string;
  last_verified_at_ms: number | null;
  updated_at_ms: number;
}

export interface UpsertByokRequest {
  category: string;
  provider: string;
  label?: string;
  secret: string;
  config_json?: Record<string, unknown>;
}

export interface TestByokResponse {
  verified: boolean;
  category: string;
  provider: string;
  details: string;
  verified_at_ms: number;
}

export interface CopilotResponse {
  provider: string;
  model: string;
  answer_markdown: string;
  root_cause_step: string | null;
  dropoff_rate_pct: number | null;
  cited_sessions: string[];
  metrics_summary: Record<string, unknown>;
  mode: string;
  exec_time_ms: number;
}

let unauthorizedHandler: (() => void) | null = null;

/** Registers a callback fired whenever a protected API call receives HTTP 401. */
export function setUnauthorizedHandler(fn: (() => void) | null): void {
  unauthorizedHandler = fn;
}

/** True when a fetch rejection came from an AbortController cancellation. */
export function isAbortError(err: unknown): boolean {
  return (err instanceof DOMException || err instanceof Error) && err.name === "AbortError";
}

// Credential-submission and auth-action endpoints where 401 means invalid credentials/token rather than an expired session.
const CREDENTIAL_AUTH_PATHS = new Set([
  "/v1/auth/login",
  "/v1/auth/signup",
  "/v1/auth/password",
  "/v1/auth/password-reset",
  "/v1/auth/exchange",
  "/v1/auth/verify-email",
  "/v1/auth/invites/accept",
  "/v1/auth/join-org",
  "/v1/auth/logout",
]);

function notifyIfSessionExpired(url: string, status: number, hadToken: boolean): void {
  if (status !== 401 || !hadToken) return;
  const path = url.split("?")[0];
  if (CREDENTIAL_AUTH_PATHS.has(path)) return;
  unauthorizedHandler?.();
}

export function projectPath(projectId: string, suffix = ""): string {
  return `/v1/projects/${encodeURIComponent(projectId)}${suffix}`;
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const token = getStoredAdminToken();
  const hadAuth = Boolean(token) || hasCookieSessionHint();
  const authHeaders: Record<string, string> = token
    ? { Authorization: `Bearer ${token}` }
    : {};
  const res = await fetch(url, {
    credentials: "same-origin",
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...authHeaders,
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    if (res.status === 401 || res.status === 403) {
      notifyIfSessionExpired(url, res.status, hadAuth);
      throw new ApiAuthError(res.status, text || `HTTP ${res.status}`);
    }
    throw new Error(text || `HTTP ${res.status}`);
  }
  const body = await res.text();
  if (!body) return null as T;
  try {
    return JSON.parse(body) as T;
  } catch {
    throw new Error("Server returned an unexpected response format.");
  }
}

async function fetchText(url: string, init?: RequestInit): Promise<string> {
  const token = getStoredAdminToken();
  const hadAuth = Boolean(token) || hasCookieSessionHint();
  const authHeaders: Record<string, string> = token
    ? { Authorization: `Bearer ${token}` }
    : {};
  const res = await fetch(url, {
    credentials: "same-origin",
    ...init,
    headers: {
      ...authHeaders,
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    if (res.status === 401 || res.status === 403) {
      notifyIfSessionExpired(url, res.status, hadAuth);
      throw new ApiAuthError(res.status, text || `HTTP ${res.status}`);
    }
    throw new Error(text || `HTTP ${res.status}`);
  }
  return res.text();
}

export function default14DayRange(): TimeRange {
  const now = Date.now();
  return {
    start_ts_ms: now - 14 * 86_400_000,
    end_ts_ms: now + 3_600_000,
  };
}

export const api = {
  getAuthConfig: () => fetchJson<AuthConfig>("/v1/auth/config"),
  signUp: (body: { email: string; password: string; name?: string; setup_code?: string; org_name?: string; project_name?: string }) =>
    // A session, or `verification_required` when the address must be proven first.
    fetchJson<AuthSession | { verification_required: true; email: string }>("/v1/auth/signup", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  verifyEmail: (token: string) =>
    fetchJson<AuthSession>("/v1/auth/verify-email", { method: "POST", body: JSON.stringify({ token }) }),
  joinOrg: (token: string) =>
    fetchJson<AuthUser>("/v1/auth/join-org", { method: "POST", body: JSON.stringify({ token }) }),
  logIn: (body: { email: string; password: string }) =>
    fetchJson<AuthSession>("/v1/auth/login", { method: "POST", body: JSON.stringify(body) }),
  exchangeLoginCode: (code: string) =>
    fetchJson<AuthSession>("/v1/auth/exchange", { method: "POST", body: JSON.stringify({ code }) }),
  getMe: () => fetchJson<AuthUser>("/v1/auth/me"),
  updateMe: (body: { name: string }) =>
    fetchJson<AuthUser>("/v1/auth/me", { method: "PATCH", body: JSON.stringify(body) }),
  changePassword: (body: { current_password: string; new_password: string }) =>
    fetchJson<{ other_sessions_signed_out: number }>("/v1/auth/password", { method: "POST", body: JSON.stringify(body) }),
  listSessions: () => fetchJson<ActiveSession[]>("/v1/auth/sessions"),
  revokeSession: (id: string) =>
    fetchJson<unknown>(`/v1/auth/sessions/${encodeURIComponent(id)}`, { method: "DELETE" }),
  revokeOtherSessions: () => fetchJson<{ signed_out: number }>("/v1/auth/sessions/revoke-others", { method: "POST" }),
  getOrg: () => fetchJson<OrgInfo>("/v1/org"),
  renameOrg: (name: string) => fetchJson<OrgInfo>("/v1/org", { method: "PATCH", body: JSON.stringify({ name }) }),
  listMembers: () => fetchJson<OrgMember[]>("/v1/org/members"),
  setMemberRole: (userId: string, role: Role) =>
    fetchJson<unknown>(`/v1/org/members/${encodeURIComponent(userId)}`, { method: "PATCH", body: JSON.stringify({ role }) }),
  removeMember: (userId: string) =>
    fetchJson<unknown>(`/v1/org/members/${encodeURIComponent(userId)}`, { method: "DELETE" }),
  memberResetLink: (userId: string) =>
    fetchJson<{ reset_url: string }>(`/v1/org/members/${encodeURIComponent(userId)}/password-reset`, { method: "POST" }),
  listInvites: () => fetchJson<OrgInvite[]>("/v1/org/invites"),
  createInvite: (body: { email: string; role: Role }) =>
    fetchJson<OrgInvite>("/v1/org/invites", { method: "POST", body: JSON.stringify(body) }),
  deleteInvite: (id: string) => fetchJson<unknown>(`/v1/org/invites/${encodeURIComponent(id)}`, { method: "DELETE" }),
  getInvite: (token: string) =>
    fetchJson<{ email: string; role: Role; org_name: string | null; has_account: boolean }>(
      `/v1/auth/invites/${encodeURIComponent(token)}`,
    ),
  acceptInvite: (body: { token: string; password: string; name?: string }) =>
    fetchJson<AuthSession>("/v1/auth/invites/accept", { method: "POST", body: JSON.stringify(body) }),
  requestPasswordReset: (email: string) =>
    fetchJson<unknown>("/v1/auth/password-reset/request", { method: "POST", body: JSON.stringify({ email }) }),
  resetPassword: (body: { token: string; new_password: string }) =>
    fetchJson<unknown>("/v1/auth/password-reset", { method: "POST", body: JSON.stringify(body) }),
  logOut: () => fetchJson<unknown>("/v1/auth/logout", { method: "POST" }),
  listReports: (projectId: string) =>
    fetchJson<SavedReport[]>(`/v1/projects/${encodeURIComponent(projectId)}/saved-reports`),
  createReport: (projectId: string, body: { name: string; report_kind: ReportKind; query_json: unknown }) =>
    fetchJson<SavedReport>(`/v1/projects/${encodeURIComponent(projectId)}/saved-reports`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateReport: (projectId: string, reportId: string, body: { name: string; query_json: unknown }) =>
    fetchJson<SavedReport>(
      `/v1/projects/${encodeURIComponent(projectId)}/saved-reports/${encodeURIComponent(reportId)}`,
      { method: "PUT", body: JSON.stringify(body) },
    ),
  deleteReport: (projectId: string, reportId: string) =>
    fetchJson<unknown>(
      `/v1/projects/${encodeURIComponent(projectId)}/saved-reports/${encodeURIComponent(reportId)}`,
      { method: "DELETE" },
    ),
  listDashboards: (projectId: string) =>
    fetchJson<Dashboard[]>(`/v1/projects/${encodeURIComponent(projectId)}/dashboards`),
  getDashboard: (projectId: string, id: string) =>
    fetchJson<Dashboard>(
      `/v1/projects/${encodeURIComponent(projectId)}/dashboards/${encodeURIComponent(id)}`,
    ),
  createDashboard: (projectId: string, body: DashboardInput) =>
    fetchJson<Dashboard>(`/v1/projects/${encodeURIComponent(projectId)}/dashboards`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateDashboard: (projectId: string, id: string, body: DashboardInput) =>
    fetchJson<Dashboard>(
      `/v1/projects/${encodeURIComponent(projectId)}/dashboards/${encodeURIComponent(id)}`,
      { method: "PUT", body: JSON.stringify(body) },
    ),
  deleteDashboard: (projectId: string, id: string) =>
    fetchJson<unknown>(
      `/v1/projects/${encodeURIComponent(projectId)}/dashboards/${encodeURIComponent(id)}`,
      { method: "DELETE" },
    ),


  listAnnotations: (projectId: string, startMs: number, endMs: number) =>
    fetchJson<Annotation[]>(
      `/v1/projects/${encodeURIComponent(projectId)}/annotations?start_ts_ms=${startMs}&end_ts_ms=${endMs}`,
    ),
  createAnnotation: (projectId: string, body: { ts_ms: number; text: string }) =>
    fetchJson<Annotation>(`/v1/projects/${encodeURIComponent(projectId)}/annotations`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  deleteAnnotation: (projectId: string, id: string) =>
    fetchJson<unknown>(`/v1/projects/${encodeURIComponent(projectId)}/annotations/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),

  updateProject: (projectId: string, body: { name?: string; timezone?: string }) =>
    fetchJson<ProjectInfo>(`/v1/projects/${encodeURIComponent(projectId)}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),

  createProject: (body: { name: string; timezone: string }) =>
    fetchJson<{ project: ProjectInfo; api_key: CreatedApiKeyResponse }>(`/v1/projects`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getStatus: (projectId?: string) =>
    fetchJson<NodeStatus>(
      projectId ? `/v1/status?project_id=${encodeURIComponent(projectId)}` : "/v1/status",
    ),

  listApiKeys: (projectId: string) =>
    fetchJson<ApiKeyRecord[]>(`/v1/projects/${encodeURIComponent(projectId)}/api-keys`),

  revokeApiKey: (projectId: string, keyId: string) =>
    fetchJson<unknown>(`/v1/projects/${encodeURIComponent(projectId)}/api-keys/${encodeURIComponent(keyId)}`, {
      method: "DELETE",
    }),

  createApiKey: (projectId: string, body: { label: string; role?: string }) =>
    fetchJson<CreatedApiKeyResponse>(`/v1/projects/${encodeURIComponent(projectId)}/api-keys`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  flushStorage: () =>
    fetchJson<{
      status: string;
      segments_flushed: number;
      partitions_compacted: number;
    }>("/v1/admin/flush", {
      method: "POST",
    }),

  queryInsights: (projectId: string, body: unknown, signal?: AbortSignal) =>
    fetchJson<InsightsResult>(projectPath(projectId, "/query/insights"), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),

  /** CSV of every user who stopped before funnel step `step` (0-based index of the step not reached). */
  exportFunnelDropped: (projectId: string, body: unknown, step: number) =>
    fetchText(projectPath(projectId, `/query/funnel/dropped?step=${step}`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  queryFunnel: (projectId: string, body: unknown, signal?: AbortSignal) =>
    fetchJson<FunnelResult>(projectPath(projectId, "/query/funnel"), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),

  queryFlows: (projectId: string, body: unknown, signal?: AbortSignal) =>
    fetchJson<FlowsResult>(projectPath(projectId, "/query/flows"), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),

  queryRetention: (projectId: string, body: unknown, signal?: AbortSignal) =>
    fetchJson<RetentionResult>(projectPath(projectId, "/query/retention"), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),

  searchSessions: (projectId: string, body: unknown, signal?: AbortSignal) =>
    fetchJson<SessionSearchResult>(projectPath(projectId, "/sessions"), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),

  getReplay: (projectId: string, sessionId: string, offsetKb = 0, signal?: AbortSignal) =>
    fetchJson<ReplayStreamResponse>(
      projectPath(projectId, `/replay/${encodeURIComponent(sessionId)}?offset_kb=${offsetKb}`),
      { signal },
    ),

  getLexicon: (projectId: string, signal?: AbortSignal) =>
    fetchJson<{ events: LexiconEvent[]; properties: LexiconProperty[] }>(
      projectPath(projectId, "/lexicon"),
      { signal },
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

  previewCohort: (projectId: string, definition: unknown, signal?: AbortSignal) =>
    fetchJson<{ user_count: number; exec_time_us: number }>(
      projectPath(projectId, "/cohorts/preview"),
      {
        method: "POST",
        body: JSON.stringify({ definition }),
        signal,
      },
    ),

  /** Saves exactly the users who didn't reach `step` as a fixed cohort. */
  cohortFromFunnelDropped: (projectId: string, query: unknown, step: number, name: string, description: string) =>
    fetchJson<{ id: string }>(
      `/v1/projects/${encodeURIComponent(projectId)}/cohorts/from-funnel-dropped?step=${step}`,
      { method: "POST", body: JSON.stringify({ name, description, query }) },
    ),

  createCohort: (projectId: string, body: { name: string; description: string; definition: unknown }) =>
    fetchJson(`/v1/projects/${encodeURIComponent(projectId)}/cohorts`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  updateCohort: (projectId: string, cohortId: string, body: { name: string; description: string; definition: unknown }) =>
    fetchJson(projectPath(projectId, `/cohorts/${encodeURIComponent(cohortId)}`), {
      method: "PUT",
      body: JSON.stringify(body),
    }),

  /** Refused with 409 while a cohort, flag, saved report or dashboard still uses it. */
  deleteCohort: (projectId: string, cohortId: string) =>
    fetchJson(projectPath(projectId, `/cohorts/${encodeURIComponent(cohortId)}`), { method: "DELETE" }),

  getSchemaExtensions: (projectId: string, signal?: AbortSignal) =>
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
      /** Profile property keys; use them as `user.<key>` in filters and splits. */
      user_properties?: string[];
    }>(projectPath(projectId, "/schema-extensions"), { signal }),

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
    body: FlagPatch,
  ) =>
    fetchJson<FeatureFlag>(
      `/v1/projects/${encodeURIComponent(projectId)}/flags/${encodeURIComponent(flagId)}`,
      {
        method: "PATCH",
        body: JSON.stringify(body),
      },
    ),

  /** Stop (`concluded`), pause or resume an experiment. */
  patchExperiment: (projectId: string, expId: string, body: { status?: "running" | "paused" | "concluded"; end_ts_ms?: number }) =>
    fetchJson<Experiment>(
      `/v1/projects/${encodeURIComponent(projectId)}/experiments/${encodeURIComponent(expId)}`,
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  createExperiment: (
    projectId: string,
    body: {
      name: string;
      flag_key: string;
      flag_id: string;
      hypothesis: string;
      start_ts_ms: number;
      end_ts_ms: number;
      primary_metric: ExperimentMetric;
      guardrail_metrics: ExperimentMetric[];
      analysis: ExperimentAnalysis;
      kind?: ExperimentKind;
      bandit?: BanditConfig;
    },
  ) =>
    fetchJson<Experiment>(`/v1/projects/${encodeURIComponent(projectId)}/experiments`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

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

  getExperimentResults: (projectId: string, expId: string, segment?: string) =>
    fetchJson<ExperimentResults>(
      `/v1/projects/${encodeURIComponent(projectId)}/experiments/${encodeURIComponent(expId)}/results` +
        (segment ? `?segment=${encodeURIComponent(segment)}` : ""),
    ),

  listUsers: (projectId: string, search = "", limit = 100, offset = 0, signal?: AbortSignal) =>
    fetchJson<UserProfileItem[]>(
      projectPath(projectId, `/users?search=${encodeURIComponent(search)}&limit=${limit}&offset=${offset}`),
      { signal },
    ),

  getUserDetail: (projectId: string, distinctId: string, canonicalUid?: number, signal?: AbortSignal) =>
    fetchJson<UserDetailResponse>(
      projectPath(projectId, `/users/${encodeURIComponent(distinctId)}${canonicalUid ? `?uid=${canonicalUid}` : ""}`),
      { signal },
    ),

  deleteUserDpdp: (projectId: string, distinctId: string) =>
    fetchJson<DeletionAuditRecord>(
      projectPath(projectId, `/users/${encodeURIComponent(distinctId)}`),
      { method: "DELETE" },
    ),

  listDeletionAudit: (projectId: string, signal?: AbortSignal) =>
    fetchJson<DeletionAuditRecord[]>(projectPath(projectId, "/deletions"), { signal }),

  exportCohortCsv: (projectId: string, cohortId: string) =>
    fetchText(
      projectPath(projectId, `/cohorts/${encodeURIComponent(cohortId)}/export?format=csv`),
    ),

  importEvents: (
    projectId: string,
    body: string,
    contentType: "application/json" | "application/x-ndjson" | "text/csv",
  ) =>
    fetchJson<BatchImportResponse>(projectPath(projectId, "/import"), {
      method: "POST",
      headers: { "Content-Type": contentType },
      body,
    }),

  exportRawEvents: (
    projectId: string,
    format: "ndjson" | "csv" | "json" = "ndjson",
    limit = 2000,
  ) =>
    fetchText(
      projectPath(projectId, `/export?format=${encodeURIComponent(format)}&limit=${limit}`),
    ),

  getEncryptionStatus: () =>
    fetchJson<EncryptionStatusResponse>("/v1/system/encryption"),

  rotateEncryptionKey: (body: {
    secret?: string;
    disable_new_writes?: boolean;
  }) =>
    fetchJson<EncryptionStatusResponse>("/v1/system/encryption/rotate", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  listAuditLogs: (limit = 100) =>
    fetchJson<EnterpriseAuditRecord[]>(`/v1/audit-logs?limit=${limit}`),

  listByokCredentials: (projectId: string) =>
    fetchJson<ByokCredentialItem[]>(
      `/v1/projects/${encodeURIComponent(projectId)}/byok`,
    ),

  upsertByokCredential: (projectId: string, body: UpsertByokRequest) =>
    fetchJson<ByokCredentialItem>(
      `/v1/projects/${encodeURIComponent(projectId)}/byok`,
      {
        method: "PUT",
        body: JSON.stringify(body),
      },
    ),

  deleteByokCredential: (projectId: string, category: string) =>
    fetchJson<{ status: string; category: string }>(
      `/v1/projects/${encodeURIComponent(projectId)}/byok/${encodeURIComponent(category)}`,
      { method: "DELETE" },
    ),

  verifyByokCredential: (projectId: string, category: string) =>
    fetchJson<TestByokResponse>(
      `/v1/projects/${encodeURIComponent(projectId)}/byok-verify`,
      {
        method: "POST",
        body: JSON.stringify({ category }),
      },
    ),

  exportWarehouse: (
    projectId: string,
    opts?: { limit?: number; start_ts_ms?: number; end_ts_ms?: number },
  ) =>
    fetchJson<{
      status: string;
      destination_url: string;
      object_key: string;
      events_exported: number;
      bytes_written: number;
    }>(
      `/v1/projects/${encodeURIComponent(projectId)}/export/warehouse`,
      {
        method: "POST",
        body: JSON.stringify(opts ?? {}),
      },
    ),

  askByokCopilot: (projectId: string, prompt: string) =>
    fetchJson<CopilotResponse>(
      `/v1/projects/${encodeURIComponent(projectId)}/ai/copilot`,
      {
        method: "POST",
        body: JSON.stringify({ prompt }),
      },
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
