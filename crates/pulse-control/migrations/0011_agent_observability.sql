-- AI Agent Observability & Evaluation Control-Plane Schema (SQLite WAL + PostgreSQL compatible).
-- Trace/span payloads live exclusively in the binary AgentTraceStore (PTWL/PLSS on ObjectStore).

CREATE TABLE IF NOT EXISTS agent_evaluations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT 'trace',
    session_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT,
    span_id TEXT,
    experiment_run_id TEXT,
    evaluator_id TEXT NOT NULL,
    evaluator_name TEXT NOT NULL,
    evaluator_type TEXT NOT NULL DEFAULT 'deterministic',
    evaluator_version TEXT NOT NULL DEFAULT '1.0.0',
    score DOUBLE PRECISION NOT NULL,
    score_type TEXT NOT NULL DEFAULT 'numeric',
    passed INTEGER,
    label TEXT,
    reasoning TEXT,
    execution_ms BIGINT NOT NULL DEFAULT 0,
    error TEXT,
    metadata_json TEXT NOT NULL DEFAULT 'null',
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_trace
    ON agent_evaluations (project_id, trace_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_session
    ON agent_evaluations (project_id, session_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_span
    ON agent_evaluations (project_id, span_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_target
    ON agent_evaluations (project_id, target_type, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_ts
    ON agent_evaluations (project_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_proj_exp_run
    ON agent_evaluations (project_id, experiment_run_id);

CREATE TABLE IF NOT EXISTS agent_eval_rollups (
    project_id TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    avg_score DOUBLE PRECISION NOT NULL,
    pass_rate DOUBLE PRECISION,
    eval_count BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, target_type, target_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_eval_rollups_proj
    ON agent_eval_rollups (project_id, target_type, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS agent_evaluator_configs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    target_type TEXT NOT NULL DEFAULT 'trace',
    evaluator_type TEXT NOT NULL DEFAULT 'deterministic',
    kind TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0.0',
    enabled INTEGER NOT NULL DEFAULT 1,
    agent_name TEXT,
    environment TEXT,
    sample_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_eval_cfg_proj
    ON agent_evaluator_configs (project_id, enabled);

CREATE TABLE IF NOT EXISTS agent_datasets (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    tags_json TEXT NOT NULL DEFAULT '[]',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_datasets_proj
    ON agent_datasets (project_id, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS agent_dataset_items (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    dataset_version INTEGER NOT NULL DEFAULT 1,
    name TEXT,
    input_json TEXT NOT NULL DEFAULT 'null',
    expected_output_json TEXT NOT NULL DEFAULT 'null',
    expected_behavior_json TEXT NOT NULL DEFAULT 'null',
    source_trace_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT 'null',
    tags_json TEXT NOT NULL DEFAULT '[]',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_dataset_items_ds
    ON agent_dataset_items (project_id, dataset_id, dataset_version);

CREATE TABLE IF NOT EXISTS agent_experiments (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    dataset_id TEXT NOT NULL,
    dataset_version INTEGER NOT NULL DEFAULT 1,
    agent_name TEXT NOT NULL,
    agent_version TEXT NOT NULL DEFAULT 'unversioned',
    prompt_version TEXT,
    model TEXT,
    evaluator_ids_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'completed',
    total_items INTEGER NOT NULL DEFAULT 0,
    completed_items INTEGER NOT NULL DEFAULT 0,
    passed_items INTEGER NOT NULL DEFAULT 0,
    avg_score DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    total_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    metadata_json TEXT NOT NULL DEFAULT 'null',
    created_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_agent_experiments_proj
    ON agent_experiments (project_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS agent_experiment_runs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    experiment_id TEXT NOT NULL,
    dataset_item_id TEXT NOT NULL,
    trace_id TEXT,
    status TEXT NOT NULL DEFAULT 'ok',
    actual_output_json TEXT NOT NULL DEFAULT 'null',
    latency_ms BIGINT NOT NULL DEFAULT 0,
    cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    avg_score DOUBLE PRECISION,
    passed INTEGER,
    error TEXT,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_exp_runs_exp
    ON agent_experiment_runs (project_id, experiment_id);

CREATE TABLE IF NOT EXISTS agent_alert_rules (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    agent_name TEXT,
    environment TEXT,
    metric TEXT NOT NULL,
    operator TEXT NOT NULL DEFAULT 'gt',
    threshold DOUBLE PRECISION NOT NULL,
    window_minutes INTEGER NOT NULL DEFAULT 15,
    cooldown_minutes INTEGER NOT NULL DEFAULT 30,
    enabled INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'ok',
    webhook_url TEXT,
    last_evaluated_ms BIGINT,
    last_triggered_ms BIGINT,
    last_resolved_ms BIGINT,
    last_value DOUBLE PRECISION,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_alert_rules_proj
    ON agent_alert_rules (project_id, enabled);

CREATE TABLE IF NOT EXISTS agent_alert_events (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    metric TEXT NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    delivery_error TEXT,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_alert_events_proj
    ON agent_alert_events (project_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS agent_privacy_policies (
    project_id TEXT PRIMARY KEY,
    capture_inputs INTEGER NOT NULL DEFAULT 1,
    capture_outputs INTEGER NOT NULL DEFAULT 1,
    capture_tool_args INTEGER NOT NULL DEFAULT 1,
    capture_tool_results INTEGER NOT NULL DEFAULT 1,
    capture_prompts INTEGER NOT NULL DEFAULT 1,
    capture_responses INTEGER NOT NULL DEFAULT 1,
    capture_user_id INTEGER NOT NULL DEFAULT 0,
    redact_pii INTEGER NOT NULL DEFAULT 1,
    redact_keys_json TEXT NOT NULL DEFAULT '["password","secret","token","api_key","authorization","cookie","ssn","credit_card"]',
    exclude_attributes_json TEXT NOT NULL DEFAULT '[]',
    retention_days BIGINT NOT NULL DEFAULT 30,
    sample_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_model_pricing (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_pattern TEXT NOT NULL,
    input_per_1m_usd DOUBLE PRECISION NOT NULL,
    output_per_1m_usd DOUBLE PRECISION NOT NULL,
    cached_input_per_1m_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_pricing_proj
    ON agent_model_pricing (project_id);

CREATE TABLE IF NOT EXISTS agent_erasure_tombstones (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    level TEXT NOT NULL,
    target_id TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_agent_erasure_proj_exp
    ON agent_erasure_tombstones (project_id, expires_at_ms);
CREATE INDEX IF NOT EXISTS idx_agent_erasure_proj_target
    ON agent_erasure_tombstones (project_id, level, target_id);
