-- Experiment kind ('ab' or 'bandit') and bandit settings. NULL kind reads as 'ab'.
ALTER TABLE experiments ADD COLUMN kind TEXT;
ALTER TABLE experiments ADD COLUMN bandit_json TEXT;

-- Every set of variant weights a bandit has used, so results can be stratified by the weights
-- users were assigned under and the Studio can chart how traffic moved.
CREATE TABLE IF NOT EXISTS experiment_weight_history (
    experiment_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    effective_from_ms BIGINT NOT NULL,
    weights_json TEXT NOT NULL,
    PRIMARY KEY (experiment_id, effective_from_ms)
);
