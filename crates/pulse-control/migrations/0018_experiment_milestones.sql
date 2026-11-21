-- Experiment milestones already announced (significance, planned sample, guardrail problems), so each is sent once.
CREATE TABLE IF NOT EXISTS experiment_milestones (
    project_id TEXT NOT NULL,
    experiment_id TEXT NOT NULL,
    milestone TEXT NOT NULL,
    sent_at_ms BIGINT NOT NULL,
    PRIMARY KEY (experiment_id, milestone)
);
