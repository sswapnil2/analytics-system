-- Secondary metrics (reported, never decide), who created an experiment, and a holdout's saved impact metric.
ALTER TABLE experiments ADD COLUMN secondary_metrics_json TEXT;
ALTER TABLE experiments ADD COLUMN created_by TEXT;
ALTER TABLE traffic_groups ADD COLUMN metric_event TEXT;
