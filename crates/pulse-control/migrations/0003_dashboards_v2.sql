ALTER TABLE dashboards ADD COLUMN description TEXT;
ALTER TABLE dashboards ADD COLUMN cards_json TEXT;
ALTER TABLE dashboards ADD COLUMN created_by TEXT;
ALTER TABLE dashboards ADD COLUMN updated_at_ms BIGINT;
ALTER TABLE saved_reports ADD COLUMN updated_at_ms BIGINT;
