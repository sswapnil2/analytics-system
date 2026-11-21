-- How an experiment is analysed (sequential testing, alpha, planned sample). NULL on rows
-- created before this column existed; those read as fixed-horizon at 5%.
ALTER TABLE experiments ADD COLUMN analysis_json TEXT;
