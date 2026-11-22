-- What was decided when an experiment was concluded (outcome, reason, what happened to the flag).
ALTER TABLE experiments ADD COLUMN decision_json TEXT;
