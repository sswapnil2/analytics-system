-- Why an experiment stopped when Pulse stopped it (a guardrail got worse). NULL when a person
-- stopped it, or it is still running.
ALTER TABLE experiments ADD COLUMN stop_reason TEXT;
