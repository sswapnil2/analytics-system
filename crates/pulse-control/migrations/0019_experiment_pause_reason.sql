-- Why an experiment was paused; cleared on resume.
ALTER TABLE experiments ADD COLUMN pause_reason TEXT;
