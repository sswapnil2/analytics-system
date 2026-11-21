-- The problem an experiment addresses, who owns it, and whether its milestones go to the project webhook.
ALTER TABLE experiments ADD COLUMN problem TEXT;
ALTER TABLE experiments ADD COLUMN owner TEXT;
ALTER TABLE experiments ADD COLUMN notify INTEGER NOT NULL DEFAULT 1;
