-- Track owning `node_name` on `writer_leases` so a newly designated `project_owner`
-- can immediately take over a lease left by a deposed node while still fencing
-- same-node split-brain (`HeldByOther`).
ALTER TABLE writer_leases ADD COLUMN owner_node_name TEXT;
