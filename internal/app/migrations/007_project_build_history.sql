-- Latest build and bounded per-project history lookups.
CREATE INDEX IF NOT EXISTS idx_builds_project_history ON builds(project_id, id DESC);
