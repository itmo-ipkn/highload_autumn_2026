BEGIN;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS processing_order BIGINT
    CHECK (processing_order > 0);
CREATE UNIQUE INDEX IF NOT EXISTS processed_events_user_order
    ON processed_events (user_id, processing_order) WHERE processing_order IS NOT NULL;
CREATE TABLE IF NOT EXISTS user_processing_state (
    user_id TEXT PRIMARY KEY,
    last_processing_order BIGINT NOT NULL DEFAULT 0 CHECK (last_processing_order >= 0)
);
COMMIT;
