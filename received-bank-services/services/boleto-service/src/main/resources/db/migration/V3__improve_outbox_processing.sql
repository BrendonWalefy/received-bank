ALTER TABLE outbox_events
    ADD COLUMN next_attempt_at TIMESTAMP,
    ADD COLUMN processing_started_at TIMESTAMP;

UPDATE outbox_events
SET next_attempt_at = created_at
WHERE status = 'PENDING'
  AND next_attempt_at IS NULL;

CREATE INDEX idx_outbox_events_pending_next_attempt
    ON outbox_events (status, next_attempt_at, created_at);

CREATE INDEX idx_outbox_events_processing_started_at
    ON outbox_events (status, processing_started_at);
