CREATE TABLE IF NOT EXISTS outbox_events (
    event_id CHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    schema_version INT UNSIGNED NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(16) NOT NULL,
    available_at DATETIME(6) NOT NULL,
    claimed_by VARCHAR(128) NULL,
    claim_token CHAR(32) NULL,
    lease_until DATETIME(6) NULL,
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    last_error VARCHAR(1024) NULL,
    published_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id),
    KEY idx_outbox_claim (status, available_at, lease_until, created_at),
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'CLAIMED', 'PUBLISHED', 'FAILED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
