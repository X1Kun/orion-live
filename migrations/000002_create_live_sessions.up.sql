CREATE TABLE IF NOT EXISTS live_sessions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    host_user_id BIGINT UNSIGNED NOT NULL,
    title VARCHAR(255) NOT NULL,
    cover_url VARCHAR(2048) NULL,
    status VARCHAR(16) NOT NULL,
    active_host_user_id BIGINT UNSIGNED GENERATED ALWAYS AS (
        CASE WHEN status = 'LIVE' THEN host_user_id ELSE NULL END
    ) STORED,
    started_at DATETIME(6) NULL,
    ended_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY idx_live_sessions_host_status_id (host_user_id, status, id),
    UNIQUE KEY uk_live_sessions_active_host (active_host_user_id),
    CONSTRAINT fk_live_sessions_host_user FOREIGN KEY (host_user_id) REFERENCES users(id),
    CONSTRAINT chk_live_sessions_status CHECK (status IN ('SCHEDULED', 'LIVE', 'ENDED')),
    CONSTRAINT chk_live_sessions_timestamps CHECK (
        (status = 'SCHEDULED' AND started_at IS NULL AND ended_at IS NULL) OR
        (status = 'LIVE' AND started_at IS NOT NULL AND ended_at IS NULL) OR
        (status = 'ENDED' AND started_at IS NOT NULL AND ended_at IS NOT NULL)
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
