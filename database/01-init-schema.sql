-- ============================================================
-- ChatFlow Messaging System: schema (squashed, single-source-of-truth)
-- Engine: MySQL 8.x / InnoDB
-- ============================================================

CREATE DATABASE IF NOT EXISTS chatflow;
USE chatflow;

-- ------------------------------------------------------------
-- Drop in reverse dependency order. No foreign keys exist in
-- this schema (summary tables are denormalized counters, not
-- children of `messages`), so order is conventional, not
-- enforced.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS user_message_summary;
DROP TABLE IF EXISTS room_message_summary;
DROP TABLE IF EXISTS dead_letter_messages;
DROP TABLE IF EXISTS messages;

-- ============================================================
-- messages: append-only chat log
-- ============================================================
-- Design rationale:
--   - message_id (UUID) as PK ensures idempotent writes
--     (INSERT IGNORE silently drops duplicates from at-least-once
--     RabbitMQ delivery).
--   - room_id and user_id as INT for fast indexing and join performance.
--   - timestamp stored as DATETIME(3) for millisecond precision.
--   - InnoDB chosen for row-level locking under concurrent batch writes.
--   - Composite indexes aligned to the four hot query access patterns
--     served by MetricsAPI; analytics queries that would otherwise scan
--     this table are routed to the summary tables below.
-- ============================================================
CREATE TABLE messages (
    message_id   VARCHAR(36)                        NOT NULL,
    room_id      INT                                NOT NULL,
    user_id      INT                                NOT NULL,
    username     VARCHAR(20)                        NOT NULL,
    message      VARCHAR(500)                       NOT NULL,
    message_type ENUM('TEXT', 'JOIN', 'LEAVE')      NOT NULL,
    timestamp    DATETIME(3)                        NOT NULL,
    server_id    VARCHAR(50)                        DEFAULT NULL,
    client_ip    VARCHAR(45)                        DEFAULT NULL,
    created_at   DATETIME(3)                        NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (message_id),

    -- Q1: SELECT ... FROM messages WHERE room_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp
    -- Covering index: includes selected columns to avoid table lookups (excludes `message`, too large).
    INDEX idx_room_timestamp       (room_id, timestamp, message_id, user_id, username, message_type),

    -- Q2: SELECT ... FROM messages WHERE user_id = ? [AND timestamp BETWEEN ? AND ?] ORDER BY timestamp DESC
    INDEX idx_user_timestamp       (user_id, timestamp),

    -- Q3: SELECT COUNT(DISTINCT user_id) FROM messages WHERE timestamp BETWEEN ? AND ?
    -- Covering index: timestamp + user_id satisfies the query from index alone.
    INDEX idx_timestamp_user       (timestamp, user_id),

    -- Q3 fast path: SELECT COUNT(DISTINCT user_id) FROM messages
    -- Narrow index on user_id only for fast loose index scan.
    INDEX idx_user_id              (user_id),

    -- Q4: SELECT room_id, MAX(timestamp) FROM messages WHERE user_id = ? GROUP BY room_id
    INDEX idx_user_room_activity   (user_id, room_id, timestamp)
)
ENGINE             = InnoDB
DEFAULT CHARSET    = utf8mb4
COLLATE            = utf8mb4_unicode_ci
ROW_FORMAT         = DYNAMIC;

-- ============================================================
-- dead_letter_messages: failed-write audit and replay source
-- ============================================================
-- Populated when a write batch fails after circuit-breaker exhaustion.
-- Stores the original JSON for manual inspection or programmatic replay.
-- The `resolved` flag tracks remediation; `last_retry` enables exponential
-- backoff for replay tooling.
-- ============================================================
CREATE TABLE dead_letter_messages (
    id           BIGINT       AUTO_INCREMENT PRIMARY KEY,
    message_id   VARCHAR(36)  NOT NULL,
    message_json TEXT         NOT NULL,
    error_reason VARCHAR(500) DEFAULT NULL,
    retry_count  INT          DEFAULT 0,
    created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_retry   DATETIME(3)  DEFAULT NULL,
    resolved     BOOLEAN      DEFAULT FALSE,

    INDEX idx_dl_unresolved (resolved, last_retry),
    INDEX idx_dl_message_id (message_id)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4;

-- ============================================================
-- user_message_summary: pre-aggregated user counters
-- room_message_summary: pre-aggregated room counters
-- ============================================================
-- Read-path acceleration: routes top-active-user / top-active-room
-- queries away from full-table GROUP BY scans into O(num_users) /
-- O(num_rooms) point reads.
--
-- Maintained per write batch by the message-processor when started with
-- -Dsummary.tables=true (profiles O2, O12) via:
--     INSERT ... ON DUPLICATE KEY UPDATE total_messages = total_messages + VALUES(total_messages)
--
-- Constraints:
--   - total_messages is incrementally maintainable.
--   - unique_users is intentionally NOT tracked (cannot deduplicate across batches).
--   - last_seen / last_activity use GREATEST(existing, new) per batch.
-- ============================================================
CREATE TABLE user_message_summary (
    user_id        INT          NOT NULL,
    username       VARCHAR(20)  NOT NULL DEFAULT '',
    total_messages BIGINT       NOT NULL DEFAULT 0,
    last_seen      DATETIME(3)  NOT NULL,

    PRIMARY KEY (user_id),
    INDEX idx_ums_total (total_messages DESC)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4;

CREATE TABLE room_message_summary (
    room_id        INT          NOT NULL,
    total_messages BIGINT       NOT NULL DEFAULT 0,
    last_activity  DATETIME(3)  NOT NULL,

    PRIMARY KEY (room_id),
    INDEX idx_rms_total (total_messages DESC)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4;
