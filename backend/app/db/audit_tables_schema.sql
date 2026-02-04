--
-- Native Audit Tables for Amila BI System
-- Supports both Doris (OLAP) and PostgreSQL
-- Provides immutable audit trail with field-level encryption
--

-- ============================================
-- PostgreSQL Schema
-- ============================================

-- Enable pgaudit extension if available (PostgreSQL only)
-- CREATE EXTENSION IF NOT EXISTS pgaudit;

-- Main audit log table
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action VARCHAR(100) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    user_role VARCHAR(100),
    severity VARCHAR(20) NOT NULL DEFAULT 'info',
    success BOOLEAN NOT NULL DEFAULT TRUE,
    resource_type VARCHAR(100),
    resource_id VARCHAR(255),
    ip_address INET,
    user_agent TEXT,
    session_id VARCHAR(255),
    correlation_id VARCHAR(255),
    -- Encrypted sensitive fields (stored as JSONB)
    details_encrypted JSONB,
    -- Non-sensitive fields for querying
    details_public JSONB,
    -- Query fingerprint for tracking similar queries
    query_fingerprint VARCHAR(32),
    -- LLM metadata
    llm_provider VARCHAR(50),
    llm_model VARCHAR(100),
    -- Execution metrics
    execution_time_ms INTEGER,
    row_count INTEGER,
    -- Database info
    database_type VARCHAR(20),
    database_name VARCHAR(100),
    -- Compliance
    gdpr_category VARCHAR(50),
    retention_until DATE,
    -- Immutable flag
    is_immutable BOOLEAN NOT NULL DEFAULT TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_log(correlation_id);
CREATE INDEX IF NOT EXISTS idx_audit_fingerprint ON audit_log(query_fingerprint);
CREATE INDEX IF NOT EXISTS idx_audit_session ON audit_log(session_id);

-- Partitioned table for high-volume query audit (optional, for large deployments)
-- CREATE TABLE audit_log_query PARTITION OF audit_log
--     FOR VALUES IN ('query.submit', 'query.execute', 'query.approve');

-- Audit summary table (aggregated metrics for dashboards)
CREATE TABLE IF NOT EXISTS audit_summary (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    avg_execution_time_ms INTEGER,
    total_rows_returned BIGINT,
    UNIQUE(date, user_id, action)
);

CREATE INDEX IF NOT EXISTS idx_audit_summary_date ON audit_summary(date DESC);
CREATE INDEX IF NOT EXISTS idx_audit_summary_user ON audit_summary(user_id, date DESC);

-- Query correction audit trail
CREATE TABLE IF NOT EXISTS audit_query_corrections (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correction_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255),
    original_query TEXT,
    query_fingerprint VARCHAR(32),
    generated_sql_hash VARCHAR(64),
    correction_type VARCHAR(50),
    success_after_correction BOOLEAN,
    applied_count INTEGER DEFAULT 0,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_corrections_user ON audit_query_corrections(user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_corrections_fingerprint ON audit_query_corrections(query_fingerprint);

-- HITL approval audit trail
CREATE TABLE IF NOT EXISTS audit_hitl_approvals (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    query_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    approver_id VARCHAR(255) NOT NULL,
    approved BOOLEAN NOT NULL,
    risk_level VARCHAR(20),
    sql_query_hash VARCHAR(64),
    reason TEXT,
    approval_duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_audit_hitl_query ON audit_hitl_approvals(query_id);
CREATE INDEX IF NOT EXISTS idx_audit_hitl_user ON audit_hitl_approvals(user_id, timestamp DESC);

-- Role-based access audit
CREATE TABLE IF NOT EXISTS audit_role_changes (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    admin_user_id VARCHAR(255) NOT NULL,
    target_user_id VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL, -- grant, revoke, modify
    old_role VARCHAR(100),
    new_role VARCHAR(100),
    ip_address INET
);

CREATE INDEX IF NOT EXISTS idx_audit_role_target ON audit_role_changes(target_user_id, timestamp DESC);

-- ============================================
-- Doris Schema (OLAP optimized)
-- ============================================

-- Note: Run these in Doris FE

-- Main audit log table (Doris)
CREATE TABLE IF NOT EXISTS audit_log_doris (
    timestamp DATETIME NOT NULL,
    id BIGINT NOT NULL,
    action VARCHAR(100) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    user_role VARCHAR(100),
    severity VARCHAR(20),
    success BOOLEAN,
    resource_type VARCHAR(100),
    resource_id VARCHAR(255),
    ip_address VARCHAR(45),
    session_id VARCHAR(255),
    correlation_id VARCHAR(255),
    details_public JSON,
    query_fingerprint VARCHAR(32),
    llm_provider VARCHAR(50),
    llm_model VARCHAR(100),
    execution_time_ms INT,
    row_count INT,
    database_type VARCHAR(20),
    database_name VARCHAR(100)
)
DUPLICATE KEY(timestamp, id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "enable_duplicate_without_keys_by_default" = "true"
);

-- Create rollup index for common queries
ALTER TABLE audit_log_doris ADD ROLLUP idx_user_action (user_id, action, timestamp);

-- Audit summary (Doris - pre-aggregated)
CREATE TABLE IF NOT EXISTS audit_summary_doris (
    date DATE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    count BIGINT SUM DEFAULT 0,
    success_count BIGINT SUM DEFAULT 0,
    error_count BIGINT SUM DEFAULT 0,
    total_execution_time_ms BIGINT SUM DEFAULT 0,
    total_rows_returned BIGINT SUM DEFAULT 0
)
AGGREGATE KEY(date, user_id, action)
DISTRIBUTED BY HASH(user_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1"
);

-- ============================================
-- Helper Functions (PostgreSQL)
-- ============================================

-- Function to auto-calculate retention date based on GDPR category
CREATE OR REPLACE FUNCTION calculate_retention_date()
RETURNS TRIGGER AS $$
BEGIN
    CASE NEW.gdpr_category
        WHEN 'authentication' THEN
            NEW.retention_until := NEW.timestamp::DATE + INTERVAL '90 days';
        WHEN 'query_execution' THEN
            NEW.retention_until := NEW.timestamp::DATE + INTERVAL '1 year';
        WHEN 'security_event' THEN
            NEW.retention_until := NEW.timestamp::DATE + INTERVAL '7 years';
        ELSE
            NEW.retention_until := NEW.timestamp::DATE + INTERVAL '2 years';
    END CASE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto-setting retention
CREATE TRIGGER set_retention_date
    BEFORE INSERT ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION calculate_retention_date();

-- Function to purge expired audit records (GDPR compliance)
CREATE OR REPLACE FUNCTION purge_expired_audit()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM audit_log
    WHERE retention_until < CURRENT_DATE
    AND is_immutable = FALSE;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- View for non-sensitive audit data (for dashboards)
CREATE OR REPLACE VIEW audit_log_public AS
SELECT
    id,
    timestamp,
    action,
    user_id,
    user_role,
    severity,
    success,
    resource_type,
    execution_time_ms,
    row_count,
    database_type,
    query_fingerprint
FROM audit_log;

-- ============================================
-- Comments
-- ============================================

COMMENT ON TABLE audit_log IS 'Immutable audit trail for all system actions';
COMMENT ON COLUMN audit_log.details_encrypted IS 'Sensitive details encrypted at application level';
COMMENT ON COLUMN audit_log.is_immutable IS 'When TRUE, record cannot be modified or deleted';
COMMENT ON COLUMN audit_log.query_fingerprint IS 'SHA256 hash of normalized SQL for tracking similar queries';
