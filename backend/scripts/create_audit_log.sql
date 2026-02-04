CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    action VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    user_role VARCHAR(100),
    severity VARCHAR(50) DEFAULT 'info',
    success BOOLEAN DEFAULT TRUE,
    resource_type VARCHAR(255),
    resource_id VARCHAR(255),
    ip_address VARCHAR(45),
    user_agent TEXT,
    session_id VARCHAR(255),
    correlation_id VARCHAR(255),
    details_encrypted TEXT,
    details_public TEXT,
    query_fingerprint VARCHAR(64),
    llm_provider VARCHAR(100),
    llm_model VARCHAR(100),
    execution_time_ms INTEGER,
    row_count INTEGER,
    database_type VARCHAR(50),
    database_name VARCHAR(100),
    gdpr_category VARCHAR(100),
    is_immutable BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX idx_audit_user_id ON audit_log(user_id);
CREATE INDEX idx_audit_action ON audit_log(action);
