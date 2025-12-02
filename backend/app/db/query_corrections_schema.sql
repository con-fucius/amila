-- Query Corrections Schema
-- Stores user SQL edits for learning and improvement

CREATE TABLE amil_query_corrections (
    correction_id VARCHAR2(100) PRIMARY KEY,
    user_id VARCHAR2(100) NOT NULL,
    session_id VARCHAR2(100) NOT NULL,
    original_query CLOB NOT NULL,
    generated_sql CLOB NOT NULL,
    corrected_sql CLOB NOT NULL,
    correction_type VARCHAR2(50) CHECK (correction_type IN ('user_edit', 'error_fix', 'optimization', 'clarification')),
    diff_summary CLOB, -- JSON: added/removed/changed lines
    intent VARCHAR2(1000),
    success_after_correction NUMBER(1) DEFAULT 1 CHECK (success_after_correction IN (0,1)),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    applied_count NUMBER DEFAULT 0, -- How many times this correction pattern was auto-applied
    metadata CLOB -- JSON: error before correction, execution times, etc.
);

CREATE INDEX idx_corrections_user ON amil_query_corrections(user_id, created_at DESC);
CREATE INDEX idx_corrections_applied ON amil_query_corrections(applied_count DESC);
CREATE INDEX idx_corrections_type ON amil_query_corrections(correction_type);

COMMIT;
