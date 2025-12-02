-- Persistent Memory Schema for AMIL Agent
-- Tables for conversations, user preferences, learned mappings

-- Conversations table - stores full query conversations
CREATE TABLE amil_conversations (
    conversation_id VARCHAR2(100) PRIMARY KEY,
    user_id VARCHAR2(100) NOT NULL,
    session_id VARCHAR2(100) NOT NULL,
    user_query CLOB NOT NULL,
    intent CLOB,
    sql_query CLOB,
    execution_status VARCHAR2(50) CHECK (execution_status IN ('success', 'error', 'pending', 'rejected')),
    result_summary CLOB, -- JSON summary of results
    error_message CLOB,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    metadata CLOB -- JSON metadata: confidence, llm_provider, etc.
);

CREATE INDEX idx_conversations_user ON amil_conversations(user_id, created_at DESC);
CREATE INDEX idx_conversations_session ON amil_conversations(session_id);
CREATE INDEX idx_conversations_status ON amil_conversations(execution_status);

-- User preferences table
CREATE TABLE amil_user_preferences (
    preference_id VARCHAR2(100) PRIMARY KEY,
    user_id VARCHAR2(100) NOT NULL,
    preference_key VARCHAR2(255) NOT NULL,
    preference_value CLOB NOT NULL,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT uk_user_pref UNIQUE (user_id, preference_key)
);

CREATE INDEX idx_user_prefs_user ON amil_user_preferences(user_id);

-- Learned mappings table - stores concept-to-column mappings
CREATE TABLE amil_learned_mappings (
    mapping_id VARCHAR2(100) PRIMARY KEY,
    concept VARCHAR2(500) NOT NULL, -- User concept (e.g., "customer name")
    table_name VARCHAR2(128) NOT NULL,
    column_name VARCHAR2(128) NOT NULL,
    mapping_type VARCHAR2(50) CHECK (mapping_type IN ('exact', 'semantic', 'derived', 'user_taught')),
    confidence NUMBER(5,2) CHECK (confidence BETWEEN 0 AND 100),
    usage_count NUMBER DEFAULT 1,
    success_rate NUMBER(5,2) CHECK (success_rate BETWEEN 0 AND 100),
    created_by VARCHAR2(100),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_used_at TIMESTAMP,
    metadata CLOB -- JSON: derivation logic, examples, etc.
);

CREATE INDEX idx_learned_mappings_concept ON amil_learned_mappings(UPPER(concept));
CREATE INDEX idx_learned_mappings_table ON amil_learned_mappings(table_name);
CREATE INDEX idx_learned_mappings_conf ON amil_learned_mappings(confidence DESC, usage_count DESC);

-- Query patterns table - stores common query patterns
CREATE TABLE amil_query_patterns (
    pattern_id VARCHAR2(100) PRIMARY KEY,
    pattern_type VARCHAR2(100) NOT NULL, -- aggregation, time_series, join, etc.
    example_query CLOB NOT NULL,
    sql_template CLOB NOT NULL,
    usage_count NUMBER DEFAULT 1,
    success_rate NUMBER(5,2) CHECK (success_rate BETWEEN 0 AND 100),
    avg_execution_time_ms NUMBER,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_used_at TIMESTAMP,
    metadata CLOB -- JSON: intent keywords, tables involved, etc.
);

CREATE INDEX idx_query_patterns_type ON amil_query_patterns(pattern_type);
CREATE INDEX idx_query_patterns_usage ON amil_query_patterns(usage_count DESC, success_rate DESC);

COMMIT;
