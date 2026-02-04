from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field

# Constants
MAX_QUERY_LENGTH = 10000
MAX_SQL_LENGTH = 50000
QUERY_TIMEOUT_SECONDS = 600.0


class QueryRequest(BaseModel):
    query: str
    connection_name: Optional[str] = "TestUserCSV"
    database_type: Optional[str] = (
        "oracle"  # "oracle", "doris", or "postgres"/"postgresql"
    )

    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 10000


class QueryResponse(BaseModel):
    query_id: str
    status: str
    message: str
    sql: Optional[str] = None
    results: Optional[Dict[str, Any]] = None
    result_ref: Optional[Dict[str, Any]] = None
    results_truncated: Optional[bool] = None
    execution_time_ms: Optional[int] = None
    timestamp: Optional[str] = None


class ApprovalRequest(BaseModel):
    approved: bool
    modified_sql: Optional[str] = None
    rejection_reason: Optional[str] = None
    decision_reason: Optional[str] = Field(
        None,
        description="Explicit reason for approval/rejection decision for audit trail",
    )

    constraints_applied: Optional[List[str]] = Field(
        None,
        description="List of constraints applied during approval (e.g., ['LIMIT 100', 'removed_PII'])",
    )
    execution_time_ms: Optional[int] = None
    timestamp: Optional[str] = None


class OrchestratorQueryRequest(BaseModel):
    """Request model for orchestrator-based query processing"""

    query: str
    user_id: Optional[str] = "default_user"
    session_id: Optional[str] = None
    database_type: Optional[str] = (
        "oracle"  # "oracle", "doris", or "postgres"/"postgresql"
    )
    auto_approve: Optional[bool] = False

    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 10000


class OrchestratorQueryResponse(BaseModel):
    """Response model for orchestrator-based query processing"""

    query_id: str
    status: str
    sql_query: Optional[str] = None
    validation: Optional[Dict[str, Any]] = None
    results: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    result_ref: Optional[Dict[str, Any]] = None
    results_truncated: Optional[bool] = None
    visualization: Optional[Dict[str, Any]] = None
    needs_approval: Optional[bool] = False
    llm_metadata: Optional[Dict[str, Any]] = None  # LLM verification metadata
    error: Optional[str] = None
    timestamp: Optional[str] = None
    # Enhancements
    sql_explanation: Optional[str] = None
    insights: Optional[list[str]] = None
    suggested_queries: Optional[list[str]] = None
    approval_context: Optional[Dict[str, Any]] = None
    clarification_message: Optional[str] = None
    clarification_details: Optional[Dict[str, Any]] = None
    sql_confidence: Optional[int] = None
    optimization_suggestions: Optional[list[Dict[str, Any]]] = None
    # Conversational response fields
    message: Optional[str] = None
    is_conversational: Optional[bool] = (
        None  # None = not set, avoids always serializing
    )
    intent: Optional[str] = None
    # Data freshness and observability metadata
    execution_time_ms: Optional[int] = None  # Query execution time
    data_source: Optional[str] = None  # Source database (Oracle/Doris)
    data_freshness: Optional[Dict[str, Any]] = None  # Last updated, refresh interval
    query_cost: Optional[Dict[str, Any]] = None  # Token usage, DB time, estimated cost

    class Config:
        extra = "allow"
        # Exclude None values from serialization to reduce payload size
        json_encoders = {type(None): lambda v: None}


class ClarificationRequest(BaseModel):
    """Request model for providing clarification to reprocess query"""

    query_id: str
    clarification: str
    original_query: Optional[str] = None
    database_type: Optional[str] = None

    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 5000


class EnhanceQueryRequest(BaseModel):
    """Request model for query enhancement (refinement)"""

    query: str
    conversation_history: Optional[list[Dict[str, str]]] = None
    database_type: Optional[str] = None
    use_llm: Optional[bool] = True

    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 5000


class EnhanceQueryResponse(BaseModel):
    """Response model for query enhancement"""

    original_query: str
    enhanced_query: str
    method: str
    context_used: Optional[bool] = None


class ReportRequest(BaseModel):
    """Request model for generating executive reports"""

    query_results: list[Dict[str, Any]]
    format: str = "html"  # html, pdf, docx
    title: Optional[str] = None
    user_queries: Optional[list[str]] = None


class VisualizationRequest(BaseModel):
    """Request model for generating Python-based visualizations"""

    columns: list[str]
    rows: list[list]
    chart_type: Optional[str] = None  # bar, line, pie, scatter, area, heatmap
    title: Optional[str] = None
    # Enhanced visualization options
    show_mean: Optional[bool] = True  # Show mean line on bar/line charts
    show_peaks: Optional[bool] = False  # Annotate peak values
    color_scheme: Optional[str] = None  # Custom color scheme

    class Config:
        str_strip_whitespace = True
