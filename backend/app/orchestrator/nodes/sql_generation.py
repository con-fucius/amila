"""
Orchestrator Node: Sql_Generation
"""

import logging
import time
import re
import hashlib
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.utils.oracle_error_parser import extract_invalid_identifier
from app.utils.oracle_identifiers import format_qualified_identifier, normalize_oracle_identifiers
from app.orchestrator.utils import (
    emit_state_event,
    langfuse_span,
    update_node_history,
    METRICS_AVAILABLE,
    record_llm_usage,
    record_sql_generation,
    set_state_error,
)
from app.core.config import settings
from app.core.langfuse_client import log_generation
from app.services.schema_enrichment_service import SchemaEnrichmentService
from app.agents.sql_generation_skills import SQLGenerationSkillsOrchestrator
from app.core.sql_dialect_converter import SQLDialectConverter, convert_sql, SQLDialect
from app.core.redis_client import redis_client

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)

QUERY_FINGERPRINT_PREFIX = "query:fingerprint:"
QUERY_FINGERPRINT_TTL = 86400 * 30  # 30 days
MAX_FINGERPRINT_QUERY_LEN = 2000
ENABLE_COST_AWARE_GENERATION = getattr(settings, "COST_AWARE_GENERATION_ENABLED", True)


def _sanitize_llm_sql_response(text: str) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", cleaned, re.DOTALL | re.IGNORECASE)
    if fenced:
        cleaned = fenced.group(1).strip()
    cleaned = re.sub(r"^\s*SQL\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    lines = cleaned.splitlines()
    start_idx = 0
    for i, line in enumerate(lines):
        if re.match(r"^\s*(WITH|SELECT|INSERT|UPDATE|DELETE|--)", line, re.IGNORECASE):
            start_idx = i
            break
    cleaned = "\n".join(lines[start_idx:]).strip()
    cleaned = cleaned.replace("```", "").strip()
    return cleaned


def _normalize_query_for_fingerprint(text: str) -> str:
    if not text:
        return ""
    lowered = text.lower()
    lowered = re.sub(r"\b\d+\b", "?", lowered)
    lowered = re.sub(r"[^\w\s\?]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered[:MAX_FINGERPRINT_QUERY_LEN]


def _compute_schema_fingerprint(schema_metadata: dict) -> str:
    tables = schema_metadata.get("tables", {}) if isinstance(schema_metadata, dict) else {}
    views = schema_metadata.get("views", {}) if isinstance(schema_metadata, dict) else {}
    table_parts = []
    for name, cols in tables.items():
        table_parts.append(f"{name}:{len(cols) if isinstance(cols, list) else 0}")
    for name, cols in views.items():
        table_parts.append(f"{name}:{len(cols) if isinstance(cols, list) else 0}")
    raw = "|".join(sorted(table_parts))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16] if raw else "no_schema"


def _fingerprint_key(user_query: str, intent: str, db_type: str, schema_fp: str) -> str:
    normalized = _normalize_query_for_fingerprint(user_query)
    intent_norm = _normalize_query_for_fingerprint(intent)
    content = f"{db_type}:{schema_fp}:{normalized}:{intent_norm}"
    fp = hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]
    return f"{QUERY_FINGERPRINT_PREFIX}{fp}"


async def generate_sql_node(state: QueryState) -> QueryState:
    """Entry point for SQL generation with Langfuse span instrumentation."""

    state["current_stage"] = "generate_sql"
    
    # Record pipeline stage entry
    from app.services.diagnostic_service import record_query_pipeline_stage
    from datetime import datetime, timezone
    
    query_id = state.get("query_id", "unknown")
    stage_start = datetime.now(timezone.utc)
    
    # Track node execution for reasoning visibility
    await update_node_history(state, "generate_sql", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Generating SQL query from intent and schema", "status": "in-progress", "timestamp": stage_start.isoformat()}
    ])

    async with langfuse_span(
        state,
        "orchestrator.generate_sql",
        input_data={
            "intent": state.get("intent"),
            "user_query": (state.get("user_query") or "")[:500],
            "hypothesis": (state.get("hypothesis") or "")[:500],
        },
        metadata={"stage": "generate_sql"},
    ) as span:
        span.setdefault("output", {})
        
        try:
            result = await _generate_sql_node_inner(state, span)
            
            # Record successful completion
            stage_end = datetime.now(timezone.utc)
            await record_query_pipeline_stage(
                query_id=query_id,
                stage="sql_generation",
                status="completed",
                entered_at=stage_start,
                exited_at=stage_end,
                metadata={
                    "sql_generated": bool(state.get("sql_query")),
                    "confidence": state.get("sql_confidence"),
                    "needs_approval": state.get("needs_approval")
                }
            )
            
        except Exception as e:
            # Record failure
            stage_end = datetime.now(timezone.utc)
            await record_query_pipeline_stage(
                query_id=query_id,
                stage="sql_generation",
                status="failed",
                entered_at=stage_start,
                exited_at=stage_end,
                error_details=str(e)
            )
            raise

        if state.get("sql_query"):
            span["output"]["sql_preview"] = state["sql_query"][:500]
        if state.get("sql_confidence") is not None:
            span["output"]["sql_confidence"] = state["sql_confidence"]
        span["output"]["next_action"] = state.get("next_action")
        return result


async def _generate_sql_node_inner(state: QueryState, span: dict) -> QueryState:
    """
    Node 3: Generate SQL query from intent and hypothesis using Skills-based approach
    
    Uses Skills orchestrator for systematic column mapping and SQL generation:
    1. SchemaAnalysisSkill - Identify relevant tables
    2. ColumnMappingSkill - Map concepts to physical columns OR derived expressions
    3. SQLGenerationSkill - Build enhanced prompt with validated mappings
    4. LLM generation with explicit column mappings
    
    Now includes hypothesis from previous node for improved accuracy
    """
    logger.info(f"Generating SQL query using Skills orchestrator...")
    
    db_type = (state.get("database_type") or "oracle").lower()
    try:
        from app.services.role_based_limits_service import RoleBasedLimitsService
        role_limits = RoleBasedLimitsService.get_role_limits(state.get("user_role", "viewer"))
        max_tables = role_limits.max_tables
        max_joins = role_limits.max_joins
    except Exception:
        max_tables = 0
        max_joins = 0
    
    # Define limit syntax based on database type
    if db_type in ["postgres", "postgresql", "doris"]:
        limit_syntax_1000 = "LIMIT 1000"
        limit_syntax_100 = "LIMIT 100"
        limit_syntax_10 = "LIMIT 10"
        limit_syntax_5 = "LIMIT 5"
        limit_syntax_n = "LIMIT n"
    else:
        limit_syntax_1000 = "FETCH FIRST 1000 ROWS ONLY"
        limit_syntax_100 = "FETCH FIRST 100 ROWS ONLY"
        limit_syntax_10 = "FETCH FIRST 10 ROWS ONLY"
        limit_syntax_5 = "FETCH FIRST 5 ROWS ONLY"
        limit_syntax_n = "FETCH FIRST n ROWS ONLY"
    
    # Stream: Starting schema analysis
    if ExecState:
        await emit_state_event(state, ExecState.PREPARED, {
            "thinking_steps": [
                {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-2", "content": "Retrieved schema context", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-3", "content": "Analyzing schema and identifying tables...", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
            ],
            "todo_items": [
                {"id": "todo-1", "title": "Schema Analysis", "status": "in-progress", "details": "Identifying relevant tables and columns"},
                {"id": "todo-2", "title": "Column Mapping", "status": "pending", "details": "Map user concepts to database columns"},
                {"id": "todo-3", "title": "SQL Generation", "status": "pending", "details": "Generate SQL with validated mappings"},
                {"id": "todo-4", "title": "Validation", "status": "pending", "details": "Validate SQL syntax and security"}
            ]
        })
    
    # Initialize Skills orchestrator
    from app.agents.sql_generation_skills import SQLGenerationSkillsOrchestrator
    skills_orchestrator = SQLGenerationSkillsOrchestrator()
    
    llm = get_llm()
    
    # Fetch schema and enriched context
    schema_metadata = state.get("context", {}).get("schema_metadata")
    enriched_schema = state.get("context", {}).get("enriched_schema")

    if not schema_metadata or not isinstance(schema_metadata, dict) or not schema_metadata.get("tables"):
        logger.warning(f"Schema metadata missing in state - triggering on-demand refresh")
        try:
            from app.services.database_router import DatabaseRouter

            refreshed = await DatabaseRouter.get_database_schema(
                database_type=db_type,
                connection_name=state.get("context", {}).get("connection_name"),
                use_cache=False
            )
            if refreshed.get("status") == "success":
                schema_metadata = refreshed.get("schema") or {}
                state.setdefault("context", {})["schema_metadata"] = schema_metadata
                state["context"].setdefault("schema_refresh_metadata", {})["refreshed_at"] = datetime.now(timezone.utc).isoformat()
                logger.info(
                    " Schema metadata refreshed on-demand with %d tables",
                    len(schema_metadata.get("tables", {})),
                )
                span["output"]["schema_refreshed"] = True
                span["output"]["schema_tables"] = len(schema_metadata.get("tables", {}))
            else:
                logger.error(
                    " On-demand schema refresh failed: %s",
                    refreshed.get("error") or "unknown error",
                )
                span["output"]["schema_refresh_error"] = refreshed.get("error") or "unknown error"
        except Exception as refresh_err:
            logger.error(f"Failed to refresh schema metadata during SQL generation: %s", refresh_err, exc_info=True)
            span["output"]["schema_refresh_error"] = str(refresh_err)

    # ========== QUERY REUSE (fingerprint cache) ==========
    try:
        schema_fp = _compute_schema_fingerprint(schema_metadata or {})
        cache_key = _fingerprint_key(
            state.get("user_query", ""),
            state.get("intent", ""),
            db_type,
            schema_fp
        )
        cached = await redis_client.get(cache_key)
        if cached and isinstance(cached, dict) and cached.get("sql_query"):
            state["sql_query"] = cached.get("sql_query", "")
            state["sql_confidence"] = int(cached.get("sql_confidence", 95))
            state["sql_reused"] = True
            state["next_action"] = "validate"
            state["llm_metadata"] = {
                "provider": "cache",
                "model": "fingerprint_cache",
                "generated_successfully": True,
                "generation_timestamp": datetime.now(timezone.utc).isoformat(),
                "sql_length": len(state["sql_query"]),
                "cache_key": cache_key,
            }
            span["output"]["sql_reused"] = True
            span["output"]["cache_key"] = cache_key
            return state
    except Exception as cache_err:
        logger.warning(f"Query reuse lookup failed (non-fatal): {cache_err}")

    # ========== SKILLS-BASED SQL GENERATION (feature-flagged) ==========
    # Use Skills orchestrator to systematically map user concepts, unless disabled
    enhanced_prompt = None
    column_mappings = []
    if getattr(settings, 'QUERY_SQL_SKILLS_ENABLED', True):
        try:
            skills_result = await skills_orchestrator.generate_sql(
                user_query=state["user_query"],
                intent=state.get("intent", ""),
                schema_data=schema_metadata or {},
                enriched_schema=enriched_schema,
                database_type=db_type,
            )
            
            # Check if clarification needed (unmapped concepts)
            if skills_result.get("clarification_needed"):
                clarification_message = skills_result.get("clarification_message", "")
                clarification_details = skills_result.get("clarification_details")
                confidence = skills_result.get("confidence", 0)
                
                logger.warning(f"Skills orchestrator requests clarification - unmapped concepts (confidence: {confidence}%)")
                logger.warning(f"  Clarification message: {clarification_message[:200]}")
                span["output"].update({
                    "clarification_needed": True,
                    "clarification_message": clarification_message[:500],
                    "skills_confidence": confidence,
                })

                # Request clarification when Skills orchestrator detects unmapped concepts
                state["sql_query"] = ""
                state["sql_confidence"] = 0
                await set_state_error(
                    state,
                    "generate_sql",
                    clarification_message,
                        {"clarification_details": clarification_details} if clarification_details else None,
                    )
                state["needs_approval"] = False  # FIXED: Clarification does NOT need approval
                state["next_action"] = "request_clarification"
                
                # Track clarification details for downstream consumers
                if clarification_details:
                    state["clarification_details"] = clarification_details

                # Track LLM metadata
                llm_provider = get_query_llm_provider()
                llm_model = get_query_llm_model(settings.GRAPHITI_LLM_MODEL)
                state["llm_metadata"] = {
                    "provider": llm_provider,
                    "model": llm_model,
                    "generated_successfully": False,
                    "clarification_needed": True,
                    "clarification_message": clarification_message,
                    "clarification_details": clarification_details,
                    "generation_timestamp": datetime.now(timezone.utc).isoformat(),
                    "skills_metadata": skills_result.get("metadata", {}),
                }
                
                logger.info(f"Clarification request: {clarification_message[:100]}...")
                return state
            
            # Skills validated - use enhanced prompt
            enhanced_prompt = skills_result.get("enhanced_prompt", "")
            column_mappings = skills_result.get("column_mappings", [])
            
            logger.info(f"Skills orchestrator validated {len(column_mappings)} column mappings")
            state["skills_used"] = True
            span["output"].update({
                "clarification_needed": False,
                "column_mappings": len(column_mappings),
                "skills_confidence": skills_result.get("confidence"),
            })

            # Stream Skills validation success with detailed mappings
            if ExecState:
                mappings_summary = [
                    {"concept": m.concept, "type": m.mapping_type.value, "confidence": m.confidence, "expression": m.expression[:50]}
                    for m in column_mappings[:10]  # First 10 mappings
                ]
                # Count mapping types for insights
                physical_count = len([m for m in column_mappings if m.mapping_type.value == "physical"])
                derived_count = len([m for m in column_mappings if m.mapping_type.value == "derived"])
                
                await emit_state_event(state, ExecState.PREPARED, {
                    "thinking_steps": [
                        {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-2", "content": "Retrieved schema context", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-3", "content": "Analyzed schema and identified relevant tables", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-4", "content": f"Mapped {len(column_mappings)} concepts: {physical_count} physical columns, {derived_count} derived expressions", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-5", "content": "Constructing SQL with validated mappings...", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
                    ],
                    "todo_items": [
                        {"id": "todo-1", "title": "Schema Analysis", "status": "completed", "details": f"Identified tables and columns"},
                        {"id": "todo-2", "title": "Column Mapping", "status": "completed", "details": f"Mapped {len(column_mappings)} concepts using semantic matching"},
                        {"id": "todo-3", "title": "SQL Generation", "status": "in-progress", "details": "Generate SQL with validated mappings"},
                        {"id": "todo-4", "title": "Validation", "status": "pending", "details": "Validate SQL syntax and security"}
                    ],
                    "discoveries": {
                        "column_mappings": mappings_summary,
                        "physical_columns": physical_count,
                        "derived_expressions": derived_count,
                        "skills_used": True,
                        "semantic_matching_enabled": True
                    }
                })
            
            # Store column mappings for validation later
            state["column_mappings"] = [
                {
                    "concept": m.concept,
                    "type": m.mapping_type.value,
                    "expression": m.expression,
                    "table": m.table,
                    "confidence": m.confidence,
                    "note": m.note,
                }
                for m in column_mappings
            ]
            
        except Exception as e:
            logger.error(f"Skills orchestrator failed: {e}")
            span["output"]["skills_error"] = str(e)
            state["skills_used"] = False
            state["skills_fallback"] = True
            state["skills_fallback_reason"] = str(e)
            state.setdefault("warnings", []).append({
                "stage": "generate_sql",
                "message": f"Skills fallback activated: {str(e)}"
            })
            # Fallback to legacy prompt approach
            logger.warning(f"Falling back to legacy SQL generation approach")
            enhanced_prompt = None
            column_mappings = []
    else:
        logger.info(f"Skills disabled via feature flag - proceeding with legacy SQL generation only")
        enhanced_prompt = None
        column_mappings = []
        span["output"]["skills_disabled"] = True
        state["skills_used"] = False
        state["skills_disabled"] = True
    
    # ========== HYBRID APPROACH: SKILLS + LEGACY ==========
    # Skills orchestrator ENHANCES the legacy prompt with validated column mappings
    # This provides: Skills validation + LLM flexibility
    
    column_mapping_enhancement = ""
    if enhanced_prompt and column_mappings:
        # Skills succeeded - inject validated mappings into legacy prompt
        column_mapping_enhancement = "\n\n" + "="*80 + "\n"
        column_mapping_enhancement += " VALIDATED COLUMN MAPPINGS (from Skills Analysis)\n"
        column_mapping_enhancement += "="*80 + "\n"
        column_mapping_enhancement += "\nThe following mappings have been validated against the schema:\n\n"
        
        for m in column_mappings:
            if m.mapping_type.value != "not_found":
                column_mapping_enhancement += f"-  '{m.concept}' -> {m.expression}"
                if m.mapping_type.value == "derived":
                    column_mapping_enhancement += " (DERIVED EXPRESSION - use as-is, not a column name)"
                column_mapping_enhancement += f"\n  Type: {m.mapping_type.value}, Confidence: {m.confidence}%\n"
                column_mapping_enhancement += f"  Note: {m.note}\n\n"
        
        column_mapping_enhancement += "="*80 + "\n"
        column_mapping_enhancement += "  CRITICAL: Prefer using these validated mappings over guessing column names!\n"
        column_mapping_enhancement += "="*80 + "\n\n"
        
        logger.info(f"Enhancing legacy prompt with Skills-validated column mappings")
    
    # Continue with legacy prompt construction (now enhanced with Skills mappings)
    if schema_metadata and isinstance(schema_metadata, dict):
        # Extract table names mentioned in user query (case-insensitive)
        user_request_lower = state["user_query"].lower()
        combined_text = f"{state['user_query'].upper()} {state.get('intent', '').upper()}"
        
        tables = schema_metadata.get("tables", {})
        views = schema_metadata.get("views", {})
        
        # Find mentioned tables
        mentioned_tables = []
        for table_name in tables.keys():
            if table_name.upper() in combined_text:
                mentioned_tables.append((table_name, tables[table_name]))
        
        # Find mentioned views
        mentioned_views = []
        for view_name in views.keys():
            if view_name.upper() in combined_text:
                mentioned_views.append((view_name, views[view_name]))
        
        # Build schema context with PROMINENT section for mentioned tables
        schema_context = ""
        
        if mentioned_tables or mentioned_views:
            schema_context += "\n" + "" * 100 + "\n"
            schema_context += " MANDATORY SCHEMA CONSTRAINTS - COLUMN NAMES ARE STRICT \n"
            schema_context += "" * 100 + "\n\n"
            
            for table_name, columns in mentioned_tables:
                schema_context += f" TABLE: {table_name}\n"
                schema_context += "    EXACT COLUMN NAMES (use quotes if marked):\n"
                for col in columns:
                    nullable_marker = " (nullable)" if col.get('nullable') else " (required)"
                    col_name = col['name']
                    # Add quoting indicator
                    if col.get('requires_quoting'):
                        col_display = f'"{col_name}" [REQUIRES QUOTES]'
                    else:
                        col_display = col_name
                    schema_context += f"       {col_display} ({col['type']}{nullable_marker})\n"
                schema_context += "\n"
            
            for view_name, columns in mentioned_views:
                schema_context += f" VIEW: {view_name}\n"
                schema_context += "    EXACT COLUMN NAMES (use quotes if marked):\n"
                for col in columns:
                    nullable_marker = " (nullable)" if col.get('nullable') else " (required)"
                    col_name = col['name']
                    # Add quoting indicator
                    if col.get('requires_quoting'):
                        col_display = f'"{col_name}" [REQUIRES QUOTES]'
                    else:
                        col_display = col_name
                    schema_context += f"       {col_display} ({col['type']}{nullable_marker})\n"
                schema_context += "\n"
            
            # Precompute presence flags for targeted guidance
            try:
                all_cols_upper = [col['name'].upper() for _, cols in mentioned_tables for col in cols]
            except Exception:
                all_cols_upper = []
            has_date_column = 'DATE' in all_cols_upper
            ref_date_mentioned = any(t[0].upper() == 'REF_DATE' for t in mentioned_tables)

            schema_context += "" * 100 + "\n"
            schema_context += " ABSOLUTE RULES - VIOLATIONS WILL CAUSE IMMEDIATE FAILURE:\n"
            schema_context += "" * 100 + "\n"
            schema_context += "1. USE ONLY THE EXACT COLUMN NAMES LISTED ABOVE - CHARACTER-FOR-CHARACTER\n"
            schema_context += "2. DO NOT invent column names like 'SERVICE_DATE', 'DAY_OF_MONTH', 'MONTH_NAME', 'YEAR'\n"
            schema_context += "3. DO NOT use semantic guessing (e.g., DATE exists but SERVICE_DATE does not)\n"
            schema_context += "4. DO NOT assume column names based on table purpose\n"
            schema_context += "5. IF a column is not listed above, use DERIVED expressions or request clarification\n"
            if has_date_column:
                schema_context += "6.  CRITICAL: If a column named \"DATE\" exists, ALWAYS quote it as \"DATE\" (Oracle reserved word).\n"
                schema_context += "\n RESERVED WORD ALERT: The column name DATE is reserved in Oracle!\n"
                schema_context += " WRONG: SELECT DATE FROM ...  (will cause ORA-00904 error)\n"
                schema_context += " CORRECT: SELECT \"DATE\" FROM ...  (quotes required!)\n"
            if ref_date_mentioned:
                schema_context += "\n INVALID (will fail): SELECT DAY_OF_MONTH, MONTH_NAME, YEAR FROM REF_DATE\n"
                schema_context += " VALID (will work): SELECT DT_DAY, DS_MNTH, DS_YEAR FROM REF_DATE\n"
            schema_context += "" * 100 + "\n\n"
            
            # Add POSITIVE EXAMPLES section to guide LLM
            schema_context += "\n" + "" * 100 + "\n"
            schema_context += " CORRECT QUERY EXAMPLES - Learn from these patterns:\n"
            schema_context += "" * 100 + "\n\n"
            
            # Example 1: Basic aggregation with proper column names
            if mentioned_tables:
                schema_context += "Example 1: Aggregate metric by month using a derived expression\n"
                schema_context += "```sql\n"
                schema_context += "SELECT \n"
                schema_context += "  TO_CHAR(<validated_date_expression>, 'YYYY-MM') AS month_key,\n"
                schema_context += "  SUM(<validated_metric_column>) AS total_metric\n"
                schema_context += "FROM <primary_table>\n"
                schema_context += "GROUP BY TO_CHAR(<validated_date_expression>, 'YYYY-MM')\n"
                schema_context += "ORDER BY month_key\n"
                schema_context += f"{limit_syntax_10};\n"
                schema_context += "```\n"
                schema_context += "-- Replace placeholders with the exact expressions/columns confirmed in the schema mappings above.\n\n"
            
            # Example 2: JOIN with date reference table (CRITICAL - shows exact pattern)
            if any(t[0] == 'REF_DATE' for t in mentioned_tables):
                schema_context += "Example 2: JOIN CUSTOMER_DATA with REF_DATE (EXACT PATTERN TO FOLLOW)\n"
                schema_context += "```sql\n"
                schema_context += "--  THIS IS THE CORRECT PATTERN - USE THIS EXACT STRUCTURE\n"
                schema_context += "SELECT \n"
                schema_context += "  RD.DT_DAY,  --  Correct: DT_DAY (NOT day, DAY_OF_MONTH, or day_of_month)\n"
                schema_context += "  RD.DS_MNTH, --  Correct: DS_MNTH (NOT month, MONTH_NAME, or month_name)\n"
                schema_context += "  RD.DS_YEAR, --  Correct: DS_YEAR (NOT year, YEAR, or year_number)\n"
                schema_context += "  SUM(CD.USED_RESOURCES) AS total_network_data\n"
                schema_context += "FROM CUSTOMER_DATA CD\n"
                schema_context += "JOIN REF_DATE RD\n"
                schema_context += "  ON TO_CHAR(TO_DATE(CD.\"DATE\", 'DD/MM/YYYY'), 'YYYYMMDD') = RD.ID_DATE  --  Convert DD/MM/YYYY to YYYYMMDD format\n"
                schema_context += "WHERE \n"
                schema_context += "  RD.DS_MNTH = 'JANUARY'  --  CORRECT: UPPERCASE month name\n"
                schema_context += "  AND RD.DS_YEAR = '2025'  --  CORRECT: Year as string\n"
                schema_context += "GROUP BY RD.DT_DAY, RD.DS_MNTH, RD.DS_YEAR\n"
                schema_context += "ORDER BY total_network_data DESC\n"
                schema_context += f"{limit_syntax_5};\n"
                schema_context += "```\n\n"
            
            schema_context += " KEY TAKEAWAYS:\n"
            schema_context += "  -  Always use exact column names from schema (DT_DAY, DS_MNTH, DS_YEAR, DATE)\n"
            schema_context += "  -  GROUP BY must include all non-aggregated SELECT columns\n"
            schema_context += f"  -  Use {limit_syntax_n} (not {'FETCH FIRST' if db_type in ['postgres', 'postgresql', 'doris'] else 'LIMIT'})\n"
            schema_context += "  -  Check sample data for filter value formats (e.g., 'JANUARY' not 'January')\n"
            schema_context += "" * 100 + "\n\n"
        
        # Full schema reference (compact format)
        schema_context += "Available Database Schema (Full Reference):\n\n"
        
        if tables:
            schema_context += "TABLES:\n"
            for table_name, columns in tables.items():
                column_list = ", ".join([col['name'] for col in columns])
                schema_context += f"  -  {table_name}: {column_list}\n"
        
        if views:
            schema_context += "\nVIEWS:\n"
            for view_name, columns in views.items():
                column_list = ", ".join([col['name'] for col in columns])
                schema_context += f"  -  {view_name}: {column_list}\n"
    else:
        # Critical: No schema available - cannot generate accurate SQL
        logger.error(f"CRITICAL: No schema metadata available! Agent cannot generate valid SQL.")
        schema_context = """
Schema metadata not available!

To fix this issue:
1. Call POST /api/v1/schema/refresh to discover your database schema
2. Ensure Redis cache is running and accessible
3. Verify database connection is active

Without schema metadata, SQL generation will fail.
"""
    
    # Incorporate context from Graphiti if available
    context_section = ""
    if state.get("context") and state["context"].get("graphiti_available"):
        similar_queries = state["context"].get("similar_queries", [])
        if similar_queries:
            context_section = "\n\nSimilar Query Patterns (from knowledge graph):\n"
            for idx, query in enumerate(similar_queries[:3], 1):
                context_section += f"{idx}. {query['name']} (created: {query['created_at']})\n"
            context_section += "\nUse these patterns to inform your SQL generation.\n"

    # Hypothesis constraints (structured plan)
    hypothesis_section = ""
    hypothesis_structured = state.get("hypothesis_structured") if isinstance(state.get("hypothesis_structured"), dict) else {}
    if hypothesis_structured:
        hypothesis_section = "\n\n HYPOTHESIS CONSTRAINTS (must be respected):\n"
        hypothesis_section += "=" * 60 + "\n"
        hypothesis_section += f"Main table: {hypothesis_structured.get('main_table')}\n"
        hypothesis_section += f"Additional tables: {', '.join(hypothesis_structured.get('additional_tables') or [])}\n"
        hypothesis_section += f"Filters: {', '.join(hypothesis_structured.get('filters') or [])}\n"
        hypothesis_section += f"Aggregations: {', '.join(hypothesis_structured.get('aggregations') or [])}\n"
        hypothesis_section += f"Group by: {', '.join(hypothesis_structured.get('group_by') or [])}\n"
        hypothesis_section += f"Order by: {', '.join(hypothesis_structured.get('order_by') or [])}\n"
        hypothesis_section += f"Limit: {hypothesis_structured.get('limit') if hypothesis_structured.get('limit') is not None else 'none'}\n"
        hypothesis_section += f"Grain: {hypothesis_structured.get('grain')}\n"
        hypothesis_section += "=" * 60 + "\n"
    
    # Add sample data context (NEW FEATURE - helps with column inference)
    sample_data_section = ""
    if state.get("context"):
        sample_data = state["context"].get("sample_data", {})
        if sample_data:
            sample_data_section = "\n\n SAMPLE DATA (for column type inference):\n"
            sample_data_section += "=" * 60 + "\n"
            for table_name, rows in sample_data.items():
                if rows:
                    sample_data_section += f"\n{table_name} (sample rows):\n"
                    # Show first 2 rows as examples
                    for i, row in enumerate(rows[:2], 1):
                        sample_data_section += f"  Row {i}: {row}\n"
            sample_data_section += "=" * 60 + "\n"
            sample_data_section += " Use these samples to understand column data types and values.\n"
    
    # Add table relationship context (NEW FEATURE - helps with JOINs)
    relationship_section = ""
    if state.get("context"):
        relationships = state["context"].get("table_relationships", [])
        if relationships:
            relationship_section = "\n\n TABLE RELATIONSHIPS (for JOINs):\n"
            relationship_section += "=" * 60 + "\n"
            for rel in relationships[:10]:  # Limit to avoid token overflow
                rel_type = rel.get("type", "unknown")
                table1 = rel.get("table1", "")
                table2 = rel.get("table2", "")
                join_hint = rel.get("join_hint", "")
                relationship_section += f"  -  [{rel_type}] {table1} <-> {table2}\n"
                if join_hint:
                    relationship_section += f"    JOIN hint: {join_hint}\n"
            relationship_section += "=" * 60 + "\n"
            relationship_section += " Use these relationships to construct proper JOINs.\n"

    join_paths_section = ""
    if state.get("context"):
        join_paths = state["context"].get("join_paths", [])
        if join_paths:
            join_paths_section = "\n\n RANKED JOIN PATHS (preferred order):\n"
            join_paths_section += "=" * 60 + "\n"
            for path in join_paths[:5]:
                from_table = path.get("from_table")
                to_table = path.get("to_table")
                join_paths_section += f"Path {from_table} -> {to_table} (hops={path.get('hops')}):\n"
                for edge in path.get("path", []):
                    hint = edge.get("join_hint", "")
                    join_paths_section += f"  - {edge.get('from')} -> {edge.get('to')} ({edge.get('type')})"
                    if hint:
                        join_paths_section += f" :: {hint}"
                    join_paths_section += "\n"
            join_paths_section += "=" * 60 + "\n"
            join_paths_section += " Prefer these join paths unless user explicitly requests another.\n"
    
    # Derived column hints (e.g., QUARTER from DATE)
    derived_hints_section = ""
    try:
        derived = state.get("context", {}).get("enriched_schema", {}).get("derived_hints", {})
        if derived:
            derived_hints_section = "\n\n DERIVED COLUMN HINTS (you may use these expressions; they are not physical columns):\n"
            derived_hints_section += "=" * 60 + "\n"
            shown = 0
            for table, hints in list(derived.items())[:5]:
                for h in hints[:3]:
                    derived_hints_section += f"  -  {table}.{h.get('concept')}: {h.get('expression')}  -- {h.get('note','')}\n"
                    shown += 1
                    if shown >= 12:
                        break
                if shown >= 12:
                    break
            derived_hints_section += "=" * 60 + "\n"
            derived_hints_section += " Prefer using these when the user asks for concepts like quarter/year without exact columns.\n"
    except Exception:
        pass
    
    # Learned Corrections - retrieve and inject relevant corrections
    corrections_section = ""
    try:
        from app.services.query_corrections_service import QueryCorrectionsService
        
        relevant_corrections = await QueryCorrectionsService.get_relevant_corrections(
            original_query=state["user_query"],
            intent=state.get("intent", ""),
            limit=3
        )
        
        if relevant_corrections:
            corrections_section = QueryCorrectionsService.format_corrections_for_prompt(relevant_corrections)
            logger.info(f"Injected {len(relevant_corrections)} learned corrections into prompt")
    except Exception as e:
        logger.warning(f"Failed to retrieve learned corrections: {e}")
    
    # Metrics Layer - suggest relevant business metrics
    metrics_section = ""
    try:
        from app.services.metrics_layer_service import suggest_metrics_for_query
        
        metric_suggestions = await suggest_metrics_for_query(state["user_query"])
        
        if metric_suggestions:
            metrics_section = "\n\n" + "="*80 + "\n"
            metrics_section += " CANONICAL BUSINESS METRICS AVAILABLE\n"
            metrics_section += "="*80 + "\n\n"
            metrics_section += "The following canonical metrics may be relevant to this query:\n\n"
            
            for i, metric in enumerate(metric_suggestions[:3], 1):
                metrics_section += f"{i}. {metric['display_name']} (relevance: {metric['relevance_score']:.0%})\n"
                metrics_section += f"   Definition: {metric['description']}\n"
                metrics_section += f"   SQL Template: {metric['sql_template']}\n\n"
            
            metrics_section += "="*80 + "\n"
            metrics_section += " Consider using these metric definitions for consistency.\n"
            metrics_section += "="*80 + "\n"
            
            logger.info(f"Injected {len(metric_suggestions)} metric suggestions into prompt")
            span["output"]["metric_suggestions_count"] = len(metric_suggestions)
    except Exception as e:
        logger.warning(f"Failed to retrieve metric suggestions: {e}")
    
    # Few-shot example for QoQ growth and outlier flagging (GENERIC - use placeholder names)
    example_sql = """
-- EXAMPLE PATTERN (replace <TABLE>, <DATE_COL>, <METRIC_COL>, <DIMENSION_COL> with actual schema columns)
WITH base AS (
  SELECT 
    <DIMENSION_COL>,
    TRUNC(<DATE_COL>, 'Q') AS QUARTER_START,
    TO_CHAR(<DATE_COL>,'YYYY') AS YEAR,
    TO_CHAR(<DATE_COL>,'Q')     AS QUARTER,
    SUM(<METRIC_COL>)          AS TOTAL_METRIC,
    COUNT(DISTINCT <ID_COL>) AS COUNT_DISTINCT
  FROM <TABLE>
  GROUP BY <DIMENSION_COL>, TRUNC(<DATE_COL>,'Q'), TO_CHAR(<DATE_COL>,'YYYY'), TO_CHAR(<DATE_COL>,'Q')
), growth AS (
  SELECT 
    <DIMENSION_COL>, QUARTER_START, TOTAL_METRIC, COUNT_DISTINCT,
    LAG(TOTAL_METRIC) OVER (PARTITION BY <DIMENSION_COL> ORDER BY QUARTER_START) AS PREV_METRIC
  FROM base
), metrics AS (
  SELECT 
    <DIMENSION_COL>, QUARTER_START, TOTAL_METRIC, COUNT_DISTINCT,
    CASE WHEN PREV_METRIC IS NULL OR PREV_METRIC = 0 THEN NULL 
         ELSE (TOTAL_METRIC - PREV_METRIC) / PREV_METRIC END AS QUARTER_OVER_QUARTER_GROWTH
  FROM growth
), stats AS (
  SELECT QUARTER_START, AVG(QUARTER_OVER_QUARTER_GROWTH) AS MEAN_G,
         STDDEV(QUARTER_OVER_QUARTER_GROWTH) AS SD_G
  FROM metrics
  GROUP BY QUARTER_START
)
SELECT m.<DIMENSION_COL>,
       m.QUARTER_START AS QUARTER,
       m.TOTAL_METRIC,
       m.COUNT_DISTINCT,
       m.QUARTER_OVER_QUARTER_GROWTH,
       CASE WHEN ABS(m.QUARTER_OVER_QUARTER_GROWTH - s.MEAN_G) > 2*s.SD_G THEN 1 ELSE 0 END AS IS_OUTLIER
FROM metrics m
LEFT JOIN stats s USING (QUARTER_START)
ORDER BY m.<DIMENSION_COL>, m.QUARTER_START
{limit_syntax_1000};
"""

    # Generate database-specific system prompt
    if db_type == "postgres" or db_type == "postgresql":
        db_specific_rules = """
6. **POSTGRESQL-SPECIFIC SYNTAX**
   - Use LIMIT n (NOT FETCH FIRST)
   - Date functions: DATE_TRUNC(), NOW(), CURRENT_DATE
   - String functions: COALESCE() for NULL handling
   - Type casting: column::TYPE or CAST(column AS TYPE)
   - Array operations: ARRAY[...], ANY(), ALL()
   - JSON operations: ->, ->>, @>, etc.
   - **CRITICAL: PostgreSQL is case-sensitive for identifiers!**
     - Table names and column names are LOWERCASE by default
     - If schema shows uppercase names (TEST_TABLE), use lowercase in SQL (test_table)
     - Only use quotes if the identifier has mixed case or special characters
     - Example: Schema shows "TEST_AGG_EBU_IFRS_DAY" -> Use "test_agg_ebu_ifrs_day" in SQL
"""
        db_name = "PostgreSQL"
    elif db_type == "doris":
        db_specific_rules = """
6. **DORIS-SPECIFIC SYNTAX**
   - Use LIMIT n (NOT FETCH FIRST)
   - Date functions: NOW(), CURDATE(), DATE_FORMAT()
   - String functions: IFNULL() for NULL handling
   - MySQL-compatible syntax
   - Optimized for OLAP queries
"""
        db_name = "Doris"
    else:
        db_specific_rules = """
6. **ORACLE-SPECIFIC SYNTAX**
   - Use FETCH FIRST n ROWS ONLY (NOT LIMIT)
   - Date functions: TRUNC(), ADD_MONTHS(), SYSDATE
   - String functions: NVL() for NULL handling
   - Reserved words MUST be quoted: "DATE", "USER", etc.
"""
        db_name = "Oracle"

    scope_section = ""
    if max_tables or max_joins:
        scope_section = "\n\n QUERY SCOPE CONSTRAINTS:\n"
        scope_section += "=" * 60 + "\n"
        if max_tables:
            scope_section += f" - Maximum tables: {max_tables}\n"
        if max_joins:
            scope_section += f" - Maximum joins: {max_joins}\n"
        scope_section += " Do not exceed these limits unless the user explicitly requests.\n"
        scope_section += "=" * 60 + "\n"
    
    system_prompt = f"""You are an expert {db_name} SQL query generator specialized in accurate schema mapping.
Database Type: {db_name}
Dialect Constraint: Generate SQL ONLY in {db_name} dialect. Do not output other dialects.

{column_mapping_enhancement}
{schema_context}{context_section}{hypothesis_section}{sample_data_section}{relationship_section}{join_paths_section}{derived_hints_section}{corrections_section}{metrics_section}{scope_section}

EXAMPLE QUERY TEMPLATE (adjust to actual columns):
```sql
{example_sql}
```


 CRITICAL RULES - VIOLATING THESE WILL CAUSE QUERY FAILURE


1. **EXACT COLUMN NAMES & QUOTING RULES**
   
   **CRITICAL REQUIREMENTS:**
   - Copy column names EXACTLY as shown in schema above
   - Columns marked [REQUIRES QUOTES] MUST be quoted: "month", "DATE"
   - Oracle is case-sensitive: lowercase/mixed-case columns need quotes
   - Reserved words (DATE, USER, etc.) MUST be quoted
   - For simple "show rows" queries, prefer SELECT * over listing all columns
   
    **Examples:**
     CORRECT: SELECT * FROM CUSTOMER_DATA {limit_syntax_5}
    CORRECT: SELECT "DATE", "month", PRODUCT FROM CUSTOMER_DATA
    WRONG: SELECT DATE, month FROM CUSTOMER_DATA (missing quotes)
    WRONG: SELECT day, MONTH_NAME FROM REF_DATE (wrong column names)

1b. **JOIN SAFETY (NO CARTESIAN PRODUCTS)**
   - Every JOIN must include ON or USING predicates
   - Do NOT use CROSS JOIN unless explicitly requested
   - Avoid comma joins (FROM a, b) without predicates

2. **COLUMN MAPPING STRATEGY (STRICT VALIDATION REQUIRED)**
   
   **Step 1: EXACT NAME MATCH (highest priority)**
   User says "customer_name" -> Look ONLY for exact column "CUSTOMER_NAME" in schema
   
   **Step 2: CHECK SCHEMA LIST (do not assume)**
   User says "customer" -> CHECK if columns CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER exist
   DO NOT assume they exist - verify against schema list above!
   
   **Step 3: ABBREVIATION PATTERNS (common in this schema)**
   - DT_ prefix -> Date field (DT_DAY = day)
   - DS_ prefix -> Description field (DS_MNTH = month name, DS_YEAR = year)
   - ID_ prefix -> ID/numeric field (ID_DATE, ID_MNTH, ID_YEAR)
   - NR_ prefix -> Number field
   
   **Step 4: ONLY IF NO MATCH -> Use DERIVED expression or request clarification**
   User says "when/date" but no DATE column -> Use DERIVED: TO_DATE(date_string_col, format)
   User says "quarter" but no QUARTER column -> Use DERIVED: TO_CHAR(date_col, 'Q')
   
   **CRITICAL: Semantic guessing is DISABLED. Only use columns that exist in schema!**

3. **USE SAMPLE DATA TO UNDERSTAND COLUMNS**
   - Check sample rows to see actual data values
   - Infer data types from samples (number, text, date)
   - Identify categorical values (e.g., 'Safaricom', 'Airtel Kenya')
   - Match user's filter values against sample data

4. **TEMPORAL DATA HANDLING & DERIVATIONS**
   - User says "by month" -> Use TRUNC(date_column, 'MM') or TO_CHAR(date_column, 'YYYY-MM')
   - User says "by quarter" or mentions quarter but column not present -> Prefer:
       -  TO_CHAR(date_column, 'Q') as QUARTER (and include YEAR for uniqueness), or
       -  CEIL(month_column/3) when only MONTH exists
     Always GROUP BY the same expression used in SELECT
   - User says "last year" -> WHERE date_column >= ADD_MONTHS(SYSDATE, -12)
   - Always GROUP BY the same date expression used in SELECT
   
   **CRITICAL: MONTH/YEAR FILTERING - CHECK SCHEMA AND SAMPLE DATA**
   - When filtering by month name (e.g., "January") or year:
     1. FIRST check schema for columns containing 'MONTH', 'MNTH', 'MTH', 'YEAR', 'YR' in their names
     2. Check SAMPLE DATA to see actual values (e.g., 'JANUARY' vs '01' vs 'Jan' vs 1)
     3. Match the filter format to the sample data format:
        - If samples show 'JANUARY', 'FEBRUARY' -> use uppercase month names
        - If samples show '01', '02' -> use zero-padded numbers
        - If samples show 1, 2 -> use integers
        - If samples show '2025', '2024' -> use year as string
        - If samples show 2025, 2024 -> use year as integer
     4. Prefer descriptive columns (names with 'DESC', 'NAME', 'DS', 'DESCRIPTION') over ID columns for text filters
     5. Always verify column data type and sample values before constructing WHERE clause
   
   **DATE FORMAT DETECTION FROM SAMPLE DATA:**
   - Inspect sample data to determine actual date format (e.g., '11/07/2025', '2025-11-07', '07-NOV-2025')
   - Common formats:
     -  MM/DD/YYYY (US format): '11/07/2025' = November 7, 2025
     -  DD/MM/YYYY (EU format): '11/07/2025' = July 11, 2025
     -  YYYY-MM-DD (ISO): '2025-11-07' = November 7, 2025
   - When filtering by date, use TO_DATE() with correct format mask:
     -  WHERE date_col >= TO_DATE('11/07/2025', 'MM/DD/YYYY')
     -  WHERE date_col = TO_DATE('2025-11-07', 'YYYY-MM-DD')
   - If sample data shows VARCHAR dates, use TO_DATE() for conversion
   - If ambiguous, assume DD/MM/YYYY unless context suggests otherwise

5. **AGGREGATIONS & ANALYTICS**
   - "average per subscriber" -> AVG(column) grouped by subscriber column
   - QoQ growth -> Use LAG(metric) OVER (PARTITION BY sector ORDER BY year, quarter) and (curr - prev)/NULLIF(prev,0)
   - Outlier flag -> Compare growth against AVG(growth) and STDDEV(growth) OVER (PARTITION BY year, quarter) and flag ABS(growth - mean) > 2*stddev
   - "compare between X and Y" -> Use CASE statements or separate WHERE clauses
   - "grouped by month" -> GROUP BY TRUNC(date_column, 'MM')

{db_specific_rules}

7. **QUERY STRUCTURE & SELECT * USAGE**
    SELECT clause: 
      - For "show me rows" or "preview table" queries -> USE SELECT * (simpler, safer)
      - For specific columns or aggregations -> List exact column names with proper quoting
      - Columns marked [REQUIRES QUOTES] MUST be quoted: SELECT "month", "DATE"
    FROM clause: Use exact table names
    WHERE clause: Match filters to actual column values (check samples!)
    GROUP BY: Include all non-aggregated SELECT columns
    ORDER BY: Optional, for sorted results
    FETCH FIRST / ROW LIMITING:
      - **CRITICAL: When user specifies row count, ALWAYS honor it exactly!**
      - "first 5 rows" / "top 5" / "5 records" -> {limit_syntax_5}
      - "first 10" / "top 10" -> {limit_syntax_10}
      - "show me 20" -> {"LIMIT 20" if db_type in ["postgres", "postgresql", "doris"] else "FETCH FIRST 20 ROWS ONLY"}
      - No specific count mentioned -> {limit_syntax_100} (default preview)
      - Large result sets -> {limit_syntax_1000} (max limit)
      - **NEVER use ROWNUM for limiting - use {limit_syntax_n} syntax**
    
    **CRITICAL: MULTIPLE TABLES HANDLING**
      - **NEVER generate multiple SELECT statements in one query!**
      - If user asks for "all tables" or "multiple tables", you MUST:
        1. Pick the MOST RELEVANT table based on the query intent
        2. Generate a SINGLE SELECT statement for that table only
        3. Add a comment explaining which table was chosen and why
      - Example: User asks "show first 5 rows of all tables"
        WRONG: SELECT * FROM table1 LIMIT 5; SELECT * FROM table2 LIMIT 5;
        CORRECT: SELECT * FROM most_relevant_table LIMIT 5;
                 -- Note: Showing most_relevant_table as it contains the primary data
      - If truly ambiguous, request clarification about which specific table to query

8. **IF COLUMN/VALUE NOT FOUND - REQUEST CLARIFICATION**
   
   **IMPORTANT:** Distinguish between MISSING COLUMNS and DERIVED METRICS:
   
    DERIVED METRICS (compute them, don't request clarification):
   - "daily" -> GROUP BY TRUNC(date_col, 'DD') or TO_CHAR(date_col, 'YYYY-MM-DD')
   - "monthly" -> GROUP BY TRUNC(date_col, 'MM')
   - "total volume" -> SUM(data_column)
   - "mean" / "average" -> AVG(column)
   - "count" -> COUNT(*) or COUNT(column)
   - "quarter_over_quarter_growth" -> LAG() window function
   - "outliers" -> Compare to AVG() and STDDEV()
   
    ONLY REQUEST CLARIFICATION FOR MISSING **BASE COLUMNS**:
   - User asks for "customer_name" but no NAME/CUSTOMER_NAME column exists
   - User filters by "region" but no REGION/AREA column exists
   
   **Before requesting clarification, ask yourself:**
   - Can I compute this metric using available columns?
   - Is this a derived aggregation/calculation?
   - If YES -> Generate SQL with the computation
   - If NO (truly missing base column) -> Request clarification
   
   **Clarification format (only for missing base columns):**
   -- ERROR: Cannot generate query. Column '<concept>' does not exist in table '<TABLE_NAME>'.
   -- Available columns: [list ALL actual columns]
   -- Please clarify which column to use for: <concept>
   -- CONFIDENCE: 0%

9. **CONFIDENCE SCORING**
   Always end with a confidence comment:
   -- CONFIDENCE: 85%
   
   Scoring guide:
   - 90-100%: All columns exist, clear mapping, tested patterns
   - 70-89%: Good mapping with minor assumptions
   - 40-69%: Uncertain mappings or complex logic
   - 0-39%: Missing columns or ambiguous mappings -> request clarification



Output format:
 - Return SQL only. No markdown, no code fences, no explanations.
 - If clarification is required, return ONLY the clarification comment block.

Do not request clarification solely because a metric name is not a physical column. If the metric can be expressed using available columns (e.g., quarter_over_quarter_growth), compute it with SQL expressions/window functions and alias it with the requested name.
"""
    
    # Build messages for LLM (hybrid approach uses legacy message structure)
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Intent: {state['intent']}\n\nUser Query: {state['user_query']}")
    ]
    
    try:
        import time
        sql_gen_start = time.time()
        
        # Track LLM usage metadata
        llm_provider = get_query_llm_provider()
        llm_model = get_query_llm_model(settings.GRAPHITI_LLM_MODEL)
        
        logger.info(f"Calling LLM: {llm_provider}/{llm_model}")
        
        response = await llm.ainvoke(messages)
        sql_query = _sanitize_llm_sql_response(response.content)
        span["output"].update({
            "llm_provider": llm_provider,
            "llm_model": llm_model,
        })

        logger.info(f"LLM raw response length: {len(sql_query)} chars")
        logger.debug(f"LLM raw response (first 200 chars): {sql_query[:200]}")

        if not sql_query:
            raise ValueError("LLM returned empty SQL output")

        # Dialect validation and fallback conversion (only if needed)
        try:
            dialect_enum = SQLDialect.ORACLE
            if db_type in ["postgres", "postgresql"]:
                dialect_enum = SQLDialect.POSTGRES
            elif db_type == "doris":
                dialect_enum = SQLDialect.DORIS

            validation_result = SQLDialectConverter.validate_for_dialect(sql_query, dialect_enum)
            if not validation_result.success and db_type in ["doris", "postgres", "postgresql"]:
                original_sql = sql_query
                conversion_result = convert_sql(
                    sql=sql_query,
                    from_dialect="oracle",
                    to_dialect="postgres" if db_type in ["postgres", "postgresql"] else "doris",
                    strict=False
                )
                if conversion_result.success:
                    sql_query = conversion_result.sql
                    if sql_query != original_sql:
                        logger.info(f"Converted SQL to {db_type} dialect after validation failure")
                        span["output"]["sql_dialect_conversion"] = {
                            "from": "oracle",
                            "to": db_type,
                            "warnings": conversion_result.warnings,
                            "unsupported_features": conversion_result.unsupported_features
                        }
                else:
                    logger.warning(f"SQL dialect conversion failed: {conversion_result.errors}")
                    span["output"]["sql_dialect_conversion_error"] = conversion_result.errors
            else:
                span["output"]["sql_dialect_validation"] = "passed"
        except Exception as dialect_err:
            logger.warning(f"Dialect validation failed (non-fatal): {dialect_err}")
            span["output"]["sql_dialect_validation_error"] = str(dialect_err)

        # ========== ERROR MESSAGE DETECTION ==========
        # Check if LLM returned an error message instead of SQL
        # These should NOT be sent to MCP/validator - route to clarification instead
        if sql_query.strip().startswith("-- ERROR:") or sql_query.strip().startswith("--ERROR:"):
            logger.warning(f"LLM returned error message instead of SQL - requesting clarification")

            # Extract clarification message from comments
            clarification_lines = []
            for line in sql_query.split('\n'):
                if line.strip().startswith('--'):
                    clarification_lines.append(line.strip()[2:].strip())
            
            clarification_message = '\n'.join(clarification_lines)
            clarification_details = None  # Initialize for this code path
            span["output"].update({
                "clarification_needed": True,
                "clarification_message": clarification_message[:500],
                "llm_status": "clarification",
            })

            # Store error state for HITL clarification
            state["sql_query"] = ""  # No valid SQL generated
            state["sql_confidence"] = 0
            await set_state_error(
                state,
                "generate_sql",
                clarification_message,
                {"clarification_details": clarification_details} if clarification_details else None,
            )
            state["needs_approval"] = False  # FIXED: Clarification does NOT need approval
            state["next_action"] = "request_clarification"
            
            # Track clarification payload for downstream consumers
            if clarification_details:
                state["clarification_details"] = clarification_details

            logger.info(f"Clarification request: {clarification_message[:100]}...")
            return state
        
        # Extract confidence score from SQL comment (NEW FEATURE)
        confidence_score = 100  # Default high confidence
        confidence_match = re.search(r'--\s*CONFIDENCE:\s*(\d+)%?', sql_query, re.IGNORECASE)
        if confidence_match:
            confidence_score = int(confidence_match.group(1))
            logger.info(f"SQL Generation Confidence: {confidence_score}%")
            # Remove confidence comment from SQL before execution
            sql_query = re.sub(r'--\s*CONFIDENCE:.*$', '', sql_query, flags=re.MULTILINE).strip()
        span["output"]["sql_confidence"] = confidence_score

        # Cost-aware optimization (pre-validation, optional)
        user_role = (state.get("user_role") or "viewer").lower()
        if ENABLE_COST_AWARE_GENERATION and not state.get("sql_reused") and user_role != "viewer" and len(sql_query) < 5000:
            try:
                from app.services.query_cost_estimator import QueryCostEstimator, CostLevel
                db_type_for_cost = (state.get("database_type") or "oracle").lower()
                pre_cost = await QueryCostEstimator.estimate_query_cost(
                    sql_query=sql_query,
                    connection_name=settings.oracle_default_connection if db_type_for_cost == "oracle" else None,
                    database_type=db_type_for_cost,
                    include_plan=False,
                )

                state["cost_estimate_pre"] = {
                    "total_cost": pre_cost.total_cost,
                    "cardinality": pre_cost.cardinality,
                    "cost_level": pre_cost.cost_level.value,
                    "has_full_table_scan": pre_cost.has_full_table_scan,
                    "warnings": pre_cost.warnings,
                    "recommendations": pre_cost.recommendations,
                }
                span["output"]["cost_estimate_pre"] = state["cost_estimate_pre"]

                should_optimize = (
                    pre_cost.cost_level in [CostLevel.HIGH, CostLevel.CRITICAL]
                    or pre_cost.has_full_table_scan
                )

                if should_optimize and pre_cost.recommendations:
                    llm_opt = get_llm()
                    if llm_opt:
                        optimize_prompt = f"""You are a SQL performance optimizer for {db_type_for_cost}.
Preserve the query's semantics and result set. Do NOT add or remove filters, limits, or grouping unless explicitly requested by the user.
Use index-friendly patterns and avoid full table scans when possible.
If helpful, add optimizer hints supported by {db_type_for_cost} WITHOUT changing results.
Return only the optimized SQL. Do not include explanations.

User Query:
{state.get('user_query', '')}

Current SQL:
{sql_query}

Cost Warnings:
{'; '.join(pre_cost.warnings[:5])}

Recommendations:
{'; '.join(pre_cost.recommendations[:5])}
"""
                        opt_resp = await llm_opt.ainvoke([
                            SystemMessage(content="Optimize SQL for performance without changing semantics."),
                            HumanMessage(content=optimize_prompt)
                        ])
                        optimized = _sanitize_llm_sql_response(
                            opt_resp.content if hasattr(opt_resp, "content") else str(opt_resp)
                        )
                        if optimized and optimized != sql_query:
                            sql_query = optimized
                            state["cost_optimized"] = True
                            span["output"]["cost_optimized"] = True
                        else:
                            state["cost_optimized"] = False
            except Exception as cost_opt_err:
                logger.warning(f"Cost-aware optimization skipped: {cost_opt_err}")

        # Store confidence in state for potential clarification routing
        state["sql_confidence"] = confidence_score

        # Normalize identifiers to ensure Oracle-compliant quoting using schema metadata
        try:
            normalized_sql = normalize_oracle_identifiers(sql_query, schema_metadata or {})
            if normalized_sql != sql_query:
                logger.debug(f"Normalized SQL identifiers for Oracle compliance")
                sql_query = normalized_sql
                span["output"]["identifiers_normalized"] = True
        except Exception as norm_err:
            logger.warning(f"Identifier normalization skipped due to error: {norm_err}")
            span["output"]["identifier_normalization_error"] = str(norm_err)
        
        # CRITICAL: Enforce single statement rule - split and take only first statement
        # This is a safety net in case LLM ignores the prompt instructions
        if ';' in sql_query.strip():
            statements = [s.strip() for s in sql_query.split(';') if s.strip()]
            if len(statements) > 1:
                logger.warning(f"LLM generated {len(statements)} statements, enforcing single statement rule")
                logger.warning(f"  Original: {sql_query[:200]}...")
                sql_query = statements[0]  # Take only the first statement
                logger.info(f"  Enforced: {sql_query[:200]}...")
                span["output"]["multiple_statements_prevented"] = True
                span["output"]["original_statement_count"] = len(statements)
        
        # PostgreSQL-specific normalization: lowercase table and column names
        if db_type in ["postgres", "postgresql"]:
            try:
                # Simple regex-based approach to lowercase identifiers for PostgreSQL
                # PostgreSQL treats unquoted identifiers as lowercase
                import re
                
                def normalize_postgres_sql(sql_text):
                    """Convert uppercase table/column names to lowercase for PostgreSQL using regex"""
                    # Split by semicolons to handle multiple statements (though we should prevent this)
                    statements = sql_text.split(';')
                    normalized_statements = []
                    
                    for stmt in statements:
                        if not stmt.strip():
                            continue
                            
                        # Pattern to match table names after FROM, JOIN, INTO, UPDATE
                        # This matches: FROM table_name, JOIN table_name, INTO table_name, UPDATE table_name
                        stmt = re.sub(
                            r'\b(FROM|JOIN|INTO|UPDATE)\s+([A-Z_][A-Z0-9_]*)\b',
                            lambda m: f"{m.group(1)} {m.group(2).lower()}",
                            stmt,
                            flags=re.IGNORECASE
                        )
                        
                        # Pattern to match column names (more conservative - only in SELECT clause)
                        # Match: SELECT column_name or SELECT table.column_name
                        stmt = re.sub(
                            r'\bSELECT\s+([A-Z_][A-Z0-9_]*(?:\s*,\s*[A-Z_][A-Z0-9_]*)*)',
                            lambda m: f"SELECT {m.group(1).lower()}",
                            stmt,
                            flags=re.IGNORECASE
                        )
                        
                        normalized_statements.append(stmt)
                    
                    # CRITICAL: Only return the FIRST statement if multiple were generated
                    # This prevents the "multiple statements" error
                    if len(normalized_statements) > 1:
                        logger.warning(f"Multiple SQL statements detected ({len(normalized_statements)}), using only the first one")
                        return normalized_statements[0].strip()
                    
                    return ';'.join(normalized_statements).strip()
                
                normalized_pg_sql = normalize_postgres_sql(sql_query)
                if normalized_pg_sql != sql_query:
                    logger.info(f"Normalized PostgreSQL SQL: {len(sql_query)} -> {len(normalized_pg_sql)} chars")
                    sql_query = normalized_pg_sql
                    span["output"]["postgres_identifiers_normalized"] = True
            except Exception as pg_norm_err:
                logger.warning(f"PostgreSQL identifier normalization failed (non-fatal): {pg_norm_err}")
                span["output"]["postgres_normalization_error"] = str(pg_norm_err)

        # ========== COLUMN NAME VALIDATION - CATCH INVENTED COLUMNS ==========
        # Extract all column references from SQL and validate against schema
        validation_failed = False
        validation_errors = []
        
        if schema_metadata and isinstance(schema_metadata, dict):
            try:
                import sqlparse
                from sqlparse.sql import Identifier, IdentifierList, Function
                from sqlparse.tokens import Keyword, DML
                
                # Build set of valid column names from schema
                valid_columns = set()
                tables = schema_metadata.get("tables", {})
                views = schema_metadata.get("views", {})
                
                for table_name, columns in tables.items():
                    for col in columns:
                        valid_columns.add(col['name'].upper())
                
                for view_name, columns in views.items():
                    for col in columns:
                        valid_columns.add(col['name'].upper())
                
                # Parse SQL to extract column references
                parsed = sqlparse.parse(sql_query)
                if parsed:
                    stmt = parsed[0]
                    
                    # Extract identifiers (table.column references)
                    def extract_column_refs(token):
                        """Recursively extract column references from SQL token tree"""
                        refs = []
                        
                        if hasattr(token, 'tokens'):
                            for t in token.tokens:
                                refs.extend(extract_column_refs(t))
                        
                        # Identifier: table.column or column
                        if isinstance(token, Identifier):
                            # Get the last part after dot (the column name)
                            parts = str(token).split('.')
                            if len(parts) >= 2:
                                column_name = parts[-1].strip().upper()
                                # Remove aliases (e.g., "COL AS ALIAS" -> "COL")
                                if ' AS ' in column_name:
                                    column_name = column_name.split(' AS ')[0].strip()
                                refs.append(column_name)
                        
                        return refs
                    
                    extracted_columns = extract_column_refs(stmt)
                    
                    # Filter out SQL keywords, functions, and aliases
                    sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 
                                   'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
                                   'LIKE', 'IS', 'NULL', 'AS', 'DISTINCT', 'ALL', 'FETCH', 'FIRST', 'ROWS', 'ONLY',
                                   'WITH', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'OVER', 'PARTITION'}
                    
                    sql_functions = {'SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'TRUNC', 'TO_CHAR', 'TO_DATE', 'NVL', 
                                    'COALESCE', 'ROUND', 'CEIL', 'FLOOR', 'SUBSTR', 'INSTR', 'LENGTH', 'UPPER', 'LOWER',
                                    'TRIM', 'DECODE', 'LAG', 'LEAD', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE',
                                    'ADD_MONTHS', 'SYSDATE', 'MONTHS_BETWEEN', 'EXTRACT', 'CAST', 'STDDEV'}
                    
                    # Check each extracted column
                    for col_ref in extracted_columns:
                        # Clean up column reference - remove parentheses, commas, etc.
                        col_clean = col_ref.strip()
                        for char in ['(', ')', ',', ';', ' ']:
                            col_clean = col_clean.replace(char, '')
                        
                        if not col_clean:  # Skip empty strings
                            continue
                        
                        # Skip keywords and functions
                        if col_clean in sql_keywords or col_clean in sql_functions:
                            continue
                        
                        # Skip numeric literals and string literals
                        if col_clean.replace('.', '').replace('-', '').isdigit() or col_clean.startswith("'") or col_clean.startswith('"'):
                            continue
                        
                        # Skip common SQL aliases and operators
                        if col_clean in {'AS', 'DESC', 'ASC', 'NULLS', 'LAST', 'FIRST', 'TRUE', 'FALSE'}:
                            continue
                        
                        # Check if column exists in schema
                        if col_clean not in valid_columns:
                            validation_errors.append(col_clean)
                    
                    # If invented columns detected, reject SQL
                    if validation_errors:
                        validation_failed = True
                        
                        # Log the rejected SQL for debugging BEFORE clearing it
                        logger.error(f"Column validation failed: {validation_errors}")
                        logger.error(f"REJECTED SQL (contains invalid columns):\n{sql_query[:500]}..." if len(sql_query) > 500 else f" REJECTED SQL:\n{sql_query}")
                        span["output"].update({
                            "invalid_columns": validation_errors,
                            "llm_status": "invalid_columns",
                        })
                        span.setdefault("level", "WARN")
                        span.setdefault("status_message", "invalid_columns_detected")

                        error_message = f" SQL VALIDATION FAILED: Generated SQL uses non-existent columns\n\n"
                        error_message += f"Invalid columns: {', '.join(validation_errors)}\n\n"
                        
                        state["llm_metadata"] = {
                            "validation_failed": True,
                            "invalid_columns": validation_errors,
                            "generation_timestamp": datetime.now(timezone.utc).isoformat(),
                            "sql_length": 0,
                            "token_usage": {},
                        }
                        
                        # Set error state and route to error
                        state["error"] = error_message
                        state["next_action"] = "error"
                        
                        logger.warning(f"Routing to error due to column validation failure")
                        return state
            
            except ImportError:
                logger.warning(f"sqlparse not available - skipping column name validation")
            except Exception as e:
                logger.warning(f"Column validation failed: {e}")
        
        # Low confidence threshold - request clarification
        # Fix 3: Increased threshold from 30% to 70% as per requirements
        # If confidence < 70%, auto-trigger clarification instead of execution
        CONFIDENCE_THRESHOLD = 70  # 70% confidence required for auto-execution
        should_request_clarification = False
        
        if confidence_score < CONFIDENCE_THRESHOLD:
            logger.warning(f"Low confidence ({confidence_score}%) below threshold ({CONFIDENCE_THRESHOLD}%) - requesting clarification")
            state["needs_approval"] = False  # FIXED: Clarification does NOT need approval
            state["next_action"] = "request_clarification"
            state["clarification_reason"] = f"SQL generation confidence ({confidence_score}%) is below the threshold ({CONFIDENCE_THRESHOLD}%). Please review or rephrase your query for better accuracy."
            should_request_clarification = True
            span["output"].update({
                "low_confidence": confidence_score,
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "llm_status": "low_confidence_clarification",
                "clarification_triggered": True
            })
            
            # Add clarification message to state
            state["messages"].append(AIMessage(
                content=f" The generated SQL has low confidence ({confidence_score}%). I'm not certain about the column mappings. Could you clarify which columns you'd like to see?"
            ))

        # Analyze query for optimization opportunities (NEW FEATURE)
        try:
            from app.services.query_optimizer_service import QueryOptimizerService
            optimizer = QueryOptimizerService()
            
            schema_context_for_optimizer = state.get("context", {}).get("enriched_schema", {})
            optimization_suggestions = optimizer.analyze_query(sql_query, schema_context_for_optimizer)
            
            if optimization_suggestions:
                state["optimization_suggestions"] = [
                    {
                        "type": sug.type,
                        "severity": sug.severity,
                        "description": sug.description,
                        "suggested_fix": sug.suggested_fix
                    }
                    for sug in optimization_suggestions
                ]
                
                # Log critical suggestions
                critical = [s for s in optimization_suggestions if s.severity == "critical"]
                if critical:
                    logger.warning(f"{len(critical)} critical optimization issues detected")
                
                logger.info(f"Generated {len(optimization_suggestions)} optimization suggestions")
        except Exception as e:
            logger.warning(f"Query optimization analysis failed: {e}")
            state["optimization_suggestions"] = []
        
        state["sql_query"] = sql_query
        state["messages"].append(AIMessage(content=f"Generated SQL (confidence: {confidence_score}%):\n```sql\n{sql_query}\n```"))

        # Add thinking step
        if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
            state["llm_metadata"] = {}
        if "thinking_steps" not in state["llm_metadata"]:
            state["llm_metadata"]["thinking_steps"] = []
        state["llm_metadata"]["thinking_steps"].append({
            "content": f"Generated SQL with {confidence_score}% confidence: {sql_query[:200]}...",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stage": "generate_sql"
        })

        # Natural language SQL explanation
        try:
            if confidence_score >= 50 and len(sql_query) < 8000:
                llm_exp = get_llm()
                exp_resp = await llm_exp.ainvoke([
                    SystemMessage(content="Explain the SQL in simple, non-technical terms for a business user in 3-6 short bullet points."),
                    HumanMessage(content=sql_query)
                ])
                state["sql_explanation"] = exp_resp.content[:1500]

                trace_id = state.get("trace_id")
                if trace_id:
                    try:
                        log_generation(
                            trace_id=trace_id,
                            name="orchestrator.generate_sql.sql_explanation",
                            model=llm_model,
                            input_data={"sql_preview": sql_query[:500]},
                            output_data={"explanation": state["sql_explanation"]},
                            metadata={"stage": "generate_sql"},
                        )
                    except Exception:
                        pass
            else:
                state["sql_explanation"] = ""
        except Exception:
            state["sql_explanation"] = ""

        # Only set validate action if we're not requesting clarification
        if not should_request_clarification:
            state["next_action"] = "validate"
        
        # Track token usage for cost monitoring
        token_usage = {}
        if hasattr(response, 'usage_metadata'):
            token_usage = {
                "input_tokens": getattr(response.usage_metadata, 'input_tokens', 0),
                "output_tokens": getattr(response.usage_metadata, 'output_tokens', 0),
                "total_tokens": getattr(response.usage_metadata, 'total_tokens', 0),
            }
            logger.info(f"Token usage: {token_usage['total_tokens']} total ({token_usage['input_tokens']} in, {token_usage['output_tokens']} out)")

        trace_id = state.get("trace_id")
        if trace_id:
            try:
                log_generation(
                    trace_id=trace_id,
                    name="orchestrator.generate_sql.sql_generation",
                    model=llm_model,
                    input_data={
                        "intent": state.get("intent"),
                        "user_query": state.get("user_query"),
                        "hypothesis": state.get("hypothesis"),
                    },
                    output_data={
                        "sql": sql_query[:1000],
                        "confidence": confidence_score,
                        "invalid_columns": validation_errors,
                    },
                    metadata={"stage": "generate_sql"},
                    usage=token_usage or None,
                )
            except Exception:
                pass

        # Store LLM metadata for verification
        state["llm_metadata"] = {
            "provider": llm_provider,
            "model": llm_model,
            "generated_successfully": True,
            "generation_timestamp": datetime.now(timezone.utc).isoformat(),
            "sql_length": len(sql_query),
            "token_usage": token_usage,
        }

        # Store query fingerprint cache for reuse
        try:
            schema_fp = _compute_schema_fingerprint(schema_metadata or {})
            cache_key = _fingerprint_key(
                state.get("user_query", ""),
                state.get("intent", ""),
                db_type,
                schema_fp
            )
            existing = await redis_client.get(cache_key)
            usage_count = 1
            if isinstance(existing, dict):
                usage_count = int(existing.get("usage_count", 0)) + 1
            await redis_client.set(cache_key, {
                "sql_query": sql_query,
                "sql_confidence": confidence_score,
                "database_type": db_type,
                "schema_fingerprint": schema_fp,
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "usage_count": usage_count,
            }, ttl=QUERY_FINGERPRINT_TTL)
        except Exception as cache_err:
            logger.warning(f"Query reuse cache update failed (non-fatal): {cache_err}")
        span["output"].update({
            "sql_length": len(sql_query),
            "token_usage": token_usage,
            "llm_status": span["output"].get("llm_status", "success"),
        })

        logger.info(f"SQL generated by {llm_provider}/{llm_model}: {sql_query[:100]}...")
        
        # Record SQL generation metrics
        if METRICS_AVAILABLE:
            sql_gen_duration = time.time() - sql_gen_start
            record_sql_generation(sql_gen_duration, llm_provider, success=True)
            
            # Record LLM usage
            prompt_len = len(system_prompt) + len(state.get('intent', '')) + len(state.get('user_query', ''))
            record_llm_usage(
                provider=llm_provider,
                model=llm_model,
                prompt_tokens=token_usage.get('input_tokens', prompt_len // 4),
                completion_tokens=token_usage.get('output_tokens', len(sql_query) // 4),
                status="success",
                latency=sql_gen_duration
            )
        
        # Stream SQL generation success with comprehensive progress
        if ExecState:
            mapped_concepts = state.get("column_mappings", [])
            await emit_state_event(state, ExecState.PREPARED, {
                "thinking_steps": [
                    {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-2", "content": "Retrieved schema context", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-3", "content": "Analyzed schema and identified relevant tables", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-4", "content": f"Mapped {len(mapped_concepts)} concepts to columns using semantic matching", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-5", "content": f"Generated SQL query with {confidence_score}% confidence", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-6", "content": "Validating SQL syntax and security...", "status": "pending", "timestamp": datetime.now(timezone.utc).isoformat()}
                ],
                "todo_items": [
                    {"id": "todo-1", "title": "Schema Analysis", "status": "completed", "details": "Identified tables and columns"},
                    {"id": "todo-2", "title": "Column Mapping", "status": "completed", "details": f"Mapped {len(mapped_concepts)} concepts"},
                    {"id": "todo-3", "title": "SQL Generation", "status": "completed", "details": f"Generated SQL (confidence: {confidence_score}%)"},
                    {"id": "todo-4", "title": "Validation", "status": "in-progress", "details": "Validating SQL syntax and security"}
                ],
                "sql_generated": sql_query[:200] + "..." if len(sql_query) > 200 else sql_query,
                "confidence": confidence_score,
                "llm_provider": state.get("llm_metadata", {}).get("provider", "unknown") if isinstance(state.get("llm_metadata"), dict) else "unknown"
            })
        
        return state

    except Exception as e:
        logger.error(f"SQL generation failed: {e}")

        # Track LLM failure for verification
        llm_provider = get_query_llm_provider()
        llm_model = get_query_llm_model(settings.GRAPHITI_LLM_MODEL)

        # Record failure metrics
        if METRICS_AVAILABLE:
            sql_gen_duration = time.time() - sql_gen_start
            record_sql_generation(sql_gen_duration, llm_provider, success=False)
            record_llm_usage(
                provider=llm_provider,
                model=llm_model,
                status="error",
                latency=sql_gen_duration
            )
            record_llm_usage(provider=llm_provider, model=llm_model, status="error")

        state["llm_metadata"] = {
            "provider": llm_provider,
            "model": llm_model,
            "generated_successfully": False,
            "error_message": str(e),
            "generation_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        span["output"]["error"] = str(e)
        span["level"] = "ERROR"
        span["status_message"] = "sql_generation_failed"

        await set_state_error(state, "generate_sql", str(e))
        state["next_action"] = "error"
        return state
