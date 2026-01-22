"""
Orchestrator Node: Context
"""

import logging
import time
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.orchestrator.utils import emit_state_event, langfuse_span, METRICS_AVAILABLE, record_llm_usage
from app.core.config import settings
from app.core.client_registry import registry
from app.services.schema_enrichment_service import SchemaEnrichmentService

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def retrieve_context_node(state: QueryState) -> QueryState:
    """
    Node 1.5: Retrieve relevant context from Graphiti knowledge graph + DYNAMIC schema exploration
    
    Uses dynamic ALL_TAB_COLUMNS querying based on intent keywords
    
    Retrieves:
    - Similar past queries and their SQL patterns (Graphiti)
    - DYNAMIC schema metadata via ALL_TAB_COLUMNS
    - Table relationships and foreign keys
    - Sample data for column inference
    - User preferences and common query patterns
    - Historical corrections and improvements
    
    Improvement: Ranks tables by relevance, limits to top 10-15 tables (prevents context overflow)
    """
    logger.info(f"Retrieving enriched context with DYNAMIC schema exploration...")
    state["current_stage"] = "retrieve_context"
    db_type = state.get("database_type", "oracle")
    logger.info(f"Context node database_type from state: {db_type}")
    
    # Track node execution
    from app.orchestrator.utils import update_node_history
    await update_node_history(state, "retrieve_context", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Retrieving schema context and metadata", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    context = {
        "similar_queries": [],
        "schema_metadata": {},
        "user_patterns": [],
        "graphiti_available": False,
        "enriched_schema": None,
        "sample_data": {},
        "table_relationships": []
    }
    
    async with langfuse_span(
        state,
        "orchestrator.retrieve_context",
        input_data={
            "user_query": state.get("user_query"),
            "intent": state.get("intent"),
        },
        metadata={"stage": "retrieve_context"},
    ) as span:
        try:
            # Get Graphiti client from registry
            graphiti_client = registry.get_graphiti_client()

            graphiti_available = graphiti_client is not None
            if not graphiti_available:
                logger.warning(f"Graphiti client not available, proceeding with schema-only context")

            context["graphiti_available"] = graphiti_available

            # Search for similar queries based on intent (only if Graphiti available)
            if graphiti_available:
                search_query = f"SQL query for: {state['intent'][:200]}"
                logger.info(f"Searching knowledge graph for: {search_query[:100]}...")

                search_results = await graphiti_client.search(
                    query=search_query,
                    num_results=5,
                    search_type="hybrid"
                )

                logger.info(f"Found {len(search_results)} relevant context items")

                # Extract relevant context
                if search_results:
                    for result in search_results[:3]:  # Top 3 most relevant
                        # Extract nodes from search results
                        if hasattr(result, 'nodes') and result.nodes:
                            for node in result.nodes[:2]:  # Limit to avoid token overflow
                                if hasattr(node, 'name') and hasattr(node, 'created_at'):
                                    context["similar_queries"].append({
                                        "name": node.name,
                                        "created_at": str(node.created_at),
                                        "relevance": "high"  # Can be enhanced with scoring
                                    })
            
            # Schema enrichment - database-aware
            try:
                from app.services.schema_enrichment_service import SchemaEnrichmentService
                enrichment_service = SchemaEnrichmentService()
                
                enriched_schema = await enrichment_service.get_enriched_schema_context(
                    user_query=state["user_query"],
                    intent=state.get("intent", ""),
                    include_samples=True,
                    include_relationships=True,
                    sample_limit=3,
                    database_type=db_type,
                    connection_name=state.get("context", {}).get("connection_name")
                )
                
                context["enriched_schema"] = enriched_schema
                context["sample_data"] = enriched_schema.get("samples", {})
                context["table_relationships"] = enriched_schema.get("relationships", [])
            
                logger.info(f"Enriched schema context: {len(enriched_schema.get('tables', {}))} tables, "
                           f"{len(enriched_schema.get('samples', {}))} samples, "
                           f"{len(enriched_schema.get('relationships', []))} relationships")
            except Exception as e:
                logger.warning(f"Schema enrichment failed, falling back to basic schema: {e}")
            
            try:
                from app.services.schema_service import SchemaService
                from app.services.doris_schema_service import DorisSchemaService
                from app.services.postgres_schema_service import PostgresSchemaService
                
                logger.info(f"Starting DYNAMIC schema exploration based on query keywords...")
                
                # Use dynamic schema exploration, routed by database_type
                if db_type == "doris":
                    dynamic_schema_result = await DorisSchemaService.get_dynamic_schema(
                        user_query=state["user_query"],
                        intent=state.get("intent", ""),
                        max_tables=15,
                    )
                elif db_type in ["postgres", "postgresql"]:
                    dynamic_schema_result = await PostgresSchemaService.get_dynamic_schema(
                        user_query=state["user_query"]
                    )
                else:
                    dynamic_schema_result = await SchemaService.get_dynamic_schema(
                        user_query=state["user_query"],
                        intent=state.get("intent", ""),
                        max_tables=15,  # Limit to top 15 relevant tables
                    )
                
                logger.info(f"Dynamic schema result status: {dynamic_schema_result.get('status')}")
                logger.info(f"Dynamic schema result keys: {list(dynamic_schema_result.keys())}")
                
                dynamic_schema_tables: dict = {}
                if dynamic_schema_result.get("status") == "success":
                    schema_data = dynamic_schema_result.get("schema", {})
                    dynamic_schema_tables = schema_data.get("tables", {}) or {}
                    context["schema_metadata"] = schema_data
                    
                    source = dynamic_schema_result.get("source", "unknown")
                    keywords_used = dynamic_schema_result.get("keywords_used", [])
                    tables_analyzed = dynamic_schema_result.get("tables_analyzed", 0)
                    
                    logger.info(
                        f" Dynamic schema retrieved: {source}, "
                        f"{tables_analyzed} tables analyzed, "
                        f"{len(dynamic_schema_tables)} tables in result, "
                        f"keywords: {keywords_used[:5]}"
                    )
                    logger.info(f"Table names: {list(dynamic_schema_tables.keys())[:10]}")
                    
                    try:
                        from app.core.redis_client import redis_client
                        cache_key = f"dynamic_schema:{hash(state['user_query'])}"
                        await redis_client.cache_schema_metadata(cache_key, schema_data, ttl=1800)  # 30 min
                    except Exception as cache_err:
                        logger.warning(f"Schema cache failure (non-fatal): {cache_err}")
                else:
                    error_msg = dynamic_schema_result.get("error", "unknown error")
                    logger.error(f"Dynamic schema exploration failed: {error_msg}")
                    logger.info(f"Falling back to static cached schema...")
                    try:
                        if db_type == "doris":
                            static_schema_result = await DorisSchemaService.get_database_schema()
                        elif db_type in ["postgres", "postgresql"]:
                            static_schema_result = await PostgresSchemaService.get_full_schema()
                        else:
                            static_schema_result = await SchemaService.get_database_schema(use_cache=True)

                        if static_schema_result.get("status") == "success":
                            schema_data = static_schema_result.get("schema", {})
                            dynamic_schema_tables = schema_data.get("tables", {}) or {}
                            context["schema_metadata"] = schema_data
                            logger.info(f"Fallback successful: {len(dynamic_schema_tables)} tables from static schema")
                        else:
                            context["schema_metadata"] = {}
                            dynamic_schema_tables = {}
                            logger.error(f"Static schema fallback also failed")
                    except Exception as fallback_err:
                        logger.error(f"Static schema fallback error: {fallback_err}")
                        context["schema_metadata"] = {}
                        dynamic_schema_tables = {}
                    
            except Exception as e:
                logger.error(f"Dynamic schema exploration error: {e}", exc_info=True)
                logger.info(f"Falling back to static cached schema...")
                try:
                    if db_type == "doris":
                        static_schema_result = await DorisSchemaService.get_database_schema()
                    elif db_type in ["postgres", "postgresql"]:
                        static_schema_result = await PostgresSchemaService.get_full_schema()
                    else:
                        static_schema_result = await SchemaService.get_database_schema(use_cache=True)
                    
                    if static_schema_result.get("status") == "success":
                        schema_data = static_schema_result.get("schema", {})
                        dynamic_schema_tables = schema_data.get("tables", {}) or {}
                        context["schema_metadata"] = schema_data
                        logger.info(f"Fallback successful: {len(dynamic_schema_tables)} tables from static schema")
                    else:
                        context["schema_metadata"] = {}
                        dynamic_schema_tables = {}
                        logger.error(f"Static schema fallback also failed")
                except Exception as fallback_err:
                    logger.error(f"Static schema fallback error: {fallback_err}")
                    context["schema_metadata"] = {}
                    dynamic_schema_tables = {}
            
            try:
                from app.services.semantic_schema_index_service import SemanticSchemaIndexService
                from app.services.context_manager_service import SmartContextManager
                sem = SemanticSchemaIndexService()
                await sem.ensure_built_if_empty()
                semantic_hits = await sem.search(state["user_query"], top_k=10)
                context["semantic_candidates"] = semantic_hits
                user_query_upper = state["user_query"].upper()
                explicitly_mentioned = []
                
                # Find tables explicitly mentioned in query
                for table_name in dynamic_schema_tables.keys():
                    if table_name.upper() in user_query_upper:
                        explicitly_mentioned.append(table_name)
                
                # If no explicit mentions, use first table from dynamic discovery
                if not explicitly_mentioned and dynamic_schema_tables:
                    explicitly_mentioned = [list(dynamic_schema_tables.keys())[0]]
                
                selected_tables = explicitly_mentioned[:1]  # Only use ONE table
                if selected_tables and dynamic_schema_tables:
                    # Filter schema_metadata tables down to selected set
                    filtered = {t: cols for t, cols in dynamic_schema_tables.items() if t in selected_tables}
                    if filtered:
                        if not context.get("schema_metadata"):
                            context["schema_metadata"] = {"tables": {}, "views": {}}
                        context["schema_metadata"]["tables"] = filtered
                        logger.info(f"Smart context selected tables: {list(filtered.keys())[:6]}")
            except Exception as e:
                logger.warning(f"Smart context/semantic index unavailable: {e}")
            
            # Retrieve user-specific query patterns from Graphiti (only if available)
            if graphiti_available:
                user_id = state.get("user_id", "default_user")
                try:
                    user_patterns = await graphiti_client.search(
                        query=f"user:{user_id} query patterns history",
                        num_results=5
                    )
                    context["user_patterns"] = []
                    if user_patterns:
                        for pattern in user_patterns[:5]:
                            if hasattr(pattern, 'nodes') and pattern.nodes:
                                for node in pattern.nodes:
                                    context["user_patterns"].append({
                                        "pattern": node.name if hasattr(node, 'name') else str(node),
                                        "timestamp": str(node.created_at) if hasattr(node, 'created_at') else "unknown"
                                    })
                    logger.info(f"Retrieved {len(context['user_patterns'])} user-specific patterns")
                except Exception as e:
                    logger.warning(f"Failed to retrieve user patterns: {e}")
                    context["user_patterns"] = []
            
            state["context"] = context
            user_patterns_count = len(context.get('user_patterns', []))
            state["messages"].append(AIMessage(
                content=f"Context retrieved: {len(context['similar_queries'])} similar patterns, {user_patterns_count} user patterns found"
            ))
            state["next_action"] = "generate_sql"

            span["output"].update({
                "graphiti_available": graphiti_available,
                "schema_tables": len(context.get("schema_metadata", {}).get("tables", {})),
                "enriched_tables": len((context.get("enriched_schema") or {}).get("tables", {})),
            })

            # Stream lifecycle: prepared with discoveries
            if ExecState:
                discovered_tables = context.get("enriched_schema", {}).get("tables", {})
                discovered_relationships = context.get("table_relationships", [])
                await emit_state_event(state, ExecState.PREPARED, {
                    "similar": len(context.get("similar_queries", [])),
                    "thinking_steps": [
                        {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-2", "content": f"Retrieved schema context ({len(discovered_tables)} tables found)", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                        {"id": "step-3", "content": "Generating SQL query...", "status": "pending", "timestamp": datetime.now(timezone.utc).isoformat()}
                    ],
                    "discoveries": {
                        "tables": list(discovered_tables.keys())[:5],
                        "relationships_count": len(discovered_relationships),
                        "schema_enriched": context.get("enriched_schema") is not None
                    }
                })
            
            logger.info(f"Context enrichment complete")
            
            # Mark node as completed
            await update_node_history(state, "retrieve_context", "completed", thinking_steps=[
                {"id": "step-1", "content": f"Retrieved schema context ({len(discovered_tables)} tables found)", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
            
            return state
        except Exception as e:
            logger.error(f"Context retrieval failed: {e}")
            span["output"]["error"] = str(e)
            span["level"] = "ERROR"
            span["status_message"] = "context_enrichment_failed"
            
            # Mark node as failed
            await update_node_history(state, "retrieve_context", "failed", error=str(e))
            
            # Don't fail the entire workflow, just proceed without context
            state["context"] = context
            state["next_action"] = "generate_sql"
            state.setdefault("warnings", []).append({
                "stage": "retrieve_context",
                "message": str(e),
            })
            logger.warning(f"Proceeding without context enrichment")
            return state
