import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Any

from app.core.config import settings
from app.core.client_registry import registry
from app.core.doris_server import doris_server
from app.core.doris_client import doris_client
from app.core.redis_client import redis_client
from app.core.sqlcl_pool import SQLclProcessPool
from app.core.mcp_client import create_mcp_client

logger = logging.getLogger(__name__)

async def init_doris() -> Tuple[bool, Optional[str]]:
    if not settings.DORIS_MCP_ENABLED:
        return False, None
    
    try:
        logger.info("Initializing Doris MCP server...")
        doris_server_ready = await doris_server.initialize()
        
        if doris_server_ready:
            doris_client_ready = await doris_client.initialize()
            if doris_client_ready:
                registry.set_doris_client(doris_client)
                logger.info("Doris MCP client registered")
                return True, None
            else:
                return False, "Doris MCP Client initialization failed"
        else:
            return False, "Doris MCP Server failed to start or become ready"
            
    except Exception as e:
        logger.error(f"Doris initialization error: {e}", exc_info=True)
        return False, str(e)

async def init_redis() -> Tuple[bool, Optional[str]]:
    try:
        logger.info("Connecting to Redis...")
        await redis_client.connect()
        
        # Configure Redis memory policy to prevent unbounded growth
        await configure_redis_memory_policy()
        
        logger.info("Redis connection established")
        return True, None
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False, str(e)


async def configure_redis_memory_policy() -> None:
    """
    Configure Redis memory policy to prevent unbounded memory growth.
    
    Sets maxmemory-policy to allkeys-lru which evicts least recently used keys
    when memory limit is reached.
    """
    try:
        # Get the underlying Redis client
        client = redis_client._client
        if not client:
            logger.warning("Redis client not available for memory policy configuration")
            return
        
        # Check current memory configuration
        info = await client.info("memory")
        current_maxmemory = info.get("maxmemory", 0)
        current_policy = info.get("maxmemory_policy", "noeviction")
        
        logger.info(f"Current Redis memory config: maxmemory={current_maxmemory}, policy={current_policy}")
        
        # Set memory policy to allkeys-lru if not already set
        # This ensures LRU eviction when memory is full
        if current_policy == "noeviction":
            try:
                await client.config_set("maxmemory-policy", "allkeys-lru")
                logger.info("Redis maxmemory-policy set to allkeys-lru")
            except Exception as e:
                # This might fail if Redis is configured to not allow runtime config changes
                logger.warning(f"Could not set Redis maxmemory-policy: {e}")
        
        # Set maxmemory if not configured (default to 256MB for development)
        # In production, this should be set via Redis configuration file
        if current_maxmemory == 0:
            try:
                # Default to 256MB - adjust based on environment
                default_maxmemory = "256mb"
                if settings.environment == "production":
                    default_maxmemory = "1gb"  # Higher limit for production
                
                await client.config_set("maxmemory", default_maxmemory)
                logger.info(f"Redis maxmemory set to {default_maxmemory}")
            except Exception as e:
                logger.warning(f"Could not set Redis maxmemory: {e}")
        
    except Exception as e:
        logger.warning(f"Failed to configure Redis memory policy: {e}")

async def init_semantic_index() -> Tuple[bool, Optional[str]]:
    try:
        logger.info("Initializing semantic schema index...")
        from app.services.semantic_schema_index_service import SemanticSchemaIndexService
        semantic_service = SemanticSchemaIndexService()
        await semantic_service.ensure_index()
        await semantic_service.ensure_built_if_empty()
        logger.info("Semantic schema index initialized")
        return True, None
    except Exception as e:
        logger.warning(f"Semantic index initialization failed: {e}")
        return False, str(e)

async def init_graphiti() -> Tuple[bool, Optional[str]]:
    try:
        logger.info("Initializing Graphiti Knowledge Graph...")
        from app.core.graphiti_client import create_graphiti_client
        graphiti_client = await create_graphiti_client()
        registry.set_graphiti_client(graphiti_client)
        logger.info("Graphiti client initialized successfully")
        return True, None
    except Exception as e:
        logger.warning(f"Graphiti initialization failed: {e}")
        return False, str(e)

async def init_sqlcl_pool() -> Tuple[bool, Optional[str]]:
    try:
        logger.info("Initializing SQLcl process pool...")
        pool = SQLclProcessPool(
            pool_size=settings.sqlcl_max_processes,
            max_queries_per_process=1000,
            process_timeout=settings.sqlcl_timeout,
            health_check_interval=60,
        )
        pool_success = await pool.initialize()
        
        if pool_success:
            registry.set_sqlcl_pool(pool)
            logger.info("SQLcl process pool initialized")
            
            # Initialize fallback MCP client
            try:
                mcp_client = create_mcp_client()
                if await mcp_client.initialize():
                    connect_res = await mcp_client.connect_database(settings.oracle_default_connection)
                    if connect_res.get("status") == "connected":
                        registry.set_mcp_client(mcp_client)
                        logger.info("Fallback MCP client connected")
                    else:
                        logger.warning(f"Fallback MCP connect failed: {connect_res.get('message')}")
                else:
                    logger.warning("Fallback MCP init failed")
            except Exception as mcp_err:
                logger.warning(f"Fallback MCP error: {mcp_err}")
                
            return True, None
        else:
            return False, "SQLcl process pool initialization failed"
            
    except Exception as e:
        logger.error(f"SQLcl pool error: {e}")
        return False, str(e)

async def init_orchestrator(app_state=None) -> Tuple[bool, Optional[str], Any]:
    """Returns (success, error, checkpointer_context)"""
    try:
        logger.info("Initializing LangGraph orchestrator...")
        from app.orchestrator import create_query_orchestrator
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

        checkpointer_context = AsyncSqliteSaver.from_conn_string(settings.LANGGRAPH_CHECKPOINT_DB)
        checkpointer = await checkpointer_context.__aenter__()
        
        orchestrator = await create_query_orchestrator(checkpointer)
        registry.set_query_orchestrator(orchestrator)
        registry.set_langgraph_checkpointer(checkpointer, checkpointer_context)
        
        logger.info("LangGraph orchestrator initialized")
        return True, None, checkpointer_context
        
    except Exception as e:
        logger.error(f"Orchestrator init failed: {e}")
        return False, str(e), None
