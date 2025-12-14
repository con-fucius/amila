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
        logger.info("Doris MCP integration is disabled (DORIS_MCP_ENABLED=false)")
        return False, "Doris MCP disabled"
    
    try:
        logger.info("Initializing Doris MCP server...")
        
        # Check if doris-mcp-server package is available
        try:
            import doris_mcp_server
            logger.info(f"Doris MCP Server package version: {getattr(doris_mcp_server, '__version__', 'unknown')}")
        except ImportError:
            logger.error("doris-mcp-server package not installed. Install with: pip install doris-mcp-server")
            return False, "doris-mcp-server package not installed"
        
        doris_server_ready = await doris_server.initialize()
        
        if doris_server_ready:
            doris_client_ready = await doris_client.initialize()
            if doris_client_ready:
                registry.set_doris_client(doris_client)
                logger.info("Doris MCP client registered successfully")
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
        if current_policy == "noeviction":
            try:
                await client.config_set("maxmemory-policy", "allkeys-lru")
                logger.info("Redis maxmemory-policy set to allkeys-lru")
            except Exception as e:
                logger.warning(f"Could not set Redis maxmemory-policy: {e}")
        
        # Set maxmemory if not configured (default to 256MB for development)
        if current_maxmemory == 0:
            try:
                default_maxmemory = "256mb"
                if settings.environment == "production":
                    default_maxmemory = "1gb"
                
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
        error_msg = str(e)
        # Check if it's a RediSearch module missing error
        if "FT.CREATE" in error_msg or "unknown command" in error_msg.lower():
            logger.warning(
                "RediSearch module not available. Semantic index disabled. "
                "Use redis/redis-stack-server image for full functionality."
            )
        else:
            logger.warning(f"Semantic index initialization failed: {e}")
        return False, error_msg

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
    import shutil
    from pathlib import Path
    
    try:
        # Check if SQLcl is available
        sqlcl_path = settings.sqlcl_path
        sqlcl_exists = False
        
        # Check if it's an absolute path that exists
        if Path(sqlcl_path).is_absolute():
            sqlcl_exists = Path(sqlcl_path).exists()
        else:
            # Check if it's in PATH
            sqlcl_exists = shutil.which(sqlcl_path) is not None
        
        if not sqlcl_exists:
            logger.warning(f"SQLcl not found at '{sqlcl_path}'. SQLcl MCP integration will be disabled.")
            logger.info("To enable SQLcl: download from Oracle and set SQLCL_PATH environment variable")
            return False, f"SQLcl not found at {sqlcl_path}"
        
        # Check if Oracle connection is configured
        if not settings.oracle_default_connection:
            logger.warning("No Oracle default connection configured. SQLcl MCP integration will be disabled.")
            return False, "No Oracle default connection configured"
        
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
            return False, "SQLcl process pool initialization failed - check Oracle connection configuration"
            
    except Exception as e:
        logger.error(f"SQLcl pool error: {e}")
        return False, str(e)

async def init_orchestrator(app_state=None) -> Tuple[bool, Optional[str], Any]:
    """Returns (success, error, checkpointer_context)"""
    try:
        logger.info("Initializing LangGraph orchestrator...")
        from app.orchestrator import create_query_orchestrator
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

        if not settings.LANGGRAPH_CHECKPOINT_DB:
            logger.warning("LANGGRAPH_CHECKPOINT_DB not set, using in-memory checkpointer (NOT PERSISTENT)")
            
        logger.info(f"Initializing LangGraph checkpointer at {settings.LANGGRAPH_CHECKPOINT_DB}")
        checkpointer_context = AsyncSqliteSaver.from_conn_string(settings.LANGGRAPH_CHECKPOINT_DB)
        checkpointer = await checkpointer_context.__aenter__()
        
        # Verify connection immediately
        try:
            await checkpointer.alist({"configurable": {"thread_id": "test_connection"}})
            logger.info("Checkpointer connection verified successfully")
        except Exception as e:
            logger.warning(f"Checkpointer connection test warning: {e}")

        orchestrator = await create_query_orchestrator(checkpointer)
        registry.set_query_orchestrator(orchestrator)
        registry.set_langgraph_checkpointer(checkpointer, checkpointer_context)
        
        logger.info(f"LangGraph orchestrator initialized (cp_id={id(checkpointer)})")
        return True, None, checkpointer_context
        
    except Exception as e:
        logger.error(f"Orchestrator init failed: {e}")
        return False, str(e), None
