from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any

from app.core.config import settings


async def _run_periodic_cleanup():
    """
    Background task to periodically clean up expired query metadata, states, and checkpoints.
    Runs every 5 minutes to prevent memory leaks and unbounded growth.
    """
    logger = logging.getLogger(__name__)
    cleanup_interval = 300  # 5 minutes
    checkpoint_cleanup_interval = 3600  # 1 hour for checkpoint cleanup
    last_checkpoint_cleanup = 0
    
    while True:
        try:
            await asyncio.sleep(cleanup_interval)
            current_time = time.time()
            
            # Import here to avoid circular imports
            from app.services.query_state_manager import get_query_state_manager
            
            state_manager = await get_query_state_manager()
            
            # Clean up expired metadata (older than 24 hours)
            cleaned_metadata = await state_manager.cleanup_expired_metadata()
            
            # Clean up terminal query states (finished, error, rejected)
            cleaned_states = await state_manager.cleanup_terminal_states()
            
            if cleaned_metadata > 0 or cleaned_states > 0:
                logger.info(f"Periodic cleanup: {cleaned_metadata} metadata, {cleaned_states} states removed")
            
            # Clean up LangGraph checkpoints (every hour)
            if current_time - last_checkpoint_cleanup >= checkpoint_cleanup_interval:
                try:
                    await _cleanup_langgraph_checkpoints()
                    last_checkpoint_cleanup = current_time
                except Exception as e:
                    logger.error(f"Checkpoint cleanup error: {e}")
                
        except asyncio.CancelledError:
            logger.info("Cleanup task cancelled")
            break
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")


async def _cleanup_langgraph_checkpoints():
    """
    Clean up old LangGraph SQLite checkpoints to prevent unbounded growth.
    Removes checkpoints older than 7 days and runs VACUUM to reclaim space.
    """
    import os
    import sqlite3
    from pathlib import Path
    
    logger = logging.getLogger(__name__)
    checkpoint_db_path = Path(settings.LANGGRAPH_CHECKPOINT_DB)
    
    if not checkpoint_db_path.exists():
        logger.debug("Checkpoint database does not exist, skipping cleanup")
        return
    
    try:
        # Get file size before cleanup
        size_before = checkpoint_db_path.stat().st_size
        
        # Connect to SQLite database
        conn = sqlite3.connect(str(checkpoint_db_path))
        cursor = conn.cursor()
        
        # Delete checkpoints older than 7 days
        # LangGraph stores checkpoints with timestamps
        seven_days_ago = int((time.time() - (7 * 24 * 60 * 60)) * 1000)  # milliseconds
        
        # Check if checkpoints table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints'")
        if cursor.fetchone():
            cursor.execute("DELETE FROM checkpoints WHERE checkpoint_ns < ?", (seven_days_ago,))
            deleted_count = cursor.rowcount
            
            # Run VACUUM to reclaim space
            conn.commit()
            cursor.execute("VACUUM")
            conn.commit()
            
            # Get file size after cleanup
            size_after = checkpoint_db_path.stat().st_size
            size_freed = size_before - size_after
            
            if deleted_count > 0 or size_freed > 0:
                logger.info(
                    f"Checkpoint cleanup: {deleted_count} old checkpoints removed, "
                    f"{size_freed / 1024 / 1024:.2f} MB freed"
                )
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to cleanup checkpoints: {e}")
from app.core.observability import setup_observability
from app.core.logging_config import setup_logging
from app.core.client_registry import registry
from app.core.graphiti_client import close_graphiti_client
from app.core.initializers import (
    init_doris,
    init_postgres,
    init_redis,
    init_semantic_index,
    init_graphiti,
    init_sqlcl_pool,
    init_orchestrator
)

# Initialize logger
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager
    Handles startup and shutdown events
    """
    # Startup
    start_time = time.time()
    component_status: Dict[str, Dict[str, Any]] = {}
    trace_identifier: str | None = None
    startup_errors = []
    environment = getattr(settings, "environment", "development")
    log_level = getattr(settings, "log_level", "INFO")

    # Initialize structured logging
    try:
        from app.core.structured_logging import configure_structured_logging, get_logger
        configure_structured_logging(
            log_level=log_level,
            json_format=environment.lower() == 'production',
            enable_colors=environment.lower() == 'development'
        )
        logger = get_logger(__name__)
        logger.info("Structured logging initialized")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Structured logging initialization failed: {e}")
    
    # Initialize degraded mode manager early
    try:
        from app.core.degraded_mode_manager import degraded_mode_manager
        
        # Register all components upfront
        degraded_mode_manager.register_component(
            "redis",
            impact_description="Session management, caching, and rate limiting"
        )
        degraded_mode_manager.register_component(
            "celery",
            impact_description="Background task processing (reports, schema refresh)"
        )
        degraded_mode_manager.register_component(
            "langgraph_checkpointer",
            impact_description="Query state persistence and HITL approval resumption"
        )
        degraded_mode_manager.register_component(
            "graphiti",
            impact_description="Context-aware query generation"
        )
        degraded_mode_manager.register_component(
            "postgres",
            impact_description="PostgreSQL database connectivity"
        )
        
        logger.info("Degraded mode manager initialized with component registry")
    except Exception as e:
        logger.warning(f"Degraded mode manager initialization failed: {e}")

    # Langfuse Setup
    langfuse_helpers = {}
    try:
        import app.core.langfuse_client as lfc
        langfuse_helpers = {
            'create_trace': lfc.create_trace,
            'get_client': lfc.get_langfuse_client,
            'trace_span': lfc.trace_span,
            'update_trace': lfc.update_trace,
            'log_event': lfc.log_event,
        }
    except Exception:
        pass

    if langfuse_helpers and settings.LANGFUSE_ENABLED:
        try:
            lf_client = langfuse_helpers['get_client']()
            if lf_client:
                startup_trace_key = f"startup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                trace_identifier = langfuse_helpers['create_trace'](
                    query_id=startup_trace_key,
                    user_id="system",
                    user_query="backend_startup",
                    metadata={
                        "entrypoint": "lifespan.startup",
                        "environment": environment,
                        "host": settings.LANGFUSE_HOST,
                    },
                ) or startup_trace_key
                component_status["application"] = {
                    "status": "initializing",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                setattr(app.state, "langfuse_startup_trace_id", trace_identifier)
        except Exception as e:
            logger.warning(f"Langfuse init failed: {e}")

    @asynccontextmanager
    async def startup_span(name: str, metadata: Dict[str, Any] | None = None):
        if trace_identifier and langfuse_helpers.get('trace_span'):
            async with langfuse_helpers['trace_span'](
                trace_identifier, name, metadata=metadata
            ) as span:
                yield span
        else:
            yield {"output": {}}

    logger.info("Starting Amila backend")

    # 1. Doris
    if settings.DORIS_MCP_ENABLED:
        success, err = await init_doris()
        app.state.doris_initialized = success
        if not success:
            startup_errors.append(f"Doris init failed: {err}")
            logger.warning(f"Doris init failed: {err}")
        else:
            logger.info("Doris MCP initialized successfully")
    
    # 1.5 PostgreSQL
    if settings.POSTGRES_ENABLED:
        async with startup_span("startup.postgres", metadata={"component": "postgres"}) as span:
            success, err = await init_postgres()
            app.state.postgres_initialized = success
            span["output"] = {"status": "success" if success else "error", "error": err}
            if not success:
                startup_errors.append(f"PostgreSQL init failed: {err}")
                logger.warning(f"PostgreSQL init failed: {err}")
            else:
                logger.info("PostgreSQL client initialized successfully")

    # 2. Encryption Service
    async with startup_span("startup.encryption", metadata={"component": "encryption"}) as span:
        try:
            from app.core.encryption import get_encryption_service
            encryption_service = get_encryption_service()
            if encryption_service.is_enabled():
                logger.info("Encryption service initialized and enabled")
                span["output"] = {"status": "success", "enabled": True}
                component_status["encryption"] = {"status": "success", "enabled": True}
            else:
                logger.warning("Encryption service initialized but disabled (no key configured)")
                span["output"] = {"status": "success", "enabled": False}
                component_status["encryption"] = {"status": "success", "enabled": False}
        except Exception as e:
            startup_errors.append(f"Encryption service failed: {e}")
            span["output"] = {"status": "error", "error": str(e)}
            component_status["encryption"] = {"status": "error", "error": str(e)}
            logger.error(f"Encryption service initialization failed: {e}")

    # 3. Observability
    async with startup_span("startup.observability", metadata={"component": "observability"}) as span:
        try:
            setup_observability()
            span["output"] = {"status": "success"}
            component_status["observability"] = {"status": "success"}
        except Exception as e:
            startup_errors.append(f"Observability failed: {e}")
            span["output"] = {"status": "error", "error": str(e)}
            component_status["observability"] = {"status": "error", "error": str(e)}

    # 4. Redis
    async with startup_span("startup.redis", metadata={"component": "redis"}) as span:
        success, err = await init_redis()
        status = "success" if success else "error"
        span["output"] = {"status": status, "error": err}
        component_status["redis"] = {"status": status, "error": err}
        if not success: startup_errors.append(f"Redis failed: {err}")
    
    # 4.5 Check Celery worker availability
    try:
        from app.core.celery_fallback import celery_fallback_handler
        from app.core.degraded_mode_manager import degraded_mode_manager, ComponentStatus
        
        if celery_fallback_handler.is_celery_available():
            logger.info("Celery workers available")
            degraded_mode_manager.update_component_status(
                "celery",
                ComponentStatus.OPERATIONAL
            )
        else:
            logger.warning("Celery workers not available, using fallback execution")
            degraded_mode_manager.update_component_status(
                "celery",
                ComponentStatus.DEGRADED,
                degradation_reason="No workers available",
                fallback_active=True,
                fallback_type="synchronous_execution",
                recovery_actions=[
                    "Start Celery workers: celery -A app.core.celery_app worker",
                    "Check Redis broker connectivity",
                    "Review Celery worker logs"
                ]
            )
    except Exception as e:
        logger.warning(f"Celery health check failed: {e}")

    # 4. Semantic Index
    if component_status.get("redis", {}).get("status") == "success":
        async with startup_span("startup.semantic_index", metadata={"component": "semantic_index"}) as span:
            success, err = await init_semantic_index()
            span["output"] = {"status": "success" if success else "error", "error": err}
    
    # 5. Graphiti
    async with startup_span("startup.graphiti", metadata={"component": "graphiti"}) as span:
        success, err = await init_graphiti()
        app.state.graphiti_initialized = success
        span["output"] = {"status": "success" if success else "error", "error": err}
        if not success: startup_errors.append(f"Graphiti failed: {err}")

    # 6. SQLcl Pool
    async with startup_span("startup.sqlcl_pool", metadata={"component": "sqlcl_pool"}) as span:
        success, err = await init_sqlcl_pool()
        span["output"] = {"status": "success" if success else "error", "error": err}
        if not success: startup_errors.append(f"SQLcl failed: {err}")

    # 7. Orchestrator
    async with startup_span("startup.langgraph", metadata={"component": "langgraph"}) as span:
        success, err, cp_context = await init_orchestrator()
        span["output"] = {"status": "success" if success else "error", "error": err}
        if success:
            app.state.checkpointer_context = cp_context
        else:
            startup_errors.append(f"Orchestrator failed: {err}")

    # Finalize
    app.state.startup_errors = startup_errors
    app.state.start_time = start_time
    
    if startup_errors:
        logger.warning(f"System started with {len(startup_errors)} warnings")
    else:
        logger.info("All systems ready")
    
    # Start background cleanup task for query state manager
    cleanup_task = None
    mcp_probe_task = None
    try:
        cleanup_task = asyncio.create_task(_run_periodic_cleanup())
        app.state.cleanup_task = cleanup_task
        logger.info("Started periodic cleanup background task")
    except Exception as e:
        logger.warning(f"Failed to start cleanup task: {e}")
    
    # Start background MCP probe task for diagnostics
    try:
        from app.services.diagnostic_service import start_mcp_probe_task
        mcp_probe_task = asyncio.create_task(start_mcp_probe_task())
        app.state.mcp_probe_task = mcp_probe_task
        logger.info("Started MCP probe background task")
    except Exception as e:
        logger.warning(f"Failed to start MCP probe task: {e}")

    if trace_identifier and langfuse_helpers.get('update_trace'):
        try:
            langfuse_helpers['update_trace'](
                trace_identifier,
                output_data={
                    "status": "ready" if not startup_errors else "degraded",
                    "warnings": startup_errors
                },
                tags=["startup", "success" if not startup_errors else "warning"]
            )
        except Exception:
            pass

    yield

    # Shutdown
    logger.info("Shutting down...")
    
    # Cancel cleanup task
    if hasattr(app.state, "cleanup_task") and app.state.cleanup_task:
        try:
            app.state.cleanup_task.cancel()
            await asyncio.wait_for(app.state.cleanup_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.warning(f"Cleanup task cancellation error: {e}")
    
    # Cancel MCP probe task
    if hasattr(app.state, "mcp_probe_task") and app.state.mcp_probe_task:
        try:
            app.state.mcp_probe_task.cancel()
            await asyncio.wait_for(app.state.mcp_probe_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.warning(f"MCP probe task cancellation error: {e}")
    
    # Cleanup checkpointer
    if hasattr(app.state, "checkpointer_context") and app.state.checkpointer_context:
        try:
            await app.state.checkpointer_context.__aexit__(None, None, None)
        except Exception as e:
            logger.error(f"Checkpointer cleanup failed: {e}")

    # Cleanup Graphiti
    try:
        await close_graphiti_client()
    except Exception:
        pass
        
    # Cleanup Redis
    try:
        from app.core.redis_client import redis_client
        await redis_client.close()
    except Exception:
        pass
        
    # Cleanup SQLcl
    try:
        sqlcl_pool = registry.get_sqlcl_pool()
        if sqlcl_pool:
            await sqlcl_pool.shutdown()
    except Exception:
        pass
        
    # Cleanup Doris server
    if settings.DORIS_MCP_ENABLED:
        try:
            from app.core.doris_server import doris_server
            doris_server.stop()
        except Exception:
            pass
    
    # Cleanup PostgreSQL
    if settings.POSTGRES_ENABLED:
        try:
            from app.core.postgres_client import postgres_client
            await postgres_client.close()
        except Exception:
            pass
            
    logger.info("Shutdown complete")
