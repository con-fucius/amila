from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging
import time
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any

from app.core.config import settings
from app.core.observability import setup_observability
from app.core.logging_config import setup_logging
from app.core.client_registry import registry
from app.core.graphiti_client import close_graphiti_client
from app.core.initializers import (
    init_doris,
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

    # 2. Observability
    async with startup_span("startup.observability", metadata={"component": "observability"}) as span:
        try:
            setup_observability()
            span["output"] = {"status": "success"}
            component_status["observability"] = {"status": "success"}
        except Exception as e:
            startup_errors.append(f"Observability failed: {e}")
            span["output"] = {"status": "error", "error": str(e)}
            component_status["observability"] = {"status": "error", "error": str(e)}

    # 3. Redis
    async with startup_span("startup.redis", metadata={"component": "redis"}) as span:
        success, err = await init_redis()
        status = "success" if success else "error"
        span["output"] = {"status": status, "error": err}
        component_status["redis"] = {"status": status, "error": err}
        if not success: startup_errors.append(f"Redis failed: {err}")

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
            
    logger.info("Shutdown complete")
