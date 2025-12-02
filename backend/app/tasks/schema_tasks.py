"""
Schema metadata refresh and caching tasks
"""

import logging
import time
from typing import Dict, Any

from app.core.celery_app import celery_app
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.schema_tasks.refresh_schema_cache",
    bind=True,
)
def refresh_schema_cache(self) -> Dict[str, Any]:
    """
    Refresh database schema metadata cache
    
    Runs periodically (hourly) to keep schema info up-to-date
    
    Returns:
        Statistics dict
    """
    import asyncio
    from app.core.mcp_client import mcp_client
    
    try:
        logger.info(f"Starting schema cache refresh...")
        start_time = time.time()
        
        # Fetch schema from MCP using isolated client with explicit connection
        from app.core.mcp_client import create_mcp_client
        from app.core.config import settings
        client = create_mcp_client()
        asyncio.run(client.initialize())
        conn = settings.oracle_default_connection
        connect_res = asyncio.run(client.connect_database(conn))
        if connect_res.get("status") != "connected":
            raise RuntimeError(f"Schema task DB connection failed: {connect_res.get('message')}")
        schema_data = asyncio.run(client.get_schema(conn))
        
        if schema_data:
            # Cache the schema metadata
            asyncio.run(redis_client.cache_schema_metadata(
                "oracle_schema",
                schema_data,
                ttl=3600  # 1 hour TTL
            ))
            
            tables_count = len(schema_data.get("tables", []))
            columns_count = sum(
                len(table.get("columns", [])) 
                for table in schema_data.get("tables", [])
            )
            
            stats = {
                "tables_cached": tables_count,
                "columns_cached": columns_count,
                "execution_time_ms": int((time.time() - start_time) * 1000),
            }
            
            logger.info(f"Schema cache refreshed: {stats}")
            return stats
        else:
            logger.warning(f"No schema data returned from MCP")
            return {"tables_cached": 0, "columns_cached": 0, "execution_time_ms": 0}
        
    except Exception as e:
        logger.error(f"Schema cache refresh failed: {e}")
        raise self.retry(exc=e, countdown=300)  # Retry after 5 minutes


@celery_app.task(name="app.tasks.schema_tasks.invalidate_schema_cache")
def invalidate_schema_cache() -> int:
    """
    Invalidate all schema cache entries
    
    Useful when database schema changes detected
    
    Returns:
        Number of cache entries invalidated
    """
    import asyncio
    
    try:
        logger.info("Invalidating schema cache...")
        
        count = asyncio.run(redis_client.invalidate_schema_cache("schema:*"))
        
        logger.info(f"Invalidated {count} schema cache entries")
        return count
        
    except Exception as e:
        logger.error(f"Failed to invalidate schema cache: {e}")
        return 0