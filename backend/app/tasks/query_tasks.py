"""
Query execution background tasks

Handles long-running queries, caching, and cleanup
"""

import asyncio
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from app.core.celery_app import celery_app
from app.core.redis_client import redis_client
from app.core.client_registry import registry

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.query_tasks.execute_query_async",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def execute_query_async(self, query_id: str, sql_query: str, user_id: str) -> Dict[str, Any]:
    """
    Execute SQL query asynchronously via MCP client
    
    Args:
        query_id: Unique query identifier
        sql_query: SQL query string
        user_id: User who submitted the query
        
    Returns:
        Query execution result dict
    """
    try:
        logger.info(f"Executing async query: {query_id} for user {user_id}")
        
        # Create an isolated MCP client for the task to avoid relying on shared state
        from app.core.mcp_client import create_mcp_client
        from app.core.config import settings
        client = create_mcp_client()
        # Initialize and connect
        asyncio.run(client.initialize())
        conn = settings.oracle_default_connection
        connect_res = asyncio.run(client.connect_database(conn))
        if connect_res.get("status") != "connected":
            raise RuntimeError(f"Task DB connection failed: {connect_res.get('message')}")
        # Execute SQL query using MCP client with explicit connection
        result = asyncio.run(client.execute_sql(sql_query, connection_name=conn))
        
        # Add query metadata
        result["query_id"] = query_id
        result["user_id"] = user_id
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        # Cache result in Redis
        query_hash = hashlib.sha256(sql_query.encode()).hexdigest()
        cache_data = {
            "result": result,
            "query": sql_query,
            "cached_at": datetime.now(timezone.utc).isoformat()
        }
        asyncio.run(redis_client.cache_query_result(query_hash, cache_data, ttl=300))
        
        logger.info(f"Query {query_id} completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query {query_id} failed: {e}")
        # Retry with exponential backoff
        raise self.retry(exc=e, countdown=2 ** self.request.retries)


@celery_app.task(name="app.tasks.query_tasks.cleanup_old_results")
def cleanup_old_results(days_old: int = 7) -> int:
    """
    Remove query results older than specified days
    
    Args:
        days_old: Number of days to keep results
        
    Returns:
        Number of results cleaned up
    """
    try:
        logger.info(f"Starting cleanup of query results older than {days_old} days")
        
        # TODO: Implement actual cleanup logic
        # For now, just log
        count = 0
        
        logger.info(f"Cleaned up {count} old query results")
        return count
        
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        return 0


@celery_app.task(name="app.tasks.query_tasks.cache_popular_queries")
def cache_popular_queries() -> int:
    """
    Pre-cache frequently executed queries
    
    Returns:
        Number of queries cached
    """
    try:
        logger.info("Caching popular queries...")
        
        # TODO: Implement query popularity tracking and caching
        count = 0
        
        logger.info(f"Cached {count} popular queries")
        return count
        
    except Exception as e:
        logger.error(f"Failed to cache popular queries: {e}")
        return 0