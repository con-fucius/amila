"""
Query Results Store
Centralized helper for caching query results and building transport payloads.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.core.sql_utils import normalize_sql

# Transport thresholds (SSE + REST responses)
STREAM_RESULT_MAX_ROWS = 200
STREAM_RESULT_PREVIEW_ROWS = 50

# Result reference TTL (seconds)
RESULT_REF_TTL_SECONDS = 6 * 60 * 60


def compute_query_hash(sql: str, database_type: str) -> str:
    norm_sql = normalize_sql(sql or "")
    hash_input = f"{database_type}:{norm_sql}"
    return hashlib.sha256(hash_input.encode()).hexdigest()


async def store_query_result(
    *,
    query_id: str,
    sql_query: str,
    database_type: str,
    result: Dict[str, Any],
) -> str:
    """Store query result in cache and register a result reference by query_id."""
    query_hash = compute_query_hash(sql_query, database_type)
    row_count = result.get("row_count", len(result.get("rows", [])))

    await redis_client.cache_query_result(
        query_hash,
        result,
        result_size=row_count,
    )
    await redis_client.set_query_result_ref(query_id, query_hash, ttl=RESULT_REF_TTL_SECONDS)
    await redis_client.cache_query_result_by_id(query_id, result, ttl=RESULT_REF_TTL_SECONDS)
    return query_hash


async def register_query_result_ref(
    *,
    query_id: str,
    sql_query: str,
    database_type: str,
) -> str:
    """Register query_id -> query_hash mapping without storing a new result."""
    query_hash = compute_query_hash(sql_query, database_type)
    await redis_client.set_query_result_ref(query_id, query_hash, ttl=RESULT_REF_TTL_SECONDS)
    return query_hash


async def fetch_result_by_query_id(query_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve cached query result using a query_id reference."""
    ref = await redis_client.get_query_result_ref(query_id)
    query_hash = ref.get("query_hash") if isinstance(ref, dict) else None
    if query_hash:
        cached = await redis_client.get_cached_query_result(query_hash)
        if cached:
            return cached
    # Fallback to direct query_id storage
    return await redis_client.get_query_result_by_id(query_id)


def build_transport_payload(
    *,
    query_id: str,
    result: Dict[str, Any],
    cache_status: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool]:
    """Build a transport-safe payload (preview + reference for large results)."""
    columns = result.get("columns", [])
    rows = result.get("rows", []) or []
    row_count = result.get("row_count", len(rows))
    execution_time_ms = result.get("execution_time_ms", 0)
    timestamp = result.get("timestamp") or datetime.now(timezone.utc).isoformat()

    data_quality = result.get("data_quality")

    if row_count > STREAM_RESULT_MAX_ROWS or len(rows) > STREAM_RESULT_MAX_ROWS:
        preview_rows = rows[:STREAM_RESULT_PREVIEW_ROWS]
        preview = {
            "status": result.get("status", "success"),
            "columns": columns,
            "rows": preview_rows,
            "row_count": row_count,
            "execution_time_ms": execution_time_ms,
            "timestamp": timestamp,
            "truncated": True,
            "preview_rows": len(preview_rows),
        }
        if data_quality is not None:
            preview["data_quality"] = data_quality
        result_ref = {
            "query_id": query_id,
            "row_count": row_count,
            "columns": columns,
            "cache_status": cache_status,
        }
        return preview, result_ref, True

    if data_quality is not None and "data_quality" not in result:
        result = {**result, "data_quality": data_quality}
    return result, None, False
