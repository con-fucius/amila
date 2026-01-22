"""
User Activity Analytics Endpoints (Gap #33)
Provides insights into user query patterns, popular tables, and system usage
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
import re

from app.core.audit import audit_logger, AuditAction
from app.core.rbac import require_permission, Permission, rbac_manager
from app.core.redis_client import redis_client
from app.core.structured_logging import get_iso_timestamp
from app.models.internal_models import safe_parse_json, AuditEntryData
from app.core.encryption import get_encryption_service

router = APIRouter()
logger = logging.getLogger(__name__)

# Initialize encryption service
encryption_service = get_encryption_service()


@router.get("/user-activity")
@require_permission(Permission.SYSTEM_METRICS)
async def get_user_activity_analytics(
    days: int = 7,
    limit: int = 50,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Get user activity analytics - which users query most (Gap #33)
    
    GDPR Compliance: User IDs are pseudonymized using hashed identifiers
    
    Args:
        days: Number of days to analyze (default: 7)
        limit: Maximum number of users to return (default: 50)
    
    Returns:
        User activity statistics
    """
    try:
        # Get audit logs from Redis for QUERY_EXECUTE actions
        pattern = f"audit:*:*:{AuditAction.QUERY_EXECUTE.value}"
        keys = await redis_client.keys(pattern)
        
        # Calculate time threshold
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Aggregate user activity
        user_stats = defaultdict(lambda: {
            "query_count": 0,
            "total_execution_time_ms": 0,
            "success_count": 0,
            "error_count": 0,
            "last_activity": None
        })
        
        for key in keys:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    # Validate and decrypt audit entry
                    entry_data = safe_parse_json(entry_json, AuditEntryData, default=None, log_errors=True)
                    if not entry_data:
                        continue
                    
                    entry = encryption_service.decrypt_audit_entry(entry_data.model_dump())
                    
                    # Check if within time window
                    entry_time = datetime.fromisoformat(entry["timestamp"])
                    if entry_time < cutoff_time:
                        continue
                    
                    user = entry["user"]
                    details = entry.get("details", {})
                    
                    # Update statistics
                    user_stats[user]["query_count"] += 1
                    user_stats[user]["total_execution_time_ms"] += details.get("execution_time_ms", 0)
                    
                    if entry["success"]:
                        user_stats[user]["success_count"] += 1
                    else:
                        user_stats[user]["error_count"] += 1
                    
                    # Update last activity
                    if not user_stats[user]["last_activity"] or entry_time > datetime.fromisoformat(user_stats[user]["last_activity"]):
                        user_stats[user]["last_activity"] = entry["timestamp"]
                        
            except Exception as e:
                logger.error(f"Failed to process audit entry {key}: {e}")
                continue
        
        # Sort by query count and limit
        sorted_users = sorted(
            [{"user": k, **v} for k, v in user_stats.items()],
            key=lambda x: x["query_count"],
            reverse=True
        )[:limit]
        
        # Calculate average execution time
        for user in sorted_users:
            if user["query_count"] > 0:
                user["avg_execution_time_ms"] = round(
                    user["total_execution_time_ms"] / user["query_count"], 2
                )
            else:
                user["avg_execution_time_ms"] = 0
        
        return {
            "period_days": days,
            "total_users": len(user_stats),
            "top_users": sorted_users,
            "timestamp": get_iso_timestamp()
        }
        
    except Exception as e:
        logger.error(f"Failed to generate user activity analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate analytics")


@router.get("/table-usage")
@require_permission(Permission.SYSTEM_METRICS)
async def get_table_usage_analytics(
    days: int = 7,
    limit: int = 50,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Get table usage analytics - which tables are queried most (Gap #33)
    
    Args:
        days: Number of days to analyze (default: 7)
        limit: Maximum number of tables to return (default: 50)
    
    Returns:
        Table usage statistics
    """
    try:
        # Get audit logs for query executions
        pattern = f"audit:*:*:{AuditAction.QUERY_EXECUTE.value}"
        keys = await redis_client.keys(pattern)
        
        # Calculate time threshold
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Extract table names from queries
        table_stats = defaultdict(lambda: {
            "query_count": 0,
            "unique_users": set(),
            "avg_execution_time_ms": [],
            "success_count": 0,
            "error_count": 0
        })
        
        for key in keys:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    # Validate and decrypt audit entry
                    entry_data = safe_parse_json(entry_json, AuditEntryData, default=None, log_errors=True)
                    if not entry_data:
                        continue
                    
                    entry = encryption_service.decrypt_audit_entry(entry_data.model_dump())
                    
                    # Check if within time window
                    entry_time = datetime.fromisoformat(entry["timestamp"])
                    if entry_time < cutoff_time:
                        continue
                    
                    details = entry.get("details", {})
                    sql_query = details.get("sql_query", "")
                    
                    # Extract table names using simple regex (FROM and JOIN clauses)
                    tables = _extract_table_names(sql_query)
                    
                    for table in tables:
                        table_stats[table]["query_count"] += 1
                        table_stats[table]["unique_users"].add(entry["user"])
                        
                        exec_time = details.get("execution_time_ms", 0)
                        if exec_time > 0:
                            table_stats[table]["avg_execution_time_ms"].append(exec_time)
                        
                        if entry["success"]:
                            table_stats[table]["success_count"] += 1
                        else:
                            table_stats[table]["error_count"] += 1
                            
            except Exception as e:
                logger.error(f"Failed to process audit entry {key}: {e}")
                continue
        
        # Convert sets to counts and calculate averages
        table_results = []
        for table, stats in table_stats.items():
            avg_time = (
                round(sum(stats["avg_execution_time_ms"]) / len(stats["avg_execution_time_ms"]), 2)
                if stats["avg_execution_time_ms"] else 0
            )
            
            table_results.append({
                "table_name": table,
                "query_count": stats["query_count"],
                "unique_users": len(stats["unique_users"]),
                "avg_execution_time_ms": avg_time,
                "success_count": stats["success_count"],
                "error_count": stats["error_count"],
                "success_rate": round(
                    (stats["success_count"] / stats["query_count"] * 100) if stats["query_count"] > 0 else 0,
                    2
                )
            })
        
        # Sort by query count and limit
        sorted_tables = sorted(
            table_results,
            key=lambda x: x["query_count"],
            reverse=True
        )[:limit]
        
        return {
            "period_days": days,
            "total_tables": len(table_stats),
            "top_tables": sorted_tables,
            "timestamp": get_iso_timestamp()
        }
        
    except Exception as e:
        logger.error(f"Failed to generate table usage analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate analytics")


@router.get("/query-patterns")
@require_permission(Permission.SYSTEM_METRICS)
async def get_query_pattern_analytics(
    days: int = 7,
    limit: int = 20,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Get query pattern analytics - most common query types and patterns
    
    Args:
        days: Number of days to analyze (default: 7)
        limit: Maximum number of patterns to return (default: 20)
    
    Returns:
        Query pattern statistics
    """
    try:
        # Get audit logs for query executions
        pattern = f"audit:*:*:{AuditAction.QUERY_EXECUTE.value}"
        keys = await redis_client.keys(pattern)
        
        # Calculate time threshold
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Collect query fingerprints and types
        query_fingerprints = Counter()
        query_types = Counter()
        total_queries = 0
        
        for key in keys:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    # Validate and decrypt audit entry
                    entry_data = safe_parse_json(entry_json, AuditEntryData, default=None, log_errors=True)
                    if not entry_data:
                        continue
                    
                    entry = encryption_service.decrypt_audit_entry(entry_data.model_dump())
                    
                    # Check if within time window
                    entry_time = datetime.fromisoformat(entry["timestamp"])
                    if entry_time < cutoff_time:
                        continue
                    
                    total_queries += 1
                    details = entry.get("details", {})
                    
                    # Count fingerprints (similar queries)
                    fingerprint = details.get("query_fingerprint", "unknown")
                    query_fingerprints[fingerprint] += 1
                    
                    # Categorize query type
                    sql_query = details.get("sql_query", "").upper()
                    query_type = _categorize_query_type(sql_query)
                    query_types[query_type] += 1
                    
            except Exception as e:
                logger.error(f"Failed to process audit entry {key}: {e}")
                continue
        
        # Get most common fingerprints
        top_fingerprints = [
            {"fingerprint": fp, "count": count, "percentage": round((count / total_queries * 100), 2)}
            for fp, count in query_fingerprints.most_common(limit)
        ]
        
        # Get query type distribution
        type_distribution = [
            {"type": qtype, "count": count, "percentage": round((count / total_queries * 100), 2)}
            for qtype, count in query_types.most_common()
        ]
        
        return {
            "period_days": days,
            "total_queries": total_queries,
            "unique_patterns": len(query_fingerprints),
            "top_query_patterns": top_fingerprints,
            "query_type_distribution": type_distribution,
            "timestamp": get_iso_timestamp()
        }
        
    except Exception as e:
        logger.error(f"Failed to generate query pattern analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate analytics")


def _extract_table_names(sql_query: str) -> List[str]:
    """
    Extract table names from SQL query using regex patterns
    
    Args:
        sql_query: SQL query string
    
    Returns:
        List of table names
    """
    tables = set()
    
    # Normalize query
    query_upper = sql_query.upper()
    
    # Pattern for FROM clause: FROM table_name or FROM schema.table_name
    from_pattern = r'FROM\s+([A-Z0-9_]+\.)?([A-Z0-9_]+)'
    from_matches = re.findall(from_pattern, query_upper)
    for match in from_matches:
        table = match[1] if match[1] else match[0]
        if table:
            tables.add(table)
    
    # Pattern for JOIN clause: JOIN table_name or JOIN schema.table_name
    join_pattern = r'JOIN\s+([A-Z0-9_]+\.)?([A-Z0-9_]+)'
    join_matches = re.findall(join_pattern, query_upper)
    for match in join_matches:
        table = match[1] if match[1] else match[0]
        if table:
            tables.add(table)
    
    return list(tables)


def _categorize_query_type(sql_query: str) -> str:
    """
    Categorize SQL query by type
    
    Args:
        sql_query: SQL query string (uppercase)
    
    Returns:
        Query type category
    """
    if sql_query.strip().startswith("SELECT"):
        if "JOIN" in sql_query:
            return "SELECT_JOIN"
        elif "GROUP BY" in sql_query or "AGGREGATE" in sql_query:
            return "SELECT_AGGREGATE"
        else:
            return "SELECT_SIMPLE"
    elif sql_query.strip().startswith("INSERT"):
        return "INSERT"
    elif sql_query.strip().startswith("UPDATE"):
        return "UPDATE"
    elif sql_query.strip().startswith("DELETE"):
        return "DELETE"
    elif sql_query.strip().startswith("CREATE"):
        return "DDL_CREATE"
    elif sql_query.strip().startswith("ALTER"):
        return "DDL_ALTER"
    elif sql_query.strip().startswith("DROP"):
        return "DDL_DROP"
    else:
        return "OTHER"