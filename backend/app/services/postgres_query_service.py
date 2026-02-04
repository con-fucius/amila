"""
PostgreSQL Query Service
Secure read-only query execution via direct psycopg3 connection pool
Includes PostgreSQL-specific optimizations for performance
"""

import logging
import hashlib
import re
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import asyncio

from app.core.config import settings
from app.core.exceptions import ExternalServiceException
from app.core.audit import log_audit_event
from app.core.postgres_client import postgres_client

logger = logging.getLogger(__name__)


class PostgresQueryService:
    """
    High-level PostgreSQL query service with audit logging, error handling,
    and PostgreSQL-specific optimizations.
    Uses direct psycopg3 connection pool for simplicity and performance.
    """
    
    # LRU cache for query plans (query_hash -> plan result)
    _plan_cache: Dict[str, Dict[str, Any]] = {}
    _plan_cache_max_size = 100
    
    # Optimizer hints mapping
    _OPTIMIZER_HINTS = {
        "INDEX_SCAN": "INDEX",
        "SEQ_SCAN": "SEQSCAN",
        "INDEX_ONLY_SCAN": "INDEXONLYSCAN",
        "BITMAP_SCAN": "BITMAPSCAN",
        "NESTED_LOOP": "NESTLOOP",
        "HASH_JOIN": "HASHJOIN",
        "MERGE_JOIN": "MERGEJOIN",
    }
    
    @classmethod
    async def execute_sql_query(
        cls,
        sql_query: str,
        user_id: str,
        user_role: Optional[str] = None,
        request_id: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query with audit logging.
        
        Args:
            sql_query: SQL query to execute
            user_id: User ID for audit
            user_role: User role for audit
            request_id: Request ID for tracing
            timeout: Query timeout in seconds
            
        Returns:
            Query result
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        start_time = datetime.now(timezone.utc)
        
        try:
            result = await postgres_client.execute_query(
                sql=sql_query,
                user_id=user_id or "unknown",
                request_id=request_id or "unknown",
                timeout=timeout
            )
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            await log_audit_event(
                event_type="postgres_query_success",
                user_id=user_id,
                details={
                    "request_id": request_id,
                    "user_role": user_role,
                    "sql": sql_query[:500],
                    "row_count": result.get("row_count", 0),
                    "execution_time_ms": execution_time,
                    "database": settings.POSTGRES_DATABASE,
                    "read_only": settings.POSTGRES_READ_ONLY
                }
            )
            
            return {
                "status": "success",
                "results": {
                    "columns": result.get("columns", []),
                    "rows": result.get("rows", []),
                    "row_count": result.get("row_count", 0),
                    "execution_time_ms": result.get("execution_time_ms", 0)
                }
            }
            
        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            await log_audit_event(
                event_type="postgres_query_failed",
                user_id=user_id,
                details={
                    "request_id": request_id,
                    "user_role": user_role,
                    "sql": sql_query[:500],
                    "error": str(e),
                    "execution_time_ms": execution_time,
                    "database": settings.POSTGRES_DATABASE
                }
            )
            
            raise
    
    @classmethod
    async def get_schema_info(cls, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get database schema information"""
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        return await postgres_client.get_schema_info(schema_name)
    
    @classmethod
    async def health_check(cls) -> Dict[str, Any]:
        """Check PostgreSQL service health"""
        if not settings.POSTGRES_ENABLED:
            return {
                "status": "disabled",
                "healthy": False
            }
        
        return await postgres_client.health_check()
    
    # =========================================================================
    # PostgreSQL-Specific Optimization Methods
    # =========================================================================
    
    @classmethod
    async def analyze_query_plan(
        cls,
        sql_query: str,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze query execution plan with caching for optimization.
        
        Uses EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for detailed plan information.
        Cached results are returned for repeated identical queries.
        
        Args:
            sql_query: SQL query to analyze
            use_cache: Whether to use cached plan if available
            
        Returns:
            Dictionary with plan analysis including:
            - estimated_cost: Query cost estimate
            - actual_time_ms: Actual execution time if ANALYZE is used
            - index_usage: List of indexes used
            - full_scans: Tables with sequential scans
            - recommendations: Optimization suggestions
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        query_hash = cls._hash_query(sql_query)
        
        # Check cache
        if use_cache and query_hash in cls._plan_cache:
            cached = cls._plan_cache[query_hash]
            # Cache expiry: 5 minutes
            cache_age = (datetime.now(timezone.utc) - cached.get("cached_at", datetime.min.replace(tzinfo=timezone.utc))).total_seconds()
            if cache_age < 300:
                logger.debug(f"Using cached query plan for hash: {query_hash[:8]}")
                return cached["plan"]
        
        try:
            # Use EXPLAIN with ANALYZE and BUFFERS for detailed metrics
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql_query}"
            
            result = await postgres_client.execute_query(
                sql=explain_sql,
                user_id="system",
                request_id=f"plan_{query_hash[:8]}"
            )
            
            import json
            
            rows = result.get("rows", [])
            if not rows:
                return {"error": "No plan data returned"}
            
            # Parse JSON plan
            plan_data = rows[0][0] if isinstance(rows[0], (list, tuple)) else list(rows[0].values())[0]
            if isinstance(plan_data, str):
                plan_data = json.loads(plan_data)
            
            if not isinstance(plan_data, list) or len(plan_data) == 0:
                return {"error": "Invalid plan format"}
            
            plan = plan_data[0]
            plan_node = plan.get("Plan", {})
            
            # Analyze plan node recursively
            analysis = cls._analyze_plan_node(plan_node)
            
            # Add execution stats
            if "Execution Time" in plan:
                analysis["execution_time_ms"] = plan["Execution Time"]
            
            # Cache the result
            if len(cls._plan_cache) >= cls._plan_cache_max_size:
                # Remove oldest entry
                oldest_key = min(cls._plan_cache.keys(), key=lambda k: cls._plan_cache[k].get("cached_at", datetime.min.replace(tzinfo=timezone.utc)))
                del cls._plan_cache[oldest_key]
            
            cls._plan_cache[query_hash] = {
                "plan": analysis,
                "cached_at": datetime.now(timezone.utc)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Query plan analysis failed: {e}")
            return {"error": str(e)}
    
    @classmethod
    def _analyze_plan_node(cls, node: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively analyze a plan node and extract optimization insights"""
        analysis = {
            "estimated_cost": node.get("Total Cost", 0),
            "estimated_rows": node.get("Plan Rows", 0),
            "node_type": node.get("Node Type", "Unknown"),
            "index_usage": [],
            "full_scans": [],
            "warnings": [],
            "recommendations": []
        }
        
        # Check for sequential scans
        node_type = node.get("Node Type", "")
        relation_name = node.get("Relation Name", "")
        
        if node_type == "Seq Scan" and relation_name:
            analysis["full_scans"].append(relation_name)
            analysis["warnings"].append(f"Sequential scan on {relation_name}")
            analysis["recommendations"].append(f"Consider adding index on {relation_name}")
            
            # Check for large sequential scans
            rows = node.get("Plan Rows", 0)
            if rows > 100000:
                analysis["warnings"].append(f"Large sequential scan: ~{rows} rows")
                analysis["recommendations"].append(f"Consider partitioning {relation_name}")
        
        # Check for index usage
        if node_type in ["Index Scan", "Index Only Scan", "Bitmap Index Scan"]:
            index_name = node.get("Index Name", "")
            if index_name:
                analysis["index_usage"].append({
                    "table": relation_name,
                    "index": index_name,
                    "type": node_type
                })
        
        # Check for hash joins with large datasets
        if node_type == "Hash Join":
            hash_cond = node.get("Hash Cond", "")
            rows = node.get("Plan Rows", 0)
            if rows > 50000:
                analysis["warnings"].append(f"Large hash join: ~{rows} rows")
                analysis["recommendations"].append("Ensure adequate work_mem for hash operations")
        
        # Check for nested loops on large datasets
        if node_type == "Nested Loop":
            rows = node.get("Plan Rows", 0)
            if rows > 10000:
                analysis["warnings"].append(f"Nested loop on large dataset: ~{rows} rows")
                analysis["recommendations"].append("Consider enabling merge or hash join for better performance")
        
        # Check for sorts
        if node_type == "Sort":
            sort_keys = node.get("Sort Key", [])
            sort_space = node.get("Sort Space Used", 0)
            analysis["recommendations"].append(f"Sort operation on {', '.join(sort_keys)} - ensure adequate work_mem")
            if sort_space > 0:
                analysis["warnings"].append(f"External sort used ({sort_space} KB disk)")
        
        # Recurse into child nodes
        for child in node.get("Plans", []):
            child_analysis = cls._analyze_plan_node(child)
            analysis["index_usage"].extend(child_analysis["index_usage"])
            analysis["full_scans"].extend(child_analysis["full_scans"])
            analysis["warnings"].extend(child_analysis["warnings"])
            analysis["recommendations"].extend(child_analysis["recommendations"])
        
        # Deduplicate
        analysis["index_usage"] = list({json.dumps(i, sort_keys=True) for i in analysis["index_usage"]})
        analysis["full_scans"] = list(set(analysis["full_scans"]))
        analysis["warnings"] = list(set(analysis["warnings"]))
        analysis["recommendations"] = list(set(analysis["recommendations"]))
        
        return analysis
    
    @classmethod
    async def apply_optimization_hints(
        cls,
        sql_query: str,
        hints: Dict[str, Any]
    ) -> str:
        """
        Apply PostgreSQL optimizer hints using pg_hint_plan syntax.
        
        Args:
            sql_query: Original SQL query
            hints: Dictionary of hints to apply:
                - scan_hints: {"table": "INDEX_SCAN|SEQ_SCAN|INDEX_ONLY_SCAN"}
                - join_hints: {"tables": ["t1", "t2"], "method": "HASH_JOIN|NESTED_LOOP|MERGE_JOIN"}
                - parallel: {"workers": 4} - number of parallel workers
                
        Returns:
            Modified SQL with hints prepended as comments
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        hint_parts = []
        
        # Scan hints
        scan_hints = hints.get("scan_hints", {})
        for table, scan_type in scan_hints.items():
            hint_name = cls._OPTIMIZER_HINTS.get(scan_type.upper())
            if hint_name:
                hint_parts.append(f"{hint_name}({table})")
        
        # Join hints
        join_hints = hints.get("join_hints", {})
        if join_hints:
            tables = join_hints.get("tables", [])
            method = join_hints.get("method", "HASH_JOIN")
            hint_name = cls._OPTIMIZER_HINTS.get(method.upper())
            if hint_name and tables:
                hint_parts.append(f"{hint_name}({' '.join(tables)})")
        
        # Parallel hints
        parallel = hints.get("parallel", {})
        if parallel:
            workers = parallel.get("workers", 4)
            table = parallel.get("table", "")
            if table:
                hint_parts.append(f"PARALLEL({table} {workers})")
        
        # Row count hints (for improving cardinality estimates)
        rows_hints = hints.get("rows_hints", {})
        for table, estimated_rows in rows_hints.items():
            hint_parts.append(f"ROWS({table} #{estimated_rows})")
        
        # Lead/Join order hints
        leading = hints.get("leading", [])
        if leading:
            hint_parts.append(f"LEADING({' '.join(leading)})")
        
        if hint_parts:
            hint_comment = f"/*+ {' '.join(hint_parts)} */"
            return f"{hint_comment}\n{sql_query}"
        
        return sql_query
    
    @classmethod
    async def recommend_indexes(
        cls,
        sql_query: str,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Recommend indexes based on query analysis.
        
        Uses PostgreSQL's query plan to identify potential index opportunities.
        
        Args:
            sql_query: SQL query to analyze
            schema_name: Schema to analyze (default: search path)
            
        Returns:
            Dictionary with index recommendations
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        try:
            # Get plan analysis
            plan_analysis = await cls.analyze_query_plan(sql_query, use_cache=False)
            
            recommendations = []
            
            # Analyze sequential scans
            for table in plan_analysis.get("full_scans", []):
                # Extract WHERE clauses from query for this table
                where_columns = cls._extract_where_columns(sql_query, table)
                
                if where_columns:
                    # Recommend B-tree index on WHERE columns
                    recommendations.append({
                        "table": table,
                        "type": "btree",
                        "columns": where_columns,
                        "reason": f"Sequential scan on {table} with filter on {', '.join(where_columns)}",
                        "sql": f"CREATE INDEX idx_{table}_{'_'.join(where_columns)} ON {table} ({', '.join(where_columns)});"
                    })
                else:
                    # Generic recommendation
                    recommendations.append({
                        "table": table,
                        "type": "btree",
                        "columns": ["unknown"],
                        "reason": f"Sequential scan detected on {table}",
                        "sql": f"-- Analyze query filters and create appropriate index on {table}"
                    })
            
            # Check for sorting operations
            if any("Sort" in w for w in plan_analysis.get("warnings", [])):
                order_columns = cls._extract_order_columns(sql_query)
                if order_columns:
                    table = order_columns.get("table")
                    columns = order_columns.get("columns", [])
                    if table and columns:
                        recommendations.append({
                            "table": table,
                            "type": "btree",
                            "columns": columns,
                            "reason": "Sort operation detected - index can avoid sort",
                            "sql": f"CREATE INDEX idx_{table}_{'_'.join(columns)} ON {table} ({', '.join(columns)});"
                        })
            
            return {
                "recommendations": recommendations,
                "estimated_benefit": f"Potentially reduce query cost by 30-70%",
                "analysis_summary": plan_analysis
            }
            
        except Exception as e:
            logger.error(f"Index recommendation failed: {e}")
            return {"error": str(e), "recommendations": []}
    
    @classmethod
    def _extract_where_columns(cls, sql_query: str, table_name: str) -> list:
        """Extract column names from WHERE clause for a specific table"""
        columns = []
        try:
            # Simple regex-based extraction - for production, use SQL parser
            # Match patterns like: table.column, table_alias.column, or just column
            pattern = rf"{re.escape(table_name)}\.(\w+)\s*[=<>!]"
            matches = re.findall(pattern, sql_query, re.IGNORECASE)
            columns.extend(matches)
            
            # Also look for joined table references
            join_pattern = rf"JOIN\s+{re.escape(table_name)}\s+(?:AS\s+)?\w*\s+ON.*?=\s*(\w+)\.(\w+)"
            join_matches = re.findall(join_pattern, sql_query, re.IGNORECASE)
            for match in join_matches:
                columns.append(match[1])
        except Exception:
            pass
        
        return list(set(columns))[:3]  # Limit to first 3 columns
    
    @classmethod
    def _extract_order_columns(cls, sql_query: str) -> Dict[str, Any]:
        """Extract columns from ORDER BY clause"""
        try:
            order_match = re.search(r"ORDER\s+BY\s+([\w.,\s]+)(?:\s+(?:ASC|DESC))?(?:,|$)", sql_query, re.IGNORECASE)
            if order_match:
                order_clause = order_match.group(1)
                parts = order_clause.split(".")
                if len(parts) >= 2:
                    return {
                        "table": parts[0].strip(),
                        "columns": [p.strip() for p in parts[1:] if p.strip()]
                    }
        except Exception:
            pass
        return {}
    
    @classmethod
    def _hash_query(cls, sql_query: str) -> str:
        """Generate hash for query caching"""
        normalized = " ".join(sql_query.split()).lower()
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    @classmethod
    async def get_performance_stats(cls) -> Dict[str, Any]:
        """Get PostgreSQL performance statistics"""
        if not settings.POSTGRES_ENABLED:
            return {"status": "disabled"}
        
        try:
            # Get connection pool stats
            pool_stats = await postgres_client.get_pool_stats()
            
            # Get server statistics
            stats_query = """
            SELECT
                (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle_connections,
                (SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL) as waiting_connections,
                (SELECT sum(xact_commit) FROM pg_stat_database) as total_commits,
                (SELECT sum(xact_rollback) FROM pg_stat_database) as total_rollbacks,
                (SELECT sum(blks_read) FROM pg_stat_database) as blocks_read,
                (SELECT sum(blks_hit) FROM pg_stat_database) as blocks_hit
            """
            
            result = await postgres_client.execute_query(
                sql=stats_query,
                user_id="system",
                request_id="stats"
            )
            
            rows = result.get("rows", [])
            if rows:
                stats = rows[0]
                total_blocks = (stats[4] or 0) + (stats[5] or 0)
                cache_hit_ratio = (stats[5] / total_blocks * 100) if total_blocks > 0 else 0
                
                return {
                    "status": "active",
                    "pool": pool_stats,
                    "database": {
                        "active_connections": stats[0],
                        "idle_connections": stats[1],
                        "waiting_connections": stats[2],
                        "total_commits": stats[3],
                        "total_rollbacks": stats[4],
                        "cache_hit_ratio": f"{cache_hit_ratio:.2f}%",
                        "blocks_read": stats[4],
                        "blocks_hit": stats[5]
                    }
                }
            
            return {"status": "active", "pool": pool_stats}
            
        except Exception as e:
            logger.error(f"Failed to get performance stats: {e}")
            return {"status": "error", "error": str(e)}
