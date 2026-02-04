"""
Schema Service - Business logic for schema metadata management
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from app.core.client_registry import registry
from app.core.exceptions import MCPException

logger = logging.getLogger(__name__)


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings.
    Used for fuzzy matching table/column names.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def fuzzy_match_name(
    query_name: str,
    candidates: List[str],
    max_distance: int = 2,
    threshold_ratio: float = 0.3
) -> List[Tuple[str, int, float]]:
    """
    Find fuzzy matches for a name against a list of candidates.
    
    Args:
        query_name: The name to match (possibly misspelled)
        candidates: List of valid names to match against
        max_distance: Maximum Levenshtein distance to consider a match
        threshold_ratio: Maximum ratio of distance to name length
        
    Returns:
        List of (candidate, distance, similarity_score) tuples, sorted by distance
    """
    query_upper = query_name.upper()
    matches = []
    
    for candidate in candidates:
        candidate_upper = candidate.upper()
        
        # Exact match
        if query_upper == candidate_upper:
            matches.append((candidate, 0, 1.0))
            continue
        
        # Calculate distance
        distance = levenshtein_distance(query_upper, candidate_upper)
        
        # Check if within acceptable distance
        max_len = max(len(query_upper), len(candidate_upper))
        ratio = distance / max_len if max_len > 0 else 1.0
        
        if distance <= max_distance or ratio <= threshold_ratio:
            similarity = 1.0 - ratio
            matches.append((candidate, distance, similarity))
    
    # Sort by distance (closest first)
    matches.sort(key=lambda x: (x[1], -x[2]))
    return matches


def generate_did_you_mean_suggestions(
    query_name: str,
    candidates: List[str],
    max_suggestions: int = 3
) -> List[str]:
    """
    Generate "Did you mean?" suggestions for a potentially misspelled name.
    
    Args:
        query_name: The name that wasn't found
        candidates: List of valid names
        max_suggestions: Maximum number of suggestions to return
        
    Returns:
        List of suggested names
    """
    matches = fuzzy_match_name(query_name, candidates, max_distance=3, threshold_ratio=0.4)
    
    # Filter out exact matches and return top suggestions
    suggestions = [m[0] for m in matches if m[1] > 0][:max_suggestions]
    return suggestions


class SchemaService:
    """Service for handling schema-related business logic"""
    
    @staticmethod
    async def get_table_relationships(
        table_name: str,
        connection_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            table_name: Name of the table
            connection_name: Database connection name
            
        Returns:
            List of relationship objects
        """
        logger.info(f"Fetching relationships for table: {table_name}")
        
        from app.core.config import settings
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            return []
            
        conn = connection_name or settings.oracle_default_connection
        
        # SQL to find foreign keys where this table is either parent or child
        sql = f"""
SELECT 
    a.constraint_name, 
    a.table_name as child_table, 
    a.column_name as child_column,
    c_pk.table_name as parent_table, 
    c_pk.column_name as parent_column
FROM 
    all_cons_columns a
JOIN 
    all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
JOIN 
    all_cons_columns c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
WHERE 
    c.constraint_type = 'R'
    AND (a.table_name = '{table_name.upper()}' OR c_pk.table_name = '{table_name.upper()}')
"""
        try:
            result = await mcp_client.execute_sql(sql, conn)
            if result.get("status") != "success":
                return []
                
            rows = result.get("results", {}).get("rows", [])
            relationships = []
            
            for row in rows:
                # Handle both dict and list results from MCP
                if isinstance(row, dict):
                    relationships.append({
                        "constraint_name": row.get("CONSTRAINT_NAME"),
                        "child_table": row.get("CHILD_TABLE"),
                        "child_column": row.get("CHILD_COLUMN"),
                        "parent_table": row.get("PARENT_TABLE"),
                        "parent_column": row.get("PARENT_COLUMN")
                    })
                else:
                    relationships.append({
                        "constraint_name": row[0],
                        "child_table": row[1],
                        "child_column": row[2],
                        "parent_table": row[3],
                        "parent_column": row[4]
                    })
            
            return relationships
        except Exception as e:
            logger.error(f"Failed to fetch relationships: {e}")
            return []

    @staticmethod
    async def get_column_comments(
        table_name: str,
        connection_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Get comments for all columns in a table.
        """
        from app.core.config import settings
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            return {}
            
        conn = connection_name or settings.oracle_default_connection
        
        sql = f"""
SELECT COLUMN_NAME, COMMENTS 
FROM ALL_COL_COMMENTS 
WHERE TABLE_NAME = '{table_name.upper()}' AND OWNER = USER
"""
        try:
            result = await mcp_client.execute_sql(sql, conn)
            if result.get("status") != "success":
                return {}
                
            rows = result.get("results", {}).get("rows", [])
            comments = {}
            for row in rows:
                if isinstance(row, dict):
                    comments[row.get("COLUMN_NAME")] = row.get("COMMENTS")
                else:
                    comments[row[0]] = row[1]
            return comments
        except Exception as e:
            logger.error(f"Failed to fetch column comments: {e}")
            return {}

    @staticmethod
    async def get_lightweight_stats(
        table_name: str,
        connection_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get lightweight statistics (NDV, Nulls) for a table's columns.
        Uses APPROX_COUNT_DISTINCT for performance on Oracle.
        """
        from app.core.config import settings
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            return []
            
        conn = connection_name or settings.oracle_default_connection
        
        # Get columns first to build the query
        cols_sql = f"SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}' AND OWNER = USER"
        try:
            cols_result = await mcp_client.execute_sql(cols_sql, conn)
            if cols_result.get("status") != "success":
                return []
            
            cols = cols_result.get("results", {}).get("rows", [])
            if not cols:
                return []
                
            # Build an optimized query for top 15 columns to avoid OOM or timeouts
            stat_parts = []
            target_cols = cols[:15]
            for col in target_cols:
                cname = col.get("COLUMN_NAME") if isinstance(col, dict) else col[0]
                stat_parts.append(f"APPROX_COUNT_DISTINCT({cname}) as NDV_{cname}")
                stat_parts.append(f"SUM(CASE WHEN {cname} IS NULL THEN 1 ELSE 0 END) as NULLS_{cname}")
            
            stats_sql = f"SELECT {', '.join(stat_parts)} FROM {table_name.upper()}"
            stats_result = await mcp_client.execute_sql(stats_sql, conn)
            
            if stats_result.get("status") != "success":
                return []
                
            row_data = stats_result.get("results", {}).get("rows", [])
            if not row_data:
                return []
            row = row_data[0]
            
            stats_list = []
            
            for i, col in enumerate(target_cols):
                cname = col.get("COLUMN_NAME") if isinstance(col, dict) else col[0]
                ctype = col.get("DATA_TYPE") if isinstance(col, dict) else col[1]
                
                if isinstance(row, dict):
                    stats_list.append({
                        "column": cname,
                        "type": ctype,
                        "distinct_count": row.get(f"NDV_{cname}"),
                        "null_count": row.get(f"NULLS_{cname}")
                    })
                else:
                    stats_list.append({
                        "column": cname,
                        "type": ctype,
                        "distinct_count": row[i*2],
                        "null_count": row[i*2 + 1]
                    })
            return stats_list
        except Exception as e:
            logger.error(f"Failed to fetch lightweight stats: {e}")
            return []

    
    @staticmethod
    async def get_dynamic_schema(
        user_query: str,
        intent: str = "",
        connection_name: Optional[str] = None,
        max_tables: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch schema for ONLY the explicitly mentioned table in user query
        
        Simplified approach:
        - Extract table name directly from user query (users always mention table)
        - Fetch schema for that specific table only
        - No keyword extraction or fuzzy matching
        - Always returns latest schema (no stale cache)
        
        Args:
            user_query: User's natural language query
            intent: Classified intent string (optional)
            connection_name: Database connection name
            max_tables: Maximum tables (default 1 - single table focus)
            
        Returns:
            Dict with schema metadata for mentioned table
        """
        logger.info(f"Fetching schema for table mentioned in: {user_query[:100]}...")
        
        # Extract table name directly from query (case-insensitive search)
        table_name = SchemaService._extract_table_name(user_query, intent)
        
        if not table_name:
            logger.warning(f"No table explicitly mentioned in query")
            return {
                "status": "error",
                "error": "No table name found in query. Please specify which table to query.",
                "schema": {"tables": {}, "views": {}},
                "suggestions": []
            }
        
        logger.info(f"Identified table: {table_name}")
        
        # Fetch schema for this specific table only
        # CRITICAL FIX: Add filters to exclude system tables and improve query reliability
        # Use ALL_TAB_COLUMNS without OWNER restriction to find tables user has SELECT privilege on
        discovery_sql = f"""
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = '{table_name.upper()}'
  AND TABLE_NAME NOT LIKE 'BIN$%'
  AND TABLE_NAME NOT LIKE 'SYS_%'
ORDER BY COLUMN_ID
"""
        
        try:
            # Execute query via MCP client
            from app.core.config import settings
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                raise MCPException(
                    "MCP client not available",
                    details={"client_available": False}
                )
            
            conn = connection_name or settings.oracle_default_connection
            
            # Fetch schema for the specific table
            result = await mcp_client.execute_sql(discovery_sql, conn)
            
            if result.get("status") != "success":
                error_detail = result.get("error", "unknown")
                logger.error(f"Schema fetch failed for table {table_name}: {error_detail}")
                
                # Try fuzzy matching to suggest alternatives
                suggestions = await SchemaService._get_table_suggestions(table_name, conn, mcp_client)
                
                return {
                    "status": "error",
                    "error": f"Table '{table_name}' not found or inaccessible",
                    "schema": {"tables": {}, "views": {}},
                    "suggestions": suggestions,
                    "did_you_mean": f"Did you mean: {', '.join(suggestions)}?" if suggestions else None
                }
            
            rows = result.get("results", {}).get("rows", [])
            
            if not rows:
                logger.warning(f"Table {table_name} has no columns or doesn't exist")
                
                # Try fuzzy matching to suggest alternatives
                suggestions = await SchemaService._get_table_suggestions(table_name, conn, mcp_client)
                
                error_msg = f"Table '{table_name}' not found or has no columns"
                if suggestions:
                    error_msg += f". Did you mean: {', '.join(suggestions)}?"
                
                return {
                    "status": "error",
                    "error": error_msg,
                    "schema": {"tables": {}, "views": {}},
                    "suggestions": suggestions,
                    "hint": "Check table name spelling and ensure you have SELECT privileges on the table"
                }
            
            def get_val(r, idx, name):
                if isinstance(r, dict):
                    return r.get(name) or r.get(name.upper())
                try:
                    return r[idx]
                except (IndexError, TypeError):
                    return None

            # Build schema for this table
            schema_data = {"tables": {}, "views": {}}
            columns = []
            
            for row in rows:
                col_name = get_val(row, 1, "COLUMN_NAME")
                data_type = get_val(row, 2, "DATA_TYPE")
                nullable_val = get_val(row, 3, "NULLABLE")
                nullable = nullable_val == "Y"
                
                if not col_name:
                    continue
                    
                # Check if column needs quoting
                needs_quoting = col_name != col_name.upper() or not col_name.replace('_', '').isalnum()
                
                columns.append({
                    "name": col_name,
                    "type": data_type,
                    "nullable": nullable,
                    "requires_quoting": needs_quoting
                })
            
            schema_data["tables"][table_name.upper()] = columns
            
            logger.info(f"Schema retrieved for {table_name}: {len(columns)} columns")
            
            return {
                "status": "success",
                "source": "specific_table",
                "schema": schema_data,
                "table_name": table_name.upper(),
                "column_count": len(columns)
            }
            
        except Exception as e:
            logger.error(f"Schema fetch failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "schema": {"tables": {}, "views": {}}
            }
    
    @staticmethod
    async def _get_table_suggestions(
        table_name: str,
        connection_name: str,
        mcp_client
    ) -> List[str]:
        """
        Get fuzzy match suggestions for a table name that wasn't found.
        
        Args:
            table_name: The table name that wasn't found
            connection_name: Database connection name
            mcp_client: MCP client instance
            
        Returns:
            List of suggested table names
        """
        try:
            # Get list of all accessible tables
            tables_sql = """
SELECT DISTINCT TABLE_NAME 
FROM ALL_TAB_COLUMNS 
WHERE OWNER = USER AND TABLE_NAME NOT LIKE 'BIN$%' AND TABLE_NAME NOT LIKE 'SYS_%'
"""
            result = await mcp_client.execute_sql(tables_sql, connection_name)
            
            if result.get("status") != "success":
                return []
            
            rows = result.get("results", {}).get("rows", [])
            
            def get_row_val(r, idx):
                if isinstance(r, dict):
                    return next(iter(r.values()))
                return r[idx]
                
            all_tables = [get_row_val(row, 0) for row in rows if row]
            
            # Generate suggestions using fuzzy matching
            suggestions = generate_did_you_mean_suggestions(table_name, all_tables, max_suggestions=3)
            
            if suggestions:
                logger.info(f"Fuzzy match suggestions for '{table_name}': {suggestions}")
            
            return suggestions
            
        except Exception as e:
            logger.warning(f"Failed to generate table suggestions: {e}")
            return []
    
    @staticmethod
    async def get_column_suggestions(
        column_name: str,
        table_name: str,
        connection_name: Optional[str] = None
    ) -> List[str]:
        """
        Get fuzzy match suggestions for a column name that wasn't found.
        
        Args:
            column_name: The column name that wasn't found
            table_name: The table to search in
            connection_name: Database connection name
            
        Returns:
            List of suggested column names
        """
        try:
            from app.core.config import settings
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                return []
            
            conn = connection_name or settings.oracle_default_connection
            
            # Get columns for the table
            columns_sql = f"""
SELECT COLUMN_NAME 
FROM ALL_TAB_COLUMNS 
WHERE OWNER = USER AND TABLE_NAME = '{table_name.upper()}'
"""
            result = await mcp_client.execute_sql(columns_sql, conn)
            
            if result.get("status") != "success":
                return []
            
            rows = result.get("results", {}).get("rows", [])
            
            def get_row_val(r, idx):
                if isinstance(r, dict):
                    return next(iter(r.values()))
                return r[idx]
                
            all_columns = [get_row_val(row, 0) for row in rows if row]
            
            # Generate suggestions using fuzzy matching
            suggestions = generate_did_you_mean_suggestions(column_name, all_columns, max_suggestions=3)
            
            if suggestions:
                logger.info(f"Fuzzy match suggestions for column '{column_name}' in {table_name}: {suggestions}")
            
            return suggestions
            
        except Exception as e:
            logger.warning(f"Failed to generate column suggestions: {e}")
            return []
    
    @staticmethod
    def _extract_table_name(user_query: str, intent: str) -> Optional[str]:
        """
        Extract table name directly from user query
        
        Users always mention the table name in their query.
        Look for common patterns:
        - "from table X"
        - "in table X"
        - "table X"
        - Or just the table name itself (CUSTOMER_DATA, MOBILE_DATA, etc.)
        
        Returns:
            Table name (uppercase) or None if not found
        """
        import re
        
        combined_text = f"{user_query} {intent}".upper()
        
        # Pattern 1: "FROM TABLE X", "IN TABLE X", "OF TABLE_NAME", etc.
        patterns = [
            r'FROM\s+TABLE\s+([A-Z_][A-Z0-9_]*)',
            r'IN\s+TABLE\s+([A-Z_][A-Z0-9_]*)',
            r'TABLE\s+([A-Z_][A-Z0-9_]*)',
            r'FROM\s+([A-Z_][A-Z0-9_]*)',
            r'OF\s+([A-Z_][A-Z0-9_]*)',  # "rows of CUSTOMER_DATA"
            r'ROWS\s+OF\s+([A-Z_][A-Z0-9_]*)',  # "first five rows of CUSTOMER_DATA"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, combined_text)
            if match:
                table_name = match.group(1)
                logger.info(f"Extracted table name via pattern: {table_name}")
                return table_name
        
        # Pattern 2: Look for common table name patterns (CUSTOMER_DATA, MOBILE_DATA, etc.)
        # Match words that look like table names (UPPERCASE with underscores)
        table_pattern = r'\b([A-Z][A-Z0-9_]{3,})\b'
        matches = re.findall(table_pattern, combined_text)
        
        if matches:
            # Return first match that looks like a table name
            # Filter out common SQL keywords and English words that might appear in queries
            non_table_words = {
                # SQL keywords
                'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
                'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW',
                'LIMIT', 'OFFSET', 'FETCH', 'FIRST', 'ROWS', 'ONLY', 'WITH', 'CASE', 'WHEN', 'THEN',
                # Common English words in queries
                'SHOW', 'GIVE', 'LIST', 'FIND', 'TELL', 'DISPLAY', 'WHAT', 'WHICH', 'THAT', 'THIS',
                'ROWS', 'DATA', 'RESULTS', 'QUERY', 'COLUMN', 'COLUMNS', 'FIELD', 'FIELDS',
                'FIRST', 'LAST', 'TOP', 'BOTTOM', 'ALL', 'EACH', 'EVERY', 'SOME', 'MANY',
                'TOTAL', 'COUNT', 'AVERAGE', 'MAXIMUM', 'MINIMUM',
            }
            for match in matches:
                if match not in non_table_words and len(match) > 4:
                    logger.info(f"Extracted table name via pattern matching: {match}")
                    return match
        
        logger.warning(f"Could not extract table name from query")
        return None
    
    @staticmethod
    async def get_database_schema(
        connection_name: Optional[str] = None,
        use_cache: bool = True,
        table_names: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve database schema metadata using DIRECT query to ALL_TAB_COLUMNS
        
        Args:
            connection_name: Database connection name
            use_cache: Whether to use cached schema (default: True). Ignored if table_names is provided.
            table_names: Optional list of specific tables to fetch schema for
            
        Returns:
            Schema metadata
        """
        logger.info(f"Retrieving database schema (tables={table_names})...")
        
        # Skip cache if fetching specific tables (dynamic fetch)
        if use_cache and not table_names:
            # Try Redis cache first
            try:
                from app.core.redis_client import redis_client as _redis_client
                cached_schema = await _redis_client.get_schema_metadata("oracle_schema")
                if cached_schema and cached_schema.get("tables"):
                    logger.info(f"Schema retrieved from cache: {len(cached_schema.get('tables', {}))} tables")
                    return {
                        "status": "success",
                        "source": "cache",
                        "schema": cached_schema,
                    }
            except Exception as cache_err:
                logger.debug(f"Cache check failed: {cache_err}")
        
        # Fetch from database using DIRECT SQL query to ALL_TAB_COLUMNS
        from app.core.config import settings
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            raise MCPException(
                "MCP client not available",
                details={"client_available": False}
            )
        
        try:
            conn = connection_name or settings.oracle_default_connection
            
            # DIRECT approach: Query ALL_TAB_COLUMNS for ALL accessible tables
            # CRITICAL: Don't restrict to OWNER = USER, get all tables user has access to
            # We want all accessible tables, usually found in ALL_TAB_COLUMNS
            logger.info("Fetching schema directly from ALL_TAB_COLUMNS...")
            
            # Note: Removed OWNER = USER to allow seeing shared tables
            where_clause = "WHERE TABLE_NAME NOT LIKE 'BIN$%' AND TABLE_NAME NOT LIKE 'SYS_%'"
            
            if table_names:
                # Sanitize and format table names for IN clause
                safe_names = [t.replace("'", "''").upper() for t in table_names if t]
                if safe_names:
                    tables_str = ", ".join(f"'{t}'" for t in safe_names)
                    where_clause += f" AND TABLE_NAME IN ({tables_str})"
            
            schema_sql = f"""
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
FROM ALL_TAB_COLUMNS
{where_clause}
ORDER BY TABLE_NAME, COLUMN_ID
"""
            
            result = await mcp_client.execute_sql(schema_sql, conn)
            
            if result.get("status") != "success":
                logger.error(f"Schema query failed: {result.get('message')}")
                raise MCPException(
                    f"Schema query failed: {result.get('message')}",
                    details={"result": result}
                )
            
            rows = result.get("results", {}).get("rows", [])
            
            # If specific tables requested but no rows found, it might mean they don't exist or permission denied
            if not rows and table_names:
                logger.warning(f"No schema found for requested tables: {table_names}")
            elif not rows:
                logger.error(f"No schema data returned from ALL_TAB_COLUMNS")
                # Don't raise MCPException, return empty schema instead
            
            # Build schema_data from rows
            schema_data = {"tables": {}, "views": {}}
            
            def get_val(r, idx, name):
                if isinstance(r, dict):
                    return r.get(name) or r.get(name.upper())
                try:
                    return r[idx]
                except (IndexError, TypeError):
                    return None

            for row in rows:
                table_name = get_val(row, 0, "TABLE_NAME")
                column_name = get_val(row, 1, "COLUMN_NAME")
                data_type = get_val(row, 2, "DATA_TYPE")
                nullable_val = get_val(row, 3, "NULLABLE")
                nullable = nullable_val == "Y"
                
                if not table_name or not column_name:
                    continue
                    
                if table_name not in schema_data["tables"]:
                    schema_data["tables"][table_name] = []
                
                # Check if column needs quoting (contains lowercase or special chars)
                needs_quoting = column_name != column_name.upper() or not column_name.replace('_', '').isalnum()
                
                schema_data["tables"][table_name].append({
                    "name": column_name,
                    "type": data_type,
                    "nullable": nullable,
                    "requires_quoting": needs_quoting
                })
            
            logger.info(f"Schema retrieved from database: {len(schema_data['tables'])} tables, {len(rows)} columns")
            
            # Cache the result ONLY if it's a full fetch
            if use_cache and not table_names:
                try:
                    from app.core.redis_client import redis_client as _redis_client
                    await _redis_client.cache_schema_metadata(
                        "oracle_schema",
                        schema_data,
                        ttl=3600  # 1 hour
                    )
                    logger.info(f"Schema cached for 1 hour")
                except Exception as cache_err:
                    logger.warning(f"Failed to cache schema: {cache_err}")
            
            return {
                "status": "success",
                "source": "database",
                "schema": schema_data,
            }
            
        except Exception as e:
            logger.error(f"Schema retrieval failed: {e}", exc_info=True)
            raise
    
    @staticmethod
    async def invalidate_schema_cache() -> int:
        """
        Invalidate all schema cache entries
        
        Returns:
            Number of entries invalidated
        """
        logger.info("Invalidating schema cache...")
        
        try:
            from app.core.redis_client import redis_client as _redis_client
            count = await _redis_client.invalidate_schema_cache("schema:*")
            logger.info(f"Invalidated {count} schema cache entries")
            return count
            
        except Exception as e:
            logger.error(f"Schema cache invalidation failed: {e}")
            raise
    
    @staticmethod
    async def refresh_schema_cache(
        connection_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Force refresh of schema cache
        
        Args:
            connection_name: Database connection name
            
        Returns:
            Refresh statistics
        """
        logger.info("Refreshing schema cache...")
        
        # Invalidate existing cache
        await SchemaService.invalidate_schema_cache()
        
        # Fetch fresh schema
        result = await SchemaService.get_database_schema(
            connection_name=connection_name,
            use_cache=False,
        )
        
        logger.info(f"Schema cache refreshed")
        return {
            "status": "success",
            "tables_refreshed": len(result.get("schema", {}).get("tables", {})),
            "source": result.get("source"),
        }
    
    @staticmethod
    async def enrich_schema_with_ai(
        database_type: str = "oracle",
        table_names: Optional[List[str]] = None,
        connection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Enrich schema with AI-generated descriptions and column inferences.
        
        Args:
            database_type: Database type (oracle, doris, postgres)
            table_names: Optional list of specific tables to enrich
            connection_name: Database connection name
            
        Returns:
            Enriched schema with AI-generated metadata
        """
        logger.info(f"Enriching schema with AI for {database_type}")
        
        try:
            # Get base schema
            schema_result = await SchemaService.get_database_schema(
                connection_name=connection_name,
                use_cache=True,
                table_names=table_names
            )
            
            if schema_result.get("status") != "success":
                return schema_result
            
            schema_data = schema_result.get("schema", {})
            tables = schema_data.get("tables", {})
            
            if not tables:
                return {
                    "status": "error",
                    "error": "No tables found to enrich"
                }
            
            # Limit enrichment to avoid overwhelming LLM
            tables_to_enrich = dict(list(tables.items())[:5]) if len(tables) > 5 else tables
            
            enriched_tables = {}
            
            for table_name, columns in tables_to_enrich.items():
                logger.info(f"Enriching table: {table_name}")
                
                enriched_columns = []
                
                for column in columns[:20]:  # Limit columns per table
                    col_name = column.get("name")
                    col_type = column.get("type")
                    
                    # Infer column meaning
                    inferred = await SchemaService._infer_column_meaning(
                        table_name, col_name, col_type, connection_name
                    )
                    
                    enriched_column = {
                        **column,
                        "description": inferred.get("description"),
                        "inferred_name": inferred.get("inferred_name"),
                        "business_meaning": inferred.get("business_meaning"),
                        "ai_confidence": inferred.get("confidence")
                    }
                    
                    enriched_columns.append(enriched_column)
                
                enriched_tables[table_name] = enriched_columns
            
            logger.info(f"Schema enrichment complete: {len(enriched_tables)} tables enriched")
            
            return {
                "status": "success",
                "enriched_schema": {
                    "tables": enriched_tables,
                    "database_type": database_type
                },
                "tables_enriched": len(enriched_tables),
                "source": "ai_enhanced"
            }
            
        except Exception as e:
            logger.error(f"Schema enrichment failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    async def _infer_column_meaning(
        table_name: str,
        column_name: str,
        data_type: str,
        connection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Infer the business meaning of a column using LLM.
        
        Args:
            table_name: Table name
            column_name: Column name
            data_type: Data type
            connection_name: Database connection name
            
        Returns:
            Dict with inferred description, name, and confidence
        """
        try:
            # Get sample values for better inference
            from app.services.introspection_service import IntrospectionService
            samples = await IntrospectionService.get_sample_values(
                table_name, column_name, limit=3, connection_name=connection_name
            )
            
            # Check if LLM is available
            from app.core.client_registry import registry
            llm = registry.get_llm()
            
            if not llm:
                # Fallback to rule-based inference
                return SchemaService._rule_based_inference(
                    table_name, column_name, data_type, samples
                )
            
            # Build prompt for LLM
            prompt = f"""Analyze this database column and provide its business meaning.

Table: {table_name}
Column: {column_name}
Data Type: {data_type}
Sample Values: {', '.join(str(s) for s in samples[:3]) if samples else 'N/A'}

Provide:
1. A concise human-readable description (1 sentence)
2. An inferred business-friendly name (if column name is ambiguous like col01, amt_02, etc.)
3. The business meaning/purpose of this column

Format your response as:
DESCRIPTION: [one sentence description]
INFERRED_NAME: [business-friendly name or original if clear]
BUSINESS_MEANING: [business purpose]
"""
            
            response = await llm.ainvoke(prompt)
            content = response.content if hasattr(response, 'content') else str(response)
            
            # Parse response
            description = ""
            inferred_name = column_name
            business_meaning = ""
            
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith("DESCRIPTION:"):
                    description = line.replace("DESCRIPTION:", "").strip()
                elif line.startswith("INFERRED_NAME:"):
                    inferred_name = line.replace("INFERRED_NAME:", "").strip()
                elif line.startswith("BUSINESS_MEANING:"):
                    business_meaning = line.replace("BUSINESS_MEANING:", "").strip()
            
            return {
                "description": description or f"{column_name} column in {table_name}",
                "inferred_name": inferred_name,
                "business_meaning": business_meaning,
                "confidence": "high" if description else "low"
            }
            
        except Exception as e:
            logger.warning(f"LLM inference failed, using rule-based: {e}")
            return SchemaService._rule_based_inference(
                table_name, column_name, data_type, samples if 'samples' in locals() else []
            )
    
    @staticmethod
    def _rule_based_inference(
        table_name: str,
        column_name: str,
        data_type: str,
        samples: List[Any]
    ) -> Dict[str, Any]:
        """
        Rule-based column inference fallback.
        
        Args:
            table_name: Table name
            column_name: Column name
            data_type: Data type
            samples: Sample values
            
        Returns:
            Dict with inferred metadata
        """
        import re
        
        # Common patterns
        patterns = {
            r".*ID$": ("Identifier", "ID field"),
            r".*_ID$": ("Identifier", "Foreign key or ID"),
            r".*AMT.*": ("Amount", "Monetary or numeric amount"),
            r".*_AMT$": ("Amount", "Monetary value"),
            r".*DT$": ("Date", "Date or timestamp field"),
            r".*_DT$": ("Date", "Date field"),
            r".*CD$": ("Code", "Code or classification"),
            r".*_CD$": ("Code", "Category code"),
            r".*NM$": ("Name", "Name or description"),
            r".*_NM$": ("Name", "Name field"),
            r".*CNT$": ("Count", "Counter or quantity"),
            r".*_CNT$": ("Count", "Count field"),
            r"COL\d+": ("Column", "Generic column - requires manual verification"),
        }
        
        inferred_name = column_name
        business_meaning = "Data field"
        
        for pattern, (meaning, description) in patterns.items():
            if re.match(pattern, column_name.upper()):
                inferred_name = f"{table_name.split('_')[0]} {meaning}".title()
                business_meaning = description
                break
        
        # Enhance with data type info
        if "NUMBER" in data_type or "DECIMAL" in data_type:
            business_meaning = f"{business_meaning} (numeric)"
        elif "DATE" in data_type or "TIMESTAMP" in data_type:
            business_meaning = f"{business_meaning} (temporal)"
        elif "VARCHAR" in data_type or "CHAR" in data_type:
            business_meaning = f"{business_meaning} (text)"
        
        description = f"{column_name} is a {business_meaning.lower()} in {table_name}"
        
        return {
            "description": description,
            "inferred_name": inferred_name,
            "business_meaning": business_meaning,
            "confidence": "medium"
        }