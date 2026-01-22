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
        # CRITICAL: Use ALL_TAB_COLUMNS without OWNER = USER to find tables user has access to
        # This handles cases where user has SELECT privilege but doesn't own the table
        discovery_sql = f"""
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = USER AND TABLE_NAME = '{table_name.upper()}'
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
                
                return {
                    "status": "error",
                    "error": f"Table '{table_name}' not found or has no columns",
                    "schema": {"tables": {}, "views": {}},
                    "suggestions": suggestions,
                    "did_you_mean": f"Did you mean: {', '.join(suggestions)}?" if suggestions else None
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
            logger.info("Fetching schema directly from ALL_TAB_COLUMNS...")
            
            where_clause = "WHERE OWNER = USER AND TABLE_NAME NOT LIKE 'BIN$%' AND TABLE_NAME NOT LIKE 'SYS_%'"
            
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
            "message": "Schema cache refreshed",
            "schema": result.get("schema"),
        }