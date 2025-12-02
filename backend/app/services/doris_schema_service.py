"""
Doris Schema Service - Business logic for Doris schema metadata management
"""

import logging
from typing import Dict, Any, Optional
import re

from app.core.client_registry import registry
from app.core.config import settings

logger = logging.getLogger(__name__)


class DorisSchemaService:
    """Service for handling Doris schema-related business logic"""
    
    @staticmethod
    async def get_dynamic_schema(
        user_query: str,
        intent: str = "",
        max_tables: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch schema for tables mentioned in user query via Doris MCP
        
        Args:
            user_query: User's natural language query
            intent: Classified intent string (optional)
            max_tables: Maximum tables to fetch
            
        Returns:
            Dict with schema metadata
        """
        logger.info(f"Fetching Doris schema for query: {user_query[:100]}...")
        if not settings.DORIS_MCP_ENABLED:
            logger.error("Doris MCP integration is disabled; cannot fetch schema")
            return {
                "status": "error",
                "error": "Doris integration is disabled",
                "schema": {"tables": {}, "views": {}},
            }
        
        # Extract table name from query
        table_name = DorisSchemaService._extract_table_name(user_query, intent)
        
        if not table_name:
            logger.warning(f"No table explicitly mentioned in query")
            return {
                "status": "error",
                "error": "No table name found in query. Please specify which table to query.",
                "schema": {"tables": {}, "views": {}}
            }
        
        logger.info(f"Identified table: {table_name}")
        
        try:
            # Get Doris MCP client
            doris_client = registry.get_doris_client()
            if not doris_client:
                raise RuntimeError("Doris MCP client not available")
            
            # Optional health guard if client exposes is_healthy property
            if hasattr(doris_client, "is_healthy") and not doris_client.is_healthy:
                logger.error("Doris MCP client is not healthy")
                return {
                    "status": "error",
                    "error": "Doris MCP client is not healthy",
                    "schema": {"tables": {}, "views": {}}
                }
            
            # Use MCP tool to get table schema
            tool_result = await doris_client.call_tool(
                getattr(doris_client, "get_table_schema_tool", "get_table_schema"),
                {"table_name": table_name},
            )
            
            if tool_result.get("status") != "success":
                error_detail = (
                    tool_result.get("error")
                    or tool_result.get("message")
                    or "unknown"
                )
                error_type = tool_result.get("error_type")
                logger.error(
                    f" Schema fetch failed for table {table_name}: {error_detail} (type={error_type})"
                )
                user_error = (
                    f"Doris schema service unavailable: {error_detail}"
                    if error_type in {"unavailable", "network", "protocol", "tool_call", "no_tools"}
                    else f"Table '{table_name}' not found or inaccessible"
                )
                return {
                    "status": "error",
                    "error": user_error,
                    "schema": {"tables": {}, "views": {}}
                }

            # Doris MCP server may return either:
            # - a dict with a "columns" key, or
            # - a bare list of column descriptors.
            raw_result = tool_result.get("result") or tool_result.get("schema") or {}
            if isinstance(raw_result, list):
                columns = raw_result
            elif isinstance(raw_result, dict):
                columns = raw_result.get("columns") or raw_result.get("COLUMNS") or []
            else:
                columns = []
            
            if not columns:
                logger.warning(f"Table {table_name} has no columns or doesn't exist")
                return {
                    "status": "error",
                    "error": f"Table '{table_name}' not found or has no columns",
                    "schema": {"tables": {}, "views": {}}
                }
            
            # Build schema structure
            schema_data = {"tables": {}, "views": {}}
            formatted_columns = []
            
            for col in columns:
                col_name = None
                col_type = None
                if isinstance(col, dict):
                    col_name = col.get("name") or col.get("column_name") or col.get("COLUMN_NAME")
                    col_type = col.get("type") or col.get("data_type") or col.get("TYPE")
                formatted_columns.append({
                    "name": col_name,
                    "type": col_type,
                    "nullable": isinstance(col, dict) and col.get("nullable", True),
                    "requires_quoting": False,  # Doris typically uses backticks
                })

            schema_data["tables"][table_name.upper()] = formatted_columns
            
            logger.info(f"Schema retrieved for {table_name}: {len(formatted_columns)} columns")
            
            return {
                "status": "success",
                "source": "doris_mcp",
                "schema": schema_data,
                "table_name": table_name.upper(),
                "column_count": len(formatted_columns)
            }
            
        except Exception as e:
            logger.error(f"Doris schema fetch failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "schema": {"tables": {}, "views": {}}
            }
    
    @staticmethod
    def _extract_table_name(user_query: str, intent: str) -> Optional[str]:
        """
        Extract table name from user query
        
        Args:
            user_query: Natural language query
            intent: Classified intent
            
        Returns:
            Table name or None
        """
        combined_text = f"{user_query} {intent}"
        
        # Common patterns for table mentions (case-insensitive)
        patterns = [
            r'from\s+[`"]?(\w+)[`"]?',  # FROM clause
            r'in\s+[`"]?(\w+)[`"]?\s+table',  # IN table
            r'table\s+[`"]?(\w+)[`"]?',  # TABLE keyword
            r'[`"]?(\w+)[`"]?\s+table',  # table name before "table"
            r'of\s+[`"]?(\w+)[`"]?',  # "rows of TABLE_NAME"
            r'rows\s+of\s+[`"]?(\w+)[`"]?',  # "rows of TABLE_NAME"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, combined_text, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        # Look for UPPER_CASE or CamelCase words that look like table names
        # Match words with underscores (TABLE_NAME) or all caps (TABLENAME)
        table_pattern = r'\b([A-Z][A-Z0-9_]{2,})\b'
        matches = re.findall(table_pattern, combined_text)
        if matches:
            # Filter out common non-table words
            non_tables = {'THE', 'AND', 'FOR', 'FROM', 'WITH', 'SHOW', 'SELECT', 'WHERE', 'ORDER', 'GROUP', 'LIMIT'}
            for match in matches:
                if match not in non_tables and len(match) > 3:
                    return match.upper()
        
        # Check for common table names (case-insensitive)
        common_tables = ["user_behavior", "users", "orders", "products", "sales", "customer_data", "customers"]
        query_lower = combined_text.lower()
        for table in common_tables:
            if table in query_lower:
                return table.upper()
        
        return None
    
    @staticmethod
    async def get_database_schema() -> Dict[str, Any]:
        """
        Retrieve full Doris database schema via MCP
        
        Returns:
            Schema metadata
        """
        logger.info(f"Fetching full Doris database schema...")
        
        if not settings.DORIS_MCP_ENABLED:
            logger.error("Doris MCP integration is disabled; cannot fetch database schema")
            return {
                "status": "error",
                "error": "Doris integration is disabled",
                "schema": {"tables": {}, "views": {}},
            }
        
        try:
            doris_client = registry.get_doris_client()
            if not doris_client:
                raise RuntimeError("Doris MCP client not available")
            
            # List all tables
            tool_result = await doris_client.call_tool(
                getattr(doris_client, "get_db_table_list_tool", "get_db_table_list"),
                {},
            )
            
            if tool_result.get("status") != "success":
                error_detail = (
                    tool_result.get("error")
                    or tool_result.get("message")
                    or "unknown"
                )
                error_type = tool_result.get("error_type")
                logger.error(f"Failed to list tables: {error_detail} (type={error_type})")
                user_error = (
                    f"Doris schema service unavailable: {error_detail}"
                    if error_type in {"unavailable", "network", "protocol", "tool_call", "no_tools"}
                    else f"Failed to retrieve database schema: {error_detail}"
                )
                return {
                    "status": "error",
                    "error": user_error,
                    "schema": {"tables": {}, "views": {}}
                }

            raw_tables = tool_result.get("result") or tool_result.get("tables") or []

            table_names = []
            for item in raw_tables:
                name = None
                if isinstance(item, dict):
                    name = (
                        item.get("table_name")
                        or item.get("TABLE_NAME")
                        or item.get("name")
                        or item.get("table")
                    )
                else:
                    name = str(item)

                if name:
                    table_names.append(name.upper())

            logger.info(f"Found {len(table_names)} tables in Doris database")
            
            return {
                "status": "success",
                "source": "doris_mcp",
                "tables": table_names,
                "schema": {"tables": {t: [] for t in table_names}, "views": {}}
            }
            
        except Exception as e:
            logger.error(f"Doris schema fetch failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "schema": {"tables": {}, "views": {}}
            }
