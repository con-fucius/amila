"""
Introspection Service - Interactive schema exploration and metadata management
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from app.core.client_registry import registry
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class IntrospectionService:
    """Service for interactive schema introspection and metadata management"""
    
    @staticmethod
    async def suggest_column_alias(
        table: str,
        column: str,
        database_type: str = "oracle",
        connection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Suggest a human-readable alias for a column based on its characteristics.
        
        Args:
            table: Table name
            column: Column name
            database_type: Database type (oracle, doris, postgres)
            connection_name: Database connection name
            
        Returns:
            Dict with suggested alias and reasoning
        """
        logger.info(f"Suggesting alias for {table}.{column}")
        
        # Get column metadata
        metadata = await IntrospectionService._get_column_metadata(
            table, column, database_type, connection_name
        )
        
        if not metadata:
            return {
                "suggested_alias": column,
                "reasoning": "Could not retrieve column metadata",
                "confidence": "low"
            }
        
        # Get sample values for better inference
        samples = await IntrospectionService.get_sample_values(
            table, column, limit=5, connection_name=connection_name
        )
        
        # Use heuristics to suggest alias
        data_type = metadata.get("data_type", "").upper()
        
        # Common patterns
        alias_suggestions = {
            # Amount patterns
            r".*AMT.*": "Amount",
            r".*_AMT$": "Amount",
            r"AMT_\d+": "Amount",
            # Date patterns
            r".*DT.*": "Date",
            r".*_DT$": "Date",
            r"DT_\d+": "Date",
            # ID patterns
            r".*ID.*": "ID",
            r".*_ID$": "ID",
            r"ID_\d+": "ID",
            # Code patterns
            r".*CD.*": "Code",
            r".*_CD$": "Code",
            r"CD_\d+": "Code",
            # Name patterns
            r".*NM.*": "Name",
            r".*_NM$": "Name",
            r"NM_\d+": "Name",
            # Count patterns
            r".*CNT.*": "Count",
            r".*_CNT$": "Count",
            r"CNT_\d+": "Count",
        }
        
        import re
        suggested = column
        for pattern, alias_type in alias_suggestions.items():
            if re.match(pattern, column.upper()):
                # Try to make it more specific based on context
                suggested = f"{table.split('_')[0]}_{alias_type}".title().replace("_", " ")
                break
        
        # If starts with COL, try to infer from data type and position
        if column.upper().startswith("COL"):
            if "NUMBER" in data_type or "DECIMAL" in data_type:
                suggested = "Numeric Value"
            elif "DATE" in data_type or "TIMESTAMP" in data_type:
                suggested = "Date Value"
            elif "VARCHAR" in data_type or "CHAR" in data_type:
                suggested = "Text Value"
        
        confidence = "high" if suggested != column else "low"
        
        return {
            "suggested_alias": suggested,
            "column_name": column,
            "data_type": data_type,
            "sample_values": samples[:3] if samples else [],
            "reasoning": f"Based on column name pattern and data type ({data_type})",
            "confidence": confidence
        }
    
    @staticmethod
    async def get_sample_values(
        table: str,
        column: str,
        limit: int = 10,
        connection_name: Optional[str] = None
    ) -> List[Any]:
        """
        Get sample values for a column.
        
        Args:
            table: Table name
            column: Column name
            limit: Maximum number of samples
            connection_name: Database connection name
            
        Returns:
            List of sample values
        """
        logger.info(f"Fetching sample values for {table}.{column}")
        
        try:
            from app.core.config import settings
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                logger.warning("MCP client not available")
                return []
            
            conn = connection_name or settings.oracle_default_connection
            
            # Fetch distinct sample values
            sql = f"""
SELECT DISTINCT {column}
FROM {table.upper()}
WHERE {column} IS NOT NULL
FETCH FIRST {limit} ROWS ONLY
"""
            
            result = await mcp_client.execute_sql(sql, conn)
            
            if result.get("status") != "success":
                logger.warning(f"Failed to fetch samples: {result.get('error')}")
                return []
            
            rows = result.get("results", {}).get("rows", [])
            
            # Extract values (handle both dict and list row formats)
            samples = []
            for row in rows:
                if isinstance(row, dict):
                    val = row.get(column) or row.get(column.upper())
                else:
                    val = row[0] if row else None
                
                if val is not None:
                    samples.append(val)
            
            return samples
            
        except Exception as e:
            logger.error(f"Failed to fetch sample values: {e}")
            return []
    
    @staticmethod
    async def update_column_metadata(
        table: str,
        column: str,
        description: str,
        alias: Optional[str] = None,
        database_type: str = "oracle"
    ) -> Dict[str, Any]:
        """
        Update column metadata (description, alias) in cache.
        
        Args:
            table: Table name
            column: Column name
            description: Human-readable description
            alias: Optional alias for the column
            database_type: Database type
            
        Returns:
            Updated metadata
        """
        logger.info(f"Updating metadata for {table}.{column}")
        
        try:
            # Build metadata key
            cache_key = f"schema:metadata:{database_type}:{table.upper()}:{column.upper()}"
            
            metadata = {
                "table": table.upper(),
                "column": column.upper(),
                "description": description,
                "alias": alias or column,
                "updated_at": datetime.utcnow().isoformat(),
                "updated_by": "system"  # TODO: Get from auth context
            }
            
            # Store in Redis with 7-day TTL
            await redis_client.set(
                cache_key,
                metadata,
                ttl=604800  # 7 days
            )
            
            logger.info(f"Metadata updated for {table}.{column}")
            
            return {
                "status": "success",
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to update metadata: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    async def get_column_metadata(
        table: str,
        column: str,
        database_type: str = "oracle"
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached column metadata.
        
        Args:
            table: Table name
            column: Column name
            database_type: Database type
            
        Returns:
            Metadata dict or None
        """
        try:
            cache_key = f"schema:metadata:{database_type}:{table.upper()}:{column.upper()}"
            metadata = await redis_client.get(cache_key)
            return metadata
        except Exception as e:
            logger.warning(f"Failed to fetch metadata: {e}")
            return None
    
    @staticmethod
    async def _get_column_metadata(
        table: str,
        column: str,
        database_type: str,
        connection_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get column metadata from database.
        
        Args:
            table: Table name
            column: Column name
            database_type: Database type
            connection_name: Database connection name
            
        Returns:
            Metadata dict or None
        """
        try:
            from app.core.config import settings
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                return None
            
            conn = connection_name or settings.oracle_default_connection
            
            sql = f"""
SELECT DATA_TYPE, NULLABLE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = '{table.upper()}' AND COLUMN_NAME = '{column.upper()}' AND OWNER = USER
"""
            
            result = await mcp_client.execute_sql(sql, conn)
            
            if result.get("status") != "success":
                return None
            
            rows = result.get("results", {}).get("rows", [])
            if not rows:
                return None
            
            row = rows[0]
            
            if isinstance(row, dict):
                return {
                    "data_type": row.get("DATA_TYPE"),
                    "nullable": row.get("NULLABLE") == "Y",
                    "length": row.get("DATA_LENGTH"),
                    "precision": row.get("DATA_PRECISION"),
                    "scale": row.get("DATA_SCALE")
                }
            else:
                return {
                    "data_type": row[0],
                    "nullable": row[1] == "Y",
                    "length": row[2],
                    "precision": row[3],
                    "scale": row[4]
                }
                
        except Exception as e:
            logger.error(f"Failed to fetch column metadata: {e}")
            return None
