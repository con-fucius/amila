"""
Schema Management Endpoints
Database schema metadata retrieval and caching
"""
"""
Schema Management Endpoints
Database schema metadata retrieval and caching
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import logging

from app.services.schema_service import SchemaService
from app.services.doris_schema_service import DorisSchemaService
from app.core.rbac import rbac_manager
from app.core.redis_client import redis_client
from app.core.db_timeout_retry import RetryableExecutor, DatabaseTimeoutRetryConfig, DatabaseType
from fastapi import Depends

router = APIRouter()
logger = logging.getLogger(__name__)


class SchemaResponse(BaseModel):
    status: str
    source: str
    schema_data: Dict[str, Any]


class RefreshResponse(BaseModel):
    status: str
    message: str
    schema_data: Optional[Dict[str, Any]] = None


@router.get("/", response_model=SchemaResponse)
async def get_schema(
    connection_name: Optional[str] = None,
    use_cache: bool = True,
    database_type: str = "oracle",
    tables: Optional[str] = None,
) -> SchemaResponse:
    """
    Get database schema metadata
    
    Args:
        connection_name: Database connection name
        use_cache: Whether to use cached schema (default: True)
        database_type: Type of database (default: oracle)
        tables: Optional comma-separated list of tables to fetch
    
    Returns:
        Schema metadata from cache or database
    """
    try:
        logger.info(f"Retrieving schema (cache={use_cache}, database_type={database_type}, tables={tables})...")
        
        table_list = [t.strip() for t in tables.split(",")] if tables else None
        
        if database_type.lower() == "doris":
            # Doris schema service might need similar update, but prioritizing Oracle for now as per HITL usage
            result = await DorisSchemaService.get_database_schema()
        else:
            result = await SchemaService.get_database_schema(
                connection_name=connection_name,
                use_cache=use_cache,
                table_names=table_list,
            )
        
        return SchemaResponse(
            status=result.get("status", "error"),
            source=result.get("source", "unknown"),
            schema_data=result.get("schema", {}),
        )
        
    except Exception as e:
        logger.error(f"Schema retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Schema retrieval failed: {str(e)}"
        )


@router.post("/refresh", response_model=RefreshResponse)
async def refresh_schema(
    connection_name: Optional[str] = None,
) -> RefreshResponse:
    """
    Force refresh of schema cache
    
    Invalidates existing cache and fetches fresh schema from database
    
    Args:
        connection_name: Database connection name
    
    Returns:
        Refresh status and updated schema
    """
    try:
        logger.info("Refreshing schema cache...")
        
        result = await SchemaService.refresh_schema_cache(
            connection_name=connection_name
        )
        
        return RefreshResponse(
            status=result.get("status", "error"),
            message=result.get("message", "Schema cache refreshed"),
            schema_data=result.get("schema"),
        )
        
    except Exception as e:
        logger.error(f"Schema refresh failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Schema refresh failed: {str(e)}"
        )


@router.delete("/cache")
async def invalidate_cache() -> Dict[str, Any]:
    """
    Invalidate schema cache
    
    Removes all cached schema entries
    
    Returns:
        Number of entries invalidated
    """
    try:
        logger.info("Invalidating schema cache...")
        
        count = await SchemaService.invalidate_schema_cache()
        
        return {
            "status": "success",
            "message": f"Invalidated {count} cache entries",
            "count": count,
        }
        
    except Exception as e:
        logger.error(f"Cache invalidation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Cache invalidation failed: {str(e)}"
        )


@router.get("/table/{table_name}/stats")
async def get_table_stats(
    table_name: str,
    database_type: str = "oracle",
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get column statistics for a table with caching and optimization.
    
    **RBAC:** Requires authenticated user
    **Performance:** Uses approximate counts (NDV/APPROX_COUNT_DISTINCT)
    **Caching:** Results cached for 24 hours
    """
    from app.services.database_router import DatabaseRouter
    
    try:
        # 1. Check Cache
        cache_key = f"stats:{database_type}:{table_name}"
        cached_stats = await redis_client.get(cache_key)
        if cached_stats:
            logger.info(f"Returning cached stats for {table_name}")
            return {
                "status": "success",
                "table_name": table_name,
                "stats": cached_stats,
                "source": "cache"
            }

        # 2. Get Schema (to know columns)
        if database_type.lower() == "doris":
            schema_result = await DorisSchemaService.get_database_schema()
        else:
            schema_result = await SchemaService.get_database_schema(use_cache=True)
        
        schema = schema_result.get("schema", {})
        table_columns = schema.get("tables", {}).get(table_name, [])
        
        if not table_columns:
            return {
                "status": "success",
                "table_name": table_name,
                "stats": [],
                "message": "No columns found for table"
            }
        
        # 3. Calculate Stats (Optimized)
        stats = []
        # Limit to first 10 columns to prevent massive queries, as per safety requirements
        target_columns = table_columns[:10]
        
        for col in target_columns:
            col_name = col.get("name") if isinstance(col, dict) else col[0]
            col_type = col.get("type") if isinstance(col, dict) else (col[1] if len(col) > 1 else "UNKNOWN")
            
            is_numeric = any(t in col_type.upper() for t in ["NUMBER", "INT", "DECIMAL", "FLOAT", "DOUBLE"])
            
            # Use approximate counts for performance
            if database_type.lower() == "doris":
                # Doris: NDV() is approximate count distinct
                if is_numeric:
                    sql = f"SELECT MIN({col_name}), MAX({col_name}), NDV({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
                else:
                    sql = f"SELECT NDV({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
            else:
                # Oracle: APPROX_COUNT_DISTINCT
                if is_numeric:
                    sql = f"SELECT MIN({col_name}), MAX({col_name}), APPROX_COUNT_DISTINCT({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
                else:
                    sql = f"SELECT APPROX_COUNT_DISTINCT({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
            
            try:
                # Execute with RetryableExecutor for robustness
                executor = RetryableExecutor(
                    database_type=DatabaseType.DORIS if database_type.lower() == "doris" else DatabaseType.ORACLE
                )
                
                # We need to wrap the DatabaseRouter call to use the executor's policy
                # But DatabaseRouter.execute_sql doesn't take a policy. 
                # Ideally we'd modify DatabaseRouter, but here we can just call it and let the router handle it
                # OR we can use the executor to wrap the call.
                # Since DatabaseRouter doesn't expose the underlying client easily for the executor to wrap directly without a lambda,
                # we will rely on the router's internal error handling but use the executor to manage retries if the router fails.
                
                async def _exec():
                    return await DatabaseRouter.execute_sql(
                        database_type=database_type,
                        sql_query=sql,
                    )
                
                result = await executor.execute_with_retry(_exec)
                
                if result.get("status") == "success" and result.get("rows"):
                    row = result["rows"][0]
                    stat = {
                        "column": col_name,
                        "type": col_type,
                        "distinct_count": row[0] if not is_numeric else row[2],
                        "null_count": row[1] if not is_numeric else row[3],
                    }
                    if is_numeric:
                        stat["min"] = row[0]
                        stat["max"] = row[1]
                    stats.append(stat)
                else:
                    stats.append({
                        "column": col_name,
                        "type": col_type,
                        "error": result.get("error", "No data returned")
                    })
            except Exception as col_err:
                logger.warning(f"Failed to get stats for column {col_name}: {col_err}")
                stats.append({
                    "column": col_name,
                    "type": col_type,
                    "error": str(col_err)
                })
        
        # 4. Cache Results
        if stats:
            await redis_client.setex(cache_key, 86400, stats)
        
        return {
            "status": "success",
            "table_name": table_name,
            "stats": stats,
            "source": "database"
        }
        
    except Exception as e:
        logger.error(f"Table stats retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Table stats retrieval failed: {str(e)}"
        )