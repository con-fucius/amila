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
        database_type: Type of database (oracle, doris, postgres/postgresql)
        tables: Optional comma-separated list of tables to fetch
    
    Returns:
        Schema metadata from cache or database
    """
    try:
        logger.info(f"Retrieving schema (cache={use_cache}, database_type={database_type}, tables={tables})...")
        
        table_list = [t.strip() for t in tables.split(",")] if tables else None
        
        db_type_lower = database_type.lower()
        
        if db_type_lower == "doris":
            result = await DorisSchemaService.get_database_schema()
        elif db_type_lower in ["postgres", "postgresql"]:
            from app.services.postgres_schema_service import PostgresSchemaService
            result = await PostgresSchemaService.get_full_schema()
        else:
            result = await SchemaService.get_database_schema(
                connection_name=connection_name,
                use_cache=use_cache,
                table_names=table_list,
            )
        
        return SchemaResponse(
            status=result.get("status", "error"),
            source=result.get("source", "unknown"),
            schema_data=result.get("schema", result.get("schema_data", {})),
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

        # Invalidate query result cache on schema refresh
        try:
            await redis_client.invalidate_query_cache()
        except Exception:
            pass
        
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

        # Invalidate query result cache on schema changes
        try:
            await redis_client.invalidate_query_cache()
        except Exception:
            pass
        
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


@router.get("/relationships/{table_name}")
async def get_relationships(
    table_name: str,
    connection_name: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get foreign key relationships for a table
    """
    try:
        relationships = await SchemaService.get_table_relationships(table_name, connection_name)
        return {
            "status": "success",
            "table_name": table_name,
            "relationships": relationships
        }
    except Exception as e:
        logger.error(f"Relationship retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/comments/{table_name}")
async def get_comments(
    table_name: str,
    connection_name: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get column comments for a table
    """
    try:
        comments = await SchemaService.get_column_comments(table_name, connection_name)
        return {
            "status": "success",
            "table_name": table_name,
            "comments": comments
        }
    except Exception as e:
        logger.error(f"Comments retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        elif database_type.lower() in ["postgres", "postgresql"]:
            from app.services.postgres_schema_service import PostgresSchemaService
            schema_result = await PostgresSchemaService.get_full_schema()
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
        # Limit to first 20 columns to prevent massive queries, as per safety requirements
        target_columns = table_columns[:20]
        
        for col in target_columns:
            col_name = col.get("name") if isinstance(col, dict) else col[0]
            col_type = col.get("type") if isinstance(col, dict) else (col[1] if len(col) > 1 else "UNKNOWN")
            
            is_numeric = any(t in col_type.upper() for t in ["NUMBER", "INT", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC", "BIGINT"])
            
            # Use approximate counts for performance where available
            db_type_lower = database_type.lower()
            if db_type_lower == "doris":
                # Doris: NDV() is approximate count distinct
                if is_numeric:
                    sql = f"SELECT MIN({col_name}), MAX({col_name}), NDV({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
                else:
                    sql = f"SELECT NDV({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
            elif db_type_lower in ["postgres", "postgresql"]:
                # Postgres: No built-in approximate distinct count like NDV in core, use regular count(DISTINCT)
                if is_numeric:
                    sql = f'SELECT MIN("{col_name}"), MAX("{col_name}"), COUNT(DISTINCT "{col_name}"), COUNT(*) - COUNT("{col_name}") FROM "{table_name}"'
                else:
                    sql = f'SELECT COUNT(DISTINCT "{col_name}"), COUNT(*) - COUNT("{col_name}") FROM "{table_name}"'
            else:
                # Oracle: APPROX_COUNT_DISTINCT
                if is_numeric:
                    sql = f"SELECT MIN({col_name}), MAX({col_name}), APPROX_COUNT_DISTINCT({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
                else:
                    sql = f"SELECT APPROX_COUNT_DISTINCT({col_name}), SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) FROM {table_name}"
            
            try:
                # Execute with RetryableExecutor for robustness
                exec_db_type = DatabaseType.ORACLE
                if db_type_lower == "doris":
                    exec_db_type = DatabaseType.DORIS
                elif db_type_lower in ["postgres", "postgresql"]:
                    exec_db_type = DatabaseType.POSTGRES

                executor = RetryableExecutor(
                    database_type=exec_db_type
                )
                
                async def _exec():
                    return await DatabaseRouter.execute_sql(
                        database_type=database_type,
                        sql_query=sql,
                    )
                
                result = await executor.execute_with_retry(_exec, database_type=exec_db_type)
                
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
        
        if stats:
            await redis_client.setex(cache_key, 86400, stats)
        
        # 4. Integrate metadata enhancements (Comments & Relationships)
        comments = await SchemaService.get_column_comments(table_name)
        relationships = await SchemaService.get_table_relationships(table_name)
        
        return {
            "status": "success",
            "table_name": table_name,
            "stats": stats,
            "comments": comments,
            "relationships": relationships,
            "source": "database"
        }
        
    except Exception as e:
        logger.error(f"Table stats retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Table stats retrieval failed: {str(e)}"
        )


@router.post("/enrich")
async def enrich_schema(
    database_type: str = "oracle",
    tables: Optional[str] = None,
    connection_name: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Enrich schema with AI-generated descriptions and column inferences.
    
    **RBAC:** Requires authenticated user
    **AI-Powered:** Uses LLM to infer column meanings and generate descriptions
    
    Args:
        database_type: Database type (oracle, doris, postgres)
        tables: Optional comma-separated list of tables to enrich
        connection_name: Database connection name
        
    Returns:
        Enriched schema with AI-generated metadata
    """
    try:
        logger.info(f"Enriching schema with AI for {database_type}")
        
        table_list = [t.strip() for t in tables.split(",")] if tables else None
        
        result = await SchemaService.enrich_schema_with_ai(
            database_type=database_type,
            table_names=table_list,
            connection_name=connection_name
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Schema enrichment failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Schema enrichment failed: {str(e)}"
        )


@router.post("/columns/{table}/{column}/metadata")
async def update_column_metadata(
    table: str,
    column: str,
    description: str,
    alias: Optional[str] = None,
    database_type: str = "oracle",
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Update column metadata (description, alias).
    
    **RBAC:** Requires admin permission
    **Persistence:** Stores metadata in Redis cache
    
    Args:
        table: Table name
        column: Column name
        description: Human-readable description
        alias: Optional alias for the column
        database_type: Database type
        
    Returns:
        Updated metadata
    """
    try:
        # Check admin permission
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        from app.services.introspection_service import IntrospectionService
        
        result = await IntrospectionService.update_column_metadata(
            table=table,
            column=column,
            description=description,
            alias=alias,
            database_type=database_type
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Metadata update failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Metadata update failed: {str(e)}"
        )


@router.get("/columns/{table}/{column}/samples")
async def get_column_samples(
    table: str,
    column: str,
    limit: int = 10,
    connection_name: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get sample values for a column.
    
    **RBAC:** Requires authenticated user
    
    Args:
        table: Table name
        column: Column name
        limit: Maximum number of samples (default: 10, max: 50)
        connection_name: Database connection name
        
    Returns:
        Sample values
    """
    try:
        from app.services.introspection_service import IntrospectionService
        
        # Cap limit to prevent abuse
        limit = min(limit, 50)
        
        samples = await IntrospectionService.get_sample_values(
            table=table,
            column=column,
            limit=limit,
            connection_name=connection_name
        )
        
        return {
            "status": "success",
            "table": table,
            "column": column,
            "samples": samples,
            "count": len(samples)
        }
        
    except Exception as e:
        logger.error(f"Sample retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Sample retrieval failed: {str(e)}"
        )


@router.get("/columns/{table}/{column}/suggest-alias")
async def suggest_column_alias(
    table: str,
    column: str,
    database_type: str = "oracle",
    connection_name: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Suggest a human-readable alias for a column.
    
    **RBAC:** Requires authenticated user
    **AI-Powered:** Uses heuristics and sample values to suggest aliases
    
    Args:
        table: Table name
        column: Column name
        database_type: Database type
        connection_name: Database connection name
        
    Returns:
        Suggested alias with reasoning
    """
    try:
        from app.services.introspection_service import IntrospectionService
        
        result = await IntrospectionService.suggest_column_alias(
            table=table,
            column=column,
            database_type=database_type,
            connection_name=connection_name
        )
        
        return {
            "status": "success",
            **result
        }
        
    except Exception as e:
        logger.error(f"Alias suggestion failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Alias suggestion failed: {str(e)}"
        )
