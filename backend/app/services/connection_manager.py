import logging
from typing import Dict, Any, List, Optional
from app.core.client_registry import registry
from app.core.config import settings
from app.services.doris_query_service import DorisQueryService

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Centralized manager for discovering and managing database connections
    across different providers (Oracle SQLcl, Doris MCP, etc).
    """
    
    @staticmethod
    async def list_all_connections() -> Dict[str, Any]:
        """List all available connections from all configured providers."""
        connections: List[Dict[str, Any]] = []
        errors: List[str] = []
        
        # 1. Oracle Connections
        try:
            oracle_conns = await ConnectionManager._get_oracle_connections()
            connections.extend(oracle_conns)
        except Exception as e:
            msg = str(e)
            logger.warning(f"Oracle connection listing failed: {msg}")
            errors.append(f"Oracle: {msg}")
            
        # 2. Doris Connections
        if settings.DORIS_MCP_ENABLED:
            try:
                doris_result = await DorisQueryService.list_connections()
                if doris_result.get("status") == "success":
                    doris_conns = doris_result.get("connections", [])
                    connections.extend(doris_conns)
                    logger.info(f"Retrieved {len(doris_conns)} Doris connections")
                else:
                    err = doris_result.get("error") or doris_result.get("message")
                    errors.append(f"Doris: {err}")
            except Exception as e:
                logger.error(f"Doris connection listing failed: {e}", exc_info=True)
                errors.append(f"Doris: {str(e)}")
                
        if not connections:
            if errors:
                return {
                    "status": "error",
                    "message": "No connections available. Errors: " + "; ".join(errors),
                    "connections": []
                }
            # No connections but no errors (e.g. disabled)
            return {
                "status": "success",
                "connections": [],
                "message": "No connections configured"
            }
            
        return {
            "status": "success",
            "connections": connections
        }

    @staticmethod
    async def _get_oracle_connections() -> List[Dict[str, Any]]:
        """Retrieve Oracle connections from Pool or MCP fallback."""
        # Try Pool first
        sqlcl_pool = registry.get_sqlcl_pool()
        pool_error = None
        
        if sqlcl_pool:
            try:
                async with sqlcl_pool.acquire(timeout=10) as client:
                    result = await client.list_connections()
                    if result.get("status") == "success":
                        conns = result.get("connections", [])
                        logger.info(f"Retrieved {len(conns)} Oracle connections via pool")
                        return conns
                    else:
                        pool_error = result.get("message")
            except Exception as e:
                pool_error = str(e)
                logger.warning(f"Oracle pool connection listing failed: {e}")
        
        # Fallback to single MCP Client
        mcp_client = registry.get_mcp_client()
        if mcp_client:
            logger.info("Fetching Oracle connections via fallback MCP client...")
            try:
                result = await mcp_client.list_connections()
                if result.get("status") == "success":
                    conns = result.get("connections", [])
                    logger.info(f"Retrieved {len(conns)} Oracle connections via MCP")
                    return conns
                else:
                    raise RuntimeError(f"MCP list_connections failed: {result.get('message')}")
            except Exception as e:
                raise RuntimeError(f"Oracle MCP fallback failed: {e}")
        
        error_msg = "No Oracle client available"
        if pool_error:
            error_msg += f" (Pool error: {pool_error})"
        raise RuntimeError(error_msg)
