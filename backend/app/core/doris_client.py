import json
import logging
import asyncio
from datetime import timedelta
from typing import Dict, Any, Optional, List

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class DorisMCPClient:
    """Client for the Apache Doris MCP Server using MCP Streamable HTTP transport."""

    def __init__(self):
        self.base_url = f"http://{settings.DORIS_MCP_HOST}:{settings.DORIS_MCP_PORT}"
        self.mcp_url = f"{self.base_url}/mcp"
        self.sse_url = f"{self.base_url}/sse"
        self.messages_url = f"{self.base_url}/messages"

        self._session: Optional[ClientSession] = None
        self._transport_cm = None
        self._session_id: Optional[str] = None
        self._initialized: bool = False
        self._lock = asyncio.Lock()
        self._healthy: bool = False
        self._available_tools: List[str] = []
        self._last_error_category: Optional[str] = None
        self._last_error_message: Optional[str] = None

    @property
    def is_healthy(self) -> bool:
        return self._initialized and self._healthy

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform an active health check by probing the MCP server.
        """
        if not self._initialized or not self._session:
            return {"status": "inactive", "message": "Not initialized"}
            
        try:
            # Lightweight probe: list_tools
            # We use a short timeout to ensure this doesn't block
            async with asyncio.timeout(5.0):
                await self._session.list_tools()
                self._healthy = True
                return {"status": "connected"}
        except Exception as e:
            self._healthy = False
            return {"status": "error", "message": str(e)}

    @property
    def exec_query_tool(self) -> str:
        value = getattr(settings, "DORIS_EXEC_QUERY_TOOL", None)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return "exec_query"

    @property
    def get_table_schema_tool(self) -> str:
        value = getattr(settings, "DORIS_GET_TABLE_SCHEMA_TOOL", None)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return "get_table_schema"

    @property
    def get_db_table_list_tool(self) -> str:
        value = getattr(settings, "DORIS_GET_DB_TABLE_LIST_TOOL", None)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return "get_db_table_list"

    async def initialize(self) -> bool:
        """Initialize MCP session and verify tools/list reports at least one tool."""
        if not settings.DORIS_MCP_ENABLED:
            self._healthy = False
            return False

        async with self._lock:
            if self._initialized and self._session is not None and self._healthy:
                return True

            self._healthy = False
            self._available_tools = []
            self._last_error_category = None
            self._last_error_message = None

            try:
                timeout = getattr(settings, "mcp_request_timeout", 30)
                logger.info("Connecting to Doris MCP at %s", self.mcp_url)

                self._transport_cm = streamablehttp_client(
                    self.mcp_url,
                    timeout=timedelta(seconds=timeout),
                )
                read_stream, write_stream, get_session_id = await self._transport_cm.__aenter__()

                self._session = ClientSession(read_stream, write_stream)
                await self._session.__aenter__()

                init_result = await self._session.initialize()
                self._session_id = get_session_id()

                tools_result = await self._session.list_tools()
                tools = getattr(tools_result, "tools", []) or []

                if not tools:
                    self._last_error_category = "no_tools"
                    self._last_error_message = "list_tools returned 0 tools"
                    logger.error("Doris MCP tools/list returned 0 tools; disabling Doris integration")
                    await self.close()
                    self._healthy = False
                    return False

                names: List[str] = [t.name for t in tools if getattr(t, "name", None)]
                self._available_tools = names
                if names:
                    logger.info("Doris MCP Server reports %d tools: %s", len(names), ", ".join(names))
                else:
                    logger.info("Doris MCP Server reports %d tools", len(tools))

                server_name = getattr(init_result.serverInfo, "name", "unknown")
                server_version = getattr(init_result.serverInfo, "version", "unknown")
                logger.info("Doris MCP initialized (server=%s, version=%s)", server_name, server_version)

                self._initialized = True
                self._healthy = True
                return True

            except (httpx.ConnectError, httpx.TimeoutException, ConnectionRefusedError, asyncio.TimeoutError) as e:
                category = "network"
                message = f"Connection failed: {str(e)}"
                self._last_error_category = category
                self._last_error_message = message
                logger.error("Doris MCP network error: %s", e)
                await self.close()
                self._healthy = False
                return False
                
            except (json.JSONDecodeError, ValueError) as e:
                category = "protocol"
                message = f"Protocol error: {str(e)}"
                self._last_error_category = category
                self._last_error_message = message
                logger.error("Doris MCP protocol error: %s", e)
                await self.close()
                self._healthy = False
                return False

            except Exception as e:
                message = str(e)
                exc_type = type(e).__name__
                # Fallback heuristics
                if "ConnectionError" in message or "timed out" in message or "Connect call failed" in message:
                    category = "network"
                else:
                    category = "unknown"
                
                self._last_error_category = category
                self._last_error_message = f"{exc_type}: {message}"
                logger.error("Doris MCP initialization failed (%s): %s [%s]", category, e, exc_type, exc_info=True)
                await self.close()
                self._healthy = False
                return False

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool and parse the JSON payload returned by the server."""
        ok = await self.initialize()
        if not ok or not self._session:
            return {
                "status": "error",
                "error": "Doris MCP client not initialized",
                "error_type": self._last_error_category or "unavailable",
            }

        try:
            response = await self._session.call_tool(tool_name, arguments)

            text = ""
            if getattr(response, "content", None):
                for block in response.content:
                    if getattr(block, "text", None):
                        text += block.text

            payload: Any = None
            if text:
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    payload = {"success": True, "data": text}

            if payload is None and getattr(response, "structuredContent", None) is not None:
                payload = response.structuredContent

            if isinstance(payload, dict):
                success = payload.get("success", True)
                status = "success" if success else "error"
                payload.setdefault("status", status)
                return payload

            return {"status": "success", "result": payload}

        except (httpx.ConnectError, httpx.TimeoutException, asyncio.TimeoutError) as e:
            self._healthy = False
            self._last_error_category = "network"
            self._last_error_message = str(e)
            logger.error("Tool call '%s' network error: %s", tool_name, e)
            return {
                "status": "error",
                "error": f"Network error: {str(e)}",
                "error_type": "network",
            }
            
        except (json.JSONDecodeError, ValueError) as e:
            # Protocol error usually means response wasn't valid JSON/MCP
            self._last_error_category = "protocol"
            self._last_error_message = str(e)
            logger.error("Tool call '%s' protocol error: %s", tool_name, e)
            return {
                "status": "error",
                "error": f"Protocol error: {str(e)}",
                "error_type": "protocol",
            }

        except Exception as e:
            self._healthy = False
            exc_type = type(e).__name__
            self._last_error_category = "tool_call"
            self._last_error_message = f"{exc_type}: {str(e)}"
            logger.error("Tool call '%s' failed [%s]: %s", tool_name, exc_type, e, exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "error_type": "tool_call",
                "exception_type": exc_type,
            }

    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL via Doris exec_query tool and normalize result for orchestrator."""
        tool_result = await self.call_tool(
            self.exec_query_tool,
            {"sql": sql, "max_rows": 1000, "timeout": 60},
        )

        if tool_result.get("status") != "success":
            return {
                "status": "error",
                "error": tool_result.get("error") or tool_result.get("message") or "Doris query failed",
            }

        payload = tool_result
        inner = payload.get("result") if isinstance(payload.get("result"), dict) else payload

        data = inner.get("data")
        row_count = inner.get("row_count")
        execution_time = inner.get("execution_time")
        metadata = inner.get("metadata") or {}
        columns_meta = metadata.get("columns") or []

        # Normalize Doris column metadata while keeping a strict
        # string[] contract for the public "columns" field.
        column_metadata: List[Dict[str, Any]] = []
        column_names: List[str] = []
        for col in columns_meta:
            if isinstance(col, dict):
                name = col.get("name") or col.get("column_name")
                col_type = col.get("type") or col.get("data_type")
                # Guard against missing/None names by stringifying
                safe_name = str(name) if name is not None else ""
                column_metadata.append({"name": safe_name, "type": col_type})
                column_names.append(safe_name)
            else:
                safe_name = str(col)
                column_metadata.append({"name": safe_name, "type": None})
                column_names.append(safe_name)

        rows: List[List[Any]] = []
        if isinstance(data, list) and column_names:
            for row in data:
                if isinstance(row, dict):
                    rows.append([row.get(n) for n in column_names])
                else:
                    rows.append([row])
        elif isinstance(data, list):
            for row in data:
                rows.append([row])

        results_block = {
            # Contract for downstream orchestrator + frontend: list of
            # column names as strings.
            "columns": column_names,
            # Preserve rich Doris metadata separately for future use.
            "column_metadata": column_metadata,
            "rows": rows,
            "row_count": row_count if isinstance(row_count, int) else len(rows),
            "execution_time_ms": int(execution_time * 1000) if isinstance(execution_time, (int, float)) else 0,
        }

        return {"status": "success", "results": results_block}

    async def close(self):
        if self._session is not None:
            try:
                await self._session.__aexit__(None, None, None)
            except Exception:
                pass
            self._session = None

        if self._transport_cm is not None:
            try:
                await self._transport_cm.__aexit__(None, None, None)
            except Exception:
                pass
            self._transport_cm = None

        self._initialized = False
        self._session_id = None


# Global instance
doris_client = DorisMCPClient()
