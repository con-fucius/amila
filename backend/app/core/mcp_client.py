"""
Oracle SQLcl MCP Client for the Amila backend
Implements proper subprocess-based communication with Oracle SQLcl MCP Server
"""

import asyncio
import json
import logging
import subprocess
import sys
import time
import threading
import queue
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from enum import Enum
from contextlib import asynccontextmanager
import uuid

from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
trace_propagator = TraceContextTextMapPropagator()


class MCPError(Exception):
    """Base exception for MCP-related errors"""
    pass


class MCPConnectionError(MCPError):
    """MCP connection error"""
    pass


class MCPExecutionError(MCPError):
    """MCP execution error"""
    pass


@dataclass
class MCPRequest:
    """JSON-RPC request structure for MCP with distributed tracing support"""
    method: str
    params: Dict[str, Any]
    id: str = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid.uuid4())
    
    def to_json(self) -> str:
        """Convert to JSON-RPC format with W3C trace context propagation"""
        # Inject trace context for distributed tracing (Gap #31 - OWASP)
        trace_headers = {}
        trace_propagator.inject(trace_headers)
        
        # Add trace context to params metadata if available
        if trace_headers:
            if "_trace_context" not in self.params:
                self.params["_trace_context"] = trace_headers
        
        return json.dumps({
            "jsonrpc": "2.0",
            "method": self.method,
            "params": self.params,
            "id": self.id
        })


@dataclass
class MCPResponse:
    """JSON-RPC response structure for MCP"""
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[str] = None
    
    @classmethod
    def from_json(cls, data: str) -> 'MCPResponse':
        """Parse JSON-RPC response"""
        try:
            parsed = json.loads(data)
            return cls(
                result=parsed.get("result"),
                error=parsed.get("error"),
                id=parsed.get("id")
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse MCP response: {e}")
            return cls(error={"code": -32700, "message": "Parse error"})
    
    def is_error(self) -> bool:
        """Check if response contains an error"""
        return self.error is not None


class SQLclMCPClient:
    """
    Oracle SQLcl MCP Client using STDIO/JSON-RPC communication
    Implements direct subprocess communication with SQLcl MCP Server
    """
    
    def __init__(
        self,
        sqlcl_path: str = "sql",
        sqlcl_args: List[str] = None,
        timeout: int = 600,
        encoding: str = "utf-8"
    ):
        """
        Initialize SQLcl MCP Client
        
        Args:
            sqlcl_path: Full path to SQLcl executable
            sqlcl_args: Arguments for SQLcl (defaults to ["-mcp"])
            timeout: Default timeout for operations in seconds (600s for complex queries)
            encoding: Character encoding for subprocess communication
        """
        self.sqlcl_path = sqlcl_path
        self.sqlcl_args = sqlcl_args or ["-mcp"]
        self.timeout = timeout
        self.encoding = encoding
        
        # Subprocess management
        self.process: Optional[subprocess.Popen] = None
        self.reader_thread: Optional[threading.Thread] = None
        self.response_queue: queue.Queue = queue.Queue()
        self.response_futures: Dict[str, asyncio.Future] = {}
        
        # Connection state
        self._connected = False
        self._current_connection: Optional[str] = None
        self._running = False
        
        # Response buffer for handling multi-line responses
        self._response_buffer = ""
        
    async def initialize(self) -> bool:
        """
        Initialize the SQLcl MCP server subprocess
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            logger.info(f"Starting SQLcl MCP server: {self.sqlcl_path} {' '.join(self.sqlcl_args)}")
            
            # Start SQLcl subprocess with MCP flag
            self.process = subprocess.Popen(
                [self.sqlcl_path] + self.sqlcl_args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True,
                bufsize=1  # Line buffered
            )
            
            # Skip startup banner (exactly 4 lines) - synchronously like robust_mcp_test.py
            logger.info(f"Skipping SQLcl startup banner...")
            for i in range(4):
                banner_line = self.process.stdout.readline()
                logger.debug(f"Banner line {i+1}: {banner_line.strip()}")
            
            logger.info(f"Startup banner skipped, ready for JSON-RPC")
            
            # Start reader thread for stdout (synchronous reading in background thread)
            self._running = True
            self.reader_thread = threading.Thread(target=self._read_output_sync, daemon=True)
            self.reader_thread.start()
            
            # Start async task to process responses from queue
            asyncio.create_task(self._process_responses())
            
            # Small delay to let reader thread start
            await asyncio.sleep(0.5)
            
            # Send initialization request
            response = await self._send_request(MCPRequest(
                method="initialize",
                params={
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "bi-agent-mvp",
                        "version": "1.0.0"
                    }
                }
            ))
            
            if response and not response.is_error():
                logger.info(f"Initialize successful: {response.result.get('serverInfo', {}).get('name')}")
                
                # Send required notifications/initialized message
                logger.info(f"Sending notifications/initialized...")
                def send_notification():
                    notification_json = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "notifications/initialized",
                        "params": {}
                    }) + "\n"
                    self.process.stdin.write(notification_json)
                    self.process.stdin.flush()
                
                await asyncio.get_event_loop().run_in_executor(None, send_notification)
                await asyncio.sleep(0.5)  # Small delay after notification
                
                # Test connection by listing available tools
                tools_response = await self._send_request(MCPRequest(
                    method="tools/list",
                    params={}
                ))
                
                if tools_response and not tools_response.is_error():
                    tools = tools_response.result.get('tools', [])
                    logger.info(f"SQLcl MCP server initialized successfully with {len(tools)} tools")
                    self._connected = True
                    return True
                else:
                    logger.error(f"Failed to list tools: {tools_response.error if tools_response else 'No response'}")
                    await self.close()
                    return False
            else:
                logger.error(f"Failed to initialize: {response.error if response else 'No response'}")
                await self.close()
                return False
                
        except Exception as e:
            logger.error(f"Failed to start SQLcl MCP server: {e}", exc_info=True)
            await self.close()
            return False
    
    def _read_output_sync(self):
        """Synchronously read output from SQLcl subprocess (runs in background thread)"""
        if not self.process or not self.process.stdout:
            return
            
        try:
            while self._running and self.process and self.process.poll() is None:
                try:
                    # Read line synchronously (like robust_mcp_test.py)
                    line = self.process.stdout.readline()
                    
                    if not line:
                        time.sleep(0.01)
                        continue
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Filter out Java log lines - only process JSON-RPC responses
                    if self._is_json_line(line):
                        logger.debug(f"JSON response received: {line[:100]}...")
                        
                        try:
                            response = MCPResponse.from_json(line)
                            logger.debug(f"Parsed response ID: {response.id}")
                            
                            # Put response in queue for async processing
                            self.response_queue.put(response)
                            
                        except json.JSONDecodeError as e:
                            logger.debug(f"JSON parse error: {e}")
                    else:
                        # Skip Java log lines
                        logger.debug(f"Skipping log line: {line[:50]}...")
                        
                except Exception as e:
                    logger.error(f"Error reading output: {e}", exc_info=True)
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Reader thread error: {e}", exc_info=True)
        finally:
            logger.info("Reader thread exiting")
    
    async def _process_responses(self):
        """Process responses from the queue and resolve futures"""
        while self._running:
            try:
                # Check for responses in queue (non-blocking)
                while not self.response_queue.empty():
                    try:
                        response = self.response_queue.get_nowait()
                        
                        # Resolve waiting future if exists
                        if response.id and response.id in self.response_futures:
                            future = self.response_futures.pop(response.id)
                            if not future.done():
                                future.set_result(response)
                                logger.debug(f"Resolved future for ID: {response.id}")
                        else:
                            logger.debug(f"Notification or unmatched response: {response}")
                    
                    except queue.Empty:
                        break
                        
                await asyncio.sleep(0.01)  # Small delay to prevent busy waiting
                
            except Exception as e:
                logger.error(f"Error processing responses: {e}", exc_info=True)
                await asyncio.sleep(0.1)
    
    def _is_json_line(self, line: str) -> bool:
        """Check if a line looks like JSON-RPC response"""
        line = line.strip()
        return line.startswith('{"jsonrpc"') or (line.startswith('{') and '"jsonrpc"' in line)
    
    async def _send_request(self, request: MCPRequest, timeout: Optional[int] = None, retry_count: int = 3) -> Optional[MCPResponse]:
        """
        Send a request to SQLcl MCP server and wait for response with timeout and exponential backoff retry
        Prevents hanging queries from blocking the system indefinitely
        
        Args:
            request: The MCP request to send
            timeout: Optional timeout override
            retry_count: Number of retry attempts (default 3)
            
        Returns:
            MCPResponse or None if timeout/error after all retries
        """
        if not self.process or self.process.poll() is not None:
            logger.error("SQLcl process is not running")
            return None
        
        last_error = None
        for attempt in range(retry_count):
            try:
                return await self._send_request_once(request, timeout)
            except (asyncio.TimeoutError, MCPConnectionError) as e:
                last_error = e
                if attempt < retry_count - 1:
                    backoff_delay = min(2 ** attempt, 10)  # Cap at 10s
                    logger.warning(f"Request failed (attempt {attempt + 1}/{retry_count}), retrying in {backoff_delay}s...")
                    await asyncio.sleep(backoff_delay)
                else:
                    logger.error(f"Request failed after {retry_count} attempts")
        
        return MCPResponse(error={"code": -32000, "message": f"Request failed after {retry_count} retries: {last_error}"})
    
    async def _send_request_once(self, request: MCPRequest, timeout: Optional[int] = None) -> Optional[MCPResponse]:
        """Single request attempt without retry logic"""
        if not self.process or self.process.poll() is not None:
            raise MCPConnectionError("SQLcl process is not running")
        
        try:
            # Create future for response
            future = asyncio.Future()
            self.response_futures[request.id] = future
            
            # Send request
            request_json = request.to_json()
            logger.debug(f"Sending request: {request_json}")
            
            # Send request to subprocess stdin (synchronously in thread)
            def write_request():
                self.process.stdin.write(request_json + "\n")
                self.process.stdin.flush()
            
            await asyncio.get_event_loop().run_in_executor(None, write_request)
            
            # Wait for response with timeout (prevent indefinite hangs)
            effective_timeout = timeout or self.timeout or 30  # Default 30s if not set
            response = await asyncio.wait_for(
                future,
                timeout=effective_timeout
            )
            
            return response
            
        except asyncio.TimeoutError:
            logger.error(f"Request timeout after {effective_timeout}s for method: {request.method}")
            self.response_futures.pop(request.id, None)
            # Return timeout error response
            return MCPResponse(error={"code": -32000, "message": f"Request timeout after {effective_timeout}s"})
        except Exception as e:
            logger.error(f"Error sending request: {e}", exc_info=True)
            self.response_futures.pop(request.id, None)
            return None
    
    async def list_connections(self) -> Dict[str, Any]:
        """
        List available Oracle database connections
        
        Returns:
            Dictionary with status and list of connections
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        
        try:
            response = await self._send_request(MCPRequest(
                method="tools/call",
                params={
                    "name": "list-connections",
                    "arguments": {
                        "mcp_client": "bi-agent-mvp",
                        "model": "claude-3.5-sonnet"
                    }
                }
            ))
            
            if response and not response.is_error():
                # Parse connection names from response
                connections = []
                if response.result and "content" in response.result:
                    content = response.result["content"]
                    if isinstance(content, list) and len(content) > 0:
                        # Extract connection names from the text content
                        text = content[0].get("text", "")
                        connection_names = [name.strip() for name in text.split(",") if name.strip()]
                        
                        for name in connection_names:
                            connections.append({
                                "name": name,
                                "type": "oracle",
                                "status": "available"
                            })
                
                return {
                    "status": "success",
                    "connections": connections
                }
            else:
                error_msg = response.error.get("message", "Unknown error") if response and response.error else "No response"
                return {"status": "error", "message": error_msg}
                
        except Exception as e:
            logger.error(f"Failed to list connections: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    async def connect_database(self, connection_name: str) -> Dict[str, Any]:
        """
        Connect to a specific database using a pre-configured connection
        
        Args:
            connection_name: Name of the pre-configured connection
            
        Returns:
            Dictionary with connection status
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        
        try:
            response = await self._send_request(MCPRequest(
                method="tools/call",
                params={
                    "name": "connect",
                    "arguments": {
                        "connection_name": connection_name
                    }
                }
            ))
            
            if response and not response.is_error():
                self._current_connection = connection_name
                return {
                    "status": "connected",
                    "connection_name": connection_name,
                    "message": f"Connected to {connection_name}"
                }
            else:
                error_msg = response.error.get("message", "Unknown error") if response and response.error else "No response"
                return {"status": "error", "message": error_msg}
                
        except Exception as e:
            logger.error(f"Failed to connect to {connection_name}: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    async def disconnect_database(self) -> Dict[str, Any]:
        """
        Disconnect from current database connection
        
        Returns:
            Dictionary with disconnection status
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        
        if not self._current_connection:
            return {"status": "success", "message": "No active connection"}
        
        try:
            response = await self._send_request(MCPRequest(
                method="tools/call",
                params={
                    "name": "disconnect",
                    "arguments": {}
                }
            ))
            
            if response and not response.is_error():
                prev_connection = self._current_connection
                self._current_connection = None
                return {
                    "status": "disconnected",
                    "message": f"Disconnected from {prev_connection}"
                }
            else:
                error_msg = response.error.get("message", "Unknown error") if response and response.error else "No response"
                return {"status": "error", "message": error_msg}
                
        except Exception as e:
            logger.error(f"Failed to disconnect: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    async def ensure_connection(self, connection_name: str) -> Dict[str, Any]:
        """Ensure the SQLcl session is connected to the requested connection.
        Returns {'status':'connected'} on success or {'status':'error','message':...}.
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        if self._current_connection == connection_name:
            return {"status": "connected", "connection_name": connection_name}
        return await self.connect_database(connection_name)

    async def execute_sql(self, sql: str, connection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute SQL query with audit logging
        
        Args:
            sql: SQL query to execute
            connection_name: Optional connection name (uses current if not specified)
            
        Returns:
            Dictionary with query results or error
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        
        # Use provided connection or current connection
        conn_name = connection_name or self._current_connection
        if not conn_name:
            return {"status": "error", "message": "No database connection specified or active"}
        
        # Ensure connected session to avoid 'Not authenticated' from SQLcl MCP
        ensured = await self.ensure_connection(conn_name)
        if ensured.get("status") != "connected":
            return {"status": "error", "message": ensured.get("message", "Failed to connect"), "sql": sql}
        
        # Add LLM audit marker
        audit_sql = f"/* LLM in use - BI Agent MVP */ {sql}"
        
        try:
            response = await self._send_request(
                MCPRequest(
                    method="tools/call",
                    params={
                        "name": "run-sql",
                        "arguments": {
                            "sql": audit_sql,
                            "connection": conn_name
                        }
                    }
                ),
                timeout=self.timeout  # Pass configured timeout (default 600s from pool)
            )
            
            if response and not response.is_error():
                # Parse SQL results
                return self._parse_sql_results(response.result, sql)
            else:
                # DEBUG: Log full response for error diagnosis
                logger.error(f"MCP Error Response: {json.dumps({'response': str(response), 'error': response.error if response else None}, indent=2)}")
                error_msg = response.error.get("message", "Unknown error") if response and response.error else "No response"
                return {
                    "status": "error",
                    "message": error_msg,
                    "sql": sql
                }
                
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "error_type": type(e).__name__,
                "sql": sql
            }
    
    async def execute_sqlcl_command(self, command: str, connection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute SQLcl command (like DESCRIBE, SHOW, etc.)
        
        Args:
            command: SQLcl command to execute
            connection_name: Optional connection name
            
        Returns:
            Dictionary with command results
        """
        if not self._connected:
            return {"status": "error", "message": "MCP client not connected"}
        
        conn_name = connection_name or self._current_connection
        if not conn_name:
            return {"status": "error", "message": "No database connection specified or active"}
        
        # Ensure connected session
        ensured = await self.ensure_connection(conn_name)
        if ensured.get("status") != "connected":
            return {"status": "error", "message": ensured.get("message", "Failed to connect"), "command": command}
        
        try:
            response = await self._send_request(MCPRequest(
                method="tools/call",
                params={
                    "name": "run-sqlcl",
                    "arguments": {
                        "command": command,
                        "connection": conn_name
                    }
                }
            ))
            
            if response and not response.is_error():
                return self._parse_command_results(response.result, command)
            else:
                error_msg = response.error.get("message", "Unknown error") if response and response.error else "No response"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": command
                }
                
        except Exception as e:
            logger.error(f"Failed to execute SQLcl command: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "error_type": type(e).__name__,
                "command": command
            }
    
    def _parse_sql_results(self, result: Dict[str, Any], sql: str) -> Dict[str, Any]:
        """Parse SQL execution results from MCP response (robust JSON/table parsing with error detection)."""
        try:
            # High-level summary at INFO; full payload at DEBUG (primarily for file logs)
            logger.info("MCP _parse_sql_results: result_type=%s", type(result))
            logger.info(
                "MCP _parse_sql_results: result_keys=%s",
                list(result.keys()) if isinstance(result, dict) else "None",
            )
            if result:
                logger.debug(
                    "MCP _parse_sql_results FULL result payload: %s",
                    json.dumps(result, indent=2),
                )
            
            if result and "content" in result:
                content = result["content"]
                if isinstance(content, list) and len(content) > 0:
                    text_payload = content[0].get("text", "").strip()

                    if text_payload:
                        # Detect ORA- errors early
                        if "ORA-" in text_payload:
                            # Try to extract the first ORA- line
                            ora_msg = None
                            for line in text_payload.splitlines():
                                if "ORA-" in line:
                                    ora_msg = line.strip()
                                    break
                                    ora_msg = None
                            for line in text_payload.splitlines():
                                if "ORA-" in line:
                                    ora_msg = line.strip()
                                    break
                            
                            # If we see ORA- but couldn't exact a specific line, return the start of the payload
                            msg_to_return = ora_msg if ora_msg else f"Oracle Error (details): {text_payload[:200]}"
                            return {"status": "error", "message": msg_to_return, "sql": sql}

                        # 1) JSON payload path (preferred by SQLcl MCP tools)
                        if (text_payload.startswith('{') and '"status"' in text_payload) or text_payload.startswith('['):
                            try:
                                parsed = json.loads(text_payload)
                                # Some tools wrap results at top-level; normalize
                                if isinstance(parsed, dict) and (
                                    "columns" in parsed or "results" in parsed or "status" in parsed
                                ):
                                    status = parsed.get("status", "success")
                                    results = parsed.get("results")
                                    if results is None and "columns" in parsed:
                                        results = {
                                            "columns": parsed.get("columns", []),
                                            "rows": parsed.get("rows", []),
                                            "row_count": parsed.get("row_count", 0),
                                            "execution_time_ms": parsed.get("execution_time_ms", 0),
                                        }
                                    if status != "success":
                                        error_detail = parsed.get("message") or parsed.get("error") or json.dumps(parsed)
                                        return {"status": "error", "message": str(error_detail), "sql": sql, "results": results}
                                    return {
                                        "status": "success",
                                        "query_id": f"mcp_{hash(sql) % 10000}",
                                        "sql": sql,
                                        "results": results or {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": 0},
                                    }
                            except json.JSONDecodeError:
                                # Fall through to other parsing strategies
                                pass

                        # 1b) Embedded JSON anywhere in text (defensive)
                        try:
                            start = text_payload.find('{')
                            end = text_payload.rfind('}')
                            if start != -1 and end != -1 and end > start:
                                embedded = text_payload[start:end+1]
                                parsed = json.loads(embedded)
                                if isinstance(parsed, dict) and ("columns" in parsed or "results" in parsed or "status" in parsed):
                                    status = parsed.get("status", "success")
                                    results = parsed.get("results")
                                    if results is None and "columns" in parsed:
                                        results = {
                                            "columns": parsed.get("columns", []),
                                            "rows": parsed.get("rows", []),
                                            "row_count": parsed.get("row_count", 0),
                                            "execution_time_ms": parsed.get("execution_time_ms", 0),
                                        }
                                    if status != "success":
                                        return {"status": "error", "message": parsed.get("message", "Execution error"), "sql": sql, "results": results}
                                    return {
                                        "status": "success",
                                        "query_id": f"mcp_{hash(sql) % 10000}",
                                        "sql": sql,
                                        "results": results or {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": 0},
                                    }
                        except Exception:
                            pass

                        # 2) Pipe/CSV payload paths (legacy) - Use proper CSV parsing
                        import csv
                        import io
                        
                        # Handle both \r\n (Windows) and \n (Unix) line endings
                        lines = text_payload.replace('\r\n', '\n').replace('\r', '\n').split('\n')
                        # Detailed CSV diagnostics are DEBUG-only to avoid noisy console logs
                        logger.debug(
                            "CSV PARSING DEBUG: Total lines after normalization: %d",
                            len(lines),
                        )
                        logger.debug(
                            "CSV PARSING DEBUG: First 10 lines: %s", lines[:10]
                        )
                        if lines:
                            # Detect delimiter (pipe preferred if present)
                            delimiter = '|' if '|' in lines[0] else ','
                            logger.debug(
                                "CSV PARSING DEBUG: Detected delimiter: '%s'", delimiter
                            )
                            
                            # Use Python's csv module for proper parsing of quoted fields
                            csv_text = '\n'.join(lines)
                            csv_reader = csv.reader(io.StringIO(csv_text), delimiter=delimiter, quotechar='"')
                            
                            rows = []
                            skipped_lines = []
                            headers = None
                            
                            for idx, row in enumerate(csv_reader):
                                if idx == 0:
                                    # First row is headers
                                    headers = [h.strip() for h in row]
                                    logger.debug("CSV PARSING DEBUG: Headers: %s", headers)
                                    continue
                                
                                if not row or not any(cell.strip() for cell in row):
                                    skipped_lines.append(f"Line {idx}: empty")
                                    continue
                                
                                # Skip footer like "X rows selected"
                                first_cell = row[0].strip() if row else ""
                                if ' row' in first_cell.lower() and 'selected' in first_cell.lower():
                                    skipped_lines.append(f"Line {idx}: footer '{first_cell[:50]}'")
                                    continue
                                
                                # Trim separator lines (----) often printed by SQLCL with pipe tables
                                if all(set(cell.strip()) <= {'-', ''} for cell in row):
                                    skipped_lines.append(f"Line {idx}: separator")
                                    continue
                                
                                # Strip whitespace from each cell
                                cleaned_row = [cell.strip() for cell in row]
                                
                                if len(cleaned_row) == len(headers):
                                    rows.append(cleaned_row)
                                    logger.debug(
                                        "CSV PARSING DEBUG: Added row %d with %d columns",
                                        idx,
                                        len(cleaned_row),
                                    )
                                else:
                                    # Log mismatched row for debugging
                                    skipped_lines.append(
                                        f"Line {idx}: column mismatch ({len(cleaned_row)} vs {len(headers)}): {cleaned_row[:3]}"
                                    )
                                    logger.warning(
                                        "CSV PARSING DEBUG: Skipping row %d with %d columns (expected %d): %s",
                                        idx,
                                        len(cleaned_row),
                                        len(headers),
                                        cleaned_row[:3],
                                    )
                            
                            logger.debug(
                                "CSV PARSING DEBUG: Total rows parsed: %d", len(rows)
                            )
                            logger.debug(
                                "CSV PARSING DEBUG: Skipped %d lines: %s",
                                len(skipped_lines),
                                skipped_lines[:10],
                            )
                            # Concise summary at INFO level for quick inspection
                            logger.info(
                                "CSV parse summary: rows=%d, columns=%d, skipped_lines=%d",
                                len(rows),
                                len(headers) if headers else 0,
                                len(skipped_lines),
                            )
                            # Heuristic error detection for CSV path
                            first_col = headers[0] if headers else ""
                            oracle_error_msg = "Oracle error"  # Default fallback
                            if isinstance(first_col, str) and first_col.lower().startswith("error"):
                                # Extract error message from first few rows
                                if rows:
                                    oracle_error_msg = "\n".join([" ".join(map(str, r)) for r in rows[:5]])
                                return {"status": "error", "message": oracle_error_msg, "sql": sql, "results": {"columns": headers, "rows": rows}}
                            return {
                                "status": "success",
                                "query_id": f"mcp_{hash(sql) % 10000}",
                                "sql": sql,
                                "results": {
                                    "columns": headers,
                                    "rows": rows,
                                    "row_count": len(rows),
                                    "execution_time_ms": 100,
                                },
                                "metadata": {
                                    "connection": self._current_connection,
                                    "audit_logged": True,
                                    "llm_marker": True,
                                },
                            }

            # Fallback handling: if explicit 'no rows selected' appears, return success with 0 rows
            payload_text = ""
            try:
                if result and "content" in result and isinstance(result["content"], list) and result["content"]:
                    payload_text = (result["content"][0].get("text") or "").lower()
            except Exception:
                payload_text = ""
            if payload_text and ("no rows selected" in payload_text or "0 rows selected" in payload_text):
                return {
                    "status": "success",
                    "query_id": f"mcp_{hash(sql) % 10000}",
                    "sql": sql,
                    "results": {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": 0},
                }

            # Otherwise treat as error to surface parsing problems instead of silent empty success
            return {"status": "error", "message": "Failed to parse SQL results from MCP response", "sql": sql}
        except Exception as e:
            logger.error(f"Failed to parse SQL results: {e}", exc_info=True)
            return {"status": "error", "message": f"Failed to parse results: {e}", "error_type": type(e).__name__, "sql": sql}
    
    def _parse_command_results(self, result: Dict[str, Any], command: str) -> Dict[str, Any]:
        """Parse SQLcl command results from MCP response"""
        try:
            if result and "content" in result:
                content = result["content"]
                if isinstance(content, list) and len(content) > 0:
                    text_output = content[0].get("text", "")
                    
                    return {
                        "status": "success",
                        "command": command,
                        "output": text_output,
                        "metadata": {
                            "connection": self._current_connection
                        }
                    }
            
            return {
                "status": "success",
                "command": command,
                "output": "",
                "metadata": {
                    "connection": self._current_connection
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to parse command results: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to parse results: {e}",
                "command": command
            }
    
    async def validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        Validate SQL query for security (read-only enforcement)
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Dictionary with validation status
        """
        try:
            sql_upper = sql.upper().strip()
            
            # Block dangerous operations
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE']
            
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    return {
                        "status": "blocked",
                        "reason": f"Query contains prohibited operation: {keyword}",
                        "sql": sql
                    }
            
            # Only allow SELECT statements
            if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
                return {
                    "status": "blocked",
                    "reason": "Only SELECT queries are allowed",
                    "sql": sql
                }
            
            return {
                "status": "approved",
                "sql": sql,
                "risk_level": "low"
            }
            
        except Exception as e:
            logger.error(f"SQL validation error: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    async def get_schema(self, connection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get database schema information with column details
        
        Args:
            connection_name: Optional connection name
            
        Returns:
            Dictionary with comprehensive schema information
        """
        conn_name = connection_name or self._current_connection
        if not conn_name:
            return {"status": "error", "message": "No database connection specified or active"}
        
        # Ensure connected session
        ensured = await self.ensure_connection(conn_name)
        if ensured.get("status") != "connected":
            return {"status": "error", "message": ensured.get("message", "Failed to connect")}
        
        try:
            # Get tables with column details
            tables_query = """
                SELECT 
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.nullable,
                    c.column_id
                FROM user_tables t
                LEFT JOIN user_tab_columns c ON t.table_name = c.table_name
                ORDER BY t.table_name, c.column_id
            """
            
            tables_result = await self.execute_sql(tables_query, conn_name)
            
            tables_dict = {}
            if tables_result.get("status") == "success":
                rows = tables_result.get("results", {}).get("rows", [])
                for row in rows:
                    if not row or len(row) < 5:
                        continue
                    
                    table_name = row[0]
                    column_name = row[1]
                    data_type = row[2]
                    nullable = row[3]
                    
                    if table_name not in tables_dict:
                        tables_dict[table_name] = []
                    
                    if column_name:  # Skip if no columns (shouldn't happen)
                        tables_dict[table_name].append({
                            "name": column_name,
                            "type": data_type,
                            "nullable": nullable == "Y"
                        })
            
            # Get views with column details
            views_query = """
                SELECT 
                    v.view_name,
                    c.column_name,
                    c.data_type,
                    c.nullable,
                    c.column_id
                FROM user_views v
                LEFT JOIN user_tab_columns c ON v.view_name = c.table_name
                ORDER BY v.view_name, c.column_id
            """
            
            views_result = await self.execute_sql(views_query, conn_name)
            
            views_dict = {}
            if views_result.get("status") == "success":
                rows = views_result.get("results", {}).get("rows", [])
                for row in rows:
                    if not row or len(row) < 5:
                        continue
                    
                    view_name = row[0]
                    column_name = row[1]
                    data_type = row[2]
                    nullable = row[3]
                    
                    if view_name not in views_dict:
                        views_dict[view_name] = []
                    
                    if column_name:
                        views_dict[view_name].append({
                            "name": column_name,
                            "type": data_type,
                            "nullable": nullable == "Y"
                        })
            
            logger.info(f"Retrieved schema: {len(tables_dict)} tables, {len(views_dict)} views")
            
            return {
                "status": "success",
                "tables": tables_dict,
                "views": views_dict,
                "procedures": [],  # Can be extended to fetch procedures
                "connection": conn_name
            }
            
        except Exception as e:
            logger.error(f"Failed to get schema: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on MCP client and SQLcl process
        
        Returns:
            Dictionary with health status
        """
        try:
            # Check if process is running
            if not self.process or self.process.poll() is not None:
                return {
                    "status": "unhealthy",
                    "message": "SQLcl process is not running",
                    "timestamp": time.time()
                }
            
            # Check if we can list tools (basic communication test)
            response = await self._send_request(
                MCPRequest(method="tools/list", params={}),
                timeout=5
            )
            
            if response and not response.is_error():
                return {
                    "status": "healthy",
                    "message": "SQLcl MCP server responding",
                    "timestamp": time.time(),
                    "connection": self._current_connection
                }
            else:
                return {
                    "status": "unhealthy",
                    "message": "SQLcl MCP server not responding",
                    "timestamp": time.time()
                }
                
        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time()
            }
    
    async def close(self):
        """Close SQLcl MCP server subprocess and cleanup"""
        try:
            # Stop the reader thread
            self._running = False
            
            # Wait for reader thread to finish
            if self.reader_thread and self.reader_thread.is_alive():
                self.reader_thread.join(timeout=2)
            
            # Terminate subprocess
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
                
                self.process = None
            
            self._connected = False
            self._current_connection = None
            logger.info(f"SQLcl MCP client closed")
            
        except Exception as e:
            logger.error(f"Error closing SQLcl MCP client: {e}", exc_info=True)
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Backward compatibility aliases
UnifiedMCPClient = SQLclMCPClient


def create_mcp_client(
    sqlcl_path: Optional[str] = None,
    sqlcl_args: Optional[List[str]] = None,
    **kwargs
) -> SQLclMCPClient:
    """
    Factory function to create MCP client instances
    
    Args:
        sqlcl_path: Path to SQLcl executable (uses config default if not provided)
        sqlcl_args: SQLcl arguments (uses config default if not provided)
        **kwargs: Additional client configuration
        
    Returns:
        SQLclMCPClient instance
    """
    from app.core.config import settings
    
    # Use configuration defaults if not provided
    if sqlcl_path is None:
        # Use configured SQLcl path
        sqlcl_path = settings.sqlcl_path
    
    if sqlcl_args is None:
        sqlcl_args = settings.sqlcl_args if hasattr(settings, 'sqlcl_args') else ["-mcp"]
    
    return SQLclMCPClient(
        sqlcl_path=sqlcl_path,
        sqlcl_args=sqlcl_args,
        **kwargs
    )


# Global MCP client instance (initialized later in application.py)
mcp_client: Optional[SQLclMCPClient] = None