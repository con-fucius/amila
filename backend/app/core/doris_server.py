"""
Apache Doris MCP Server Manager

Manages the lifecycle of the Apache Doris MCP Server.
The server is installed via pip (doris-mcp-server package) and started as a subprocess.
"""

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)


class DorisServerManager:
    """
    Manages the lifecycle of the Apache Doris MCP Server subprocess.
    
    The Doris MCP Server is installed via pip (doris-mcp-server package).
    This manager starts it as a subprocess and monitors its health.
    """

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.host = settings.DORIS_MCP_HOST
        self.port = settings.DORIS_MCP_PORT
        self._ready = False
        self._log_file_handle = None

    async def initialize(self) -> bool:
        """
        Initialize and start the Doris MCP server.
        """
        if not settings.DORIS_MCP_ENABLED:
            logger.info("Doris MCP integration is disabled.")
            return False

        try:
            # 1. Verify doris-mcp-server is installed
            if not self._verify_installation():
                return False

            # 2. Start subprocess
            await self._start_process()

            # 3. Wait for health check
            if await self._wait_for_ready():
                logger.info(f"Doris MCP Server running at http://{self.host}:{self.port}")
                self._ready = True
                return True
            else:
                logger.error("Doris MCP Server failed to become ready.")
                self.stop()
                return False

        except Exception as e:
            logger.error(f"Failed to initialize Doris MCP Server: {e}", exc_info=True)
            self.stop()
            return False

    def _verify_installation(self) -> bool:
        """Verify the doris-mcp-server package is installed."""
        # Check if doris-mcp-server command is available
        doris_cmd = shutil.which("doris-mcp-server")
        if doris_cmd:
            logger.info(f"Doris MCP Server found at: {doris_cmd}")
            self._doris_cmd = doris_cmd
            return True
        
        # Try importing the module directly
        try:
            import doris_mcp_server
            logger.info(f"Doris MCP Server module found: {doris_mcp_server.__file__}")
            self._doris_cmd = None  # Will use python -m
            return True
        except ImportError:
            pass
        
        logger.error(
            "Doris MCP Server not installed. Install with:\n"
            "  pip install doris-mcp-server\n"
            "Or in Docker, ensure the Dockerfile includes this installation."
        )
        return False

    async def _start_process(self):
        """Start the server process."""
        logger.info("Starting Doris MCP Server process...")
        
        # Build environment variables for Doris connection
        env = os.environ.copy()
        env.update({
            "PYTHONUNBUFFERED": "1",
            "MCP_HOST": self.host,
            "SERVER_PORT": str(self.port),
            "DORIS_HOST": settings.DORIS_DB_HOST,
            "DORIS_PORT": str(settings.DORIS_DB_PORT),
            "DORIS_USER": settings.DORIS_DB_USER,
            "DORIS_PASSWORD": settings.DORIS_DB_PASSWORD or "",
            "DORIS_DATABASE": settings.DORIS_DB_DATABASE,
            "MCP_TRANSPORT_TYPE": "http",
        })
        
        # Build command
        if self._doris_cmd:
            # Use the installed command
            cmd = [
                self._doris_cmd,
                "--transport", "http",
                "--host", self.host,
                "--port", str(self.port),
            ]
        else:
            # Use python -m
            cmd = [
                sys.executable, "-m", "doris_mcp_server.main",
                "--transport", "http",
                "--host", self.host,
                "--port", str(self.port),
            ]
        
        logger.info(f"Starting Doris MCP with command: {' '.join(cmd)}")
        
        # Create log directory
        log_dir = Path("/app/logs") if Path("/app/logs").exists() else Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "doris-mcp-server.log"
        
        # Keep file handle open for the lifetime of the subprocess
        self._log_file_handle = open(log_file, "w")
        
        self.process = subprocess.Popen(
            cmd,
            stdout=self._log_file_handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env
        )
        
        logger.info(f"Doris MCP Server logs: {log_file}")

    async def _wait_for_ready(self, timeout: int = 60) -> bool:
        """Poll the health endpoint until ready or timeout."""
        start_time = time.time()
        health_url = f"http://{self.host}:{self.port}/health"
        mcp_url = f"http://{self.host}:{self.port}/mcp"
        
        async with httpx.AsyncClient() as client:
            while time.time() - start_time < timeout:
                # Check if process exited early
                if self.process and self.process.poll() is not None:
                    logger.error(f"Doris MCP Server exited early with code {self.process.returncode}")
                    return False
                
                try:
                    # Try health endpoint first
                    try:
                        resp = await client.get(health_url, timeout=2.0)
                        if resp.status_code == 200:
                            logger.info("Doris MCP Server health check passed")
                            return True
                    except Exception:
                        pass

                    # Fallback: check MCP endpoint
                    try:
                        resp = await client.get(mcp_url, timeout=2.0)
                        if resp.status_code < 500:
                            logger.info("Doris MCP Server MCP endpoint responding")
                            return True
                    except Exception:
                        pass
                        
                except Exception as e:
                    logger.debug(f"Health check attempt failed: {e}")
                
                await asyncio.sleep(2)
        
        logger.error(f"Doris MCP Server did not become ready within {timeout}s")
        return False

    def stop(self):
        """Stop the subprocess."""
        if self.process and self.process.poll() is None:
            logger.info("Stopping Doris MCP Server...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Doris MCP Server did not terminate gracefully, killing...")
                self.process.kill()
        
        # Close log file handle
        if self._log_file_handle:
            try:
                self._log_file_handle.close()
            except Exception:
                pass
            self._log_file_handle = None
        
        self._ready = False
        logger.info("Doris MCP Server stopped")

    @property
    def is_ready(self) -> bool:
        """Check if the server is ready."""
        return self._ready and self.process and self.process.poll() is None


# Global instance
doris_server = DorisServerManager()
