import asyncio
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)


class DorisServerManager:
    """
    Manages the lifecycle of the Apache Doris MCP Server subprocess.
    
    Similar to SQLcl MCP server approach:
    - Assumes server is pre-installed at a known path
    - Just starts the subprocess and waits for it to be ready
    - No git clone or dependency installation during startup
    
    Setup (run once manually):
        cd backend/services/doris-mcp-server
        uv venv
        uv pip install -r requirements.txt
    """

    def __init__(self):
        # Get the backend directory (parent of app/core)
        backend_dir = Path(__file__).parent.parent.parent.resolve()
        
        if settings.DORIS_MCP_SERVER_PATH:
            self.server_dir = Path(settings.DORIS_MCP_SERVER_PATH).resolve()
        else:
            # Default path relative to backend directory
            self.server_dir = (backend_dir / "services" / "doris-mcp-server").resolve()
            
        self.process: Optional[subprocess.Popen] = None
        self.host = settings.DORIS_MCP_HOST
        self.port = settings.DORIS_MCP_PORT
        self._ready = False
        self._log_file_handle = None  # Keep log file open for subprocess

    async def initialize(self) -> bool:
        """
        Initialize and start the Doris MCP server.
        Assumes the server is already installed with dependencies.
        """
        if not settings.DORIS_MCP_ENABLED:
            logger.info("Doris MCP integration is disabled.")
            return False

        try:
            # 1. Verify server exists and is set up
            if not self._verify_installation():
                return False

            # 2. Configure environment
            self._configure_server()

            # 3. Start subprocess
            await self._start_process()

            # 4. Wait for health check
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
        """Verify the server is installed and ready to run."""
        if not self.server_dir.exists():
            logger.error(
                f"Doris MCP Server not found at {self.server_dir}. "
                "Please clone the repository first:\n"
                f"  git clone https://github.com/apache/doris-mcp-server {self.server_dir}"
            )
            return False
        
        # Check for venv with python
        venv_python = self.server_dir / ".venv" / "Scripts" / "python.exe"
        if not venv_python.exists():
            logger.error(
                f"Doris MCP Server venv not found. Please set up the environment:\n"
                f"  cd {self.server_dir}\n"
                f"  uv venv\n"
                f"  uv pip install -r requirements.txt"
            )
            return False
        
        logger.info(f"Doris MCP Server found at {self.server_dir}")
        return True

    def _configure_server(self):
        """Write .env file for the server."""
        env_path = self.server_dir / ".env"
        
        # Based on start_server.sh and official docs, the server expects:
        # - MCP_HOST and SERVER_PORT for the MCP server itself
        # - DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASSWORD for the database connection
        config = [
            f"MCP_HOST={self.host}",
            f"SERVER_PORT={self.port}",
            f"DORIS_HOST={settings.DORIS_DB_HOST}",
            f"DORIS_PORT={settings.DORIS_DB_PORT}",
            f"DORIS_USER={settings.DORIS_DB_USER}",
            f"DORIS_PASSWORD={settings.DORIS_DB_PASSWORD}",
            f"DORIS_DATABASE={settings.DORIS_DB_DATABASE}",
            f"MCP_TRANSPORT_TYPE=http",
            f"WORKERS=1",
        ]
        
        with open(env_path, "w") as f:
            f.write("\n".join(config))
            
        logger.info(f"Configured Doris MCP Server at {env_path}")

    async def _start_process(self):
        """Start the server process."""
        logger.info("Starting Doris MCP Server process...")
        
        # Determine python executable
        venv_python = self.server_dir / ".venv" / "Scripts" / "python.exe"
        python_cmd = str(venv_python)
        
        if not venv_python.exists():
            # This shouldn't happen if _verify_installation passed
            logger.error(f"Doris server venv Python not found at {venv_python}")
            raise RuntimeError("Doris server venv Python not found. Run setup first.")
        
        # Command based on start_server.sh: python -m doris_mcp_server.main --transport http
        cmd = [
            python_cmd, "-m", "doris_mcp_server.main",
            "--transport", "http",
            "--host", self.host,
            "--port", str(self.port),
            "--workers", "1"
        ]
        
        logger.info(f"Starting Doris MCP with command: {' '.join(cmd)}")
        
        # Create log file for server output
        log_file = self.server_dir / "logs" / "doris-mcp-server.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Keep file handle open for the lifetime of the subprocess
        self._log_file_handle = open(log_file, "w")
        
        self.process = subprocess.Popen(
            cmd,
            cwd=str(self.server_dir),
            stdout=self._log_file_handle,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "MCP_TRANSPORT_TYPE": "http"}
        )
        
        logger.info(f"Doris MCP Server logs: {log_file}")

    async def _wait_for_ready(self, timeout=30) -> bool:
        """Poll the health endpoint."""
        start_time = time.time()
        url = f"http://{self.host}:{self.port}/health" # Assuming standard health endpoint
        # If no health endpoint, we might check /sse connection
        mcp_url = f"http://{self.host}:{self.port}/mcp"
        
        async with httpx.AsyncClient() as client:
            while time.time() - start_time < timeout:
                # Check if process exited early
                if self.process.poll() is not None:
                    logger.error(f"Doris MCP Server exited early with code {self.process.returncode}")
                    return False
                
                try:
                    # First, try the explicit health endpoint
                    try:
                        resp = await client.get(url, timeout=1.0)
                        if resp.status_code == 200:
                            return True
                    except Exception:
                        pass

                    # Fallback: check MCP endpoint responsiveness
                    try:
                        resp = await client.get(mcp_url, timeout=1.0)
                        if resp.status_code < 500:
                            return True
                    except Exception:
                        pass
                        
                except Exception:
                    pass
                
                await asyncio.sleep(1)
                
        return False

    def stop(self):
        """Stop the subprocess."""
        if self.process and self.process.poll() is None:
            logger.info("Stopping Doris MCP Server...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
        
        # Close log file handle
        if self._log_file_handle:
            try:
                self._log_file_handle.close()
            except Exception:
                pass
            self._log_file_handle = None
        
        self._ready = False

# Global instance
doris_server = DorisServerManager()
