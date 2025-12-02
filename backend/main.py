"""
Main FastAPI Application Entry Point
Amila backend
"""

import os
import sys
import signal
import asyncio
import logging
from pathlib import Path

# Add the app directory to Python path for imports
sys.path.append(str(Path(__file__).parent))

from app.core.application import create_application

# Create FastAPI application instance
app = create_application()

logger = logging.getLogger(__name__)

# Graceful shutdown handler
def handle_shutdown_signal(signum, frame):
    """
    Handle shutdown signals (SIGTERM, SIGINT) for graceful shutdown
    Prevents mid-query data corruption and ensures proper cleanup
    """
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logger.info(f"Received {signal_name}, initiating graceful shutdown...")
    
    # The lifespan context manager in application.py will handle cleanup
    # This just logs the signal reception
    sys.exit(0)

# Register signal handlers (Windows-compatible)
if sys.platform == "win32":
    # Windows only supports SIGTERM and SIGINT
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)
else:
    # Unix-like systems support more signals
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

if __name__ == "__main__":
    import uvicorn
    
    # Development server configuration with security enhancements
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_excludes=["services/*", "logs/*", "*.log"],  # Exclude MCP servers and logs from watch
        log_level="info",
        access_log=True,
        # Security & Performance Settings (CVE-2023-44487 mitigation)
        limit_concurrency=50,  # Limit concurrent connections
        timeout_keep_alive=5,  # Short keep-alive timeout (prevents Rapid Reset DoS)
        limit_max_requests=1000,  # Restart worker after N requests (memory leak prevention)
        backlog=2048,  # Connection backlog size
    )