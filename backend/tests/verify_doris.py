import asyncio
import logging
import sys
from unittest.mock import MagicMock, patch

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_doris_integration():
    """
    Dry run verification of Doris MCP components.
    """
    logger.info("Starting Doris MCP verification...")
    
    # Mock settings
    with patch("app.core.config.settings") as mock_settings:
        mock_settings.DORIS_MCP_ENABLED = True
        mock_settings.DORIS_MCP_HOST = "localhost"
        mock_settings.DORIS_MCP_PORT = 8080
        mock_settings.DORIS_MCP_REPO_URL = "https://github.com/apache/doris-mcp-server.git"
        
        # Test 1: Server Manager Initialization
        logger.info("Test 1: Server Manager Initialization")
        from app.core.doris_server import DorisServerManager
        
        server = DorisServerManager()
        
        # Mock subprocess and network calls
        with patch("asyncio.create_subprocess_exec") as mock_exec, \
             patch("subprocess.Popen") as mock_popen, \
             patch("httpx.AsyncClient") as mock_client:
            
            # Mock git clone success
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.communicate = MagicMock(return_value=asyncio.Future())
            mock_proc.communicate.return_value.set_result((b"", b""))
            mock_exec.return_value = mock_proc
            
            # Mock server process
            mock_popen_instance = MagicMock()
            mock_popen_instance.poll.return_value = None
            mock_popen.return_value = mock_popen_instance
            
            # Mock health check
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client_instance = MagicMock()
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = None
            mock_client_instance.get.return_value = mock_response
            mock_client.return_value = mock_client_instance
            
            # Run initialize
            success = await server.initialize()
            if success:
                logger.info("Server Manager initialized successfully")
            else:
                logger.error("Server Manager failed to initialize")
                
        # Test 2: Client Connection
        logger.info("Test 2: Client Connection")
        from app.core.doris_client import DorisMCPClient
        
        client = DorisMCPClient()
        
        with patch("httpx.AsyncClient") as mock_client:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client_instance = MagicMock()
            mock_client_instance.get.return_value = mock_response
            mock_client.return_value = mock_client_instance
            
            success = await client.initialize()
            if success:
                logger.info("Client initialized successfully")
            else:
                logger.error("Client failed to initialize")

if __name__ == "__main__":
    # Add backend to path
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    asyncio.run(test_doris_integration())
