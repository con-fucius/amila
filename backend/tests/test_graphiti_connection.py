"""
Test script to verify Graphiti + FalkorDB connection
"""
import asyncio
import logging
from app.core.graphiti_client import GraphitiClient
from app.core.config_manager import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_graphiti_connection():
    """Test Graphiti client initialization and basic operations"""
    
    logger.info(" Testing Graphiti Connection...")
    logger.info(f"   FalkorDB Host: {settings.FALKORDB_HOST}:{settings.FALKORDB_PORT}")
    logger.info(f"   Graphiti LLM: {settings.GRAPHITI_LLM_PROVIDER} ({settings.GRAPHITI_LLM_MODEL})")
    logger.info(f"   Graphiti Embeddings: {settings.GRAPHITI_EMBEDDING_PROVIDER} ({settings.GRAPHITI_EMBEDDING_MODEL})")
    
    try:
        # Initialize Graphiti client
        logger.info("\n Initializing Graphiti client...")
        client = GraphitiClient()
        await client.initialize()
        logger.info(" Graphiti client initialized successfully!")
        
        # Test adding a simple episode
        logger.info("\n Testing episode addition...")
        result = await client.add_episode(
            content="Test episode: System initialized successfully at startup",
            episode_type="system_event",
            source="test_script"
        )
        logger.info(f" Episode added: {result}")
        
        # Test search functionality
        logger.info("\n Testing search functionality...")
        search_results = await client.search(
            query="system initialization",
            num_results=5,
            search_type="hybrid"
        )
        logger.info(f" Search completed: Found {len(search_results)} results")
        
        # Close client
        await client.close()
        logger.info("\n All tests passed! Graphiti is ready for integration.")
        return True
        
    except Exception as e:
        logger.error(f"\n Test failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = asyncio.run(test_graphiti_connection())
    exit(0 if success else 1)