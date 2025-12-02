"""
Quick test to verify Graphiti client initialization
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.core.graphiti_client import get_graphiti_client


async def test_graphiti_direct():
    """Test Graphiti client initialization directly"""
    print("Testing Graphiti client initialization...")
    
    try:
        print("1 Calling get_graphiti_client()...")
        graphiti = await get_graphiti_client()
        
        print(f"2 Client type: {type(graphiti)}")
        print(f"3 Client object: {graphiti}")
        print(f"4 Has _graphiti attr: {hasattr(graphiti, '_graphiti')}")
        
        # Try a simple search
        print("5 Attempting search...")
        results = await graphiti.search("test", num_results=1)
        print(f"    Search successful, got {len(results)} results")
        
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_graphiti_direct())