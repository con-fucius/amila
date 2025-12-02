"""
Test script for orchestrator endpoint integration
Tests Phase 2.2B: MCP client integration and /process endpoint
"""

import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path for imports
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

# Load .env file from backend directory
env_path = backend_dir / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f" Loaded environment from: {env_path}")
else:
    print(f"  Warning: .env file not found at {env_path}")


async def test_mcp_client_availability():
    """Test 1: Verify MCP client is available in registry"""
    print("\n" + "="*60)
    print("TEST 1: MCP Client Availability")
    print("="*60)
    
    try:
        from app.core.client_registry import registry
        
        mcp_client = registry.get_mcp_client()
        if mcp_client:
            print(" MCP client is available in registry")
            print(f"   Client type: {type(mcp_client).__name__}")
            return True
        else:
            print(" MCP client is NOT available in registry")
            print("   This is expected if application hasn't been started yet")
            return False
    except Exception as e:
        print(f" Error checking MCP client: {e}")
        return False


async def test_graphiti_client_availability():
    """Test 2: Verify Graphiti client is available"""
    print("\n" + "="*60)
    print("TEST 2: Graphiti Client Availability")
    print("="*60)
    
    try:
        from app.core.client_registry import registry
        
        graphiti_client = registry.get_graphiti_client()
        if graphiti_client:
            print(" Graphiti client is available in registry")
            print(f"   Client type: {type(graphiti_client).__name__}")
            return True
        else:
            print("  Graphiti client is NOT available")
            print("   This is expected if application hasn't been started yet")
            return False
    except Exception as e:
        print(f" Error checking Graphiti client: {e}")
        return False


async def test_orchestrator_import():
    """Test 3: Verify orchestrator can be imported"""
    print("\n" + "="*60)
    print("TEST 3: Orchestrator Import")
    print("="*60)
    
    try:
        from app.agents.query_orchestrator import process_query, query_orchestrator
        
        print(" Orchestrator imported successfully")
        print(f"   process_query function: {type(process_query).__name__}")
        print(f"   query_orchestrator graph: {type(query_orchestrator).__name__}")
        return True
    except Exception as e:
        print(f" Error importing orchestrator: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_endpoint_models():
    """Test 4: Verify API models are defined"""
    print("\n" + "="*60)
    print("TEST 4: API Endpoint Models")
    print("="*60)
    
    try:
        from app.api.v1.endpoints.queries import (
            OrchestratorQueryRequest,
            OrchestratorQueryResponse,
            process_query_with_orchestrator
        )
        
        print(" API models imported successfully")
        print(f"   OrchestratorQueryRequest: {OrchestratorQueryRequest}")
        print(f"   OrchestratorQueryResponse: {OrchestratorQueryResponse}")
        print(f"   process_query_with_orchestrator endpoint: {type(process_query_with_orchestrator).__name__}")
        
        # Test request model validation
        test_request = OrchestratorQueryRequest(
            query="SELECT * FROM employees",
            user_id="test_user",
            session_id="test_session"
        )
        print(f"\n Request model validation works")
        print(f"   Sample request: {test_request.model_dump()}")
        
        return True
    except Exception as e:
        print(f" Error testing API models: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_execute_node_mcp_integration():
    """Test 5: Verify execute_query_node has MCP integration"""
    print("\n" + "="*60)
    print("TEST 5: Execute Node MCP Integration")
    print("="*60)
    
    try:
        import inspect
        from app.agents.query_orchestrator import execute_query_node
        
        # Get source code of the function
        source = inspect.getsource(execute_query_node)
        
        # Check for MCP client imports
        has_registry_import = "from app.core.client_registry import registry" in source
        has_mcp_client_call = "registry.get_mcp_client()" in source
        has_execute_sql_call = "mcp_client.execute_sql" in source
        
        print(f" execute_query_node function found")
        print(f"   Has registry import: {'' if has_registry_import else ''}")
        print(f"   Has MCP client retrieval: {'' if has_mcp_client_call else ''}")
        print(f"   Has execute_sql call: {'' if has_execute_sql_call else ''}")
        
        if has_registry_import and has_mcp_client_call and has_execute_sql_call:
            print("\n MCP integration is properly implemented")
            return True
        else:
            print("\n MCP integration is incomplete")
            return False
            
    except Exception as e:
        print(f" Error checking execute node: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_configuration():
    """Test 6: Verify configuration is loaded"""
    print("\n" + "="*60)
    print("TEST 6: Configuration")
    print("="*60)
    
    try:
        from app.core.config_manager import settings
        
        print(" Configuration loaded successfully")
        print(f"   Environment: {settings.environment}")
        print(f"   LLM Provider (Graphiti): {settings.GRAPHITI_LLM_PROVIDER}")
        print(f"   LLM Model: {settings.GRAPHITI_LLM_MODEL}")
        print(f"   Embedding Provider: {settings.GRAPHITI_EMBEDDING_PROVIDER}")
        print(f"   FalkorDB Host: {settings.FALKORDB_HOST}:{settings.FALKORDB_PORT}")
        print(f"   Redis Host: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        
        # Check if Google API key is set for Gemini
        if settings.GRAPHITI_LLM_PROVIDER == "gemini":
            if settings.GOOGLE_API_KEY:
                print(f"   Google API Key: Set (length: {len(settings.GOOGLE_API_KEY)})")
            else:
                print("     Google API Key: Not set")
        
        return True
    except Exception as e:
        print(f" Error loading configuration: {e}")
        import traceback
        traceback.print_exc()
        return False


async def run_all_tests():
    """Run all integration tests"""
    print("\n" + "="*70)
    print(" PHASE 2.2B INTEGRATION TESTS")
    print("   Testing: MCP Integration + Orchestrator Endpoint")
    print("="*70)
    
    results = []
    
    # Run tests in order
    results.append(("Configuration", await test_configuration()))
    results.append(("Orchestrator Import", await test_orchestrator_import()))
    results.append(("API Models", await test_endpoint_models()))
    results.append(("Execute Node MCP Integration", await test_execute_node_mcp_integration()))
    results.append(("MCP Client Availability", await test_mcp_client_availability()))
    results.append(("Graphiti Client Availability", await test_graphiti_client_availability()))
    
    # Summary
    print("\n" + "="*70)
    print(" TEST SUMMARY")
    print("="*70)
    
    passed = 0
    failed = 0
    
    for test_name, result in results:
        status = " PASS" if result else " FAIL"
        print(f"{status} - {test_name}")
        if result:
            passed += 1
        else:
            failed += 1
    
    print("\n" + "="*70)
    print(f"Total: {passed} passed, {failed} failed out of {len(results)} tests")
    print("="*70)
    
    if failed > 0:
        print("\n  Some tests failed. This is expected if the application is not running.")
        print("   Start the backend server to enable full testing:")
        print("   cd d:\\Projects\\Amil\\bi-agent-mvp\\backend")
        print("   python main.py")
    else:
        print("\n All tests passed!")
    
    return failed == 0


if __name__ == "__main__":
    try:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)