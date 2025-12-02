"""
Test the Orchestrator API Endpoint
Tests the full LangGraph workflow with Graphiti integration via REST API
"""

import asyncio
import httpx
from datetime import datetime, timezone

BASE_URL = "http://localhost:8000/api/v1"

async def test_orchestrator_endpoint():
    """Test the /queries/process endpoint with orchestrator integration"""
    
    print("=" * 80)
    print(" TESTING ORCHESTRATOR API ENDPOINT")
    print("=" * 80)
    print()
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        
        # Test 1: Check health endpoint
        print(" Test 1: Server Health Check")
        print("-" * 80)
        try:
            response = await client.get(f"{BASE_URL}/health")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
            print(" Server is running")
        except Exception as e:
            print(f" Server not reachable: {e}")
            return
        print()
        
        # Test 2: Cold Start - Natural Language Query (No Prior Context)
        print(" Test 2: Cold Start Natural Language Query")
        print("-" * 80)
        
        nl_query = "Show me all employees who earn more than 50000"
        
        payload = {
            "query": nl_query,
            "user_id": "test_user_123",
            "session_id": "test_session_001"
        }
        
        print(f"Query: {nl_query}")
        print(f"Endpoint: POST {BASE_URL}/queries/process")
        print()
        
        try:
            start_time = datetime.now(timezone.utc)
            response = await client.post(f"{BASE_URL}/queries/process", json=payload)
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            print(f"Status: {response.status_code}")
            print(f"Elapsed: {elapsed:.2f}s")
            print()
            
            if response.status_code == 200:
                result = response.json()
                
                print(" RESPONSE:")
                print(f"  Query ID: {result.get('query_id')}")
                print(f"  Status: {result.get('status')}")
                print(f"  SQL Query: {result.get('sql_query')}")
                print()
                
                if result.get('validation'):
                    print(f"  Validation: {result['validation']}")
                    print()
                
                if result.get('result'):
                    res = result['result']
                    print(f"  Result:")
                    print(f"    Row Count: {res.get('row_count')}")
                    print(f"    Columns: {res.get('columns')}")
                    print(f"    Execution Time: {res.get('execution_time_ms')}ms")
                    print()
                
                if result.get('visualization'):
                    print(f"  Visualization: {result['visualization']}")
                    print()
                
                if result.get('error'):
                    print(f"    Error: {result['error']}")
                    print()
                
                print(" Cold start test completed")
            else:
                print(f" Request failed: {response.text}")
                
        except Exception as e:
            print(f" Test failed: {e}")
        
        print()
        print("=" * 80)
        
        # Test 3: Warm Start - Similar Query (Should Retrieve Context from Graphiti)
        print(" Test 3: Warm Start - Similar Query (With Context)")
        print("-" * 80)
        
        nl_query_2 = "What employees have salary greater than 60000?"
        
        payload_2 = {
            "query": nl_query_2,
            "user_id": "test_user_123",
            "session_id": "test_session_002"  # Different session to test context retrieval
        }
        
        print(f"Query: {nl_query_2}")
        print(f"Endpoint: POST {BASE_URL}/queries/process")
        print("Expected: Should retrieve context from previous query")
        print()
        
        try:
            start_time = datetime.now(timezone.utc)
            response = await client.post(f"{BASE_URL}/queries/process", json=payload_2)
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            print(f"Status: {response.status_code}")
            print(f"Elapsed: {elapsed:.2f}s")
            print()
            
            if response.status_code == 200:
                result = response.json()
                
                print(" RESPONSE:")
                print(f"  Query ID: {result.get('query_id')}")
                print(f"  Status: {result.get('status')}")
                print(f"  SQL Query: {result.get('sql_query')}")
                print()
                
                if result.get('error'):
                    print(f"    Error: {result['error']}")
                else:
                    print(" Warm start test completed (context may have been used)")
                    
            else:
                print(f" Request failed: {response.text}")
                
        except Exception as e:
            print(f" Test failed: {e}")
        
        print()
        print("=" * 80)
        
        # Test 4: Compare with Direct SQL Endpoint
        print(" Test 4: Compare with Direct SQL Endpoint (/queries/submit)")
        print("-" * 80)
        
        sql_query = "SELECT * FROM employees WHERE salary > 50000"
        
        payload_3 = {
            "query": sql_query,
            "connection_name": "TestUserCSV"
        }
        
        print(f"SQL: {sql_query}")
        print(f"Endpoint: POST {BASE_URL}/queries/submit")
        print()
        
        try:
            start_time = datetime.now(timezone.utc)
            response = await client.post(f"{BASE_URL}/queries/submit", json=payload_3)
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            print(f"Status: {response.status_code}")
            print(f"Elapsed: {elapsed:.2f}s")
            print()
            
            if response.status_code == 200:
                result = response.json()
                print(f"  Query ID: {result.get('query_id')}")
                print(f"  Status: {result.get('status')}")
                print(f"  Message: {result.get('message')}")
                print()
                print(" Direct SQL endpoint test completed")
            else:
                print(f" Request failed: {response.text}")
                
        except Exception as e:
            print(f" Test failed: {e}")
        
        print()
        print("=" * 80)
        print()
        print(" KEY OBSERVATIONS:")
        print("   - /queries/process: Uses LangGraph orchestrator with LLM SQL generation")
        print("   - /queries/submit: Direct SQL execution (bypasses orchestrator)")
        print("   - Check backend logs for Graphiti context retrieval evidence")
        print("   - Check FalkorDB for stored episodes after successful queries")
        print()
        print(" TEST SUITE COMPLETED")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_orchestrator_endpoint())