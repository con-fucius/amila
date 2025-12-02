<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Doris MCP Server Testing System

## Overview

This testing system adopts a layered architecture, including unit tests, integration tests, and client-server tests. The testing system assumes the server is already properly started and focuses on testing functionality rather than startup configuration.

## Testing Architecture

### 1. Unit Tests
- **Location**: `test/security/`, `test/utils/`, `test/tools/`
- **Purpose**: Test individual module functionality
- **Features**: Uses Mock objects, no dependency on external services

### 2. Integration Tests
- **Location**: `test/integration/`
- **Purpose**: Test collaboration between modules
- **Features**: Test complete workflows

### 3. Client-Server Tests
- **Location**: `test/tools/test_tools_client_server.py`, `test/utils/test_query_executor_client_server.py`
- **Purpose**: Test actual server functionality through MCP client
- **Features**: Assumes server is running, skips tests if server is not available

## Configuration Files

### test_config.json
Test configuration file defines how to connect to the running server:

```json
{
  "server_endpoints": {
    "http": {
      "url": "http://localhost:3000/mcp",
      "timeout": 30
    },
    "stdio": {
      "command": "uv",
      "args": ["run", "python", "-m", "doris_mcp_server.main", "--transport", "stdio"],
      "timeout": 30
    }
  },
  "test_settings": {
    "default_transport": "http",
    "retry_attempts": 3,
    "retry_delay": 1.0,
    "test_timeout": 60,
    "enable_performance_tests": true,
    "enable_security_tests": true
  }
}
```

## Usage

### 1. Start the Server

Before running client-server tests, you need to start the server first:

#### HTTP Mode (Recommended)
```bash
# Start HTTP server
./start_server.sh
# or
uv run python -m doris_mcp_server.main --transport http --port 3000
```

#### Stdio Mode
```bash
# Stdio mode is started directly by the client, no need to pre-start
```

### 2. Run Tests

#### Run All Tests
```bash
python -m pytest test/ -v
```

#### Run Unit Tests
```bash
# Security module tests
python -m pytest test/security/ -v

# Tools module tests
python -m pytest test/tools/test_tools_manager.py -v

# Query executor tests
python -m pytest test/utils/test_query_executor.py -v
```

#### Run Integration Tests
```bash
python -m pytest test/integration/ -v
```

#### Run Client-Server Tests
```bash
# Tools Client-Server tests
python -m pytest test/tools/test_tools_client_server.py -v

# QueryExecutor Client-Server tests
python -m pytest test/utils/test_query_executor_client_server.py -v
```

### 3. Test Configuration

#### Modify Server Endpoints
Edit the `test/test_config.json` file:

```json
{
  "server_endpoints": {
    "http": {
      "url": "http://your-server:port/mcp"
    }
  }
}
```

#### Enable/Disable Specific Tests
```json
{
  "test_settings": {
    "enable_performance_tests": false,  // Disable performance tests
    "enable_security_tests": true       // Enable security tests
  }
}
```

## Test Status

###  Completed Test Modules

1. **Security Module** (100% Pass)
   - Authentication tests: 5/5 passed
   - Authorization tests: 7/7 passed
   - Data masking tests: 13/13 passed
   - SQL validation tests: 10/10 passed
   - Security manager tests: 7/7 passed
   - Coverage: 88%

2. **Client-Server Test Architecture** (Implemented)
   - Automatic server connection status detection
   - Automatically skip tests when server is not running
   - Support for both HTTP and Stdio transport modes

###  Tests Requiring Server Running

1. **Tools Client-Server Tests**
   - Tool list retrieval
   - SQL query execution
   - Database list retrieval
   - Table schema queries
   - Performance statistics
   - Error handling
   - Security authentication

2. **QueryExecutor Client-Server Tests**
   - Simple query execution
   - Database queries
   - Information schema queries
   - Parameterized queries
   - Error handling
   - Security authentication

## Testing Best Practices

### 1. Server Startup Check
All client-server tests automatically check server connection status:
- If server is running normally, execute actual tests
- If server is not running, skip tests and display appropriate message

### 2. Test Isolation
- Unit tests use Mock objects, no dependency on external services
- Integration tests use controlled test environments
- Client-server tests connect to actually running servers

### 3. Error Handling
- Tests don't assume specific success/failure results
- Verify response structure rather than specific content
- Gracefully handle connection failures and timeouts

### 4. Configuration Management
- Use configuration files to manage test parameters
- Support configuration switching for different environments
- Provide reasonable default values

## Troubleshooting

### 1. Server Connection Failure
```
ERROR: Server is not running or not accessible
```
**Solution**: Ensure the server is started and listening on the correct port

### 2. Import Errors
```
ImportError: cannot import name 'DorisUnifiedClient'
```
**Solution**: Check Python path and dependency installation

### 3. Test Timeouts
```
TimeoutError: Test execution timeout
```
**Solution**: Increase timeout settings in `test_config.json`

## Development Guide

### Adding New Client-Server Tests

1. Add test methods in the appropriate test file
2. Use `@pytest.mark.asyncio` decorator
3. Get test client through `client` fixture
4. Implement test callback function
5. Verify response structure

Example:
```python
@pytest.mark.asyncio
async def test_new_feature_via_client(self, client, test_config):
    """Test new feature through client"""
    async def test_callback(client_instance):
        result = await client_instance.call_tool("new_tool", {
            "param": "value"
        })
        
        assert "success" in result
        return result
    
    result = await client.connect_and_run(test_callback)
    assert "success" in result
```

### Modifying Test Configuration

Edit the `test/test_config.json` file to adjust:
- Server endpoints
- Timeout settings
- Test data
- Feature switches

## Summary

This testing system provides complete test coverage, from unit tests to end-to-end client-server tests. Through reasonable configuration and automated connection detection, it ensures tests can run stably in different environments. 