#!/bin/bash
set -e

echo "=== BI Agent Backend Startup ==="

# The virtual environment is already in PATH from Dockerfile
# No need to activate - just verify it's working
echo "Python location: $(which python)"
echo "Python version: $(python --version)"

# Verify SQLcl is available
if [ -x "/opt/sqlcl/bin/sql" ]; then
    echo "SQLcl MCP Server available"
else
    echo "SQLcl not found (Oracle queries disabled)"
fi

# Verify Doris MCP Server is available  
if python -c "import doris_mcp_server" 2>/dev/null; then
    echo "Doris MCP Server package installed"
else
    echo "Doris MCP Server not installed"
fi

# Verify uvicorn is available
if python -c "import uvicorn" 2>/dev/null; then
    echo "Uvicorn installed"
else
    echo "Uvicorn NOT found - this is a critical error"
    echo "Installed packages:"
    pip list 2>/dev/null || echo "pip not available"
    exit 1
fi

echo "=== Starting Uvicorn Server ==="

# Run the application using the venv's python
exec python -m uvicorn main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --limit-concurrency 50 \
    --timeout-keep-alive 5 \
    --limit-max-requests 1000
