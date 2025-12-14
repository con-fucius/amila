#!/bin/bash
set -e

echo "=== BI Agent Backend Startup ==="

# The virtual environment is already in PATH from Dockerfile
# No need to activate - just verify it's working
echo "Python location: $(which python)"
echo "Python version: $(python --version)"

# Verify SQLcl is available and create Oracle connection
if [ -x "/opt/sqlcl/bin/sql" ]; then
    echo "SQLcl MCP Server available"
    
    # Create Oracle connection if credentials are provided
    if [ -n "$ORACLE_USERNAME" ] && [ -n "$ORACLE_PASSWORD" ] && [ -n "$ORACLE_HOST" ]; then
        echo "Creating Oracle connection: ${ORACLE_DEFAULT_CONNECTION:-$ORACLE_USERNAME}"
        
        CONN_NAME="${ORACLE_DEFAULT_CONNECTION:-$ORACLE_USERNAME}"
        CONN_STRING="${ORACLE_USERNAME}/${ORACLE_PASSWORD}@${ORACLE_HOST}:${ORACLE_PORT:-1521}/${ORACLE_SERVICE_NAME:-XEPDB1}"
        
        # Create connection using SQLcl (non-interactive)
        echo "conn -save ${CONN_NAME} -savepwd ${CONN_STRING}
exit" | /opt/sqlcl/bin/sql /nolog > /tmp/sqlcl_setup.log 2>&1 || true
        
        if grep -q "Connection.*saved" /tmp/sqlcl_setup.log 2>/dev/null; then
            echo "Oracle connection '${CONN_NAME}' created successfully"
        else
            echo "Oracle connection setup output:"
            cat /tmp/sqlcl_setup.log 2>/dev/null || echo "(no output)"
            echo "Note: Connection may already exist or setup may have issues"
        fi
    else
        echo "Oracle credentials not fully configured - skipping connection setup"
    fi
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