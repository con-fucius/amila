@echo off
REM Docker Compose startup script for Amila BI Agent
REM This script starts the core services needed for UAT

echo ========================================
echo Starting Amila BI Agent Docker Services
echo ========================================

REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

REM Build and start core services (without observability profile)
echo.
echo Building and starting services...
docker compose up --build -d backend frontend redis falkordb doris doris-mcp-server

echo.
echo ========================================
echo Services starting up...
echo ========================================
echo.
echo Frontend:    http://localhost:3000
echo Backend:     http://localhost:8000
echo Doris MCP:   http://localhost:8808
echo Redis:       localhost:6379
echo FalkorDB:    localhost:6380
echo Doris:       localhost:9030
echo.
echo Use 'docker compose logs -f' to view logs
echo Use 'docker compose down' to stop all services
echo.
