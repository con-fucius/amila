# BI Agent MVP - Service Startup Script
# This script starts all services in the correct order

param(
    [switch]$SkipDataInit,
    [switch]$ResetDoris
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "BI Agent MVP - Service Startup" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Step 1: Start Docker services
Write-Host "[1/5] Starting Docker services..." -ForegroundColor Yellow

Set-Location $ProjectRoot

if ($ResetDoris) {
    Write-Host "  Resetting Doris container..." -ForegroundColor Gray
    docker stop bi-agent-doris 2>$null
    docker rm bi-agent-doris 2>$null
}

Write-Host "  Starting Redis, FalkorDB, and Doris..." -ForegroundColor Gray
docker-compose up -d redis falkordb doris

# Step 2: Wait for Doris to be healthy
Write-Host "`n[2/5] Waiting for Doris to be healthy..." -ForegroundColor Yellow
$maxAttempts = 40
$attempt = 0

do {
    $attempt++
    $status = docker inspect --format='{{.State.Health.Status}}' bi-agent-doris 2>$null
    
    if ($status -eq "healthy") {
        Write-Host "  Doris is healthy!" -ForegroundColor Green
        break
    }
    
    Write-Host "  Attempt $attempt/$maxAttempts - Status: $status" -ForegroundColor Gray
    Start-Sleep -Seconds 5
} while ($attempt -lt $maxAttempts)

if ($status -ne "healthy") {
    Write-Host "  ERROR: Doris did not become healthy. Check logs:" -ForegroundColor Red
    Write-Host "    docker logs bi-agent-doris --tail 50" -ForegroundColor Gray
    exit 1
}

# Step 3: Initialize Doris data
if (-not $SkipDataInit) {
    Write-Host "`n[3/5] Initializing Doris database with sample data..." -ForegroundColor Yellow
    
    # Check if faker is installed
    $fakerCheck = & "$ProjectRoot\backend\services\doris-mcp-server\.venv\Scripts\python.exe" -c "import faker" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Installing faker..." -ForegroundColor Gray
        & "$ProjectRoot\backend\services\doris-mcp-server\.venv\Scripts\pip.exe" install faker
    }
    
    # Run init script
    & "$ProjectRoot\backend\services\doris-mcp-server\.venv\Scripts\python.exe" "$ProjectRoot\scripts\init_doris_data.py"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Data initialization failed" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "`n[3/5] Skipping data initialization (--SkipDataInit)" -ForegroundColor Gray
}

# Step 4: Instructions for backend
Write-Host "`n[4/5] Backend Server" -ForegroundColor Yellow
Write-Host "  Open a new terminal and run:" -ForegroundColor Gray
Write-Host "    cd $ProjectRoot\backend" -ForegroundColor White
Write-Host "    uv run python main.py" -ForegroundColor White

# Step 5: Instructions for frontend
Write-Host "`n[5/5] Frontend Server" -ForegroundColor Yellow
Write-Host "  Open another terminal and run:" -ForegroundColor Gray
Write-Host "    cd $ProjectRoot\frontend" -ForegroundColor White
Write-Host "    pnpm dev" -ForegroundColor White

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Services Ready!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`nDocker services running:"
docker ps --format "table {{.Names}}\t{{.Status}}" --filter "name=bi-agent"

Write-Host "`nNext steps:"
Write-Host "  1. Start backend: cd backend && uv run python main.py"
Write-Host "  2. Start frontend: cd frontend && pnpm dev"
Write-Host "  3. Open http://localhost:3000"
Write-Host "  4. Select 'Doris' database and try a query"
Write-Host "`nSample queries to test:"
Write-Host '  - "Show me the first 5 rows of CUSTOMER_DATA"'
Write-Host '  - "What is the total revenue by segment?"'
Write-Host '  - "List all unique products"'
Write-Host ""
