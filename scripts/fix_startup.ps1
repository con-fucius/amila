
# Fix Startup Issues - Clean Cleanup Script
# Usage: .\scripts\fix_startup.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Amila Startup Fix & Cleanup Tool" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Stop all Docker containers to free ports
Write-Host "[1/3] Stopping all Docker containers..." -ForegroundColor Yellow
docker-compose --profile full down --remove-orphans
if ($LASTEXITCODE -eq 0) {
    Write-Host "      Docker environment stopped." -ForegroundColor Green
} else {
    Write-Host "      Warning: Docker stop had issues, manually checking ports next." -ForegroundColor Red
}

# 2. Force-kill any process on port 8000 (Backend)
Write-Host "`n[2/3] Checking for zombie processes on port 8000..." -ForegroundColor Yellow
$port8000 = Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue

if ($port8000) {
    $pid8000 = $port8000.OwningProcess
    Write-Host "      FOUND process on port 8000 (PID: $pid8000). Terminating..." -ForegroundColor Red
    Stop-Process -Id $pid8000 -Force -ErrorAction SilentlyContinue
    Write-Host "      Process terminated." -ForegroundColor Green
} else {
    Write-Host "      Port 8000 is clean." -ForegroundColor Green
}

# 3. Force-kill any process on port 3000 (Frontend)
Write-Host "`n[3/3] Checking for zombie processes on port 3000..." -ForegroundColor Yellow
$port3000 = Get-NetTCPConnection -LocalPort 3000 -ErrorAction SilentlyContinue

if ($port3000) {
    $pid3000 = $port3000.OwningProcess
    Write-Host "      FOUND process on port 3000 (PID: $pid3000). Terminating..." -ForegroundColor Red
    Stop-Process -Id $pid3000 -Force -ErrorAction SilentlyContinue
    Write-Host "      Process terminated." -ForegroundColor Green
} else {
    Write-Host "      Port 3000 is clean." -ForegroundColor Green
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Cleanup Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "You can now safely run:"
Write-Host "  1. Start Infrastructure: docker-compose --profile full up -d redis falkordb doris postgres grafana prometheus otel-collector"
Write-Host "  2. Start Backend:        python main.py (in backend folder)"
Write-Host "  3. Start Frontend:       npm run dev (in frontend folder)"
Write-Host ""
