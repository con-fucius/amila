# ============================================
# BI Agent MVP - Docker Startup Script (Windows)
# ============================================

param(
    [Parameter(Position=0)]
    [ValidateSet("minimal", "full", "dev", "stop", "logs", "rebuild")]
    [string]$Mode = "minimal"
)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  BI Agent MVP - Starting Services" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Check if .env exists
if (-not (Test-Path ".env")) {
    Write-Host "ERROR: .env file not found!" -ForegroundColor Red
    Write-Host "Copy .env.example to .env and configure it first:"
    Write-Host "  Copy-Item .env.example .env"
    exit 1
}

switch ($Mode) {
    "minimal" {
        Write-Host ""
        Write-Host "Starting MINIMAL build (core services only)..." -ForegroundColor Green
        Write-Host "Services: backend, frontend, redis, falkordb"
        Write-Host ""
        docker-compose up -d
    }
    "full" {
        Write-Host ""
        Write-Host "Starting FULL build (all services)..." -ForegroundColor Green
        Write-Host "Services: all including doris, celery, flower, prometheus, grafana"
        Write-Host ""
        docker-compose --profile full --profile observability up -d
    }
    "dev" {
        Write-Host ""
        Write-Host "Starting DEV build (with LangGraph Studio)..." -ForegroundColor Green
        Write-Host ""
        docker-compose --profile dev --profile observability up -d
    }
    "stop" {
        Write-Host ""
        Write-Host "Stopping all services..." -ForegroundColor Yellow
        docker-compose --profile dev --profile full --profile observability down
        exit 0
    }
    "logs" {
        Write-Host ""
        Write-Host "Showing backend logs..." -ForegroundColor Yellow
        docker-compose logs -f backend
        exit 0
    }
    "rebuild" {
        Write-Host ""
        Write-Host "Rebuilding and starting..." -ForegroundColor Yellow
        docker-compose down
        docker-compose build --no-cache backend frontend
        docker-compose up -d
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Services Starting..." -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Wait ~2 minutes for all services to initialize." -ForegroundColor Yellow
Write-Host ""
Write-Host "Access points:" -ForegroundColor Green
Write-Host "  Frontend:    http://localhost:3000"
Write-Host "  Backend API: http://localhost:8000"
Write-Host "  API Docs:    http://localhost:8000/docs"
Write-Host "  Health:      http://localhost:8000/health"
Write-Host ""
Write-Host "To view logs:  docker-compose logs -f backend"
Write-Host "To stop:       .\start.ps1 stop"
Write-Host ""
