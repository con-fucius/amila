# Development Environment Startup Script
# Starts both backend and frontend in development mode

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Starting Amila development environment..." -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Check if .env files exist
Write-Host "Checking environment configuration..." -ForegroundColor Yellow

$backendEnv = "backend\.env"
$frontendEnv = "frontend\.env.local"

if (-not (Test-Path $backendEnv)) {
    Write-Host " backend/.env not found!" -ForegroundColor Red
    Write-Host "   Create it with:" -ForegroundColor Yellow
    Write-Host "   CORS_ORIGINS=http://localhost:5173" -ForegroundColor Gray
    exit 1
}

if (-not (Test-Path $frontendEnv)) {
    Write-Host " frontend/.env.local not found!" -ForegroundColor Red
    Write-Host "   Create it with:" -ForegroundColor Yellow
    Write-Host "   VITE_API_URL=http://localhost:8000" -ForegroundColor Gray
    exit 1
}

Write-Host " Environment files found" -ForegroundColor Green
Write-Host ""

# Function to start backend
function Start-Backend {
    Write-Host "Starting Backend..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd backend; python main.py" -WindowStyle Normal
}

# Function to start frontend
function Start-Frontend {
    Write-Host "Starting Frontend..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd frontend; pnpm run dev" -WindowStyle Normal
}

# Start services
Start-Backend
Start-Frontend

Write-Host ""
Write-Host "================================" -ForegroundColor Green
Write-Host "Services Starting..." -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green
Write-Host ""
Write-Host "Backend:  http://localhost:8000" -ForegroundColor Cyan
Write-Host "Frontend: http://localhost:5173" -ForegroundColor Cyan
Write-Host ""
Write-Host "Health Check: http://localhost:8000/health" -ForegroundColor Gray
Write-Host "API Docs:     http://localhost:8000/docs" -ForegroundColor Gray
Write-Host ""
Write-Host "Press Ctrl+C in each window to stop services" -ForegroundColor Yellow
Write-Host ""
