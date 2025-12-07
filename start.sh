#!/bin/bash
# ============================================
# BI Agent MVP - Docker Startup Script
# ============================================

set -e

echo "============================================"
echo "  BI Agent MVP - Starting Services"
echo "============================================"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    echo "Copy .env.example to .env and configure it first:"
    echo "  cp .env.example .env"
    exit 1
fi

# Parse command line arguments
MODE="${1:-minimal}"

case "$MODE" in
    minimal)
        echo ""
        echo "Starting MINIMAL build (core services only)..."
        echo "Services: backend, frontend, redis, falkordb"
        echo ""
        docker-compose up -d
        ;;
    full)
        echo ""
        echo "Starting FULL build (all services)..."
        echo "Services: all including doris, celery, flower, prometheus, grafana"
        echo ""
        docker-compose --profile full --profile observability up -d
        ;;
    dev)
        echo ""
        echo "Starting DEV build (with LangGraph Studio)..."
        echo ""
        docker-compose --profile dev --profile observability up -d
        ;;
    stop)
        echo ""
        echo "Stopping all services..."
        docker-compose --profile dev --profile full --profile observability down
        exit 0
        ;;
    logs)
        echo ""
        echo "Showing backend logs..."
        docker-compose logs -f backend
        exit 0
        ;;
    rebuild)
        echo ""
        echo "Rebuilding and starting..."
        docker-compose down
        docker-compose build --no-cache backend frontend
        docker-compose up -d
        ;;
    *)
        echo "Usage: $0 {minimal|full|dev|stop|logs|rebuild}"
        echo ""
        echo "  minimal  - Start core services only (default)"
        echo "  full     - Start all services including monitoring"
        echo "  dev      - Start with LangGraph Studio"
        echo "  stop     - Stop all services"
        echo "  logs     - Show backend logs"
        echo "  rebuild  - Rebuild and restart"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "  Services Starting..."
echo "============================================"
echo ""
echo "Wait ~2 minutes for all services to initialize."
echo ""
echo "Access points:"
echo "  Frontend:    http://localhost:3000"
echo "  Backend API: http://localhost:8000"
echo "  API Docs:    http://localhost:8000/docs"
echo "  Health:      http://localhost:8000/health"
echo ""
echo "To view logs:  docker-compose logs -f backend"
echo "To stop:       ./start.sh stop"
echo ""
