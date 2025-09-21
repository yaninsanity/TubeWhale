#!/bin/bash

# TubeWhale Docker Startup Script
# One-click startup for a complete production-like environment

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

print_header() {
    echo -e "${PURPLE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                     TubeWhale Backend                        ║"
    echo "║                 Docker Automated Startup                     ║"
    echo "║                                                              ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Check Docker environment
check_docker() {
    print_message "$BLUE" "🔍 Checking Docker environment..."
    
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_message "$RED" "❌ Docker Compose is not installed. Please install Docker Compose."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_message "$RED" "❌ Docker service is not running. Please start Docker."
        exit 1
    fi
    
    print_message "$GREEN" "✅ Docker environment check passed"
}

# Create required directories
create_directories() {
    print_message "$BLUE" "📁 Creating required directories..."
    
    directories=(
        "media"
        "logs"
        "ssl"
        "staticfiles"
    )
    
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_message "$GREEN" "✅ Created directory: $dir"
        fi
    done
}

# Generate environment file
generate_env_file() {
    # Single source: .env. If missing, generate one with sensible defaults.
    if [ -f ".env" ]; then
        print_message "$YELLOW" "⚠️  Found .env. Skipping generation."
        return 0
    fi

    print_message "$BLUE" "⚙️  Generating .env (defaults)..."
    
    cat > .env << EOF
# Django settings
DJANGO_SETTINGS_MODULE=tubewhale_project.settings
DEBUG=false
SECRET_KEY=$(python3 -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())')
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0

# Database settings
DB_ENGINE=django.db.backends.postgresql
DB_NAME=tubewhale
DB_USER=tubewhale
DB_PASSWORD=tubewhale123
DB_HOST=postgres
DB_PORT=5432

# Redis / Celery settings
REDIS_URL=redis://:redis123@redis:6379/0
CELERY_BROKER_URL=redis://:redis123@redis:6379/0
CELERY_RESULT_BACKEND=redis://:redis123@redis:6379/0

# Locale / timezone
LANGUAGE_CODE=zh-hans
TIME_ZONE=Asia/Shanghai
USE_I18N=true
USE_TZ=true
EOF
    
    print_message "$GREEN" "✅ .env generated"
}

# Build Docker images
build_images() {
    print_message "$BLUE" "🏗️  Building Docker image..."
    
    docker-compose build --no-cache backend
    
    if [ $? -eq 0 ]; then
        print_message "$GREEN" "✅ Image build succeeded"
    else
        print_message "$RED" "❌ Image build failed"
        exit 1
    fi
}

# Start services
start_services() {
    print_message "$BLUE" "🚀 Starting Docker services..."
    
    # Start base services (database and cache)
    print_message "$CYAN" "  📊 Starting Postgres and Redis..."
    docker-compose up -d postgres redis
    
    # Wait for DB readiness (basic buffer)
    print_message "$CYAN" "  ⏳ Waiting for database to be ready..."
    sleep 15
    
    # Start backend
    print_message "$CYAN" "  🖥️  Starting backend..."
    docker-compose up -d backend
    
    # Start Celery services
    print_message "$CYAN" "  ⚡ Starting Celery worker and beat..."
    docker-compose up -d celery_worker celery_beat
    
    # Start Nginx (optional)
    if [ "$1" = "--with-nginx" ]; then
        print_message "$CYAN" "  🌐 Starting Nginx reverse proxy..."
        docker-compose up -d nginx
    fi
    
    print_message "$GREEN" "✅ All services started (waiting for backend health)"

    # Health polling
    local retries=40
    local sleep_sec=3
    local ok=0
    print_message "$BLUE" "🩺 Polling backend health endpoint..."
    while [ $retries -gt 0 ]; do
        if curl -fsS http://localhost:8000/api/v1/tubewhale/health/ > /dev/null 2>&1; then
            ok=1
            break
        fi
        retries=$((retries-1))
        sleep $sleep_sec
    done
    if [ $ok -eq 1 ]; then
        print_message "$GREEN" "✅ Backend healthy"
    else
        print_message "$YELLOW" "⚠️  Backend health not confirmed (timed out) – check logs"
    fi
}

# Show status
show_status() {
    print_message "$BLUE" "📋 Services status:"
    echo ""
    docker-compose ps
    echo ""
    
    print_message "$BLUE" "🌐 Endpoints:"
    echo -e "${GREEN}  • Django Admin: ${CYAN}http://localhost:8000/admin/${NC}"
    echo -e "${GREEN}  • API Root: ${CYAN}http://localhost:8000/api/${NC}"
    echo -e "${GREEN}  • Health: ${CYAN}http://localhost:8000/api/v1/tubewhale/health/${NC}"
    echo ""
    
    print_message "$BLUE" "📊 Databases:"
    echo -e "${GREEN}  • PostgreSQL: ${CYAN}localhost:5432${NC}"
    echo -e "${GREEN}  • Redis: ${CYAN}localhost:6379${NC}"
    echo ""
}

# Tail logs
show_logs() {
    print_message "$BLUE" "📝 Tailing backend logs (Ctrl+C to exit):"
    docker-compose logs -f backend
}

# Stop services
stop_services() {
    print_message "$BLUE" "🛑 Stopping all services..."
    docker-compose down
    print_message "$GREEN" "✅ Services stopped"
}

# Cleanup resources
cleanup() {
    print_message "$BLUE" "🧹 Cleaning Docker resources..."
    docker-compose down -v --remove-orphans
    docker system prune -f
    print_message "$GREEN" "✅ Cleanup completed"
}

# Main
main() {
    print_header
    
    case "${1:-start}" in
        "start")
            check_docker
            create_directories
            generate_env_file
            build_images
            start_services "$2"
            show_status
            echo ""
            print_message "$YELLOW" "💡 Use './start-docker.sh logs' to view live logs"
            print_message "$YELLOW" "💡 Use './start-docker.sh stop' to stop services"
            ;;
        "logs")
            show_logs
            ;;
        "stop")
            stop_services
            ;;
        "restart")
            stop_services
            sleep 2
            start_services "$2"
            show_status
            ;;
        "status")
            show_status
            ;;
        "cleanup")
            cleanup
            ;;
        "build")
            build_images
            ;;
        *)
            echo "Usage: $0 [start|stop|restart|logs|status|cleanup|build] [--with-nginx]"
            echo ""
            echo "Commands:"
            echo "  start    - Start all services (default)"
            echo "  stop     - Stop all services"
            echo "  restart  - Restart all services"
            echo "  logs     - Tail backend logs"
            echo "  status   - Show services status"
            echo "  build    - Rebuild backend image"
            echo "  cleanup  - Clean all Docker resources"
            echo ""
            echo "Options:"
            echo "  --with-nginx  - Also start Nginx reverse proxy"
            exit 1
            ;;
    esac
}

# Signal handling
trap 'print_message "$RED" "❌ Script interrupted"; exit 1' INT TERM

# Execute main
main "$@"
