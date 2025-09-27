#!/bin/bash

# TubeWhale Clean Docker Startup Script
# One-click startup with automatic cleanup and health checks

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="tubewhale"
REQUIRED_PORTS=(5432 6379 8000)
SERVICES=("postgres" "redis" "backend" "celery_worker" "celery_beat")

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

print_header() {
    echo -e "${PURPLE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                     🐋 TubeWhale Backend                     ║"
    echo "║                 Clean Docker Startup                        ║"
    echo "║                                                              ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Cleanup function
cleanup() {
    print_message "$YELLOW" "🧹 Cleaning up previous containers and volumes..."
    docker-compose down -v --remove-orphans 2>/dev/null || true
    docker system prune -f 2>/dev/null || true
    print_message "$GREEN" "✅ Cleanup completed"
}

# Check system requirements
check_requirements() {
    print_message "$BLUE" "🔍 Checking system requirements..."

    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi

    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_message "$RED" "❌ Docker Compose is not available."
        exit 1
    fi

    # Check Docker daemon
    if ! docker info &> /dev/null; then
        print_message "$RED" "❌ Docker daemon is not running. Please start Docker."
        exit 1
    fi

    # Check available disk space (at least 2GB)
    local available_space=$(df . | awk 'NR==2 {print $4}')
    if [ "$available_space" -lt 2097152 ]; then
        print_message "$YELLOW" "⚠️  Low disk space detected. At least 2GB recommended."
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi

    print_message "$GREEN" "✅ System requirements check passed"
}

# Check port availability
check_ports() {
    print_message "$BLUE" "🔍 Checking port availability..."

    for port in "${REQUIRED_PORTS[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            print_message "$YELLOW" "⚠️  Port $port is already in use"
            # Kill process using the port
            local pid=$(lsof -ti:$port)
            if [ ! -z "$pid" ]; then
                print_message "$YELLOW" "🛑 Killing process $pid using port $port"
                kill -9 $pid 2>/dev/null || true
            fi
        fi
    done

    print_message "$GREEN" "✅ Port check completed"
}

# Create required directories
create_directories() {
    print_message "$BLUE" "📁 Creating required directories..."

    local directories=("media" "logs" "staticfiles" "ssl")

    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_message "$GREEN" "✅ Created directory: $dir"
        fi
    done
}

# Wait for service to be healthy
wait_for_service() {
    local service=$1
    local max_attempts=30
    local attempt=1

    print_message "$BLUE" "⏳ Waiting for $service to be healthy..."

    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $service | grep -q "healthy\|running"; then
            print_message "$GREEN" "✅ $service is ready"
            return 0
        fi

        print_message "$YELLOW" "⏳ $service not ready yet (attempt $attempt/$max_attempts)"
        sleep 10
        ((attempt++))
    done

    print_message "$RED" "❌ $service failed to start within $(($max_attempts * 10)) seconds"
    return 1
}

# Start services
start_services() {
    print_message "$BLUE" "🚀 Starting TubeWhale services..."

    # Start services in order
    docker-compose up -d postgres redis

    # Wait for database and cache
    wait_for_service postgres
    wait_for_service redis

    # Start application services
    docker-compose up -d backend celery_worker celery_beat

    # Wait for backend
    wait_for_service backend

    print_message "$GREEN" "✅ All services started successfully"
}

# Show service status
show_status() {
    print_message "$BLUE" "📊 Service Status:"

    echo -e "${CYAN}Container Status:${NC}"
    docker-compose ps

    echo -e "\n${CYAN}Service URLs:${NC}"
    echo -e "🌐 Web Application: ${GREEN}http://localhost:8000${NC}"
    echo -e "📚 API Documentation: ${GREEN}http://localhost:8000/api/docs/${NC}"
    echo -e "❤️ Health Check: ${GREEN}http://localhost:8000/api/v1/tubewhale/health/${NC}"
    echo -e "🔐 Admin Panel: ${GREEN}http://localhost:8000/admin/${NC}"
    echo -e "   Username: ${YELLOW}admin${NC}"
    echo -e "   Password: ${YELLOW}admin123${NC}"

    echo -e "\n${CYAN}Database:${NC}"
    echo -e "🐘 PostgreSQL: ${GREEN}localhost:5432${NC}"
    echo -e "🔴 Redis: ${GREEN}localhost:6379${NC}"
}

# Main function
main() {
    print_header

    # Trap for cleanup on exit
    trap cleanup EXIT

    # Run startup sequence
    cleanup
    check_requirements
    check_ports
    create_directories
    start_services
    show_status

    print_message "$GREEN" "🎉 TubeWhale is now running!"
    print_message "$BLUE" "💡 Use './stop-docker.sh' to stop all services"
    print_message "$BLUE" "📋 Use 'docker-compose logs -f' to view logs"
}

# Handle command line arguments
case "${1:-}" in
    "stop")
        print_message "$BLUE" "🛑 Stopping TubeWhale services..."
        docker-compose down -v --remove-orphans
        print_message "$GREEN" "✅ Services stopped and cleaned up"
        ;;
    "restart")
        print_message "$BLUE" "🔄 Restarting TubeWhale services..."
        docker-compose restart
        show_status
        ;;
    "logs")
        print_message "$BLUE" "📋 Showing service logs..."
        docker-compose logs -f "${2:-}"
        ;;
    "status")
        show_status
        ;;
    "cleanup")
        cleanup
        ;;
    *)
        main
        ;;
esac

# Pre-flight checks
preflight_checks() {
    print_message "$BLUE" "✈️  Running pre-flight checks..."
    
    # Check for required files
    local required_files=("docker-compose.yml" "Dockerfile.simple" "requirements.txt" "manage.py")
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            print_message "$RED" "❌ Required file not found: $file"
            exit 1
        fi
    done
    
    # Check if ports are available
    local ports=(5432 6379 8000)
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            print_message "$YELLOW" "⚠️  Port $port is already in use. Service may fail to start."
        fi
    done
    
    print_message "$GREEN" "✅ Pre-flight checks passed"
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
    # Check for .env.docker (required for docker-compose.yml)
    if [ -f ".env.docker" ]; then
        print_message "$GREEN" "✅ Found .env.docker. Using existing configuration."
        validate_env_file
        return 0
    fi

    print_message "$BLUE" "⚙️  Generating .env.docker (defaults)..."
    
    cat > .env.docker << EOF
# TubeWhale Docker Environment Configuration
DEBUG=True
SECRET_KEY=tubewhale-docker-secret-key-change-in-production-$(date +%s)
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0,backend

# Database Configuration - PostgreSQL for Docker
DB_ENGINE=django.db.backends.postgresql
DB_NAME=tubewhale
DB_USER=tubewhale
DB_PASSWORD=tubewhale123
DB_HOST=postgres
DB_PORT=5432

# Redis Configuration - Docker internal network
REDIS_URL=redis://:redis123@redis:6379/1
CELERY_BROKER_URL=redis://:redis123@redis:6379/0
CELERY_RESULT_BACKEND=redis://:redis123@redis:6379/0

# Static/Media files
STATIC_URL=/static/
MEDIA_URL=/media/
STATIC_ROOT=/app/staticfiles
MEDIA_ROOT=/app/media

# Django Environment 
DJANGO_SETTINGS_MODULE=tubewhale_project.settings
PYTHONPATH=/app

# Timezone
TZ=Asia/Shanghai

# API Keys - 生产环境中需要替换为真实密钥
YOUTUBE_API_KEY=your-youtube-api-key-here
OPENAI_API_KEY=your-openai-api-key-here

# Default admin user (for development only)
DJANGO_SUPERUSER_USERNAME=admin
DJANGO_SUPERUSER_EMAIL=admin@tubewhale.com
DJANGO_SUPERUSER_PASSWORD=admin123
EOF
    
    print_message "$GREEN" "✅ .env.docker generated with unique secret key"
    print_message "$YELLOW" "⚠️  默认API密钥为占位符，需要替换为真实密钥"
}

# Validate environment file
validate_env_file() {
    print_message "$BLUE" "🔍 Validating .env.docker configuration..."
    
    required_vars=("DB_NAME" "DB_USER" "DB_PASSWORD" "REDIS_URL" "SECRET_KEY")
    missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if ! grep -q "^${var}=" .env.docker; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_message "$YELLOW" "⚠️  Missing environment variables: ${missing_vars[*]}"
        print_message "$BLUE" "📝 Regenerating .env.docker with all required variables..."
        rm .env.docker
        generate_env_file
        return 1
    fi
    
    print_message "$GREEN" "✅ Environment configuration validated"
}

# Wait for service to be ready
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local max_attempts=${3:-30}
    local attempt=0
    
    print_message "$CYAN" "  ⏳ Waiting for $service_name to be ready..."
    
    while [ $attempt -lt $max_attempts ]; do
        if docker-compose exec -T $service_name $check_command >/dev/null 2>&1; then
            print_message "$GREEN" "  ✅ $service_name is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    print_message "$YELLOW" "  ⚠️  $service_name readiness check timed out, continuing anyway..."
    return 1
}

# Wait for backend to be ready
wait_for_backend() {
    local max_attempts=30
    local attempt=0
    
    print_message "$CYAN" "  ⏳ Waiting for backend to be ready..."
    
    while [ $attempt -lt $max_attempts ]; do
        # Try multiple endpoints to check backend health
        if curl -fsS http://localhost:8000/ > /dev/null 2>&1 || \
           curl -fsS http://localhost:8000/admin/ > /dev/null 2>&1 || \
           docker-compose exec -T backend python manage.py check > /dev/null 2>&1; then
            print_message "$GREEN" "  ✅ Backend is ready and healthy"
            return 0
        fi
        
        # Check if container is running
        if ! docker-compose ps backend | grep -q "Up"; then
            print_message "$RED" "  ❌ Backend container is not running"
            print_message "$BLUE" "  📝 Backend logs:"
            docker-compose logs --tail=30 backend
            exit 1
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 5
    done
    
    print_message "$YELLOW" "  ⚠️  Backend readiness check timed out"
    print_message "$BLUE" "  📝 Recent backend logs:"
    docker-compose logs --tail=30 backend
    return 1
}

# Run database migrations and setup
run_migrations() {
    print_message "$CYAN" "  🗄️  Running database migrations..."
    
    # Wait a bit more for DB to be fully ready
    sleep 5
    
    if docker-compose exec -T backend python manage.py migrate --no-input; then
        print_message "$GREEN" "  ✅ Database migrations completed"
    else
        print_message "$RED" "  ❌ Database migrations failed"
        print_message "$BLUE" "  📝 Backend logs:"
        docker-compose logs --tail=30 backend
        print_message "$BLUE" "  📝 Database logs:"
        docker-compose logs --tail=10 postgres
        exit 1
    fi
    
    # Collect static files
    print_message "$CYAN" "  📁 Collecting static files..."
    if docker-compose exec -T backend python manage.py collectstatic --no-input --clear; then
        print_message "$GREEN" "  ✅ Static files collected"
    else
        print_message "$YELLOW" "  ⚠️  Static files collection failed (continuing...)"
    fi
    
    # Create superuser if it doesn't exist
    print_message "$CYAN" "  👤 Creating admin user (if not exists)..."
    docker-compose exec -T backend python manage.py shell -c "
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@tubewhale.com', 'admin123')
    print('✅ Admin user created: admin/admin123')
else:
    print('✅ Admin user already exists')
" || print_message "$YELLOW" "  ⚠️  Admin user creation failed (continuing...)"
}

# Build Docker images
build_images() {
    print_message "$BLUE" "🏗️  Building Docker images..."
    
    # Build all services that need building
    docker-compose build --no-cache
    
    if [ $? -eq 0 ]; then
        print_message "$GREEN" "✅ Images build succeeded"
    else
        print_message "$RED" "❌ Images build failed"
        exit 1
    fi
}

# Start services
start_services() {
    print_message "$BLUE" "🚀 Starting Docker services..."
    
    # Clean up any existing containers
    print_message "$CYAN" "  🧹 Cleaning up existing containers..."
    docker-compose down --remove-orphans
    
    # Start base services (database and cache)
    print_message "$CYAN" "  📊 Starting Postgres and Redis..."
    docker-compose up -d postgres redis
    
    # Wait for DB readiness with proper health check
    print_message "$CYAN" "  ⏳ Waiting for database to be ready..."
    local db_ready=0
    for i in {1..30}; do
        if docker-compose exec -T postgres pg_isready -h localhost -U tubewhale > /dev/null 2>&1; then
            db_ready=1
            break
        fi
        echo -n "."
        sleep 2
    done
    
    if [ $db_ready -eq 1 ]; then
        print_message "$GREEN" "  ✅ Database is ready"
    else
        print_message "$RED" "  ❌ Database failed to start"
        docker-compose logs postgres
        exit 1
    fi
    
    # Wait for Redis
    print_message "$CYAN" "  ⏳ Waiting for Redis to be ready..."
    local redis_ready=0
    for i in {1..15}; do
        if docker-compose exec -T redis redis-cli -a redis123 ping > /dev/null 2>&1; then
            redis_ready=1
            break
        fi
        echo -n "."
        sleep 2
    done
    
    if [ $redis_ready -eq 1 ]; then
        print_message "$GREEN" "  ✅ Redis is ready"
    else
        print_message "$YELLOW" "  ⚠️  Redis readiness check failed (continuing...)"
    fi
    
    # Start backend
    print_message "$CYAN" "  🖥️  Starting backend..."
    docker-compose up -d backend
    
    # Wait for backend to be ready
    wait_for_backend
    
    # Run database migrations
    run_migrations
    
    # Start Celery services
    print_message "$CYAN" "  ⚡ Starting Celery worker and beat..."
    docker-compose up -d celery_worker celery_beat
    
    # Start Nginx (optional)
    if [ "$1" = "--with-nginx" ]; then
        print_message "$CYAN" "  🌐 Starting Nginx reverse proxy..."
        docker-compose up -d nginx
    fi
    
    print_message "$GREEN" "✅ All services started successfully"
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
            preflight_checks
            check_docker
            create_directories
            generate_env_file
            build_images
            start_services "$2"
            show_status
            echo ""
            print_message "$GREEN" "🎉 TubeWhale is now running!"
            print_message "$CYAN" "📖 Quick Start Guide:"
            echo -e "${GREEN}  • Admin Panel: ${CYAN}http://localhost:8000/admin/${NC} (admin/admin123)"
            echo -e "${GREEN}  • API Docs: ${CYAN}http://localhost:8000/api/${NC}"
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
        "reset")
            print_message "$BLUE" "🔄 Resetting TubeWhale environment..."
            cleanup
            sleep 2
            check_docker
            create_directories  
            generate_env_file
            build_images
            start_services "$2"
            show_status
            ;;
        *)
            echo "Usage: $0 [start|stop|restart|logs|status|cleanup|build|reset] [--with-nginx]"
            echo ""
            echo "Commands:"
            echo "  start    - Start all services (default)"
            echo "  stop     - Stop all services"
            echo "  restart  - Restart all services"
            echo "  logs     - Tail backend logs"
            echo "  status   - Show services status"
            echo "  build    - Rebuild Docker images"
            echo "  cleanup  - Clean all Docker resources"
            echo "  reset    - Complete reset and fresh start"
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
