#!/bin/bash

# TubeWhale Quick Start Script
# Simple, reliable startup for team development

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Config
COMPOSE_FILE="docker-compose.simple.yml"
PROJECT_NAME="tubewhale"

# Print functions
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check Docker
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running! Please start Docker Desktop first."
        exit 1
    fi
    print_success "Docker is running"
}

# Check/create .env file
check_env() {
    if [ ! -f ".env" ]; then
        print_status "Creating .env file..."
        cat > .env << 'EOF'
# TubeWhale Development Environment

# Django
SECRET_KEY=tubewhale-dev-secret-key
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0,backend

# Database
DATABASE_URL=postgresql://tubewhale:tubewhale123@postgres:5432/tubewhale

# Redis
REDIS_URL=redis://:redis123@redis:6379/0
CELERY_BROKER_URL=redis://:redis123@redis:6379/0
CELERY_RESULT_BACKEND=redis://:redis123@redis:6379/0

# API Keys (please set real keys)
OPENAI_API_KEY=sk-your-openai-key-here
ANTHROPIC_API_KEY=sk-ant-your-anthropic-key-here

# Email
EMAIL_BACKEND=django.core.mail.backends.console.EmailBackend
EOF
        print_success "Created .env file"
    fi
}

# Wait for service
wait_for_service() {
    local service=$1
    local max_seconds=60
    local count=0
    
    print_status "Waiting for $service..."
    
    while [ $count -lt $max_seconds ]; do
        if docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps $service | grep -q "Up"; then
            print_success "$service is ready"
            return 0
        fi
        
        echo -n "."
        sleep 2
        count=$((count + 2))
    done
    
    print_error "$service failed to start within ${max_seconds}s"
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs $service --tail=10
    return 1
}

# Test connection
test_connection() {
    local url=$1
    local service=$2
    local max_attempts=10
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s $url >/dev/null 2>&1; then
            print_success "$service is responding"
            return 0
        fi
        sleep 3
        attempt=$((attempt + 1))
    done
    
    print_warning "$service connection test failed, but continuing..."
    return 0
}

# Run migrations
run_migrations() {
    print_status "Running database migrations..."
    
    local max_attempts=3
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME exec -T backend python manage.py migrate --noinput 2>/dev/null; then
            print_success "Migrations completed"
            return 0
        fi
        
        print_warning "Migration attempt $attempt failed, retrying..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    print_warning "Migrations failed, but continuing"
    return 0
}

# Create admin user
create_admin() {
    print_status "Creating admin user..."
    
    local script="
import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')
try:
    django.setup()
    from django.contrib.auth import get_user_model
    User = get_user_model()
    if not User.objects.filter(username='admin').exists():
        User.objects.create_superuser('admin', 'admin@tubewhale.com', 'admin123')
        print('Admin user created: admin/admin123')
    else:
        print('Admin user already exists')
except Exception as e:
    print(f'Could not create admin user: {e}')
"
    
    echo "$script" | docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME exec -T backend python 2>/dev/null || print_warning "Admin user creation skipped"
}

# Main start function
start_services() {
    echo "🐋 TubeWhale Quick Start"
    echo "======================="
    
    # Checks
    check_docker
    check_env
    
    # Clean up
    print_status "Cleaning up old containers..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down --remove-orphans >/dev/null 2>&1 || true
    
    # Start services step by step
    print_status "Starting PostgreSQL..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d postgres
    wait_for_service postgres
    
    print_status "Starting Redis..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d redis
    wait_for_service redis
    
    print_status "Starting Backend..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d backend
    wait_for_service backend
    
    # Test backend connection
    test_connection "http://localhost:8000" "Backend"
    
    # Setup database
    run_migrations
    create_admin
    
    # Start background services
    print_status "Starting Celery services..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d celery_worker celery_beat
    
    # Final status
    echo
    echo -e "${GREEN}✅ TubeWhale is ready!${NC}"
    echo "========================"
    echo -e "🌐 Application: ${YELLOW}http://localhost:8000${NC}"
    echo -e "⚙️  Admin Panel: ${YELLOW}http://localhost:8000/admin/${NC}"
    echo -e "👤 Admin Login: ${YELLOW}admin / admin123${NC}"
    echo "========================"
    
    # Show status
    print_status "Service Status:"
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps
}

# Stop services
stop_services() {
    print_status "Stopping services..."
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down
    print_success "Services stopped"
}

# Restart services
restart_services() {
    stop_services
    sleep 2
    start_services
}

# Reset everything
reset_services() {
    print_warning "This will delete all data!"
    echo -n "Are you sure? (y/N): "
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_status "Resetting environment..."
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v --remove-orphans
        docker system prune -f >/dev/null 2>&1 || true
        print_success "Environment reset"
        start_services
    else
        print_status "Reset cancelled"
    fi
}

# Show status
show_status() {
    print_status "TubeWhale Status:"
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps
    
    echo
    print_status "Port Status:"
    local ports=(8000 5432 6379)
    local names=("Backend" "PostgreSQL" "Redis")
    
    for i in "${!ports[@]}"; do
        local port=${ports[$i]}
        local name=${names[$i]}
        if lsof -i :$port >/dev/null 2>&1; then
            echo -e "  ${GREEN}✓${NC} $name (port $port)"
        else
            echo -e "  ${RED}✗${NC} $name (port $port)"
        fi
    done
}

# Show logs
show_logs() {
    local service=${1:-""}
    if [ -n "$service" ]; then
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail=100 "$service"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail=50
    fi
}

# Help
show_help() {
    echo "TubeWhale Quick Start Script"
    echo
    echo "Usage: $0 [command]"
    echo
    echo "Commands:"
    echo "  start     Start all services (default)"
    echo "  stop      Stop all services"
    echo "  restart   Restart all services"
    echo "  reset     Reset environment (deletes data!)"
    echo "  status    Show service status"
    echo "  logs      Show logs"
    echo "  help      Show this help"
    echo
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 logs backend"
    echo "  $0 status"
}

# Main function
main() {
    case "${1:-start}" in
        start)
            start_services
            ;;
        stop)
            stop_services
            ;;
        restart)
            restart_services
            ;;
        reset)
            reset_services
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs "$2"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Handle interrupts
trap 'print_warning "Script interrupted"; exit 1' INT TERM

# Run
main "$@"