#!/bin/bash
# TubeWhale Multilingual Production Deployment Script
# English-First with Best Industrial Practices

set -e

echo "🌍 TubeWhale Multilingual Production Deployment"
echo "=============================================="

# Configuration
DEFAULT_LANGUAGE="${DEFAULT_LANGUAGE:-en}"
ENVIRONMENT="${ENVIRONMENT:-production}"
COMPILE_LOCALES="${COMPILE_LOCALES:-true}"
BUILD_FRONTEND="${BUILD_FRONTEND:-true}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check prerequisites
check_prerequisites() {
    print_status "Checking deployment prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check required files
    required_files=(
        "Dockerfile.i18n"
        "docker-compose.i18n.yml"
        "tubewhale_project/multilingual_config.py"
        "compile_locales.sh"
        "requirements.txt"
        "django_requirements.txt"
    )
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "Required file missing: $file"
            exit 1
        fi
    done
    
    print_success "Prerequisites check passed"
}

# Environment setup
setup_environment() {
    print_status "Setting up deployment environment..."
    
    # Create environment file if it doesn't exist
    if [ ! -f ".env.production" ]; then
        print_warning "Creating default .env.production file"
        cat > .env.production << EOF
# TubeWhale Multilingual Production Environment
DJANGO_SETTINGS_MODULE=tubewhale_project.settings
DEBUG=False
ENVIRONMENT=production

# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=tubewhale_prod
DB_USER=tubewhale
DB_PASSWORD=tubewhale_secure_password_2024

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Security
SECRET_KEY=your-super-secret-key-change-this-in-production
ALLOWED_HOSTS=localhost,127.0.0.1,your-domain.com

# Multilingual Configuration
DEFAULT_LANGUAGE=en
ENABLE_I18N=true
COMPILE_LOCALES=true
SUPPORTED_LANGUAGES=en,zh-hans,zh-hant,ja,ko,es,fr,de,pt,ru,ar,hi

# Static Files
STATIC_URL=/static/
MEDIA_URL=/media/

# Email Configuration (for user notifications)
EMAIL_BACKEND=django.core.mail.backends.console.EmailBackend

# YouTube API (if needed)
YOUTUBE_API_KEY=your-youtube-api-key

# Logging
LOG_LEVEL=INFO
EOF
        print_warning "Please update .env.production with your production values"
    fi
    
    print_success "Environment setup completed"
}

# Compile locales
compile_locales() {
    if [ "$COMPILE_LOCALES" = "true" ]; then
        print_status "Compiling multilingual locales..."
        
        # Make compile script executable
        chmod +x compile_locales.sh
        
        # Run locale compilation
        ./compile_locales.sh
        
        print_success "Locales compiled successfully"
    else
        print_status "Skipping locale compilation (COMPILE_LOCALES=false)"
    fi
}

# Build Docker images
build_images() {
    print_status "Building Docker images..."
    
    # Build main application image
    docker build \
        -f Dockerfile.i18n \
        -t tubewhale:multilingual-latest \
        --build-arg ENABLE_I18N=true \
        --build-arg DEFAULT_LANGUAGE=$DEFAULT_LANGUAGE \
        --build-arg COMPILE_LOCALES=$COMPILE_LOCALES \
        .
    
    print_success "Docker images built successfully"
}

# Deploy services
deploy_services() {
    print_status "Deploying services with Docker Compose..."
    
    # Stop existing services
    docker-compose -f docker-compose.i18n.yml down
    
    # Pull external images
    docker-compose -f docker-compose.i18n.yml pull postgres redis nginx
    
    # Start services
    docker-compose -f docker-compose.i18n.yml up -d
    
    print_success "Services deployed successfully"
}

# Health check
health_check() {
    print_status "Performing health checks..."
    
    # Wait for services to start
    sleep 30
    
    # Check service status
    services=("postgres" "redis" "backend" "nginx" "celery-worker")
    
    for service in "${services[@]}"; do
        if docker-compose -f docker-compose.i18n.yml ps $service | grep -q "Up"; then
            print_success "$service is running"
        else
            print_error "$service is not running"
            docker-compose -f docker-compose.i18n.yml logs $service
        fi
    done
    
    # Test application endpoint
    print_status "Testing application endpoint..."
    if curl -f http://localhost:8080/health/ > /dev/null 2>&1; then
        print_success "Application is responding"
    else
        print_warning "Application health check failed - checking logs..."
        docker-compose -f docker-compose.i18n.yml logs backend
    fi
}

# Show deployment info
show_deployment_info() {
    print_success "🎉 TubeWhale Multilingual Deployment Complete!"
    echo ""
    echo "📍 Deployment Information:"
    echo "   Application URL: http://localhost:8080"
    echo "   Admin URL: http://localhost:8080/admin/"
    echo "   Default Language: $DEFAULT_LANGUAGE"
    echo "   i18n Enabled: $ENABLE_I18N"
    echo "   Environment: $ENVIRONMENT"
    echo ""
    echo "🗂️  Available Languages:"
    echo "   • English (en) - Primary"
    echo "   • 简体中文 (zh-hans)"
    echo "   • 繁體中文 (zh-hant)"
    echo "   • 日本語 (ja)"
    echo "   • 한국어 (ko)"
    echo "   • Español (es)"
    echo "   • Français (fr)"
    echo "   • Deutsch (de)"
    echo "   • Português (pt)"
    echo "   • Русский (ru)"
    echo "   • العربية (ar)"
    echo "   • हिन्दी (hi)"
    echo ""
    echo "🔧 Management Commands:"
    echo "   View logs: docker-compose -f docker-compose.i18n.yml logs -f"
    echo "   Stop services: docker-compose -f docker-compose.i18n.yml down"
    echo "   Restart: docker-compose -f docker-compose.i18n.yml restart"
    echo "   Update locales: ./compile_locales.sh"
    echo ""
    echo "👤 Default Admin Credentials:"
    echo "   Username: admin"
    echo "   Password: admin123"
    echo "   (Please change in production!)"
}

# Cleanup function
cleanup() {
    if [ $? -ne 0 ]; then
        print_error "Deployment failed. Cleaning up..."
        docker-compose -f docker-compose.i18n.yml down
    fi
}

# Set trap for cleanup
trap cleanup EXIT

# Main deployment flow
main() {
    echo "Starting deployment with configuration:"
    echo "  DEFAULT_LANGUAGE: $DEFAULT_LANGUAGE"
    echo "  ENVIRONMENT: $ENVIRONMENT"
    echo "  COMPILE_LOCALES: $COMPILE_LOCALES"
    echo "  BUILD_FRONTEND: $BUILD_FRONTEND"
    echo ""
    
    check_prerequisites
    setup_environment
    compile_locales
    build_images
    deploy_services
    health_check
    show_deployment_info
    
    print_success "🚀 Deployment completed successfully!"
}

# Check for command line arguments
case "${1:-}" in
    --help|-h)
        echo "TubeWhale Multilingual Production Deployment"
        echo ""
        echo "Usage: $0 [options]"
        echo ""
        echo "Options:"
        echo "  --help, -h              Show this help message"
        echo "  --check                 Only run prerequisite checks"
        echo "  --build-only            Only build Docker images"
        echo "  --deploy-only           Only deploy services (skip build)"
        echo ""
        echo "Environment Variables:"
        echo "  DEFAULT_LANGUAGE        Default language (default: en)"
        echo "  ENVIRONMENT            Deployment environment (default: production)"
        echo "  COMPILE_LOCALES        Compile locales (default: true)"
        echo "  BUILD_FRONTEND         Build frontend assets (default: true)"
        exit 0
        ;;
    --check)
        check_prerequisites
        exit 0
        ;;
    --build-only)
        check_prerequisites
        compile_locales
        build_images
        exit 0
        ;;
    --deploy-only)
        deploy_services
        health_check
        show_deployment_info
        exit 0
        ;;
    "")
        main
        ;;
    *)
        print_error "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac