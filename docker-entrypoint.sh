#!/bin/bash#!/bin/bash

# Docker entrypoint script for TubeWhale backend# TubeWhale Docker Entrypoint Script

# Initialization work before startup

set -e

set -e

echo "🔄 Starting TubeWhale backend initialization..."

# Color output

# Wait for PostgreSQL to be readyRED='\033[0;31m'

echo "⏳ Waiting for PostgreSQL..."GREEN='\033[0;32m'

while ! pg_isready -h postgres -p 5432 -U tubewhale; doYELLOW='\033[1;33m'

    echo "Waiting for PostgreSQL to be ready..."BLUE='\033[0;34m'

    sleep 2NC='\033[0m' # No Color

done

echo "✅ PostgreSQL is ready!"echo -e "${BLUE}� TubeWhale Development Environment Starting...${NC}"



# Run Django migrations# Wait for database to be ready

echo "🔄 Running Django migrations..."wait_for_db() {

python manage.py migrate --noinput    echo -e "${YELLOW}⏳ Waiting for database...${NC}"

    

# Create superuser if it doesn't exist    # Simple database connection check  

echo "👤 Creating superuser..."    while ! python -c "

python manage.py shell -c "import django

from apps.user_app.models import Userimport os

try:os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')

    if not User.objects.filter(username='admin').exists():django.setup()

        admin = User.objects.create_superuser('admin', 'admin@tubewhale.com', 'admin123')from django.db import connection

        admin.tier = 'premium'try:

        admin.save()    cursor = connection.cursor()

        print('✅ Superuser created: admin/admin123')    cursor.execute('SELECT 1')

    else:    print('Database is ready!')

        print('✅ Superuser already exists')except Exception as e:

except Exception as e:    print('Database not ready:', e)

    print(f'⚠️ Superuser creation skipped: {e}')    exit(1)

"" 2>/dev/null; do

        echo -e "${YELLOW}⏳ Database is unavailable - sleeping${NC}"

# Collect static files        sleep 2

echo "📁 Collecting static files..."    done

python manage.py collectstatic --noinput    

    echo -e "${GREEN}✅ Database is ready!${NC}"

echo "🚀 Starting Django development server..."}

exec python manage.py runserver 0.0.0.0:8000
# Run database migrations
run_migrations() {
    echo -e "${BLUE}🔄 Running database migrations...${NC}"
        python manage.py migrate --noinput || {
            echo -e "${RED}❌ Migration failed, retrying in 5s...${NC}"; sleep 5; python manage.py migrate --noinput; }
    echo -e "${GREEN}✅ Migrations completed!${NC}"
}

# Create superuser
create_superuser() {
    echo -e "${BLUE}👤 Creating superuser...${NC}"
    python manage.py shell -c "
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser(
        username='admin',
        email='admin@tubewhale.com',
        password='admin123'
    )
    print('✅ Superuser created: admin/admin123')
else:
    print('ℹ️ Superuser already exists')
"
}

# Collect static files
collect_static() {
    echo -e "${BLUE}📁 Collecting static files...${NC}"
    python manage.py collectstatic --noinput --clear || true
    echo -e "${GREEN}✅ Static files collected!${NC}"
}

# Compile translation files
compile_messages() {
    echo -e "${BLUE}🌐 Compiling translation messages...${NC}"
    python manage.py compilemessages || true
    echo -e "${GREEN}✅ Translation messages compiled!${NC}"
}

# Main function
main() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}🐋 TubeWhale Django Backend Initialization${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    # Wait for database
    if [ "$WAIT_FOR_DB" = "true" ]; then
        wait_for_db
    fi
    
    # Run migrations
    run_migrations
    
    # Create superuser
    create_superuser
    
    # Collect static files
    collect_static
    
    # Compile translations
    compile_messages
    
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}🎉 TubeWhale Backend Ready!${NC}"
    echo -e "${GREEN}📱 Admin: http://localhost:8000/admin/${NC}"
    echo -e "${GREEN}📚 API Docs: http://localhost:8000/api/docs/${NC}"
    echo -e "${GREEN}❤️ Health: http://localhost:8000/api/v1/tubewhale/health/${NC}"
    echo -e "${GREEN}🔐 Login: admin / admin123${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # Execute passed command
    exec "$@"
}

# If running this script directly
if [ "${1#-}" != "$1" ] || [ "${1%.py}" != "$1" ] || [ "$1" = "gunicorn" ] || [ "$1" = "python" ]; then
    main "$@"
elif [ "$1" = "runserver" ]; then
    # Run development server directly
    main python manage.py runserver 0.0.0.0:8001
elif [ "$1" = "wait_for_db" ]; then
    # Just wait for db and exit
    wait_for_db
else
    exec "$@"
fi
