#!/bin/bash
# TubeWhale Docker Entrypoint Script
# 启动前的初始化工作

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐋 TubeWhale Backend Starting...${NC}"

# 等待数据库就绪
wait_for_db() {
    echo -e "${YELLOW}⏳ Waiting for database...${NC}"
    
    # 如果使用PostgreSQL
    # Compose passes DB_ENGINE=django.db.backends.postgresql
    if echo "$DB_ENGINE" | grep -qi "postgres"; then
        while ! python -c "
import psycopg2
import os
try:
    psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', '5432'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', ''),
        database=os.environ.get('DB_NAME', 'tubewhale')
    )
    print('Database is ready!')
except:
    exit(1)
" 2>/dev/null; do
            echo -e "${YELLOW}⏳ PostgreSQL is unavailable - sleeping${NC}"
            sleep 2
        done
    fi
    
    # 如果使用MySQL
    if echo "$DB_ENGINE" | grep -qi "mysql"; then
        while ! python -c "
import MySQLdb
import os
try:
    MySQLdb.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', '3306')),
        user=os.environ.get('DB_USER', 'root'),
        passwd=os.environ.get('DB_PASSWORD', ''),
        db=os.environ.get('DB_NAME', 'tubewhale')
    )
    print('Database is ready!')
except:
    exit(1)
" 2>/dev/null; do
            echo -e "${YELLOW}⏳ MySQL is unavailable - sleeping${NC}"
            sleep 2
        done
    fi
    
    echo -e "${GREEN}✅ Database is ready!${NC}"
}

# 运行数据库迁移
run_migrations() {
    echo -e "${BLUE}🔄 Running database migrations...${NC}"
        python manage.py migrate --noinput || {
            echo -e "${RED}❌ Migration failed, retrying in 5s...${NC}"; sleep 5; python manage.py migrate --noinput; }
    echo -e "${GREEN}✅ Migrations completed!${NC}"
}

# 创建超级用户
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

# 收集静态文件
collect_static() {
    echo -e "${BLUE}📁 Collecting static files...${NC}"
    python manage.py collectstatic --noinput --clear || true
    echo -e "${GREEN}✅ Static files collected!${NC}"
}

# 编译翻译文件
compile_messages() {
    echo -e "${BLUE}🌐 Compiling translation messages...${NC}"
    python manage.py compilemessages || true
    echo -e "${GREEN}✅ Translation messages compiled!${NC}"
}

# 主函数
main() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}🐋 TubeWhale Django Backend Initialization${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    # 等待数据库
    if [ "$WAIT_FOR_DB" = "true" ]; then
        wait_for_db
    fi
    
    # 运行迁移
    run_migrations
    
    # 创建超级用户
    create_superuser
    
    # 收集静态文件
    collect_static
    
    # 编译翻译
    compile_messages
    
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}🎉 TubeWhale Backend Ready!${NC}"
    echo -e "${GREEN}📱 Admin: http://localhost:8000/admin/${NC}"
    echo -e "${GREEN}📚 API Docs: http://localhost:8000/api/docs/${NC}"
    echo -e "${GREEN}❤️ Health: http://localhost:8000/api/v1/tubewhale/health/${NC}"
    echo -e "${GREEN}🔐 Login: admin / admin123${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # 执行传入的命令
    exec "$@"
}

# 如果直接运行此脚本
if [ "${1#-}" != "$1" ] || [ "${1%.py}" != "$1" ] || [ "$1" = "gunicorn" ] || [ "$1" = "python" ]; then
    main "$@"
else
    exec "$@"
fi
