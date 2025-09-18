#!/bin/bash

# TubeWhale Docker 启动脚本
# 一键启动完整的生产环境

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

print_header() {
    echo -e "${PURPLE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                     TubeWhale Backend                        ║"
    echo "║                   Docker 自动化启动                         ║"
    echo "║                                                              ║"
    echo "║  🐳 多容器编排 | 🔧 最佳实践 | 🚀 生产就绪                 ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# 检查 Docker 环境
check_docker() {
    print_message "$BLUE" "🔍 检查 Docker 环境..."
    
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "❌ Docker 未安装，请先安装 Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_message "$RED" "❌ Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_message "$RED" "❌ Docker 服务未运行，请启动 Docker"
        exit 1
    fi
    
    print_message "$GREEN" "✅ Docker 环境检查通过"
}

# 创建必要的目录
create_directories() {
    print_message "$BLUE" "📁 创建必要的目录..."
    
    directories=(
        "media"
        "logs"
        "ssl"
        "staticfiles"
    )
    
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_message "$GREEN" "✅ 创建目录: $dir"
        fi
    done
}

# 生成环境配置文件
generate_env_file() {
    if [ ! -f ".env" ]; then
        print_message "$BLUE" "⚙️  生成环境配置文件..."
        
        cat > .env << EOF
# Django 配置
DJANGO_SETTINGS_MODULE=tubewhale_project.settings
DEBUG=false
SECRET_KEY=$(python3 -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())')
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0

# 数据库配置
DB_ENGINE=django.db.backends.postgresql
DB_NAME=tubewhale
DB_USER=tubewhale
DB_PASSWORD=tubewhale123
DB_HOST=postgres
DB_PORT=5432

# Redis 配置
REDIS_URL=redis://:redis123@redis:6379/0
CELERY_BROKER_URL=redis://:redis123@redis:6379/0
CELERY_RESULT_BACKEND=redis://:redis123@redis:6379/0

# 时区和语言
LANGUAGE_CODE=zh-hans
TIME_ZONE=Asia/Shanghai
USE_I18N=true
USE_TZ=true
EOF
        
        print_message "$GREEN" "✅ 环境配置文件生成完成"
    else
        print_message "$YELLOW" "⚠️  环境配置文件已存在，跳过生成"
    fi
}

# 构建 Docker 镜像
build_images() {
    print_message "$BLUE" "🏗️  构建 Docker 镜像..."
    
    docker-compose build --no-cache backend
    
    if [ $? -eq 0 ]; then
        print_message "$GREEN" "✅ 镜像构建成功"
    else
        print_message "$RED" "❌ 镜像构建失败"
        exit 1
    fi
}

# 启动服务
start_services() {
    print_message "$BLUE" "🚀 启动 Docker 服务..."
    
    # 启动基础服务 (数据库和缓存)
    print_message "$CYAN" "  📊 启动数据库和缓存服务..."
    docker-compose up -d postgres redis
    
    # 等待数据库就绪
    print_message "$CYAN" "  ⏳ 等待数据库就绪..."
    sleep 15
    
    # 启动后端服务
    print_message "$CYAN" "  🖥️  启动后端服务..."
    docker-compose up -d backend
    
    # 启动 Celery 服务
    print_message "$CYAN" "  ⚡ 启动异步任务服务..."
    docker-compose up -d celery_worker celery_beat
    
    # 启动 Nginx (可选)
    if [ "$1" = "--with-nginx" ]; then
        print_message "$CYAN" "  🌐 启动 Nginx 反向代理..."
        docker-compose up -d nginx
    fi
    
    print_message "$GREEN" "✅ 所有服务启动成功"
}

# 显示服务状态
show_status() {
    print_message "$BLUE" "📋 服务状态:"
    echo ""
    docker-compose ps
    echo ""
    
    print_message "$BLUE" "🌐 访问地址:"
    echo -e "${GREEN}  • Django Admin: ${CYAN}http://localhost:8000/admin/${NC}"
    echo -e "${GREEN}  • API 接口: ${CYAN}http://localhost:8000/api/${NC}"
    echo -e "${GREEN}  • 健康检查: ${CYAN}http://localhost:8000/api/v1/tubewhale/health/${NC}"
    echo ""
    
    print_message "$BLUE" "📊 数据库连接:"
    echo -e "${GREEN}  • PostgreSQL: ${CYAN}localhost:5432${NC}"
    echo -e "${GREEN}  • Redis: ${CYAN}localhost:6379${NC}"
    echo ""
}

# 显示日志
show_logs() {
    print_message "$BLUE" "📝 查看实时日志 (Ctrl+C 退出):"
    docker-compose logs -f backend
}

# 停止服务
stop_services() {
    print_message "$BLUE" "🛑 停止所有服务..."
    docker-compose down
    print_message "$GREEN" "✅ 服务已停止"
}

# 清理资源
cleanup() {
    print_message "$BLUE" "🧹 清理 Docker 资源..."
    docker-compose down -v --remove-orphans
    docker system prune -f
    print_message "$GREEN" "✅ 清理完成"
}

# 主函数
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
            print_message "$YELLOW" "💡 使用 './start-docker.sh logs' 查看实时日志"
            print_message "$YELLOW" "💡 使用 './start-docker.sh stop' 停止服务"
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
            echo "使用方法: $0 [start|stop|restart|logs|status|cleanup|build] [--with-nginx]"
            echo ""
            echo "命令说明:"
            echo "  start    - 启动所有服务 (默认)"
            echo "  stop     - 停止所有服务"
            echo "  restart  - 重启所有服务"
            echo "  logs     - 查看实时日志"
            echo "  status   - 显示服务状态"
            echo "  build    - 重新构建镜像"
            echo "  cleanup  - 清理所有 Docker 资源"
            echo ""
            echo "选项:"
            echo "  --with-nginx  - 同时启动 Nginx 反向代理"
            exit 1
            ;;
    esac
}

# 信号处理
trap 'print_message "$RED" "❌ 脚本被中断"; exit 1' INT TERM

# 执行主函数
main "$@"
