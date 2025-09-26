#!/bin/bash

# ============================================================================
# TubeWhale Environment Manager
# 环境变量配置管理器 - 最佳工程实践
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.docker"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}🔧 TubeWhale Environment Manager${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 检查环境文件
check_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        print_warning "环境文件不存在，从模板创建..."
        cp .env.production .env.docker
        print_success "环境文件创建完成"
    fi
}

# 显示当前配置
show_config() {
    print_header
    echo "📊 当前环境配置状态："
    echo ""
    
    if [ -f "$ENV_FILE" ]; then
        # 检查关键配置
        echo "🔑 API密钥配置："
        if grep -q "your-openai-api-key-here" "$ENV_FILE"; then
            print_warning "OpenAI API Key: 未配置（使用演示模式）"
        else
            print_success "OpenAI API Key: 已配置"
        fi
        
        if grep -q "your-youtube-api-key-here" "$ENV_FILE"; then
            print_warning "YouTube API Key: 未配置（使用演示模式）"
        else
            print_success "YouTube API Key: 已配置"
        fi
        
        echo ""
        echo "🗄️  数据库配置："
        DB_NAME=$(grep "POSTGRES_DB=" "$ENV_FILE" | cut -d'=' -f2)
        print_success "数据库名称: $DB_NAME"
        
        echo ""
        echo "📊 Redis配置："
        REDIS_URL=$(grep "REDIS_URL=" "$ENV_FILE" | cut -d'=' -f2)
        print_success "Redis URL: $REDIS_URL"
        
        echo ""
        echo "🚀 服务配置："
        DEBUG=$(grep "DJANGO_DEBUG=" "$ENV_FILE" | cut -d'=' -f2)
        if [ "$DEBUG" = "true" ]; then
            print_warning "调试模式: 开启"
        else
            print_success "调试模式: 关闭（生产模式）"
        fi
        
    else
        print_error "环境文件不存在"
    fi
}

# 配置API密钥
configure_api_keys() {
    print_header
    echo "🔑 配置API密钥"
    echo ""
    
    check_env_file
    
    # OpenAI API Key
    echo "🤖 OpenAI API Key配置"
    current_openai=$(grep "OPENAI_API_KEY=" "$ENV_FILE" | cut -d'=' -f2)
    if [ "$current_openai" != "your-openai-api-key-here" ] && [ ! -z "$current_openai" ]; then
        echo "当前已配置OpenAI API Key"
        read -p "是否要更新？(y/N): " update_openai
    else
        update_openai="y"
    fi
    
    if [[ $update_openai =~ ^[Yy]$ ]]; then
        read -p "请输入OpenAI API Key (留空跳过): " new_openai_key
        if [ ! -z "$new_openai_key" ]; then
            sed -i.bak "s|OPENAI_API_KEY=.*|OPENAI_API_KEY=$new_openai_key|" "$ENV_FILE"
            print_success "OpenAI API Key已更新"
        fi
    fi
    
    # YouTube API Key
    echo ""
    echo "📺 YouTube API Key配置"
    current_youtube=$(grep "YOUTUBE_API_KEY=" "$ENV_FILE" | cut -d'=' -f2)
    if [ "$current_youtube" != "your-youtube-api-key-here" ] && [ ! -z "$current_youtube" ]; then
        echo "当前已配置YouTube API Key"
        read -p "是否要更新？(y/N): " update_youtube
    else
        update_youtube="y"
    fi
    
    if [[ $update_youtube =~ ^[Yy]$ ]]; then
        read -p "请输入YouTube API Key (留空跳过): " new_youtube_key
        if [ ! -z "$new_youtube_key" ]; then
            sed -i.bak "s|YOUTUBE_API_KEY=.*|YOUTUBE_API_KEY=$new_youtube_key|" "$ENV_FILE"
            print_success "YouTube API Key已更新"
        fi
    fi
    
    # 清理备份文件
    rm -f "$ENV_FILE.bak"
    
    print_success "API密钥配置完成"
}

# 生成安全密钥
generate_secrets() {
    print_header
    echo "🔐 生成安全密钥"
    echo ""
    
    check_env_file
    
    # Django Secret Key
    echo "生成Django Secret Key..."
    DJANGO_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(50))")
    sed -i.bak "s|DJANGO_SECRET_KEY=.*|DJANGO_SECRET_KEY=$DJANGO_SECRET|" "$ENV_FILE"
    print_success "Django Secret Key已生成"
    
    # JWT Secret
    echo "生成JWT Secret..."
    JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    sed -i.bak "s|JWT_SECRET_KEY=.*|JWT_SECRET_KEY=$JWT_SECRET|" "$ENV_FILE"
    print_success "JWT Secret已生成"
    
    # Redis密码
    echo "生成Redis密码..."
    REDIS_PASSWORD=$(python3 -c "import secrets; print(secrets.token_urlsafe(16))")
    sed -i.bak "s|REDIS_PASSWORD=.*|REDIS_PASSWORD=$REDIS_PASSWORD|" "$ENV_FILE"
    sed -i.bak "s|redis://.*@redis:6379/0|redis://:$REDIS_PASSWORD@redis:6379/0|" "$ENV_FILE"
    print_success "Redis密码已生成"
    
    # 清理备份文件
    rm -f "$ENV_FILE.bak"
    
    print_success "所有安全密钥已生成"
}

# 验证配置
validate_config() {
    print_header
    echo "🔍 验证环境配置"
    echo ""
    
    if [ ! -f "$ENV_FILE" ]; then
        print_error "环境文件不存在"
        return 1
    fi
    
    # 检查必要的配置项
    required_vars=(
        "DJANGO_SECRET_KEY"
        "POSTGRES_DB"
        "POSTGRES_USER"
        "POSTGRES_PASSWORD"
        "REDIS_URL"
        "CELERY_BROKER_URL"
    )
    
    all_valid=true
    for var in "${required_vars[@]}"; do
        if grep -q "^$var=" "$ENV_FILE"; then
            value=$(grep "^$var=" "$ENV_FILE" | cut -d'=' -f2)
            if [ ! -z "$value" ] && [ "$value" != "your-value-here" ]; then
                print_success "$var: 已配置"
            else
                print_error "$var: 未配置或使用默认值"
                all_valid=false
            fi
        else
            print_error "$var: 缺失"
            all_valid=false
        fi
    done
    
    if $all_valid; then
        print_success "所有必要配置项验证通过"
        return 0
    else
        print_error "配置验证失败"
        return 1
    fi
}

# 重置配置
reset_config() {
    print_header
    echo "🔄 重置环境配置"
    echo ""
    
    read -p "确定要重置所有环境配置吗？这将覆盖现有配置 (y/N): " confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        cp .env.production .env.docker
        print_success "环境配置已重置"
    else
        echo "取消重置操作"
    fi
}

# 显示帮助
show_help() {
    print_header
    echo "📚 使用说明："
    echo ""
    echo "  $0 show        - 显示当前配置状态"
    echo "  $0 config      - 配置API密钥"
    echo "  $0 generate    - 生成安全密钥"
    echo "  $0 validate    - 验证配置"
    echo "  $0 reset       - 重置配置"
    echo "  $0 help        - 显示帮助"
    echo ""
}

# 主函数
main() {
    case "${1:-help}" in
        "show")
            show_config
            ;;
        "config")
            configure_api_keys
            ;;
        "generate")
            generate_secrets
            ;;
        "validate")
            validate_config
            ;;
        "reset")
            reset_config
            ;;
        "help"|*)
            show_help
            ;;
    esac
}

# 执行主函数
main "$@"