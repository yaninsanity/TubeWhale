#!/bin/bash

# TubeWhale 服务停止脚本
# 安全停止所有容器并清理资源

echo "🛑 TubeWhale 服务停止..."

# 显示当前运行的服务
echo "📊 当前运行的服务："
docker-compose ps

echo ""
read -p "确认停止所有服务？(y/N): " confirm
if [[ ! $confirm =~ ^[Yy]$ ]]; then
    echo "❌ 操作已取消"
    exit 0
fi

# 停止并移除容器
echo "⬇️  停止所有服务..."
docker-compose down

# 可选：清理数据卷
read -p "是否删除数据卷（会丢失所有数据）？(y/N): " cleanup_volumes
if [[ $cleanup_volumes =~ ^[Yy]$ ]]; then
    echo "🗑️  删除数据卷..."
    docker-compose down --volumes
    echo "⚠️  所有数据已删除"
fi

# 可选：清理镜像
read -p "是否删除构建的镜像？(y/N): " cleanup_images
if [[ $cleanup_images =~ ^[Yy]$ ]]; then
    echo "🧹 清理 Docker 镜像..."
    docker system prune -f
    echo "✅ 镜像清理完成"
fi

echo ""
echo "✅ 服务已停止"
echo "🚀 重新启动: ./quick-start.sh 或 ./start-docker.sh"
