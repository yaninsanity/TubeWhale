#!/bin/bash

# TubeWhale 快速启动脚本
# 快速启动所有服务，用于开发和测试

echo "🚀 TubeWhale 快速启动..."

# 检查Docker
if ! docker info > /dev/null 2>&1; then
    echo "❌ 请先启动 Docker"
    exit 1
fi

# 启动服务
echo "⬆️  启动所有服务..."
docker-compose up -d

# 等待启动
echo "⏳ 等待服务启动 (30秒)..."
sleep 30

# 显示状态
echo "📊 服务状态："
docker-compose ps

echo ""
echo "✅ 启动完成！"
echo "🌐 Django Admin: http://localhost:8000/admin/"
echo "📋 查看日志: docker-compose logs -f"
echo "🛑 停止服务: docker-compose down"
