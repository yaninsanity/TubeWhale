# 🎯 TubeWhale 快速使用指南

## 🚀 一键启动系统
```bash
./start_tubewhale.sh
```

## 🎬 快速分析视频
```bash
./quick-cli quick-analyze dQw4w9WgXcQ
```

## 📊 查看系统状态
```bash
./quick-cli status
```

## 🛑 停止系统
```bash
./stop_tubewhale.sh
```

## 📱 Web界面
- Dashboard: http://localhost:8000/dashboard/
- 分析任务管理: http://localhost:8000/dashboard/jobs/

## 🔧 故障排除

### 如果任务一直显示"Queued"：
1. 检查Celery Worker是否运行: `ps aux | grep celery`
2. 重启系统: `./stop_tubewhale.sh && ./start_tubewhale.sh`

### 如果API连接失败：
1. 检查Django服务器: `curl http://localhost:8000/api/v1/tubewhale/health/`
2. 查看日志: `tail django.log`

### 如果Job Retry失败：
1. 确保任务状态为"failed", "error", 或"cancelled"
2. 检查Django和Celery日志
3. ✅ **已修复**: Retry功能现在正常工作

### 日志文件位置：
- Django日志: `django.log`
- Celery日志: `celery.log`

## 🎁 最佳实践

1. **启动顺序**: 总是使用 `./start_tubewhale.sh` 来确保所有服务正确启动
2. **任务监控**: 通过Dashboard监控任务进度，不依赖CLI输出
3. **错误处理**: 遇到问题先检查日志文件，再重启相应服务

## ✅ 系统正常运行指标

- ✅ Django服务器响应健康检查
- ✅ Celery Worker处理任务
- ✅ Redis消息队列正常
- ✅ 新任务状态从"Queued"变为"Processing"

现在你的TubeWhale系统已经完全正常运行！🎉