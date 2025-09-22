"""
TubeWhale Engine URLs
"""

from django.urls import path
from . import views

app_name = 'tubewhale_engine'

urlpatterns = [
    # 公开健康检查 - 无需认证
    path('api/v1/tubewhale/health/', views.health_check_public, name='health-check-public'),
    # 简单ping测试 - 无需认证
    path('api/v1/tubewhale/ping/', views.simple_ping, name='simple-ping'),
    # CLI命令日志端点 - 无需认证
    path('api/v1/tubewhale/command-logs/', views.command_logs, name='command-logs'),
]
