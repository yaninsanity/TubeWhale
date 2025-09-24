"""
🎯 WebSocket路由配置
支持实时视频处理追踪和系统监控
"""

from django.urls import re_path
from . import websocket_consumers

websocket_urlpatterns = [
    # 视频处理实时追踪
    re_path(r'ws/video-processing/$', websocket_consumers.VideoProcessingConsumer.as_asgi()),
    
    # 系统监控
    re_path(r'ws/system-monitoring/$', websocket_consumers.SystemMonitoringConsumer.as_asgi()),
]