"""
TubeWhale Engine API Views
提供RESTful API接口调用TubeWhale功能
"""

import logging
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import AllowAny

logger = logging.getLogger(__name__)


@api_view(['GET'])
@permission_classes([AllowAny])
def health_check_public(request):
    """公开健康检查 - 无需认证"""
    return Response({
        "status": "healthy",
        "message": "TubeWhale Engine is running",
        "engine_loaded": True,
        "agents_available": True,
        "version": "1.0.0",
        "timestamp": "2025-09-17",
        "auth_required": False
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def simple_ping(request):
    """最简单的ping测试 - 无需认证"""
    return Response({"ping": "pong", "auth_required": False})
