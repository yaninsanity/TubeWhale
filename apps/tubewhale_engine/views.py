"""
TubeWhale Engine API Views
提供RESTful API接口调用TubeWhale功能
"""

import logging
import json
from datetime import datetime
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework import status
from django.utils import timezone
from .models import CLICommandLog

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


@api_view(['POST', 'GET'])
@permission_classes([AllowAny])
def command_logs(request):
    """CLI命令日志端点 - 接收和查询CLI命令日志"""
    
    if request.method == 'POST':
        # Create new CLI log entry
        try:
            data = request.data
            
            # Create log entry
            log_entry = CLICommandLog.objects.create(
                command=data.get('command', ''),
                args=data.get('args', []),
                return_code=data.get('return_code', 0),
                success=data.get('success', False),
                stdout_truncated=CLICommandLog.truncate(data.get('stdout_truncated', '')),
                stderr_truncated=CLICommandLog.truncate(data.get('stderr_truncated', '')),
                duration_ms=data.get('duration_ms'),
                meta=data.get('meta', {}),
                ip_address=request.META.get('REMOTE_ADDR')
            )
            
            logger.info(f"CLI command logged via API: {log_entry.command}")
            
            return Response({
                'status': 'success',
                'message': 'CLI command logged successfully',
                'log_id': log_entry.id
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Failed to create CLI log: {e}")
            return Response({
                'status': 'error',
                'message': f'Failed to log CLI command: {str(e)}'
            }, status=status.HTTP_400_BAD_REQUEST)
    
    elif request.method == 'GET':
        # Get recent CLI logs
        try:
            limit = int(request.GET.get('limit', 10))
            limit = min(limit, 100)  # Cap at 100
            
            logs = CLICommandLog.objects.order_by('-created_at')[:limit]
            
            results = []
            for log in logs:
                results.append({
                    'id': log.id,
                    'command': log.command,
                    'args': log.args,
                    'success': log.success,
                    'return_code': log.return_code,
                    'duration_ms': log.duration_ms,
                    'created_at': log.created_at.isoformat(),
                    'stdout_preview': log.stdout_truncated[:100] if log.stdout_truncated else '',
                    'stderr_preview': log.stderr_truncated[:100] if log.stderr_truncated else ''
                })
            
            return Response({
                'status': 'success',
                'count': len(results),
                'results': results
            })
            
        except Exception as e:
            logger.error(f"Failed to get CLI logs: {e}")
            return Response({
                'status': 'error',
                'message': f'Failed to get CLI logs: {str(e)}'
            }, status=status.HTTP_400_BAD_REQUEST)
