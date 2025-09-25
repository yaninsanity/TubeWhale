#!/usr/bin/env python3
"""
Enhanced Task API Views
支持暂停/恢复任务和实时结果下载的API接口
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from django.http import JsonResponse, HttpResponse, Http404
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.conf import settings

from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from service.enhanced_task_manager import get_task_manager, EnhancedTask, TaskPriority, TaskStatus

logger = logging.getLogger(__name__)


class EnhancedTaskAPI:
    """增强的任务API类"""
    
    def __init__(self):
        self.task_manager = get_task_manager()
    
    def submit_task(self, user_id: int, command: str, args: list, 
                   template_id: str = "", expert_slug: str = "", 
                   variables: dict = None, priority: str = "normal") -> Dict[str, Any]:
        """提交新任务"""
        try:
            # 解析优先级
            priority_map = {
                'low': TaskPriority.LOW,
                'normal': TaskPriority.NORMAL, 
                'high': TaskPriority.HIGH,
                'critical': TaskPriority.CRITICAL,
                'system': TaskPriority.SYSTEM
            }
            task_priority = priority_map.get(priority.lower(), TaskPriority.NORMAL)
            
            # 创建任务
            task = EnhancedTask(
                task_id=f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user_id}",
                command=command,
                args=args,
                template_id=template_id,
                expert_slug=expert_slug,
                variables=variables or {},
                priority=task_priority,
                user_id=user_id
            )
            
            # 提交任务
            task_id = self.task_manager.submit_task(task)
            
            return {
                'success': True,
                'task_id': task_id,
                'status': 'queued',
                'message': 'Task submitted successfully'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to submit task: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Failed to submit task'
            }
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        return self.task_manager.get_task_status(task_id)
    
    def pause_task(self, task_id: str) -> Dict[str, Any]:
        """暂停任务"""
        success = self.task_manager.pause_task(task_id)
        return {
            'success': success,
            'message': 'Task paused' if success else 'Failed to pause task'
        }
    
    def resume_task(self, task_id: str) -> Dict[str, Any]:
        """恢复任务"""
        success = self.task_manager.resume_task(task_id)
        return {
            'success': success,
            'message': 'Task resumed' if success else 'Failed to resume task'
        }
    
    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """取消任务"""
        success = self.task_manager.cancel_task(task_id)
        return {
            'success': success,
            'message': 'Task cancelled' if success else 'Failed to cancel task'
        }
    
    def get_partial_results(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取部分结果"""
        return self.task_manager.get_partial_results(task_id)
    
    def download_results(self, task_id: str, format_type: str = 'json') -> Optional[bytes]:
        """下载结果"""
        return self.task_manager.download_results(task_id, format_type)
    
    def get_queue_status(self) -> Dict[str, Any]:
        """获取队列状态"""
        return self.task_manager.get_queue_status()


# 全局API实例
enhanced_task_api = EnhancedTaskAPI()


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def submit_enhanced_task(request):
    """提交增强任务API"""
    try:
        data = request.data
        
        # 验证必需参数
        command = data.get('command')
        if not command:
            return Response({
                'success': False,
                'error': 'command is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 提交任务
        result = enhanced_task_api.submit_task(
            user_id=request.user.id,
            command=command,
            args=data.get('args', []),
            template_id=data.get('template_id', ''),
            expert_slug=data.get('expert_slug', ''),
            variables=data.get('variables', {}),
            priority=data.get('priority', 'normal')
        )
        
        if result['success']:
            return Response(result, status=status.HTTP_201_CREATED)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"❌ Submit task API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_enhanced_task_status(request, task_id):
    """获取任务状态API"""
    try:
        task_status = enhanced_task_api.get_task_status(task_id)
        
        if task_status:
            return Response({
                'success': True,
                'task_status': task_status
            })
        else:
            return Response({
                'success': False,
                'error': 'Task not found'
            }, status=status.HTTP_404_NOT_FOUND)
            
    except Exception as e:
        logger.error(f"❌ Get task status API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def pause_enhanced_task(request, task_id):
    """暂停任务API"""
    try:
        result = enhanced_task_api.pause_task(task_id)
        
        if result['success']:
            return Response(result)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"❌ Pause task API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def resume_enhanced_task(request, task_id):
    """恢复任务API"""
    try:
        result = enhanced_task_api.resume_task(task_id)
        
        if result['success']:
            return Response(result)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"❌ Resume task API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def cancel_enhanced_task(request, task_id):
    """取消任务API"""
    try:
        result = enhanced_task_api.cancel_task(task_id)
        
        if result['success']:
            return Response(result)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"❌ Cancel task API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_enhanced_task_partial_results(request, task_id):
    """获取任务部分结果API"""
    try:
        partial_results = enhanced_task_api.get_partial_results(task_id)
        
        if partial_results:
            return Response({
                'success': True,
                'partial_results': partial_results
            })
        else:
            return Response({
                'success': False,
                'error': 'Task not found or no partial results available'
            }, status=status.HTTP_404_NOT_FOUND)
            
    except Exception as e:
        logger.error(f"❌ Get partial results API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def download_enhanced_task_results(request, task_id):
    """下载任务结果API"""
    try:
        format_type = request.GET.get('format', 'json')
        result_data = enhanced_task_api.download_results(task_id, format_type)
        
        if result_data:
            # 设置响应头
            response = HttpResponse(
                result_data,
                content_type='application/json' if format_type == 'json' else 'application/octet-stream'
            )
            response['Content-Disposition'] = f'attachment; filename="task_{task_id}_results.{format_type}"'
            return response
        else:
            return JsonResponse({
                'success': False,
                'error': 'Task not found or no results available'
            }, status=404)
            
    except Exception as e:
        logger.error(f"❌ Download results API error: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_enhanced_queue_status(request):
    """获取队列状态API"""
    try:
        queue_status = enhanced_task_api.get_queue_status()
        
        return Response({
            'success': True,
            'queue_status': queue_status
        })
        
    except Exception as e:
        logger.error(f"❌ Get queue status API error: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# WebSocket支持用于实时状态更新
class TaskWebSocketConsumer:
    """WebSocket消费者，用于实时任务状态更新"""
    
    def __init__(self):
        self.task_manager = get_task_manager()
    
    def connect(self, websocket):
        """WebSocket连接"""
        websocket.accept()
        
    def disconnect(self, websocket, close_code):
        """WebSocket断开连接"""
        pass
    
    def receive(self, websocket, text_data):
        """接收WebSocket消息"""
        try:
            data = json.loads(text_data)
            action = data.get('action')
            task_id = data.get('task_id')
            
            if action == 'subscribe' and task_id:
                # 订阅任务状态更新
                task_status = self.task_manager.get_task_status(task_id)
                if task_status:
                    websocket.send(text_data=json.dumps({
                        'action': 'status_update',
                        'task_id': task_id,
                        'status': task_status
                    }))
                else:
                    websocket.send(text_data=json.dumps({
                        'action': 'error',
                        'message': 'Task not found'
                    }))
            
            elif action == 'unsubscribe':
                # 取消订阅
                websocket.send(text_data=json.dumps({
                    'action': 'unsubscribed',
                    'task_id': task_id
                }))
                
        except Exception as e:
            logger.error(f"❌ WebSocket receive error: {e}")
            websocket.send(text_data=json.dumps({
                'action': 'error',
                'message': str(e)
            }))


# Django视图类
@method_decorator(login_required, name='dispatch')
class EnhancedTaskView(View):
    """增强任务管理视图"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = enhanced_task_api
    
    def get(self, request, task_id=None):
        """GET请求 - 获取任务状态或队列状态"""
        if task_id:
            # 获取特定任务状态
            task_status = self.api.get_task_status(task_id)
            if task_status:
                return JsonResponse({
                    'success': True,
                    'task_status': task_status
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': 'Task not found'
                }, status=404)
        else:
            # 获取队列状态
            queue_status = self.api.get_queue_status()
            return JsonResponse({
                'success': True,
                'queue_status': queue_status
            })
    
    def post(self, request, task_id=None):
        """POST请求 - 创建任务或控制任务"""
        try:
            data = json.loads(request.body.decode('utf-8'))
            
            if task_id:
                # 任务控制操作
                action = data.get('action')
                
                if action == 'pause':
                    result = self.api.pause_task(task_id)
                elif action == 'resume':
                    result = self.api.resume_task(task_id)
                elif action == 'cancel':
                    result = self.api.cancel_task(task_id)
                else:
                    return JsonResponse({
                        'success': False,
                        'error': 'Invalid action'
                    }, status=400)
                
                return JsonResponse(result)
            else:
                # 创建新任务
                result = self.api.submit_task(
                    user_id=request.user.id,
                    command=data.get('command'),
                    args=data.get('args', []),
                    template_id=data.get('template_id', ''),
                    expert_slug=data.get('expert_slug', ''),
                    variables=data.get('variables', {}),
                    priority=data.get('priority', 'normal')
                )
                
                status_code = 201 if result['success'] else 400
                return JsonResponse(result, status=status_code)
                
        except Exception as e:
            logger.error(f"❌ Enhanced task view error: {e}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(login_required, name='dispatch')
class TaskResultsDownloadView(View):
    """任务结果下载视图"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = enhanced_task_api
    
    def get(self, request, task_id):
        """下载任务结果"""
        try:
            format_type = request.GET.get('format', 'json')
            partial = request.GET.get('partial', 'false').lower() == 'true'
            
            if partial:
                # 下载部分结果
                partial_results = self.api.get_partial_results(task_id)
                if partial_results:
                    result_data = json.dumps(partial_results, indent=2, ensure_ascii=False).encode('utf-8')
                    response = HttpResponse(result_data, content_type='application/json')
                    response['Content-Disposition'] = f'attachment; filename="task_{task_id}_partial.json"'
                    return response
                else:
                    raise Http404("Task not found or no partial results available")
            else:
                # 下载完整结果
                result_data = self.api.download_results(task_id, format_type)
                if result_data:
                    response = HttpResponse(
                        result_data,
                        content_type='application/json' if format_type == 'json' else 'application/octet-stream'
                    )
                    response['Content-Disposition'] = f'attachment; filename="task_{task_id}_results.{format_type}"'
                    return response
                else:
                    raise Http404("Task not found or no results available")
                    
        except Exception as e:
            logger.error(f"❌ Download results error: {e}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)