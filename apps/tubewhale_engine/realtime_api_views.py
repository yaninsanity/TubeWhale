"""
🎯 实时视频处理API视图
提供CLI集成和前端实时追踪的API接口
"""

import json
import uuid
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
from datetime import datetime
import logging

from .video_status_manager import status_manager
from .database_event_monitor import database_monitor

logger = logging.getLogger(__name__)

@csrf_exempt
@require_http_methods(["POST"])
def start_video_processing(request):
    """
    🎯 启动视频处理 - CLI和前端都可以调用
    """
    try:
        data = json.loads(request.body)
        
        video_url = data.get('url')
        video_title = data.get('title', '')
        analysis_type = data.get('analysis_type', 'comprehensive')
        user_id = data.get('user_id', 'anonymous')
        
        if not video_url:
            return JsonResponse({
                'success': False,
                'error': '缺少视频URL'
            }, status=400)
        
        # 生成处理ID
        processing_id = str(uuid.uuid4())
        
        # 创建状态追踪
        if status_manager:
            video_status = status_manager.create_video_status(
                video_id=processing_id,
                url=video_url,
                title=video_title or f"视频_{processing_id[:8]}",
                user_id=user_id
            )
            
            if video_status:
                logger.info(f"🎯 启动视频处理: {processing_id}")
                
                # 模拟开始处理
                status_manager.update_video_status(processing_id, 
                    status="downloading", 
                    progress=5,
                    current_step="开始下载视频"
                )
                
                return JsonResponse({
                    'success': True,
                    'processing_id': processing_id,
                    'message': '视频处理已启动',
                    'status': video_status
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': '创建处理状态失败'
                }, status=500)
        else:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
            
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': '无效的JSON数据'
        }, status=400)
    except Exception as e:
        logger.error(f"❌ 启动视频处理失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_processing_status(request, processing_id):
    """
    🎯 获取处理状态
    """
    try:
        if status_manager:
            video_status = status_manager.get_video_status(processing_id)
            
            if video_status:
                return JsonResponse({
                    'success': True,
                    'status': video_status
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': '未找到处理状态'
                }, status=404)
        else:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ 获取处理状态失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_all_active_videos(request):
    """
    🎯 获取所有活跃的视频处理状态
    """
    try:
        if status_manager:
            active_videos = status_manager.get_all_active_videos()
            
            return JsonResponse({
                'success': True,
                'videos': active_videos,
                'count': len(active_videos)
            })
        else:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ 获取活跃视频失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def update_processing_status(request, processing_id):
    """
    🎯 更新处理状态 - 主要供CLI使用
    """
    try:
        data = json.loads(request.body)
        
        if status_manager:
            success = status_manager.update_video_status(processing_id, **data)
            
            if success:
                return JsonResponse({
                    'success': True,
                    'message': '状态更新成功'
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': '状态更新失败'
                }, status=500)
        else:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
            
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': '无效的JSON数据'
        }, status=400)
    except Exception as e:
        logger.error(f"❌ 更新处理状态失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def simulate_processing_progress(request, processing_id):
    """
    🎯 模拟处理进度 - 测试用途
    """
    try:
        if not status_manager:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
        
        # 模拟处理流程
        stages = [
            (10, "downloading", "下载视频文件"),
            (25, "processing", "提取音频"),
            (40, "transcribing", "生成转录文本"),
            (60, "analyzing", "分析音频特征"),
            (80, "summarizing", "生成内容摘要"),
            (100, "completed", "处理完成")
        ]
        
        import threading
        import time
        
        def simulate_progress():
            for progress, status, step in stages:
                status_manager.update_video_status(processing_id,
                    status=status,
                    progress=progress,
                    current_step=step
                )
                time.sleep(2)  # 每个阶段等待2秒
            
            # 标记完成
            status_manager.mark_completed(processing_id, {
                'transcript': '模拟转录结果',
                'summary': '模拟摘要结果',
                'audio_analysis': '模拟音频分析结果'
            })
        
        # 在后台线程中运行模拟
        thread = threading.Thread(target=simulate_progress)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            'success': True,
            'message': '开始模拟处理进度'
        })
        
    except Exception as e:
        logger.error(f"❌ 模拟处理进度失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_processing_statistics(request):
    """
    🎯 获取处理统计信息
    """
    try:
        if status_manager:
            stats = status_manager.get_processing_statistics()
            
            return JsonResponse({
                'success': True,
                'statistics': stats
            })
        else:
            return JsonResponse({
                'success': False,
                'error': '状态管理器未可用'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ 获取处理统计失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_database_monitor_stats(request):
    """
    🎯 获取数据库监控统计
    """
    try:
        if database_monitor:
            stats = database_monitor.get_monitoring_stats()
            
            return JsonResponse({
                'success': True,
                'monitor_stats': stats
            })
        else:
            return JsonResponse({
                'success': False,
                'error': '数据库监控器未可用'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ 获取数据库监控统计失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def trigger_database_check(request):
    """
    🎯 手动触发数据库变化检查
    """
    try:
        if database_monitor:
            database_monitor.manually_check_changes()
            
            return JsonResponse({
                'success': True,
                'message': '数据库检查已触发'
            })
        else:
            return JsonResponse({
                'success': False,
                'error': '数据库监控器未可用'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ 触发数据库检查失败: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)