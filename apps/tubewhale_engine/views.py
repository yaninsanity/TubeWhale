"""
TubeWhale Engine Views - Clean Version
"""

from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
import logging
import json

logger = logging.getLogger(__name__)

# ============================================================================
# HEALTH CHECK ENDPOINTS
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def health_check_public(request):
    """Public health check endpoint"""
    try:
        from agents.summarizer_agent import SummarizerAgent
        
        return JsonResponse({
            'status': 'healthy',
            'timestamp': timezone.now().isoformat(),
            'engine_loaded': True,
            'agents_available': True,
            'version': '2.0.0'
        })
    except Exception as e:
        return JsonResponse({
            'status': 'degraded',
            'timestamp': timezone.now().isoformat(),
            'engine_loaded': False,
            'error': str(e)
        }, status=500)

@api_view(['GET'])
def simple_ping(request):
    """Simple ping endpoint"""
    return Response({'status': 'pong', 'timestamp': timezone.now().isoformat()})

@api_view(['POST', 'GET'])
@permission_classes([AllowAny])
def command_logs(request):
    """Get command execution logs"""
    try:
        from .models import CLICommandLog
        
        logs = CLICommandLog.objects.all().order_by('-created_at')[:50]
        log_data = []
        
        for log in logs:
            log_data.append({
                'id': log.id,
                'command': log.command,
                'success': log.success,
                'created_at': log.created_at.isoformat(),
                'duration_ms': log.duration_ms,
                'return_code': log.return_code,
                'args': log.args,
                'stdout_truncated': log.stdout_truncated[:200] + '...' if len(log.stdout_truncated) > 200 else log.stdout_truncated
            })
        
        return Response({
            'status': 'success',
            'logs': log_data,
            'total': len(log_data)
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=500)

# ============================================================================
# VIDEO ANALYSIS API
# ============================================================================

@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_video(request):
    """Enhanced video analysis API with async Celery processing"""
    
    try:
        from .models import AnalysisJob, CLICommandLog
        from .tasks import analyze_video_task
        import time
        
        data = request.data
        
        # 验证必需参数
        video_id = data.get('video_id')
        expert_role = data.get('expert_role', 'content_creator')
        template_name = data.get('template', 'basic_performance')
        output_format = data.get('output_format', 'json')
        language = data.get('language', 'zh')
        
        # 验证视频ID
        if not video_id:
            return Response({
                'status': 'error',
                'message': 'video_id is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        logger.info(f"Starting video analysis: {video_id} with role {expert_role}")
        
        # 创建分析任务记录
        analysis_job = AnalysisJob.objects.create(
            content_id=video_id,
            expert_role=expert_role,
            template_id=template_name,
            analysis_type='video',
            status='queued',
            status_message='任务已创建，等待处理...',
            content_url=f'https://www.youtube.com/watch?v={video_id}',
            content_title=f'Video Analysis: {video_id}',
            analysis_options={
                'output_format': output_format,
                'language': language
            }
        )
        
        # 记录CLI命令（如果需要）
        cli_log = CLICommandLog.objects.create(
            command='analyze_video',
            args=[video_id, f'--role={expert_role}', f'--template={template_name}'],
            success=True,
            return_code=0,
            stdout_truncated=f'Analysis task created for {video_id}',
            duration_ms=10,
            ip_address=request.META.get('REMOTE_ADDR'),
            meta={
                'job_id': analysis_job.job_id,
                'api_request': True,
                'output_format': output_format,
                'language': language
            }
        )
        
        logger.info(f"Created analysis job: {analysis_job.job_id}")
        
        # 尝试启动异步任务
        try:
            from celery import current_app
            from django.conf import settings
            
            # 检查Celery是否可用
            celery_available = False
            try:
                # 检查Celery worker是否在线
                inspector = current_app.control.inspect()
                active_workers = inspector.active()
                if active_workers:
                    celery_available = True
                    logger.info("Celery workers detected, using async processing")
                else:
                    logger.warning("No Celery workers detected, falling back to demo mode")
            except Exception as celery_check_error:
                logger.warning(f"Celery check failed: {celery_check_error}, using demo mode")
            
            if celery_available:
                # 异步处理
                task_result = analyze_video_task.delay(
                    job_id=analysis_job.job_id
                )
                
                # 保存Celery任务ID以便跟踪
                analysis_job.celery_task_id = task_result.id
                analysis_job.status = 'processing'
                analysis_job.status_message = 'Processing in background...'
                analysis_job.save()
                
                logger.info(f"Async task started: {task_result.id} for job {analysis_job.job_id}")
                
            else:
                # 同步处理（演示模式）
                logger.info("Using synchronous demo processing")
                
                analysis_job.status = 'processing'
                analysis_job.status_message = '正在分析中（演示模式）...'
                analysis_job.save()
                
                # 模拟分析过程
                import random
                from apps.tubewhale_engine.models import AnalysisResult
                
                # 更新进度
                for progress in [25, 50, 75]:
                    analysis_job.progress = progress
                    analysis_job.status_message = f'正在分析中... {progress}%'
                    analysis_job.save()
                    time.sleep(1)  # 模拟处理时间
                
                # 创建演示结果
                demo_result = {
                    "summary": f"这是对视频 {video_id} 的演示分析结果。",
                    "key_points": [
                        "内容创作质量较高",
                        "观众参与度良好",
                        "建议优化标题和缩略图"
                    ],
                    "metrics": {
                        "content_quality": random.randint(70, 95),
                        "engagement_score": random.randint(60, 90),
                        "optimization_potential": random.randint(80, 100)
                    },
                    "recommendations": [
                        "继续保持内容质量",
                        "增加观众互动元素",
                        "优化发布时间"
                    ],
                    "confidence_score": random.randint(85, 98)
                }
                
                # 保存分析结果
                AnalysisResult.objects.create(
                    job=analysis_job,
                    raw_data=demo_result,
                    file_path=f'analysis/{analysis_job.job_id}.json'
                )
                
                analysis_job.status = 'completed'
                analysis_job.progress = 100
                analysis_job.status_message = '分析完成（演示模式）'
                analysis_job.completed_at = timezone.now()
                analysis_job.save()
                
                logger.info(f"Demo analysis completed for job {analysis_job.job_id}")
                
        except Exception as process_error:
            logger.error(f"Failed to process analysis: {process_error}")
            analysis_job.status = 'failed'
            analysis_job.error_message = str(process_error)
            analysis_job.status_message = f"处理失败: {str(process_error)}"
            analysis_job.save()
            
            return Response({
                'status': 'error',
                'message': f'Failed to start analysis: {str(process_error)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # 构建响应数据
        response_data = {
            'status': 'success',
            'message': 'Analysis task created successfully',
            'job_id': analysis_job.job_id,
            'current_status': analysis_job.status,
            'progress': analysis_job.progress or 0,
            'status_message': analysis_job.status_message,
            'estimated_time': '2-5 minutes',
            'api_endpoints': {
                'progress': f'/api/analysis/{analysis_job.job_id}/progress/',
                'result': f'/api/analysis/{analysis_job.job_id}/',
                'cancel': f'/api/analysis/{analysis_job.job_id}/cancel/'
            },
            'download_links': {
                'json': f'/api/analysis/{analysis_job.job_id}/download?format=json',
                'markdown': f'/api/analysis/{analysis_job.job_id}/download?format=markdown', 
                'html': f'/api/analysis/{analysis_job.job_id}/download?format=html'
            },
            'review_url': f'/api/analysis/{analysis_job.job_id}'
        }
        
        logger.info(f"Video analysis completed: {analysis_job.job_id} for video {video_id}")
        
        return Response(response_data, status=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.error(f"Video analysis failed: {e}")
        
        # Update job status to failed if job was created
        try:
            if 'analysis_job' in locals():
                analysis_job.status = 'failed'
                analysis_job.error_message = str(e)
                analysis_job.status_message = f"分析失败: {str(e)}"
                analysis_job.completed_at = timezone.now()
                analysis_job.save()
                logger.info(f"Updated job {analysis_job.job_id} status to failed")
        except Exception as update_error:
            logger.error(f"Failed to update job status: {update_error}")
        
        return Response({
            'status': 'error',
            'message': f'Analysis failed: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ============================================================================
# ANALYSIS RESULTS MANAGEMENT API
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_analysis_result(request, job_id):
    """Get detailed analysis result by job ID"""
    
    try:
        from .models import AnalysisJob, AnalysisResult
        
        # 查找分析任务
        try:
            analysis_job = AnalysisJob.objects.select_related('result').get(job_id=job_id)
        except AnalysisJob.DoesNotExist:
            return Response({
                'status': 'error',
                'message': f'Analysis job {job_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # 检查任务状态
        if analysis_job.status == 'failed':
            return Response({
                'status': 'failed',
                'job_id': job_id,
                'error_message': analysis_job.error_message,
                'status_message': analysis_job.status_message
            }, status=status.HTTP_200_OK)
        
        if analysis_job.status in ['queued', 'processing']:
            return Response({
                'status': analysis_job.status,
                'job_id': job_id,
                'progress': analysis_job.progress or 0,
                'status_message': analysis_job.status_message,
                'estimated_completion': None  # Not available in current model
            }, status=status.HTTP_202_ACCEPTED)
        
        # 获取分析结果
        try:
            result = analysis_job.result
            return Response({
                'status': 'completed',
                'job_id': job_id,
                'created_at': analysis_job.created_at.isoformat(),
                'completed_at': analysis_job.completed_at.isoformat() if analysis_job.completed_at else None,
                'progress': 100,
                'results': result.raw_data,
                'download_links': {
                    'json': f'/api/analysis/{job_id}/download?format=json',
                    'markdown': f'/api/analysis/{job_id}/download?format=markdown',
                    'html': f'/api/analysis/{job_id}/download?format=html'
                }
            }, status=status.HTTP_200_OK)
        except AnalysisResult.DoesNotExist:
            return Response({
                'status': 'error',
                'message': 'Analysis completed but results not found'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
    except Exception as e:
        logger.error(f"Failed to get analysis result for {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to retrieve analysis result: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([AllowAny])
def get_analysis_progress(request, job_id):
    """Get analysis progress by job ID"""
    
    try:
        from .models import AnalysisJob
        
        try:
            analysis_job = AnalysisJob.objects.get(job_id=job_id)
        except AnalysisJob.DoesNotExist:
            return Response({
                'status': 'error',
                'message': f'Analysis job {job_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        response_data = {
            'job_id': job_id,
            'analysis_status': analysis_job.status,
            'progress': analysis_job.progress or 0,
            'status_message': analysis_job.status_message,
            'created_at': analysis_job.created_at.isoformat(),
        }
        
        if analysis_job.completed_at:
            response_data['completed_at'] = analysis_job.completed_at.isoformat()
        
        if analysis_job.error_message:
            response_data['error_message'] = analysis_job.error_message
            
        return Response(response_data, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Failed to get progress for job {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to get progress: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([AllowAny])
def download_analysis_result(request, job_id):
    """Download analysis result in specified format"""
    
    try:
        from .models import AnalysisJob, AnalysisResult
        
        # Get format parameter
        format_type = request.GET.get('format', 'json').lower()
        logger.info(f"Download request: job_id={job_id}, format={format_type}, query_params={request.GET}")
        
        try:
            analysis_job = AnalysisJob.objects.select_related('result').get(job_id=job_id)
        except AnalysisJob.DoesNotExist:
            return Response({
                'status': 'error',
                'message': f'Analysis job {job_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        if analysis_job.status != 'completed':
            return Response({
                'status': 'error',
                'message': f'Analysis not completed yet. Current status: {analysis_job.status}'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            result = analysis_job.result
            result_data = result.raw_data
            
            if format_type == 'json':
                response = HttpResponse(
                    json.dumps(result_data, indent=2, ensure_ascii=False),
                    content_type='application/json; charset=utf-8'
                )
                response['Content-Disposition'] = f'attachment; filename="analysis_{job_id}.json"'
                
            elif format_type == 'markdown':
                # Simple markdown content for debugging
                markdown_content = f"# Analysis Report: {job_id}\n\n"
                markdown_content += f"**Video ID**: {analysis_job.content_id}\n"
                markdown_content += f"**Expert Role**: {analysis_job.expert_role}\n"
                markdown_content += f"**Status**: {analysis_job.status}\n\n"
                markdown_content += "## Summary\nThis is a test markdown download.\n\n"
                
                response = HttpResponse(
                    markdown_content,
                    content_type='text/markdown; charset=utf-8'
                )
                response['Content-Disposition'] = f'attachment; filename="analysis_{job_id}.md"'
                
            else:
                return Response({
                    'status': 'error',
                    'message': f'Unsupported format: {format_type}. Supported: json, markdown'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            return response
            
        except AnalysisResult.DoesNotExist:
            return Response({
                'status': 'error',
                'message': 'Analysis results not found'
            }, status=status.HTTP_404_NOT_FOUND)
            
    except Exception as e:
        logger.error(f"Failed to download analysis result for {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to download result: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ============================================================================
# ANALYSIS HISTORY AND MANAGEMENT
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def list_analysis_history(request):
    """List analysis history with pagination and filtering"""
    
    try:
        from .models import AnalysisJob
        
        # Get query parameters
        page = int(request.GET.get('page', 1))
        per_page = min(int(request.GET.get('per_page', 20)), 100)  # Max 100 per page
        status_filter = request.GET.get('status')
        expert_role_filter = request.GET.get('expert_role')
        
        # Build query
        queryset = AnalysisJob.objects.all()
        
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        if expert_role_filter:
            queryset = queryset.filter(expert_role=expert_role_filter)
        
        # Order by creation time (newest first)
        queryset = queryset.order_by('-created_at')
        
        # Pagination
        start = (page - 1) * per_page
        end = start + per_page
        jobs = queryset[start:end]
        total_count = queryset.count()
        
        # Serialize data
        job_list = []
        for job in jobs:
            job_data = {
                'job_id': job.job_id,
                'video_id': job.content_id,
                'expert_role': job.expert_role,
                'template_id': job.template_id,
                'status': job.status,
                'progress': job.progress or 0,
                'status_message': job.status_message,
                'created_at': job.created_at.isoformat(),
            }
            
            if job.completed_at:
                job_data['completed_at'] = job.completed_at.isoformat()
            
            if job.error_message:
                job_data['error_message'] = job.error_message
                
            job_list.append(job_data)
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        has_next = page < total_pages
        has_prev = page > 1
        
        return Response({
            'status': 'success',
            'jobs': job_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev
            },
            'filters': {
                'status': status_filter,
                'expert_role': expert_role_filter
            }
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Failed to list analysis history: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to retrieve history: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ============================================================================
# INDIVIDUAL JOB STATUS
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def job_status(request, job_id):
    """Get individual job status - alias for get_analysis_progress"""
    return get_analysis_progress(request, job_id)

# ============================================================================
# MISSING FUNCTIONS - STUBS FOR URL COMPATIBILITY
# ============================================================================

@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_playlist(request):
    """Playlist analysis endpoint - stub implementation"""
    return Response({
        'status': 'error',
        'message': 'Playlist analysis feature coming soon'
    }, status=status.HTTP_501_NOT_IMPLEMENTED)

@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_batch(request):
    """Batch analysis endpoint - stub implementation"""
    return Response({
        'status': 'error',
        'message': 'Batch analysis feature coming soon'
    }, status=status.HTTP_501_NOT_IMPLEMENTED)

@api_view(['GET'])
@permission_classes([AllowAny])
def get_analysis_status(request, job_id):
    """Get analysis status - alias for get_analysis_progress"""
    return get_analysis_progress(request, job_id)

@api_view(['GET'])
@permission_classes([AllowAny])
def list_expert_roles(request):
    """List expert roles - stub implementation"""
    return Response({
        'status': 'success',
        'roles': [
            {'id': 'content_creator', 'name': 'Content Creator', 'description': 'YouTube content creator perspective'},
            {'id': 'marketing_expert', 'name': 'Marketing Expert', 'description': 'Marketing and growth perspective'},
            {'id': 'data_analyst', 'name': 'Data Analyst', 'description': 'Data and analytics perspective'}
        ]
    })

@api_view(['GET'])
@permission_classes([AllowAny])
def get_expert_role(request, role_id):
    """Get expert role details - stub implementation"""
    return Response({
        'status': 'success',
        'role': {
            'id': role_id,
            'name': role_id.replace('_', ' ').title(),
            'description': f'Expert role: {role_id}'
        }
    })

@api_view(['POST'])
@permission_classes([AllowAny])
def create_custom_role(request):
    """Create custom role - stub implementation"""
    return Response({
        'status': 'error',
        'message': 'Custom role creation feature coming soon'
    }, status=status.HTTP_501_NOT_IMPLEMENTED)

@api_view(['GET'])
@permission_classes([AllowAny])
def list_templates(request):
    """List templates - stub implementation"""
    return Response({
        'status': 'success',
        'templates': [
            {'id': 'basic_performance', 'name': 'Basic Performance', 'description': 'Basic video analysis'},
            {'id': 'content_optimization', 'name': 'Content Optimization', 'description': 'Content optimization analysis'},
            {'id': 'audience_engagement', 'name': 'Audience Engagement', 'description': 'Audience engagement analysis'}
        ]
    })

@api_view(['GET'])
@permission_classes([AllowAny])
def get_template(request, template_id):
    """Get template details - stub implementation"""
    return Response({
        'status': 'success',
        'template': {
            'id': template_id,
            'name': template_id.replace('_', ' ').title(),
            'description': f'Template: {template_id}'
        }
    })

@api_view(['POST'])
@permission_classes([AllowAny])
def create_custom_template(request):
    """Create custom template - stub implementation"""
    return Response({
        'status': 'error',
        'message': 'Custom template creation feature coming soon'
    }, status=status.HTTP_501_NOT_IMPLEMENTED)

@api_view(['GET'])
@permission_classes([AllowAny])
def get_job_status(request, job_id):
    """Get job status - alias for get_analysis_progress"""
    return get_analysis_progress(request, job_id)

@api_view(['GET'])
@permission_classes([AllowAny])
def get_job_results(request, job_id):
    """Get job results - alias for get_analysis_result"""
    return get_analysis_result(request, job_id)

@api_view(['GET'])
@permission_classes([AllowAny])
def download_results(request, job_id):
    """Download results - alias for download_analysis_result"""
    return download_analysis_result(request, job_id)

@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_tool_selection(request):
    """Wizard tool selection - stub implementation"""
    return Response({
        'status': 'success',
        'tools': [
            {'id': 'video_analysis', 'name': 'Video Analysis', 'description': 'Analyze individual videos'},
            {'id': 'playlist_analysis', 'name': 'Playlist Analysis', 'description': 'Analyze entire playlists'},
            {'id': 'batch_analysis', 'name': 'Batch Analysis', 'description': 'Analyze multiple videos'}
        ]
    })

@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_role_selection(request):
    """Wizard role selection - stub implementation"""
    return list_expert_roles(request)

@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_template_selection(request):
    """Wizard template selection - stub implementation"""
    return list_templates(request)

@api_view(['GET'])
@permission_classes([AllowAny])
def engine_dashboard(request):
    """Engine dashboard endpoint"""
    try:
        from .models import AnalysisJob
        
        # Get statistics
        total_jobs = AnalysisJob.objects.count()
        completed_jobs = AnalysisJob.objects.filter(status='completed').count()
        processing_jobs = AnalysisJob.objects.filter(status='processing').count()
        failed_jobs = AnalysisJob.objects.filter(status='failed').count()
        
        # Recent jobs
        recent_jobs = AnalysisJob.objects.order_by('-created_at')[:10]
        recent_jobs_data = []
        for job in recent_jobs:
            recent_jobs_data.append({
                'job_id': job.job_id,
                'content_id': job.content_id,
                'status': job.status,
                'expert_role': job.expert_role,
                'created_at': job.created_at.isoformat()
            })
        
        return Response({
            'status': 'success',
            'statistics': {
                'total_jobs': total_jobs,
                'completed_jobs': completed_jobs,
                'processing_jobs': processing_jobs,
                'failed_jobs': failed_jobs
            },
            'recent_jobs': recent_jobs_data,
            'system_status': {
                'healthy': True,
                'timestamp': timezone.now().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"Dashboard data error: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to get dashboard data: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)