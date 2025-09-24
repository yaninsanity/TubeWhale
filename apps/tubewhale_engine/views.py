"""
TubeWhale Engine API Views
提供RESTful API接口调用TubeWhale功能
"""

import logging
import json
import uuid
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


@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_video(request):
    """Enhanced video analysis API with result storage"""
    
    try:
        from .models import AnalysisJob, AnalysisResult
        import time
        start_time = time.time()
        
        data = request.data
        
        # 验证必需参数
        video_id = data.get('video_id')
        expert_role = data.get('expert_role', 'content_creator')
        template_id = data.get('template', 'basic_performance')
        
        if not video_id:
            return Response({
                'status': 'error',
                'message': 'video_id is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 创建分析任务
        analysis_job = AnalysisJob.objects.create(
            analysis_type='video',
            expert_role=expert_role,
            template_id=template_id,
            content_id=video_id,
            content_url=f'https://www.youtube.com/watch?v={video_id}',
            content_title=f'Video Analysis: {video_id}',
            status='processing',
            ip_address=request.META.get('REMOTE_ADDR'),
            user_agent=request.META.get('HTTP_USER_AGENT', ''),
            analysis_options={
                'output_format': data.get('output_format', 'json'),
                'custom_questions': data.get('custom_questions', []),
                'analysis_depth': data.get('analysis_depth', 'standard')
            }
        )
        
        # 模拟分析过程（在真实环境中这里会调用AI分析引擎）
        analysis_job.started_at = timezone.now()
        analysis_job.progress = 25
        analysis_job.save()
        
        # 生成分析结果
        processing_time = time.time() - start_time
        
        # 根据专家角色和模板生成不同的分析内容
        expert_insights = {
            'content_creator': {
                'focus': 'Engagement and creativity analysis',
                'insights': ['Strong visual storytelling', 'Good audience retention', 'Effective call-to-actions'],
                'recommendations': ['Optimize thumbnail design', 'Improve video pacing', 'Add more interactive elements']
            },
            'marketing_expert': {
                'focus': 'Marketing performance and conversion analysis', 
                'insights': ['High conversion potential', 'Strong brand messaging', 'Good market positioning'],
                'recommendations': ['Optimize for search algorithms', 'Enhance CTAs', 'Leverage trending topics']
            },
            'data_analyst': {
                'focus': 'Performance metrics and statistical analysis',
                'insights': ['Above-average engagement rates', 'Strong demographic targeting', 'Optimal publishing timing'],
                'recommendations': ['Track key performance indicators', 'A/B test video formats', 'Analyze competitor metrics']
            }
        }
        
        current_expert = expert_insights.get(expert_role, expert_insights['content_creator'])
        
        # 模拟AI分析结果
        analysis_result = AnalysisResult.objects.create(
            job=analysis_job,
            summary=f"Professional {expert_role.replace('_', ' ').title()} analysis of video {video_id} completed successfully. {current_expert['focus']} reveals significant opportunities for optimization and growth.",
            detailed_analysis=f"""
## Video Analysis Report

### Expert Perspective: {expert_role.replace('_', ' ').title()}
**Focus Area**: {current_expert['focus']}

### Key Findings
{chr(10).join([f'• {insight}' for insight in current_expert['insights']])}

### Performance Metrics
- **Engagement Score**: 8.5/10
- **Content Quality**: 9.2/10  
- **Optimization Score**: 7.8/10
- **Audience Alignment**: 8.9/10

### Technical Analysis
- Video length optimization: Excellent
- Audio quality: Professional grade
- Visual composition: Strong storytelling elements
- Content structure: Well-organized progression

### Template: {template_id.replace('_', ' ').title()}
This analysis template provides comprehensive insights into video performance across multiple dimensions.
""",
            recommendations=chr(10).join([f'• {rec}' for rec in current_expert['recommendations']]),
            raw_data={
                'video_id': video_id,
                'expert_role': expert_role,
                'template': template_id,
                'analysis_timestamp': timezone.now().isoformat(),
                'processing_details': {
                    'steps_completed': ['data_extraction', 'content_analysis', 'insights_generation', 'recommendations'],
                    'data_sources': ['video_metadata', 'engagement_metrics', 'content_analysis', 'comparative_data'],
                    'confidence_factors': ['data_completeness', 'pattern_recognition', 'expert_model_accuracy']
                }
            },
            insights={
                'engagement': {'score': 8.5, 'trend': 'positive', 'key_drivers': current_expert['insights'][:2]},
                'content_quality': {'score': 9.2, 'strengths': current_expert['insights'], 'improvements': current_expert['recommendations'][:2]},
                'optimization': {'score': 7.8, 'opportunities': current_expert['recommendations']}
            },
            metrics={
                'performance': {
                    'overall_score': 8.5,
                    'engagement_rate': 12.3,
                    'retention_rate': 78.5,
                    'click_through_rate': 5.2
                },
                'quality': {
                    'content_score': 9.2,
                    'production_quality': 8.8,
                    'audio_quality': 9.1,
                    'visual_quality': 8.9
                },
                'optimization': {
                    'seo_score': 7.8,
                    'thumbnail_effectiveness': 8.1,
                    'title_optimization': 7.5,
                    'description_quality': 8.3
                }
            },
            confidence_score=0.89,
            completeness_score=0.94,
            available_formats=['json', 'markdown', 'html']
        )
        
        # 完成任务
        analysis_job.status = 'completed'
        analysis_job.completed_at = timezone.now()
        analysis_job.processing_time_seconds = processing_time
        analysis_job.progress = 100
        analysis_job.save()
        
        # 记录CLI日志
        CLICommandLog.objects.create(
            command='analyze',
            args=[video_id, f'--role={expert_role}', f'--template={template_id}'],
            return_code=0,
            success=True,
            stdout_truncated=f'Analysis completed for {video_id}',
            duration_ms=int(processing_time * 1000),
            meta={'job_id': analysis_job.job_id, 'api_request': True},
            ip_address=request.META.get('REMOTE_ADDR')
        )
        
        # 返回结果
        response_data = {
            'status': 'success',
            'job_id': analysis_job.job_id,
            'analysis_type': 'video',
            'expert_role': expert_role,
            'template': template_id,
            'video_id': video_id,
            'created_at': analysis_job.created_at.isoformat(),
            'completed_at': analysis_job.completed_at.isoformat(),
            'processing_time': f"{processing_time:.2f}s",
            'results_summary': {
                'confidence_score': analysis_result.confidence_score,
                'completeness_score': analysis_result.completeness_score,
                'key_insights_count': len(analysis_result.insights),
                'recommendations_count': len(analysis_result.recommendations.split('\n')) if analysis_result.recommendations else 0
            },
            'download_urls': {
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
        
        # 检查是否有结果
        if not hasattr(analysis_job, 'result'):
            return Response({
                'status': 'error',
                'message': f'Analysis results not available for job {job_id}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        result = analysis_job.result
        
        # 返回完整结果
        response_data = {
            'status': 'success',
            'job': {
                'job_id': analysis_job.job_id,
                'analysis_type': analysis_job.analysis_type,
                'expert_role': analysis_job.expert_role,
                'template_id': analysis_job.template_id,
                'content_info': {
                    'id': analysis_job.content_id,
                    'title': analysis_job.content_title,
                    'url': analysis_job.content_url,
                },
                'status': analysis_job.status,
                'progress': analysis_job.progress,
                'created_at': analysis_job.created_at.isoformat(),
                'completed_at': analysis_job.completed_at.isoformat() if analysis_job.completed_at else None,
                'processing_time_seconds': analysis_job.processing_time_seconds
            },
            'results': {
                'summary': result.summary,
                'detailed_analysis': result.detailed_analysis,
                'recommendations': result.recommendations,
                'raw_data': result.raw_data,
                'insights': result.insights,
                'metrics': result.metrics,
                'confidence_score': result.confidence_score,
                'completeness_score': result.completeness_score,
                'download_count': result.download_count,
                'last_downloaded_at': result.last_downloaded_at.isoformat() if result.last_downloaded_at else None
            },
            'download_urls': {
                'json': f'/api/analysis/{job_id}/download?format=json',
                'markdown': f'/api/analysis/{job_id}/download?format=markdown',
                'html': f'/api/analysis/{job_id}/download?format=html'
            },
            'available_formats': result.available_formats
        }
        
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"Failed to get analysis result for {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to get analysis result: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def download_analysis_result(request, job_id):
    """Download analysis result in specified format"""
    
    try:
        from .models import AnalysisJob, AnalysisResult
        from django.http import HttpResponse
        
        # 获取格式参数
        format_type = request.GET.get('format', 'json')
        if format_type not in ['json', 'markdown', 'html']:
            return Response({
                'status': 'error',
                'message': 'format must be one of: json, markdown, html'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 查找分析任务和结果
        try:
            analysis_job = AnalysisJob.objects.select_related('result').get(job_id=job_id)
            result = analysis_job.result
        except AnalysisJob.DoesNotExist:
            return Response({
                'status': 'error',
                'message': f'Analysis job {job_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        except AttributeError:
            return Response({
                'status': 'error',
                'message': f'Analysis results not available for job {job_id}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # 增加下载计数
        result.increment_download_count()
        
        # 生成HTML报告需要添加方法
        def generate_markdown_report(result):
            return f"""# TubeWhale Analysis Report
            
## Job Information
- **Job ID**: {result.job.job_id}
- **Analysis Type**: {result.job.get_analysis_type_display()}
- **Expert Role**: {result.job.expert_role.replace('_', ' ').title()}
- **Template**: {result.job.template_id.replace('_', ' ').title()}
- **Content**: [{result.job.content_title}]({result.job.content_url})
- **Completed**: {result.job.completed_at.strftime('%Y-%m-%d %H:%M:%S') if result.job.completed_at else 'In Progress'}

## Executive Summary
{result.summary}

## Detailed Analysis
{result.detailed_analysis}

## Recommendations
{result.recommendations}

## Analysis Metadata
- **Confidence Score**: {result.confidence_score:.2%}
- **Completeness Score**: {result.completeness_score:.2%}
- **Processing Time**: {result.job.processing_time_seconds:.1f}s

---
*Generated by TubeWhale Professional Analysis Platform*
"""
        
        # 生成文件内容
        if format_type == 'json':
            content = json.dumps(result.get_formatted_result('json'), indent=2, ensure_ascii=False)
            content_type = 'application/json'
            file_extension = 'json'
            
        elif format_type == 'markdown':
            content = generate_markdown_report(result)
            content_type = 'text/markdown'
            file_extension = 'md'
            
        elif format_type == 'html':
            # 简化版HTML报告
            content = f"""<!DOCTYPE html>
<html>
<head>
    <title>TubeWhale Analysis Report - {result.job.job_id}</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: #161b22; padding: 30px; border-radius: 8px; }}
        h1 {{ color: #58a6ff; }}
        h2 {{ color: #3fb950; }}
        .metrics {{ display: flex; gap: 20px; }}
        .metric {{ background: #21262d; padding: 15px; border-radius: 6px; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎬 TubeWhale Analysis Report</h1>
        <p><strong>Job ID:</strong> {result.job.job_id}</p>
        <p><strong>Expert Role:</strong> {result.job.expert_role.replace('_', ' ').title()}</p>
        
        <div class="metrics">
            <div class="metric">
                <h3>{result.confidence_score:.0%}</h3>
                <p>Confidence Score</p>
            </div>
            <div class="metric">
                <h3>{result.completeness_score:.0%}</h3>
                <p>Completeness Score</p>
            </div>
        </div>
        
        <h2>Executive Summary</h2>
        <p>{result.summary}</p>
        
        <h2>Detailed Analysis</h2>
        <div style="white-space: pre-wrap;">{result.detailed_analysis}</div>
        
        <h2>Recommendations</h2>
        <div style="white-space: pre-wrap;">{result.recommendations}</div>
    </div>
</body>
</html>"""
            content_type = 'text/html'
            file_extension = 'html'
        
        # 创建HTTP响应
        response = HttpResponse(content, content_type=content_type)
        response['Content-Disposition'] = f'attachment; filename="tubewhale_analysis_{job_id}.{file_extension}"'
        response['Content-Length'] = len(content.encode('utf-8'))
        
        # 记录下载日志
        CLICommandLog.objects.create(
            command='download',
            args=[job_id, f'--format={format_type}'],
            return_code=0,
            success=True,
            stdout_truncated=f'Downloaded analysis result {job_id} as {format_type}',
            duration_ms=100,
            meta={
                'job_id': job_id, 
                'format': format_type, 
                'download_count': result.download_count,
                'api_request': True
            },
            ip_address=request.META.get('REMOTE_ADDR')
        )
        
        logger.info(f"Analysis result downloaded: {job_id} in {format_type} format")
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to download analysis result for {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to download result: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])  
def list_analysis_history(request):
    """List analysis history with filtering and pagination"""
    
    try:
        from .models import AnalysisJob
        
        # 获取查询参数
        page = int(request.GET.get('page', 1))
        per_page = min(int(request.GET.get('per_page', 20)), 100)  # 最大100条
        analysis_type = request.GET.get('type')
        expert_role = request.GET.get('role')
        status = request.GET.get('status')
        
        # 构建查询
        jobs_query = AnalysisJob.objects.all()
        
        if analysis_type:
            jobs_query = jobs_query.filter(analysis_type=analysis_type)
        if expert_role:
            jobs_query = jobs_query.filter(expert_role=expert_role)  
        if status:
            jobs_query = jobs_query.filter(status=status)
        
        # 分页
        total_count = jobs_query.count()
        offset = (page - 1) * per_page
        jobs = jobs_query.order_by('-created_at')[offset:offset + per_page]
        
        # 序列化结果
        results = []
        for job in jobs:
            job_data = {
                'job_id': job.job_id,
                'analysis_type': job.analysis_type,
                'expert_role': job.expert_role,
                'template_id': job.template_id,
                'content_info': {
                    'id': job.content_id,
                    'title': job.content_title,
                    'url': job.content_url
                },
                'status': job.status,
                'progress': job.progress,
                'created_at': job.created_at.isoformat(),
                'completed_at': job.completed_at.isoformat() if job.completed_at else None,
                'processing_time_seconds': job.processing_time_seconds,
                'has_results': hasattr(job, 'result'),
                'download_urls': {
                    'json': f'/api/analysis/{job.job_id}/download?format=json',
                    'markdown': f'/api/analysis/{job.job_id}/download?format=markdown',
                    'html': f'/api/analysis/{job.job_id}/download?format=html'
                } if hasattr(job, 'result') else None
            }
            
            # 如果有结果，添加简要统计
            if hasattr(job, 'result'):
                result = job.result
                job_data['results_summary'] = {
                    'confidence_score': result.confidence_score,
                    'completeness_score': result.completeness_score,
                    'download_count': result.download_count,
                    'last_downloaded_at': result.last_downloaded_at.isoformat() if result.last_downloaded_at else None
                }
            
            results.append(job_data)
        
        # 分页信息
        has_next = total_count > offset + per_page
        has_prev = page > 1
        
        response_data = {
            'status': 'success',
            'results': results,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total_count,
                'pages': (total_count + per_page - 1) // per_page,
                'has_next': has_next,
                'has_prev': has_prev,
                'next_page': page + 1 if has_next else None,
                'prev_page': page - 1 if has_prev else None
            },
            'filters': {
                'analysis_type': analysis_type,
                'expert_role': expert_role, 
                'status': status
            }
        }
        
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"Failed to list analysis history: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to get analysis history: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def job_status(request, job_id):
    """查询任务状态"""
    
    try:
        # 在真实环境中，这里会查询数据库中的任务状态
        # 现在返回模拟数据
        return Response({
            'job_id': job_id,
            'status': 'completed',
            'progress': 100,
            'message': 'Analysis completed successfully',
            'created_at': timezone.now().isoformat(),
            'completed_at': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to get job status for {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Failed to get job status: {str(e)}'
        }, status=status.HTTP_400_BAD_REQUEST)


# ============================================================================
# EXPERT ROLES API - Complete Role Management System
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def list_expert_roles(request):
    """List all available expert roles with their capabilities and tiers"""
    
    roles = [
        {
            'id': 'content_creator',
            'name': 'Content Creator',
            'icon': '🎨',
            'description': 'Focus on engagement, trends, and audience building',
            'tier': 'free',
            'expertise': ['Engagement Optimization', 'Trend Analysis', 'Audience Growth'],
            'focus_areas': 'Creative content strategy and audience engagement',
            'processing_time': '2-5分钟分析处理时间（不限视频长度）'
        },
        {
            'id': 'marketing_expert', 
            'name': 'Marketing Expert',
            'icon': '📈',
            'description': 'Focus on ROI, campaign strategy, and brand building',
            'tier': 'free',
            'expertise': ['Campaign Strategy', 'ROI Analysis', 'Brand Building'],
            'focus_areas': 'Marketing performance and conversion optimization',
            'processing_time': '3-6分钟分析处理时间（不限视频长度）'
        },
        {
            'id': 'data_analyst',
            'name': 'Data Analyst', 
            'icon': '📊',
            'description': 'Focus on metrics, performance, and statistical insights',
            'tier': 'free',
            'expertise': ['Performance Metrics', 'Statistical Analysis', 'KPI Tracking'],
            'focus_areas': 'Data-driven insights and performance measurement',
            'processing_time': '4-7分钟分析处理时间（不限视频长度）'
        },
        {
            'id': 'educational_specialist',
            'name': 'Educational Specialist',
            'icon': '🎓', 
            'description': 'Focus on learning effectiveness and knowledge transfer',
            'tier': 'premium',
            'expertise': ['Learning Assessment', 'Educational Design', 'Knowledge Transfer'],
            'focus_areas': 'Educational content optimization and learning outcomes',
            'processing_time': '5-8分钟分析处理时间（完整分析任意长度视频）'
        },
        {
            'id': 'business_analyst',
            'name': 'Business Analyst',
            'icon': '💼',
            'description': 'Focus on business intelligence and market analysis',
            'tier': 'premium', 
            'expertise': ['Market Research', 'Business Intelligence', 'Performance Analysis'],
            'focus_areas': 'Business metrics and market trend analysis',
            'processing_time': '4-6分钟分析处理时间（完整分析任意长度视频）'
        },
        {
            'id': 'research_scientist',
            'name': 'Research Scientist',
            'icon': '🔬',
            'description': 'Focus on academic research and scientific analysis',
            'tier': 'premium',
            'expertise': ['Research Methods', 'Academic Analysis', 'Scientific Insights'],
            'focus_areas': 'Scientific methodology and research validation',
            'processing_time': '6-10分钟分析处理时间（深度分析任意长度视频）'
        },
        {
            'id': 'social_scientist',
            'name': 'Social Scientist',
            'icon': '🧠',
            'description': 'Focus on social behavior, cultural trends, and community impact',
            'tier': 'premium',
            'expertise': ['Social Behavior Analysis', 'Cultural Trends', 'Community Impact'],
            'focus_areas': 'Social dynamics and cultural influence analysis',
            'processing_time': '5-8分钟分析处理时间（完整社会影响分析）'
        },
        {
            'id': 'hci_specialist',
            'name': 'HCI Specialist',
            'icon': '�️',
            'description': 'Focus on user experience, interface design, and interaction patterns',
            'tier': 'premium',
            'expertise': ['User Experience', 'Interface Analysis', 'Interaction Design'],
            'focus_areas': 'Human-computer interaction and user behavior patterns',
            'processing_time': '4-7分钟分析处理时间（完整UX/UI体验分析）'
        },
        {
            'id': 'music_educator',
            'name': 'Music Educator',
            'icon': '🎵',
            'description': 'Focus on music education, audio quality, and artistic expression',
            'tier': 'premium',
            'expertise': ['Music Education', 'Audio Analysis', 'Artistic Expression'],
            'focus_areas': 'Musical content analysis and educational effectiveness',
            'processing_time': '5-9分钟分析处理时间（完整音乐教育分析）'
        }
    ]
    
    # Filter by tier if requested
    tier = request.GET.get('tier')
    if tier:
        roles = [role for role in roles if role['tier'] == tier]
    
    return Response({
        'status': 'success',
        'count': len(roles),
        'roles': roles
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def get_expert_role(request, role_id):
    """Get detailed information about a specific expert role"""
    
    role_details = {
        'content_creator': {
            'id': 'content_creator',
            'name': 'Content Creator',
            'description': 'Expert in audience engagement and content strategy',
            'system_prompt': 'You are an experienced content creator with expertise in YouTube growth, audience engagement, and viral content strategies.',
            'analysis_focus': ['Engagement Rate', 'Audience Retention', 'Content Performance', 'Trend Alignment'],
            'output_style': 'Creative insights with actionable recommendations',
            'sample_questions': [
                'How can I improve audience engagement?',
                'What content trends should I follow?',
                'How to optimize thumbnails and titles?'
            ]
        },
        'marketing_expert': {
            'id': 'marketing_expert',
            'name': 'Marketing Expert',
            'description': 'Specialist in digital marketing and ROI optimization',
            'system_prompt': 'You are a digital marketing expert with deep knowledge of YouTube marketing, conversion optimization, and brand strategy.',
            'analysis_focus': ['Conversion Rates', 'Brand Visibility', 'Campaign Performance', 'ROI Metrics'],
            'output_style': 'Strategic recommendations with measurable outcomes',
            'sample_questions': [
                'How can I improve conversion rates?',
                'What is the marketing potential of this content?',
                'How to optimize for brand awareness?'
            ]
        },
        'data_analyst': {
            'id': 'data_analyst',
            'name': 'Data Analyst',
            'description': 'Expert in data analysis and performance metrics',
            'system_prompt': 'You are a data analyst specializing in YouTube analytics, statistical analysis, and performance optimization.',
            'analysis_focus': ['Statistical Trends', 'Performance Metrics', 'Data Patterns', 'Predictive Analysis'],
            'output_style': 'Data-driven insights with statistical evidence',
            'sample_questions': [
                'What are the key performance indicators?',
                'How do the metrics compare to benchmarks?',
                'What patterns can be identified in the data?'
            ]
        }
    }
    
    if role_id not in role_details:
        return Response({
            'status': 'error',
            'message': 'Role not found'
        }, status=status.HTTP_404_NOT_FOUND)
    
    return Response({
        'status': 'success',
        'role': role_details[role_id]
    })


@api_view(['POST'])
@permission_classes([AllowAny])  # In production, require authentication for premium features
def create_custom_role(request):
    """Create a custom expert role (Premium feature)"""
    
    data = request.data
    
    required_fields = ['name', 'description', 'expertise', 'focus_areas']
    for field in required_fields:
        if not data.get(field):
            return Response({
                'status': 'error',
                'message': f'{field} is required'
            }, status=status.HTTP_400_BAD_REQUEST)
    
    custom_role = {
        'id': f"custom_{uuid.uuid4().hex[:8]}",
        'name': data['name'],
        'description': data['description'],
        'expertise': data['expertise'],
        'focus_areas': data['focus_areas'],
        'system_prompt': data.get('system_prompt', f"You are a {data['name']} with expertise in {', '.join(data['expertise'])}."),
        'tier': 'premium_custom',
        'created_at': timezone.now().isoformat()
    }
    
    return Response({
        'status': 'success',
        'message': 'Custom role created successfully',
        'role': custom_role
    }, status=status.HTTP_201_CREATED)


# ============================================================================
# TEMPLATES API - Complete Template Management System
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def list_templates(request):
    """List all available analysis templates"""
    
    templates = [
        {
            'id': 'basic_performance',
            'name': 'Basic Performance Analysis',
            'description': 'Essential metrics and performance overview',
            'category': 'performance',
            'tier': 'free',
            'icon': '📊',
            'features': ['View Count', 'Engagement Rate', 'Basic Demographics'],
            'analysis_scope': '完整视频内容分析',
            'processing_time': '2-3分钟处理时间（不限视频长度）'
        },
        {
            'id': 'engagement_overview',
            'name': 'Engagement Analysis',
            'description': 'Deep dive into audience engagement patterns',
            'category': 'engagement',
            'tier': 'free',
            'icon': '💬',
            'features': ['Comments Analysis', 'Interaction Patterns', 'Engagement Timeline'],
            'analysis_scope': '完整视频内容+用户互动分析',
            'processing_time': '3-4分钟处理时间（不限视频长度）'
        },
        {
            'id': 'content_quality',
            'name': 'Content Quality Check',
            'description': 'Comprehensive content quality assessment',
            'category': 'quality',
            'tier': 'free',
            'icon': '✅',
            'features': ['Content Structure', 'Quality Metrics', 'Improvement Suggestions'],
            'analysis_scope': '完整视频内容质量评估',
            'processing_time': '4-5分钟处理时间（深度内容分析）'
        },
        {
            'id': 'competitor_analysis',
            'name': 'Competitor Analysis',
            'description': 'Compare performance against competitors',
            'category': 'strategy',
            'tier': 'premium',
            'icon': '🏆',
            'features': ['Competitive Benchmarking', 'Market Position', 'Growth Opportunities'],
            'analysis_scope': '完整竞争对手对比分析',
            'processing_time': '5-7分钟处理时间（市场分析）'
        },
        {
            'id': 'monetization_strategy',
            'name': 'Monetization Strategy',
            'description': 'Revenue optimization recommendations',
            'category': 'monetization',
            'tier': 'premium',
            'icon': '💰',
            'features': ['Revenue Analysis', 'Monetization Opportunities', 'ROI Optimization'],
            'analysis_scope': '完整收益潜力分析',
            'processing_time': '6-8分钟处理时间（商业化分析）'
        },
        {
            'id': 'seo_optimization',
            'name': 'SEO Optimization Guide',
            'description': 'Search optimization and discoverability',
            'category': 'seo',
            'tier': 'premium',
            'icon': '🔍',
            'features': ['Keyword Analysis', 'SEO Recommendations', 'Discoverability Tips'],
            'analysis_scope': '完整SEO优化建议',
            'processing_time': '4-6分钟处理时间（SEO分析）'
        },
        {
            'id': 'educational_effectiveness',
            'name': 'Educational Effectiveness',
            'description': 'Learning outcomes and educational impact analysis',
            'category': 'education',
            'tier': 'premium',
            'icon': '🎓',
            'features': ['Learning Objectives', 'Knowledge Transfer', 'Student Engagement', 'Assessment Methods'],
            'analysis_scope': '完整教育效果评估',
            'processing_time': '6-9分钟处理时间（深度教育分析）'
        },
        {
            'id': 'social_impact',
            'name': 'Social Impact Analysis',
            'description': 'Community influence and social behavior insights',
            'category': 'social',
            'tier': 'premium',
            'icon': '🌍',
            'features': ['Social Influence Metrics', 'Cultural Impact Assessment', 'Community Response Analysis'],
            'analysis_scope': '完整社会影响力分析',
            'processing_time': '5-8分钟处理时间（社会效应分析）'
        },
        {
            'id': 'outdoor_adventure',
            'name': 'Outdoor & Adventure Analysis',
            'description': 'Specialized analysis for outdoor and adventure content',
            'category': 'lifestyle',
            'tier': 'premium',
            'icon': '🏔️',
            'features': ['Safety Assessment', 'Skill Level Evaluation', 'Equipment Review', 'Location Analysis'],
            'analysis_scope': '完整户外安全+内容评估',
            'processing_time': '5-7分钟处理时间（专业户外分析）'
        },
        {
            'id': 'music_analysis',
            'name': 'Music & Audio Analysis',
            'description': 'Comprehensive music and audio content evaluation',
            'category': 'creative',
            'tier': 'premium',
            'icon': '🎵',
            'features': ['Audio Quality Assessment', 'Musical Elements Analysis', 'Educational Value', 'Performance Evaluation'],
            'analysis_scope': '完整音频+音乐理论分析',
            'processing_time': '6-10分钟处理时间（专业音乐分析）'
        },
        {
            'id': 'technical_review',
            'name': 'Technical & Computer Science',
            'description': 'Technical content analysis for programming and tech topics',
            'category': 'technology',
            'tier': 'premium',
            'icon': '💻',
            'features': ['Technical Accuracy Check', 'Code Quality Assessment', 'Complexity Analysis', 'Best Practices Review'],
            'analysis_scope': '完整技术内容+代码审查',
            'processing_time': '7-12分钟处理时间（深度技术分析）'
        },
        {
            'id': 'user_experience',
            'name': 'UX/HCI Analysis',
            'description': 'User experience and human-computer interaction evaluation',
            'category': 'design',
            'tier': 'premium',
            'icon': '🖥️',
            'features': ['Usability Assessment', 'Interaction Design Review', 'User Journey Analysis', 'Accessibility Check'],
            'analysis_scope': '完整用户体验+交互设计分析',
            'processing_time': '5-8分钟处理时间（UX专业分析）'
        },
        {
            'id': 'business_intelligence',
            'name': 'Business Intelligence Analysis',
            'description': 'Advanced business metrics and market intelligence',
            'category': 'business',
            'tier': 'premium',
            'icon': '📋',
            'features': ['Market Analysis', 'Business KPIs', 'Competitive Intelligence', 'ROI Assessment'],
            'analysis_scope': '完整商业智能+市场分析',
            'processing_time': '6-9分钟处理时间（商业分析）'
        }
    ]
    
    # Filter by category and tier
    category = request.GET.get('category')
    tier = request.GET.get('tier')
    
    if category:
        templates = [t for t in templates if t['category'] == category]
    if tier:
        templates = [t for t in templates if t['tier'] == tier]
    
    return Response({
        'status': 'success',
        'count': len(templates),
        'templates': templates
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def get_template(request, template_id):
    """Get detailed information about a specific template"""
    
    template_details = {
        'basic_performance': {
            'id': 'basic_performance',
            'name': 'Basic Performance Analysis',
            'description': 'Essential metrics and performance overview for any YouTube content',
            'questions': [
                'What is the overall performance of this video?',
                'How does it compare to typical benchmarks?',
                'What are the key strengths and weaknesses?',
                'What immediate improvements can be made?'
            ],
            'output_sections': ['Performance Summary', 'Key Metrics', 'Recommendations', 'Next Steps'],
            'customizable': True
        },
        'engagement_overview': {
            'id': 'engagement_overview', 
            'name': 'Engagement Analysis',
            'description': 'Deep analysis of audience engagement and interaction patterns',
            'questions': [
                'What are the audience engagement patterns?',
                'Which parts of the video perform best?',
                'How can engagement be improved?',
                'What content resonates most with viewers?'
            ],
            'output_sections': ['Engagement Summary', 'Interaction Analysis', 'Audience Behavior', 'Optimization Tips'],
            'customizable': True
        }
    }
    
    if template_id not in template_details:
        return Response({
            'status': 'error',
            'message': 'Template not found'
        }, status=status.HTTP_404_NOT_FOUND)
    
    return Response({
        'status': 'success',
        'template': template_details[template_id]
    })


@api_view(['POST'])
@permission_classes([AllowAny])  # In production, require authentication for premium features
def create_custom_template(request):
    """Create a custom analysis template (Premium feature)"""
    
    data = request.data
    
    required_fields = ['name', 'description', 'questions']
    for field in required_fields:
        if not data.get(field):
            return Response({
                'status': 'error',
                'message': f'{field} is required'
            }, status=status.HTTP_400_BAD_REQUEST)
    
    custom_template = {
        'id': f"custom_{uuid.uuid4().hex[:8]}",
        'name': data['name'],
        'description': data['description'],
        'questions': data['questions'],
        'output_sections': data.get('output_sections', ['Analysis', 'Insights', 'Recommendations']),
        'category': data.get('category', 'custom'),
        'tier': 'premium_custom',
        'created_at': timezone.now().isoformat()
    }
    
    return Response({
        'status': 'success',
        'message': 'Custom template created successfully',
        'template': custom_template
    }, status=status.HTTP_201_CREATED)


# ============================================================================
# ANALYSIS ENDPOINTS - Enhanced with Role and Template Support
# ============================================================================

@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_single_video(request):
    """Enhanced single video analysis with role and template support"""
    
    data = request.data
    
    # Validate required fields
    video_id = data.get('video_id')
    if not video_id:
        return Response({
            'status': 'error',
            'message': 'video_id is required'
        }, status=status.HTTP_400_BAD_REQUEST)
    
    # Get role and template
    role_id = data.get('role_id', 'content_creator')
    template_id = data.get('template_id', 'basic_performance')
    custom_questions = data.get('custom_questions', [])
    
    # Generate job
    job_id = str(uuid.uuid4())[:8]
    
    result = {
        'job_id': job_id,
        'status': 'completed',
        'analysis_type': 'single_video',
        'video_id': video_id,
        'role_id': role_id,
        'template_id': template_id,
        'created_at': timezone.now().isoformat(),
        'completed_at': timezone.now().isoformat(),
        'results': {
            'summary': f'Video analysis completed using {role_id} perspective with {template_id} template',
            'expert_insights': {
                'role': role_id,
                'perspective': f'Analysis from {role_id.replace("_", " ").title()} viewpoint',
                'key_findings': [
                    f'Professional analysis using {role_id} expertise',
                    f'Template-based insights from {template_id}',
                    'Customized recommendations based on role perspective',
                    'Industry best practices applied'
                ]
            },
            'template_analysis': {
                'template': template_id,
                'sections_completed': ['Performance', 'Insights', 'Recommendations'],
                'custom_questions_answered': len(custom_questions)
            },
            'metrics': {
                'processing_time': '3.2s',
                'confidence_score': 0.92,
                'analysis_depth': 'comprehensive'
            }
        }
    }
    
    logger.info(f"Enhanced video analysis: {job_id} for {video_id} with role {role_id}")
    
    return Response(result, status=status.HTTP_201_CREATED)


@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_playlist(request):
    """Playlist analysis with batch processing"""
    
    data = request.data
    
    playlist_id = data.get('playlist_id')
    if not playlist_id:
        return Response({
            'status': 'error',
            'message': 'playlist_id is required'
        }, status=status.HTTP_400_BAD_REQUEST)
    
    job_id = str(uuid.uuid4())[:8]
    role_id = data.get('role_id', 'data_analyst')
    template_id = data.get('template_id', 'basic_performance')
    batch_size = data.get('batch_size', 10)
    
    result = {
        'job_id': job_id,
        'status': 'processing',
        'analysis_type': 'playlist',
        'playlist_id': playlist_id,
        'role_id': role_id,
        'template_id': template_id,
        'batch_size': batch_size,
        'estimated_completion': '5-10 minutes',
        'progress': {
            'total_videos': batch_size,
            'completed': 0,
            'current_status': 'Starting playlist analysis...'
        }
    }
    
    return Response(result, status=status.HTTP_202_ACCEPTED)


@api_view(['POST'])
@permission_classes([AllowAny])
def analyze_batch(request):
    """Batch analysis for multiple videos (Premium feature)"""
    
    data = request.data
    
    video_list = data.get('video_list', [])
    if not video_list:
        return Response({
            'status': 'error',
            'message': 'video_list is required'
        }, status=status.HTTP_400_BAD_REQUEST)
    
    job_id = str(uuid.uuid4())[:8]
    
    result = {
        'job_id': job_id,
        'status': 'queued',
        'analysis_type': 'batch',
        'total_videos': len(video_list),
        'role_id': data.get('role_id'),
        'template_id': data.get('template_id'),
        'estimated_completion': f'{len(video_list) * 2}-{len(video_list) * 3} minutes'
    }
    
    return Response(result, status=status.HTTP_202_ACCEPTED)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_analysis_status(request, job_id):
    """Get analysis job status and results"""
    try:
        from .models import ProcessingJob, VideoProcessingResult
        
        # Try to get the job
        try:
            job = ProcessingJob.objects.get(id=job_id)
        except ProcessingJob.DoesNotExist:
            return Response({
                'status': 'error',
                'message': 'Job not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Prepare basic response
        response_data = {
            'status': job.status,
            'progress': job.progress_percentage or 0,
            'message': job.current_step or f'Job is {job.status}',
            'created_at': job.created_at.isoformat(),
            'updated_at': job.created_at.isoformat(),  # using created_at since no updated_at field
            'video_id': job.video_id,
            'analysis_template': job.options.get('template', 'unknown')
        }
        
        # If completed, include results
        if job.status == 'completed':
            try:
                result = VideoProcessingResult.objects.get(job=job)
                response_data['results'] = {
                    'summary': result.summary or 'Analysis completed successfully',
                    'insights': result.insights.split('\n') if result.insights else [
                        'Analysis completed', 
                        'Video processed successfully',
                        'Results available for review'
                    ],
                    'metrics': {
                        'duration': result.video_duration,
                        'processing_time': str(result.processing_time) if result.processing_time else None,
                        'template_used': job.analysis_template,
                        'video_title': result.video_title or f'Video {job.video_id}'
                    }
                }
            except VideoProcessingResult.DoesNotExist:
                # No results yet, but job marked complete
                response_data['results'] = {
                    'summary': 'Analysis completed but results processing',
                    'insights': ['Analysis task completed', 'Results are being finalized'],
                    'metrics': {
                        'template_used': job.analysis_template,
                        'video_id': job.video_id
                    }
                }
        
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"Error getting analysis status for job {job_id}: {e}")
        return Response({
            'status': 'error',
            'message': f'Server error: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ============================================================================
# WIZARD API - Step-by-Step User Flow Support
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_tool_selection(request):
    """Step 1: Tool selection options for wizard flow"""
    
    tools = [
        {
            'id': 'single_video',
            'name': 'Single Video Analysis',
            'description': 'Analyze one specific video in depth',
            'icon': '🎬',
            'tier': 'free',
            'estimated_time': '2-5 minutes',
            'best_for': ['Content optimization', 'Performance review', 'Quick insights']
        },
        {
            'id': 'playlist_analysis',
            'name': 'Playlist Analysis', 
            'description': 'Analyze entire playlist for patterns and trends',
            'icon': '📺',
            'tier': 'free',
            'estimated_time': '5-15 minutes',
            'best_for': ['Content series review', 'Batch optimization', 'Trend analysis']
        },
        {
            'id': 'batch_analysis',
            'name': 'Batch Analysis',
            'description': 'Analyze multiple videos or channels',
            'icon': '📊',
            'tier': 'premium',
            'estimated_time': '10-30 minutes',
            'best_for': ['Competitor research', 'Market analysis', 'Large scale insights']
        }
    ]
    
    return Response({
        'status': 'success',
        'step': 1,
        'title': 'Choose Your Analysis Type',
        'description': 'Select the type of analysis you want to perform',
        'tools': tools
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_role_selection(request):
    """Step 2: Expert role selection for wizard flow"""
    
    tool_type = request.GET.get('tool_type', 'single_video')
    
    # Role recommendations based on tool type
    role_recommendations = {
        'single_video': ['content_creator', 'marketing_expert'],
        'playlist_analysis': ['data_analyst', 'business_executive'],
        'batch_analysis': ['research_scientist', 'marketing_expert']
    }
    
    recommended_roles = role_recommendations.get(tool_type, [])
    
    return Response({
        'status': 'success', 
        'step': 2,
        'title': 'Select Expert Perspective',
        'description': 'Choose the expert role that matches your analysis needs',
        'recommended_for_tool': recommended_roles,
        'next_endpoint': '/api/v1/roles/'
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def wizard_template_selection(request):
    """Step 3: Template selection for wizard flow"""
    
    role_id = request.GET.get('role_id', 'content_creator')
    
    # Template recommendations based on role
    template_recommendations = {
        'content_creator': ['engagement_overview', 'content_quality'],
        'marketing_expert': ['basic_performance', 'monetization_strategy'],
        'data_analyst': ['basic_performance', 'competitor_analysis']
    }
    
    recommended_templates = template_recommendations.get(role_id, ['basic_performance'])
    
    return Response({
        'status': 'success',
        'step': 3, 
        'title': 'Choose Analysis Template',
        'description': 'Select a template that defines what insights you want to get',
        'recommended_for_role': recommended_templates,
        'next_endpoint': '/api/v1/templates/'
    })


# ============================================================================  
# JOB MANAGEMENT - Enhanced Status and Results
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_job_status(request, job_id):
    """Enhanced job status with detailed progress"""
    
    # Simulate different job statuses
    status_info = {
        'job_id': job_id,
        'status': 'completed',
        'progress': 100,
        'stages': [
            {'name': 'Video Processing', 'status': 'completed', 'progress': 100},
            {'name': 'Expert Analysis', 'status': 'completed', 'progress': 100},
            {'name': 'Template Application', 'status': 'completed', 'progress': 100},
            {'name': 'Report Generation', 'status': 'completed', 'progress': 100}
        ],
        'estimated_completion': timezone.now().isoformat(),
        'results_ready': True
    }
    
    return Response({
        'status': 'success',
        'job': status_info
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def get_job_results(request, job_id):
    """Get comprehensive job results"""
    
    results = {
        'job_id': job_id,
        'status': 'completed',
        'analysis_summary': {
            'overall_score': 85,
            'key_insights': [
                'Strong audience engagement detected',
                'Content quality exceeds platform average',
                'Optimization opportunities identified'
            ],
            'recommendations': [
                'Optimize title and thumbnail for better CTR',
                'Improve first 15 seconds for retention',
                'Consider trending topics for future content'
            ]
        },
        'detailed_metrics': {
            'engagement_rate': '8.5%',
            'retention_rate': '65%',  
            'click_through_rate': '12.3%'
        },
        'export_formats': ['PDF', 'JSON', 'CSV', 'PowerPoint']
    }
    
    return Response({
        'status': 'success',
        'results': results
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def download_results(request, job_id):
    """Download results in specified format"""
    
    format_type = request.GET.get('format', 'json')
    
    return Response({
        'status': 'success',
        'message': f'Results prepared for download in {format_type} format',
        'download_url': f'/downloads/{job_id}.{format_type}',
        'expires_at': timezone.now().isoformat()
    })


# ============================================================================  
# ENHANCED CUSTOMIZATION API - Complete Custom Role & Template System
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_customization_options(request):
    """Get comprehensive customization options for roles and templates"""
    
    role_options = {
        'icons': ['🎨', '📈', '📊', '🎓', '💼', '🔬', '🧠', '🖥️', '🎵', '🏔️', '🎭', '⚖️', '🏥', '🔧', '🌱', '🎯'],
        'expertise_areas': [
            'Content Creation', 'Marketing Strategy', 'Data Analysis', 'Educational Design',
            'Business Strategy', 'Scientific Research', 'Social Psychology', 'User Experience',  
            'Music Theory', 'Outdoor Safety', 'Creative Arts', 'Legal Analysis', 'Healthcare',
            'Technical Engineering', 'Environmental Science', 'Cultural Studies', 'Sports Analysis',
            'Financial Planning', 'Risk Management', 'Innovation Strategy'
        ],
        'analysis_frameworks': [
            'comprehensive', 'focused', 'strategic', 'tactical', 'creative', 'analytical',
            'holistic', 'comparative', 'predictive', 'diagnostic'
        ],
        'focus_areas': [
            'Performance Optimization', 'Audience Engagement', 'Content Quality', 'Growth Strategy',
            'Risk Assessment', 'Innovation Analysis', 'Competitive Intelligence', 'Market Research',
            'User Behavior', 'Social Impact', 'Educational Outcomes', 'Technical Excellence'
        ]
    }
    
    template_options = {
        'icons': ['📊', '💬', '✅', '🏆', '💰', '🔍', '🎓', '🌍', '🏔️', '🎵', '💻', '🖥️', '📋', '🎯', '⚡', '🔬'],
        'categories': [
            'performance', 'engagement', 'quality', 'strategy', 'monetization', 'seo',
            'education', 'social', 'lifestyle', 'creative', 'technology', 'design', 'business',
            'health', 'environment', 'sports', 'arts', 'science', 'finance', 'legal', 'research'
        ],
        'question_types': [
            'quantitative_analysis', 'qualitative_assessment', 'comparative_study',
            'trend_analysis', 'impact_measurement', 'user_feedback', 'performance_metrics',
            'strategic_recommendations', 'risk_assessment', 'opportunity_identification',
            'behavioral_analysis', 'content_evaluation', 'market_analysis'
        ],
        'analysis_sections': [
            'Executive Summary', 'Performance Metrics', 'Audience Analysis', 'Content Quality',
            'Engagement Patterns', 'Competitive Landscape', 'Growth Opportunities', 
            'Risk Assessment', 'Recommendations', 'Action Items', 'ROI Analysis',
            'User Experience', 'Technical Review', 'Social Impact', 'Educational Value'
        ],
        'scenario_types': [
            'business', 'education', 'entertainment', 'technology', 'healthcare', 'sports',
            'outdoor', 'music', 'arts', 'science', 'social-impact', 'startup', 'corporate'
        ]
    }
    
    return Response({
        'status': 'success',
        'role_options': role_options,
        'template_options': template_options,
        'customization_levels': ['basic', 'advanced', 'expert'],
        'available_features': [
            'custom_questions', 'specialized_metrics', 'industry_benchmarks',
            'automated_insights', 'export_options', 'collaboration_tools'
        ]
    })


@api_view(['POST'])
@permission_classes([AllowAny])
def create_advanced_custom_template(request):
    """Create advanced custom analysis template with full customization"""
    
    data = request.data
    
    # Validate required fields
    required_fields = ['name', 'description', 'category', 'questions']
    for field in required_fields:
        if not data.get(field):
            return Response({
                'status': 'error',
                'message': f'{field} is required for custom template creation'
            }, status=status.HTTP_400_BAD_REQUEST)
    
    custom_template = {
        'id': f"custom_{data.get('name', '').lower().replace(' ', '_')}_{uuid.uuid4().hex[:8]}",
        'name': data['name'],
        'icon': data.get('icon', '📝'),
        'description': data['description'],
        'category': data['category'],
        'tier': 'custom_premium',
        'features': data.get('features', []),
        'estimated_time': data.get('estimated_time', '5-10 minutes'),
        'questions': data['questions'],
        'analysis_sections': data.get('analysis_sections', ['Analysis', 'Insights', 'Recommendations']),
        'target_scenarios': data.get('target_scenarios', []),
        'metrics_focus': data.get('metrics_focus', []),
        'output_format': data.get('output_format', 'comprehensive'),
        'complexity_level': data.get('complexity_level', 'intermediate'),
        'industry_specific': data.get('industry_specific', False),
        'collaboration_features': data.get('collaboration_features', []),
        'automation_level': data.get('automation_level', 'standard'),
        'created_at': timezone.now().isoformat(),
        'user_created': True,
        'customization_level': 'advanced'
    }
    
    return Response({
        'status': 'success',
        'message': 'Advanced custom template created successfully',
        'template': custom_template,
        'next_steps': [
            'Test template with sample content',
            'Refine questions based on results', 
            'Share with team for feedback',
            'Deploy for production use'
        ]
    }, status=status.HTTP_201_CREATED)


@api_view(['POST'])
@permission_classes([AllowAny])
def create_scenario_specific_analysis(request):
    """Create scenario-specific analysis combining multiple templates and roles"""
    
    data = request.data
    
    scenario_analysis = {
        'id': f"scenario_{uuid.uuid4().hex[:8]}",
        'scenario_name': data.get('scenario_name', 'Custom Scenario'),
        'description': data.get('description', 'Multi-perspective scenario analysis'),
        'roles': data.get('roles', []),  # Multiple roles for comprehensive analysis
        'templates': data.get('templates', []),  # Multiple templates
        'custom_questions': data.get('custom_questions', []),
        'analysis_depth': data.get('analysis_depth', 'comprehensive'),
        'target_audience': data.get('target_audience', 'general'),
        'industry_context': data.get('industry_context', ''),
        'success_metrics': data.get('success_metrics', []),
        'comparative_analysis': data.get('comparative_analysis', False),
        'timeline_analysis': data.get('timeline_analysis', False),
        'collaboration_mode': data.get('collaboration_mode', 'single_user'),
        'created_at': timezone.now().isoformat(),
        'estimated_completion': f"{len(data.get('roles', [])) * len(data.get('templates', []))} analyses"
    }
    
    return Response({
        'status': 'success',
        'message': 'Scenario-specific analysis framework created',
        'scenario': scenario_analysis,
        'execution_plan': {
            'phases': ['Data Collection', 'Multi-Role Analysis', 'Cross-Template Comparison', 'Synthesis'],
            'estimated_time': f"{len(data.get('roles', [1])) * 8}-{len(data.get('templates', [1])) * 15} minutes",
            'deliverables': ['Individual Role Reports', 'Template Comparisons', 'Integrated Insights', 'Action Plan']
        }
    }, status=status.HTTP_201_CREATED)