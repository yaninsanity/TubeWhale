"""
Enhanced API Views for TubeWhale Template System
Industrial-grade REST API with job detail visualization
"""

from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
import logging

from .template_engine import template_engine, ExpertRole, AnalysisLevel
from .models import AnalysisJob, AnalysisResult

logger = logging.getLogger(__name__)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_available_templates(request):
    """Get all available analysis templates with details"""
    
    try:
        role_filter = request.GET.get('role')
        level_filter = request.GET.get('level')
        
        templates = template_engine.get_template_list()
        
        # Filter by role if specified
        if role_filter:
            try:
                role_enum = ExpertRole(role_filter)
                templates = [
                    t for t in templates 
                    if role_filter in t['compatible_roles']
                ]
            except ValueError:
                return Response({
                    'error': f'Invalid role: {role_filter}',
                    'available_roles': [role.value for role in ExpertRole]
                }, status=status.HTTP_400_BAD_REQUEST)
        
        # Filter by level if specified
        if level_filter:
            try:
                level_enum = AnalysisLevel(level_filter)
                templates = [
                    t for t in templates 
                    if t['level'] == level_filter
                ]
            except ValueError:
                return Response({
                    'error': f'Invalid level: {level_filter}',
                    'available_levels': [level.value for level in AnalysisLevel]
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response({
            'templates': templates,
            'total': len(templates),
            'filters_applied': {
                'role': role_filter,
                'level': level_filter
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting templates: {e}")
        return Response({
            'error': 'Failed to retrieve templates'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])  
def get_expert_roles(request):
    """Get all available expert roles with specializations"""
    
    try:
        roles_info = []
        for role in ExpertRole:
            role_config = template_engine.role_configurations.get(role, {})
            roles_info.append({
                'id': role.value,
                'name': role.value.replace('_', ' ').title(),
                'specialization': role_config.get('specialization', 'Professional analysis'),
                'focus_areas': role_config.get('focus_areas', []),
                'key_metrics': role_config.get('key_metrics', []),
                'compatible_templates': [
                    t['id'] for t in template_engine.get_template_list()
                    if role.value in t['compatible_roles']
                ]
            })
        
        return Response({
            'roles': roles_info,
            'total': len(roles_info)
        })
        
    except Exception as e:
        logger.error(f"Error getting roles: {e}")
        return Response({
            'error': 'Failed to retrieve expert roles'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([AllowAny])
def validate_analysis_config(request):
    """Validate template and role combination before analysis"""
    
    try:
        template_id = request.data.get('template_id')
        role = request.data.get('expert_role')
        
        if not template_id or not role:
            return Response({
                'error': 'Both template_id and expert_role are required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        validation = template_engine.validate_combination(template_id, role)
        
        if validation['valid']:
            return Response({
                'valid': True,
                'configuration': validation,
                'processing_estimate': template_engine.get_processing_estimate(template_id)
            })
        else:
            return Response({
                'valid': False,
                'error': validation['error'],
                'compatible_roles': validation.get('compatible_roles', [])
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"Error validating config: {e}")
        return Response({
            'error': 'Failed to validate configuration'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_job_details(request, job_id):
    """Get comprehensive job details with visualization data"""
    
    try:
        analysis_job = get_object_or_404(AnalysisJob, job_id=job_id)
        
        # Basic job information
        job_data = {
            'job_id': analysis_job.job_id,
            'status': analysis_job.status,
            'progress': analysis_job.progress,
            'created_at': analysis_job.created_at,
            'started_at': analysis_job.started_at,
            'completed_at': analysis_job.completed_at,
            'video_id': analysis_job.content_id,
            'expert_role': analysis_job.expert_role,
            'template_id': analysis_job.template_id,
            'status_message': analysis_job.status_message,
            'error_message': analysis_job.error_message,
        }
        
        # Add YouTube integration data
        youtube_data = {
            'video_url': f"https://www.youtube.com/watch?v={analysis_job.content_id}",
            'thumbnail_url': f"https://img.youtube.com/vi/{analysis_job.content_id}/hqdefault.jpg",
            'embed_url': f"https://www.youtube.com/embed/{analysis_job.content_id}",
            'high_res_thumbnail': f"https://img.youtube.com/vi/{analysis_job.content_id}/maxresdefault.jpg"
        }
        
        # Add template information
        template = template_engine.get_template(analysis_job.template_id)
        if template:
            template_data = {
                'name': template.name,
                'level': template.level.value,
                'sections': template.analysis_sections,
                'visualization_config': template.visualization_config,
                'estimated_time': template_engine.get_processing_estimate(analysis_job.template_id)
            }
        else:
            template_data = {'error': 'Template not found'}
        
        # Add analysis results if completed
        results_data = None
        if analysis_job.status == 'completed':
            try:
                analysis_result = analysis_job.result
                results_data = {
                    'summary': analysis_result.summary,
                    'confidence_score': analysis_result.confidence_score,
                    'completeness_score': analysis_result.completeness_score,
                    'metrics': analysis_result.metrics,
                    'insights': analysis_result.insights,
                    'available_formats': analysis_result.available_formats,
                    'download_links': {
                        'json': f'/api/analysis/{job_id}/download/?format=json',
                        'markdown': f'/api/analysis/{job_id}/download/?format=markdown',
                        'html': f'/api/analysis/{job_id}/download/?format=html',
                        'pdf': f'/api/analysis/{job_id}/download/?format=pdf'
                    },
                    'raw_data': analysis_result.raw_data
                }
            except AnalysisResult.DoesNotExist:
                results_data = {'error': 'Results not found'}
        
        return Response({
            'job': job_data,
            'youtube': youtube_data,
            'template': template_data,
            'results': results_data,
            'visualization_ready': analysis_job.status == 'completed' and results_data and 'error' not in results_data
        })
        
    except Exception as e:
        logger.error(f"Error getting job details for {job_id}: {e}")
        return Response({
            'error': f'Failed to retrieve job details: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_analysis_dashboard(request, job_id):
    """Get dashboard data for visualization"""
    
    try:
        analysis_job = get_object_or_404(AnalysisJob, job_id=job_id)
        
        if analysis_job.status != 'completed':
            return Response({
                'error': 'Analysis not completed yet',
                'status': analysis_job.status,
                'progress': analysis_job.progress
            }, status=status.HTTP_400_BAD_REQUEST)
        
        analysis_result = analysis_job.result
        raw_data = analysis_result.raw_data
        
        # Extract visualization data
        dashboard_data = {
            'overview': {
                'video_title': raw_data.get('video_data', {}).get('title', 'Unknown'),
                'expert_perspective': analysis_job.expert_role.replace('_', ' ').title(),
                'analysis_level': raw_data.get('template', {}).get('level', 'basic'),
                'confidence_score': analysis_result.confidence_score,
                'overall_score': analysis_result.metrics.get('overall_score', 0)
            },
            
            'video_metrics': {
                'views': raw_data.get('video_data', {}).get('view_count', 0),
                'likes': raw_data.get('video_data', {}).get('like_count', 0),
                'comments': raw_data.get('video_data', {}).get('comment_count', 0),
                'duration': raw_data.get('video_data', {}).get('duration', 'Unknown')
            },
            
            'analysis_insights': analysis_result.insights,
            'performance_metrics': analysis_result.metrics,
            
            'recommendations': analysis_result.recommendations.split('\n') if analysis_result.recommendations else [],
            
            'youtube_integration': raw_data.get('youtube_integration', {}),
            
            'visualization_config': raw_data.get('visualization_config', {})
        }
        
        return Response(dashboard_data)
        
    except Exception as e:
        logger.error(f"Error getting dashboard data for {job_id}: {e}")
        return Response({
            'error': f'Failed to retrieve dashboard data: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_jobs_list(request):
    """Get paginated list of analysis jobs with summary information"""
    
    try:
        # Pagination parameters
        page = int(request.GET.get('page', 1))
        per_page = min(int(request.GET.get('per_page', 20)), 100)
        
        # Filter parameters
        status_filter = request.GET.get('status')
        role_filter = request.GET.get('role')
        template_filter = request.GET.get('template')
        
        # Build query
        jobs_query = AnalysisJob.objects.all().order_by('-created_at')
        
        if status_filter:
            jobs_query = jobs_query.filter(status=status_filter)
        if role_filter:
            jobs_query = jobs_query.filter(expert_role=role_filter)
        if template_filter:
            jobs_query = jobs_query.filter(template_id=template_filter)
        
        # Pagination
        total_jobs = jobs_query.count()
        start = (page - 1) * per_page
        end = start + per_page
        jobs = jobs_query[start:end]
        
        # Format job data
        jobs_data = []
        for job in jobs:
            job_data = {
                'job_id': job.job_id,
                'video_id': job.content_id,
                'status': job.status,
                'progress': job.progress,
                'expert_role': job.expert_role,
                'template_id': job.template_id,
                'created_at': job.created_at,
                'completed_at': job.completed_at,
                'thumbnail_url': f"https://img.youtube.com/vi/{job.content_id}/hqdefault.jpg",
                'video_url': f"https://www.youtube.com/watch?v={job.content_id}",
                'has_results': job.status == 'completed'
            }
            
            # Add summary if completed
            if job.status == 'completed':
                try:
                    result = job.result
                    job_data['summary'] = result.summary[:200] + '...' if len(result.summary) > 200 else result.summary
                    job_data['confidence_score'] = result.confidence_score
                except:
                    pass
            
            jobs_data.append(job_data)
        
        return Response({
            'jobs': jobs_data,
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_jobs': total_jobs,
                'total_pages': (total_jobs + per_page - 1) // per_page,
                'has_next': end < total_jobs,
                'has_previous': page > 1
            },
            'filters_applied': {
                'status': status_filter,
                'role': role_filter,
                'template': template_filter
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting jobs list: {e}")
        return Response({
            'error': 'Failed to retrieve jobs list'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)