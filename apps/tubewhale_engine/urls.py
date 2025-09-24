"""
TubeWhale Engine URLs
"""

from django.urls import path
from . import views

app_name = 'tubewhale_engine'

urlpatterns = [
    # Health & System
    path('api/v1/tubewhale/health/', views.health_check_public, name='health-check-public'),
    path('api/v1/tubewhale/ping/', views.simple_ping, name='simple-ping'),
    path('api/v1/tubewhale/command-logs/', views.command_logs, name='command-logs'),
    
    # Analysis Endpoints - Core Features
    path('api/v1/analysis/video/', views.analyze_video, name='analyze-video'),
    path('api/v1/analysis/playlist/', views.analyze_playlist, name='analyze-playlist'),
    path('api/v1/analysis/batch/', views.analyze_batch, name='analyze-batch'),
    path('api/v1/analysis/<str:job_id>/', views.get_analysis_status, name='get-analysis-status'),
    
    # Expert Roles Management
    path('api/v1/roles/', views.list_expert_roles, name='list-roles'),
    path('api/v1/roles/<str:role_id>/', views.get_expert_role, name='get-role'),
    path('api/v1/roles/custom/', views.create_custom_role, name='create-custom-role'),
    
    # Templates Management
    path('api/v1/templates/', views.list_templates, name='list-templates'),
    path('api/v1/templates/<str:template_id>/', views.get_template, name='get-template'),
    path('api/v1/templates/custom/', views.create_custom_template, name='create-custom-template'),
    
    # Analysis Results Management - Enhanced
    path('api/analysis/<str:job_id>/', views.get_analysis_result, name='analysis-result'),
    path('api/analysis/<str:job_id>/download/', views.download_analysis_result, name='download-analysis-result'),
    path('api/analysis/history/', views.list_analysis_history, name='analysis-history'),
    
    # Legacy Job Management (kept for compatibility)
    path('api/v1/analysis/<str:job_id>/', views.get_job_status, name='job-status'),
    path('api/v1/analysis/<str:job_id>/results/', views.get_job_results, name='job-results'),
    path('api/v1/analysis/<str:job_id>/download/', views.download_results, name='download-results'),
    
    # User Flow Support
    path('api/v1/wizard/tools/', views.wizard_tool_selection, name='wizard-tools'),
    path('api/v1/wizard/roles/', views.wizard_role_selection, name='wizard-roles'),
    path('api/v1/wizard/templates/', views.wizard_template_selection, name='wizard-templates'),
]
