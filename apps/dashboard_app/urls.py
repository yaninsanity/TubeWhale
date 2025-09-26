"""
Dashboard App URLs
Main dashboard and onboarding routes
"""

from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    # Main dashboard
    path('', views.dashboard_view, name='main'),
    
    # Onboarding flow
    path('setup/environment/', views.environment_setup_view, name='environment_setup'),
    path('setup/templates/', views.template_selection_view, name='template_selection'),
    
    # Template selection and browsing
    path('templates/', views.template_selection_view, name='template_selection'),
    path('templates/wizard/', views.intelligent_wizard_view, name='intelligent_wizard'),
    path('wizard/', views.smart_wizard_view, name='smart_wizard'),
    path('templates/search/', views.theme_search_view, name='theme_search'),
    path('templates/<str:template_id>/configure/', views.template_configure_view, name='template_configure'),
    path('templates/preview/<str:template_id>/', views.template_preview_view, name='template_preview'),
    path('templates/category/<slug:category_slug>/', views.template_category_view, name='template_category'),
    path('templates/<slug:template_slug>/', views.template_detail_view, name='template_detail'),
    
    # Template API endpoints
    path('api/template/<str:template_id>/preview/', views.template_preview_api, name='template_preview'),
    path('api/templates/search/', views.template_search_api, name='template_search'),
    
    # Job management
    path('jobs/', views.jobs_view, name='jobs'),
    path('jobs/<str:job_id>/', views.job_detail_view, name='job_detail'),
    path('job-runner/', views.job_runner_view, name='job_runner'),
    path('jobs/<str:job_id>/retry/', views.retry_job_view, name='retry_job'),
    path('jobs/<str:job_id>/cancel/', views.cancel_job_view, name='cancel_job'),
    path('api/jobs/<str:job_id>/progress/', views.job_progress_api, name='job_progress'),
    
    # CLI Interface
    path('cli/', views.cli_interface_view, name='cli_interface'),
    path('api/cli/execute/', views.cli_execute_api, name='cli_execute'),
    
    # Transparent Analysis Terminal
    path('terminal/', views.transparent_analysis_terminal, name='transparent_terminal'),
    path('api/transparent-analysis/', views.transparent_analysis_api, name='transparent_analysis_api'),
    path('api/analysis-status/<str:job_id>/', views.analysis_status_api, name='analysis_status_api'),
]