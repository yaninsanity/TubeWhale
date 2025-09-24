"""
Client App URLs
客户端独立路由系统 - 四步骤分析工作流
"""

from django.urls import path
from . import views, workflow_views

app_name = 'client_app'

urlpatterns = [
    # 认证相关
    path('login/', views.client_login_view, name='login'),
    path('register/', views.client_register_view, name='register'),
    path('logout/', views.client_logout_view, name='logout'),
    
    # 用户资料
    path('profile/', views.client_profile_view, name='profile'),
    
    # 付费升级
    path('api/upgrade-tier/', views.upgrade_tier_view, name='upgrade_tier'),
    
    # === 四步骤分析工作流 ===
    # Step 0: Scenario Selection
    path('', workflow_views.client_scenario_selection, name='scenario_selection'),
    path('scenario/', workflow_views.client_scenario_selection, name='scenario_selection'),
    
    # Step 1: Engine Configuration
    path('configure/', workflow_views.client_workflow_start, name='workflow_start'),
    
    # Step 2: Template Selection  
    path('templates/', workflow_views.client_template_selection, name='template_selection'),
    
    # Step 3: Search & Execute
    path('execute/', workflow_views.client_search_execute, name='search_execute'),
    
    # Results and APIs
    path('results/<str:job_id>/', workflow_views.client_job_results, name='job_results'),
    path('api/job-status/<str:job_id>/', workflow_views.job_status_api, name='job_status_api'),
    path('download/<str:job_id>/', workflow_views.download_results, name='download_results'),
    
    # Job Management APIs
    path('api/start/single/', workflow_views.start_single_video, name='start_single_video'),
    path('api/start/playlist/', workflow_views.start_playlist, name='start_playlist'),
    path('api/start/wizard/', workflow_views.start_wizard_analysis, name='start_wizard_analysis'),
    path('api/jobs/', workflow_views.list_user_jobs_api, name='list_user_jobs'),
    path('api/jobs/<str:job_id>/cancel/', workflow_views.cancel_job_api, name='cancel_job'),
    path('api/jobs/<str:job_id>/delete/', workflow_views.delete_job_api, name='delete_job'),
    
    # Job Dashboard
    path('jobs/', workflow_views.user_jobs_dashboard, name='jobs_dashboard'),
    
    # 向后兼容的别名路径
    path('templates/<str:template_id>/configure/', workflow_views.client_template_configure_view, name='template_configure'),
    path('jobs/', workflow_views.client_job_runner_view, name='job_runner'),
]