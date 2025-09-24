"""
Admin Control Panel URLs
"""

from django.urls import path
from . import views

app_name = 'admin_control'

urlpatterns = [
    path('cli-control/', views.admin_cli_control_panel, name='cli_control_panel'),
    path('cli-execute/', views.admin_execute_cli_command, name='execute_cli_command'),
    path('job-management/', views.admin_job_management, name='job_management'),
    path('job-cancel/<int:job_id>/', views.admin_cancel_job, name='cancel_job'),
    path('system-metrics/', views.admin_system_metrics, name='system_metrics'),
]