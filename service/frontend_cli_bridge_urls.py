"""
Frontend CLI Bridge URL Configuration
前端CLI桥接API的URL配置
"""

from django.urls import path
from service.frontend_cli_bridge import (
    frontend_cli_analyze,
    frontend_cli_templates,
    frontend_cli_validate_template,
    frontend_cli_batch_analyze
)

urlpatterns = [
    # 前端CLI桥接API
    path('api/v1/frontend-cli/analyze/', frontend_cli_analyze, name='frontend_cli_analyze'),
    path('api/v1/frontend-cli/templates/', frontend_cli_templates, name='frontend_cli_templates'),
    path('api/v1/frontend-cli/validate-template/', frontend_cli_validate_template, name='frontend_cli_validate_template'),
    path('api/v1/frontend-cli/batch-analyze/', frontend_cli_batch_analyze, name='frontend_cli_batch_analyze'),
]