"""
用户引导API URL配置
User Guide API URL Configuration
"""

from django.urls import path
from . import guide_views

app_name = 'guides'

urlpatterns = [
    # 用户引导主接口
    path('', guide_views.UserGuideView.as_view(), name='user_guide'),
    
    # 工具提示接口
    path('tooltip/<str:element_id>/', guide_views.TooltipView.as_view(), name='tooltip'),
    
    # 错误帮助接口
    path('error-help/', guide_views.ErrorHelpView.as_view(), name='error_help'),
]