"""
User Interaction Flow API URL Configuration
用户交互流程API URL配置

Complete click-through experience from welcome to analysis completion
"""

from django.urls import path
from . import interaction_views

app_name = 'interaction'

urlpatterns = [
    # Interactive Flow Management
    path('flow/', interaction_views.InteractionFlowView.as_view(), name='flow_management'),
    
    # Input Analysis and Processing
    path('input/analyze/', interaction_views.InputAnalysisView.as_view(), name='input_analysis'),
]