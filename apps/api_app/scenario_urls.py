"""
场景分析API URL配置
Scenario Analysis API URL Configuration
"""

from django.urls import path
from . import scenario_views

app_name = 'scenarios'

urlpatterns = [
    # 场景列表和推荐
    path('', scenario_views.ScenarioListView.as_view(), name='scenario_list'),
    
    # 场景详情和指导
    path('<str:scenario_id>/', scenario_views.ScenarioDetailView.as_view(), name='scenario_detail'),
    
    # 开始分析
    path('analysis/start/', scenario_views.ScenarioStartAnalysisView.as_view(), name='start_analysis'),
]