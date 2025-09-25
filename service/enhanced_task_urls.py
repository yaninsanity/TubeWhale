# Enhanced Task Management URLs
# 增强任务管理的URL配置

from django.urls import path, include
from service.enhanced_task_api import (
    submit_enhanced_task,
    get_enhanced_task_status,
    pause_enhanced_task,
    resume_enhanced_task,
    cancel_enhanced_task,
    get_enhanced_task_partial_results,
    download_enhanced_task_results,
    get_enhanced_queue_status,
    EnhancedTaskView,
    TaskResultsDownloadView
)

# API路径
api_urlpatterns = [
    # 任务管理API
    path('tasks/', submit_enhanced_task, name='submit_enhanced_task'),
    path('tasks/<str:task_id>/', get_enhanced_task_status, name='get_enhanced_task_status'),
    path('tasks/<str:task_id>/pause/', pause_enhanced_task, name='pause_enhanced_task'),
    path('tasks/<str:task_id>/resume/', resume_enhanced_task, name='resume_enhanced_task'),
    path('tasks/<str:task_id>/cancel/', cancel_enhanced_task, name='cancel_enhanced_task'),
    path('tasks/<str:task_id>/partial/', get_enhanced_task_partial_results, name='get_enhanced_task_partial_results'),
    path('tasks/<str:task_id>/download/', download_enhanced_task_results, name='download_enhanced_task_results'),
    
    # 队列状态API
    path('queue/status/', get_enhanced_queue_status, name='get_enhanced_queue_status'),
]

# 页面视图路径
view_urlpatterns = [
    # 任务管理视图
    path('task-manager/', EnhancedTaskView.as_view(), name='enhanced_task_manager'),
    path('task-manager/<str:task_id>/', EnhancedTaskView.as_view(), name='enhanced_task_detail'),
    
    # 结果下载视图
    path('download/<str:task_id>/', TaskResultsDownloadView.as_view(), name='task_results_download'),
]

# 完整的URL模式
urlpatterns = [
    path('api/v1/enhanced/', include(api_urlpatterns)),
    path('enhanced/', include(view_urlpatterns)),
]