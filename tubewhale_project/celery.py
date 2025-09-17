"""
Celery Task Definitions
Celery异步任务定义
"""

import os
import sys
from celery import Celery
from django.conf import settings

# 设置Django环境
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')

# 创建Celery应用
app = Celery('tubewhale_project')

# 使用Django设置配置Celery
app.config_from_object('django.conf:settings', namespace='CELERY')

# 自动发现任务
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    """调试任务"""
    print(f'Request: {self.request!r}')
