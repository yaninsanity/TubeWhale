# 导入Celery应用以确保Django启动时加载
from .celery import app as celery_app

__all__ = ('celery_app',)
