"""
Dashboard App Configuration
"""

from django.apps import AppConfig


class DashboardAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.dashboard_app'
    verbose_name = 'Dashboard'