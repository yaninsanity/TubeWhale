"""
Admin Control App Configuration
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class AdminControlConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.admin_control'
    verbose_name = _('Admin Control Panel')