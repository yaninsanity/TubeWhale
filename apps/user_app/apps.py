from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class UserAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.user_app'
    verbose_name = _('User Management')
