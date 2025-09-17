from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class ApiAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.api_app'
    verbose_name = _('REST API Management')
