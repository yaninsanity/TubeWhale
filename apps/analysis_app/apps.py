from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class AnalysisAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.analysis_app'
    verbose_name = _('Analysis Management')
