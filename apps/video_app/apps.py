from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class VideoAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.video_app'
    verbose_name = _('Video Management')
