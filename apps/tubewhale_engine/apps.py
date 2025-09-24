from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class TubewhaleEngineConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.tubewhale_engine'
    verbose_name = _('TubeWhale Engine')
    
    def ready(self):
        """Import TubeWhale core components on application startup"""
        try:
            # Ensure agents can be imported
            from agents import audio_agent, transcript_agent, summarizer_agent, search_agent, standardizer_agent
            print("✅ TubeWhale agents loaded successfully")
        except ImportError as e:
            print(f"⚠️ Warning: Could not import TubeWhale agents: {e}")
