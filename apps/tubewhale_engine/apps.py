from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class TubewhaleEngineConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.tubewhale_engine'
    verbose_name = _('TubeWhale Engine')
    
    def ready(self):
        """在应用启动时导入TubeWhale核心组件"""
        try:
            # 确保agents可以被导入
            from agents import audio_agent, transcript_agent, summarizer_agent, search_agent, standardizer_agent
            print("✅ TubeWhale agents loaded successfully")
        except ImportError as e:
            print(f"⚠️ Warning: Could not import TubeWhale agents: {e}")
