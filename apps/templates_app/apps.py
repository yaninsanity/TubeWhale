from django.apps import AppConfig


class TemplatesAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.templates_app"
    verbose_name = "Intelligent Templates"

    def ready(self):  # pragma: no cover
        import os
        import logging
        logger = logging.getLogger(__name__)
        # Enforce psutil as a hard dependency now (no more silent degradation producing confusing banner)
        try:  # pragma: no cover - simple dependency assertion
            import psutil  # type: ignore  # noqa: F401
            logger.info("[templates_app] psutil present: system metrics fully enabled.")
        except Exception as e:  # pragma: no cover
            from django.core.exceptions import ImproperlyConfigured
            logger.error("[templates_app] psutil missing – install psutil to proceed (now required).")
            raise ImproperlyConfigured("psutil is required for runtime metrics; install psutil>=5.9,<6.0.") from e
        if os.environ.get('TUBEWHALE_AUTO_SEED', 'false').lower() == 'true':
            try:
                from django.core.management import call_command
                call_command('seed_templates_and_prompts')
            except Exception:
                # Fail silently to avoid blocking startup (e.g., during migrations)
                pass
