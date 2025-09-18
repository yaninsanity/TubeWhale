from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import CustomTemplateViewSet, health, expert_domains, expert_defaults, expert_compile, cli_config

router = DefaultRouter()
router.register(r"custom", CustomTemplateViewSet, basename="custom-template")

urlpatterns = [
    path("", include(router.urls)),
    path("health/", health, name="templates-health"),
    # Expert endpoints
    path("expert/domains/", expert_domains, name="expert-domains"),
    path("expert/defaults/", expert_defaults, name="expert-defaults"),
    path("expert/compile/", expert_compile, name="expert-compile"),
    # CLI configuration endpoint (auth)
    path("cli/config/", cli_config, name="cli-config"),
]
