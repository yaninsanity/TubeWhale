from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import CustomTemplateViewSet, health

router = DefaultRouter()
router.register(r"custom", CustomTemplateViewSet, basename="custom-template")

urlpatterns = [
    path("", include(router.urls)),
    path("health/", health, name="templates-health"),
]
