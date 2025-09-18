"""
TubeWhale Project URLs
Main URL Configuration
"""

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.utils.translation import gettext_lazy as _
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
from apps.templates_app.admin_views import env_config_view

urlpatterns = [
    # Django Admin
    path('admin/', admin.site.urls),
    path('admin/env-config/', env_config_view, name='admin-env-config'),
    
    # User App URLs (registration, language switching)
    path('user/', include('apps.user_app.urls')),
    
    # API URLs
    path('api/', include('apps.api_app.urls')),
    # Intelligent Templates API
    path('api/templates/', include('apps.templates_app.urls')),
    
    # TubeWhale Engine URLs
    path('', include('apps.tubewhale_engine.urls')),
    
    # API Documentation
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# Custom admin titles
admin.site.site_header = _("TubeWhale Admin")
admin.site.site_title = _("TubeWhale")
admin.site.index_title = _("Welcome to TubeWhale Management System")
