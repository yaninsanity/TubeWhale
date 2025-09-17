"""
URL configuration for tubewhale_project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import RedirectView

# Admin customization
admin.site.site_header = "🐋 TubeWhale Platform Admin"
admin.site.site_title = "TubeWhale Admin"
admin.site.index_title = "TubeWhale Platform Administration"


urlpatterns = [
    
    # Admin interface
    path('admin/', admin.site.urls),
    
    # Individual app URLs
    path('videos/', include('apps.video_app.urls')),
    path('users/', include('apps.user_app.urls')),
    path('analysis/', include('apps.analysis_app.urls')),
    path('experts/', include('apps.expert_app.urls')),
    path('dashboard/', include('apps.dashboard_app.urls')),
    
    # TubeWhale Engine API - 无需认证的健康检查
    path('', include('apps.tubewhale_engine.urls')),
    
    # API documentation
    path('api/docs/', include('apps.api_app.docs_urls')),
    
    # Root redirect to dashboard
    path('', RedirectView.as_view(url='/dashboard/', permanent=False)),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    
    # Debug toolbar (if available)
    if 'debug_toolbar' in settings.INSTALLED_APPS:
        import debug_toolbar
        urlpatterns = [
            path('__debug__/', include(debug_toolbar.urls)),
        ] + urlpatterns
