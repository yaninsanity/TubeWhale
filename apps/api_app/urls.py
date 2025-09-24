"""
API App URLs
REST API Route Configuration
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)
from . import premium_views

# Temporarily simplified to avoid circular imports
app_name = 'api'

urlpatterns = [
    # JWT Authentication endpoints
    path('auth/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('auth/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('auth/token/verify/', TokenVerifyView.as_view(), name='token_verify'),
    
    # Premium user configuration endpoints
    path('premium/config/status/', premium_views.premium_config_status, name='premium_config_status'),
    path('premium/config/auto-setup/', premium_views.premium_auto_configure, name='premium_auto_configure'),
    path('premium/benefits/', premium_views.premium_benefits, name='premium_benefits'),
    path('premium/api-keys/', premium_views.user_api_keys, name='user_api_keys'),
    
    # Scenario analysis API
    path('scenarios/', include('apps.api_app.scenario_urls')),
    
    # User onboarding API
    path('guides/', include('apps.api_app.guide_urls')),
    
    # User interaction workflow API
    path('interaction/', include('apps.api_app.interaction_urls')),
]