"""
Multilingual URL Configuration for TubeWhale
"""

from django.urls import path, include
from django.conf.urls.i18n import i18n_patterns
from . import multilingual_views

app_name = 'multilingual'

# Language-specific API endpoints
urlpatterns = [
    # Language switching
    path('i18n/setlang/', multilingual_views.set_language, name='set_language'),
    
    # API endpoints for language information
    path('api/i18n/info/', multilingual_views.language_info, name='language_info'),
    path('api/i18n/info/<str:language_code>/', multilingual_views.language_info, name='language_info_specific'),
    path('api/i18n/languages/', multilingual_views.available_languages, name='available_languages'),
    path('api/i18n/translations/<str:language_code>/', multilingual_views.translations, name='translations'),
    path('api/i18n/detection/', multilingual_views.language_detection, name='language_detection'),
    path('api/i18n/context/', multilingual_views.language_context_view, name='language_context'),
    
    # User language preferences (requires authentication)
    path('api/i18n/user/preference/', multilingual_views.set_user_language_preference, name='user_language_preference'),
]