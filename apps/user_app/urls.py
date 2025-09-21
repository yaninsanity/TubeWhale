"""
User App URL Configuration
"""

from django.urls import path
from . import views

app_name = 'user_app'

urlpatterns = [
    path('register/', views.register_view, name='register'),
    path('set-language/', views.set_language, name='set_language'),
    path('set-admin-language/', views.set_admin_language, name='set_admin_language'),
    path('language/', views.language_switch_view, name='language_switch'),
    path('invitation-status/', views.invitation_status_view, name='invitation_status'),
]
