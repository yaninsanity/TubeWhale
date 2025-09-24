"""
Scientist Authentication URLs
Client-side authentication routes
"""

from django.urls import path
from . import scientist_views

app_name = 'scientist'

urlpatterns = [
    # Authentication
    path('login/', scientist_views.scientist_login_view, name='login'),
    path('register/', scientist_views.scientist_register_view, name='register'),
    path('logout/', scientist_views.scientist_logout_view, name='logout'),
    
    # Profile management
    path('profile/', scientist_views.profile_view, name='profile'),
    
    # API endpoints
    path('api/auth-status/', scientist_views.check_auth_status, name='auth_status'),
]