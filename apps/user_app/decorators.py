"""
Custom authentication decorators for scientist users
"""

from functools import wraps
from django.shortcuts import redirect
from django.urls import reverse
from django.contrib import messages
from django.utils.translation import gettext_lazy as _


def scientist_login_required(view_func):
    """
    Decorator that redirects unauthenticated users to scientist login page
    instead of Django admin login page
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if request.user.is_authenticated:
            return view_func(request, *args, **kwargs)
        else:
            # Store the attempted URL for redirect after login
            next_url = request.get_full_path()
            # Use admin login instead of non-existent scientist login
            login_url = '/admin/login/'
            
            # Add a friendly message
            messages.info(request, _('Please sign in to access your research dashboard.'))
            
            # Redirect to admin login with next parameter
            return redirect(f'{login_url}?next={next_url}')
    
    return _wrapped_view


def client_login_required(view_func):
    """
    Decorator that redirects unauthenticated users to client login page
    Designed for client-facing template and analysis views
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if request.user.is_authenticated:
            return view_func(request, *args, **kwargs)
        else:
            # Store the attempted URL for redirect after login
            next_url = request.get_full_path()
            # Use client login page
            login_url = '/client/login/'
            
            # Add a friendly message
            messages.info(request, _('Please sign in to access your analysis workspace.'))
            
            # Redirect to client login with next parameter
            return redirect(f'{login_url}?next={next_url}')
    
    return _wrapped_view