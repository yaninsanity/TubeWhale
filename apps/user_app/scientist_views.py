"""
Scientist Authentication Views
Client-side authentication for YouTube researchers
"""

from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from .models import User
from .scientist_forms import ScientistRegistrationForm, ScientistLoginForm, ProfileUpdateForm


def scientist_login_view(request):
    """Scientist login page (not admin)"""
    if request.user.is_authenticated:
        return redirect('dashboard:main')
    
    if request.method == 'POST':
        form = ScientistLoginForm(data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            remember_me = form.cleaned_data.get('remember_me', False)
            
            user = authenticate(request, username=username, password=password)
            
            if user is not None:
                login(request, user)
                
                # Set session expiry based on remember me
                if remember_me:
                    request.session.set_expiry(2592000)  # 30 days
                else:
                    request.session.set_expiry(0)  # Browser session
                
                messages.success(request, _('Welcome back, {}!').format(user.get_full_name() or user.username))
                next_url = request.GET.get('next', 'dashboard:main')
                return redirect(next_url)
            else:
                messages.error(request, _('Invalid username or password.'))
    else:
        form = ScientistLoginForm()
    
    return render(request, 'client/scientist_login.html', {
        'form': form,
        'title': _('Sign in to TubeWhale'),
        'subtitle': _('Access your YouTube research tools')
    })


def scientist_register_view(request):
    """Scientist registration page"""
    if request.user.is_authenticated:
        return redirect('dashboard:main')
        
    if request.method == 'POST':
        form = ScientistRegistrationForm(request.POST)
        if form.is_valid():
            user = form.save()
            username = form.cleaned_data.get('username')
            
            # Auto-login after registration
            user = authenticate(request, username=username, password=form.cleaned_data.get('password1'))
            if user:
                login(request, user)
                messages.success(request, _('Welcome to TubeWhale, {}! Your account has been created.').format(user.first_name or username))
                return redirect('dashboard:main')
    else:
        form = ScientistRegistrationForm()
    
    return render(request, 'client/register.html', {
        'form': form,
        'title': _('Join TubeWhale'),
        'subtitle': _('Create your scientist account')
    })


def scientist_logout_view(request):
    """Scientist logout"""
    if request.user.is_authenticated:
        name = request.user.get_full_name() or request.user.username
        logout(request)
        messages.info(request, _('Goodbye, {}! You have been logged out successfully.').format(name))
    return redirect('home')


def profile_view(request):
    """Scientist profile management"""
    if not request.user.is_authenticated:
        return redirect('user:login')
    
    if request.method == 'POST':
        form = ProfileUpdateForm(request.POST, instance=request.user)
        if form.is_valid():
            form.save()
            messages.success(request, _('Your profile has been updated successfully.'))
            return redirect('user:profile')
    else:
        form = ProfileUpdateForm(instance=request.user)
    
    return render(request, 'client/profile.html', {
        'form': form,
        'user': request.user,
        'title': _('My Profile'),
        'subtitle': _('Manage your account settings')
    })


def check_auth_status(request):
    """API endpoint to check authentication status"""
    return JsonResponse({
        'authenticated': request.user.is_authenticated,
        'username': request.user.username if request.user.is_authenticated else None,
        'tier': request.user.tier if request.user.is_authenticated else None
    })