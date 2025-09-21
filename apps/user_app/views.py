"""
User App Views
Authentication, registration, and language switching views
"""

from django.shortcuts import render, redirect
from django.contrib.auth import login
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.http import JsonResponse, HttpResponseRedirect
from django.conf import settings
from django.utils import translation, timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.urls import reverse
from .forms import InvitationCodeRegistrationForm
from .models import InvitationCode


def register_view(request):
    """Registration view with invitation code validation"""
    if request.method == 'POST':
        form = InvitationCodeRegistrationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, _('Registration successful! Welcome to TubeWhale.'))
            return redirect('admin:index')  # Redirect to admin after registration
        else:
            messages.error(request, _('Please correct the errors below.'))
    else:
        form = InvitationCodeRegistrationForm()
    
    return render(request, 'user_app/register.html', {'form': form})


@csrf_exempt
@require_POST
def set_language(request):
    """Set language preference"""
    language = request.POST.get('language')
    
    if language and language in [lang[0] for lang in settings.LANGUAGES]:
        # Save to session
        request.session['language'] = language
        # Activate for current request
        translation.activate(language)
        
        response = JsonResponse({'status': 'success', 'language': language})
        # Set cookie for future requests
        response.set_cookie(
            'language', 
            language, 
            max_age=settings.SESSION_COOKIE_AGE,
            httponly=True
        )
        return response
    
    return JsonResponse({'status': 'error', 'message': 'Invalid language'})


@csrf_exempt  
def set_admin_language(request):
    """Set admin language preference with redirect"""
    if request.method == 'POST':
        language = request.POST.get('language')
        
        if language and language in [lang[0] for lang in settings.LANGUAGES]:
            # Save to session with admin-specific keys
            request.session['admin_language'] = language
            request.session['django_language'] = language
            
            # Activate for current request
            translation.activate(language)
            
            # Get redirect URL from next parameter or default to admin
            next_url = request.POST.get('next', '/admin/')
            
            response = HttpResponseRedirect(next_url)
            
            # Set admin-specific language cookies
            response.set_cookie(
                'admin_language',
                language,
                max_age=86400 * 365,  # 1 year
                path='/admin/',
                secure=request.is_secure(),
                httponly=True,
                samesite='Lax'
            )
            
            response.set_cookie(
                'django_language',
                language,
                max_age=86400 * 365,  # 1 year
                path='/',
                secure=request.is_secure(),
                httponly=False,  # Allow JS access
                samesite='Lax'
            )
            
            return response
        
        # Invalid language - redirect to admin with error
        return HttpResponseRedirect('/admin/')
    
    # GET request - show language selection or redirect
    return HttpResponseRedirect('/admin/')
def language_switch_view(request):
    """Language switching page"""
    current_language = translation.get_language()
    return render(request, 'user_app/language_switch.html', {
        'languages': settings.LANGUAGES,
        'current_language': current_language
    })


def invitation_status_view(request):
    """Check invitation code status (AJAX endpoint)"""
    code = request.GET.get('code', '').upper()
    
    if not code:
        return JsonResponse({'valid': False, 'message': _('Code is required')})
    
    try:
        invitation = InvitationCode.objects.get(code=code)
        if invitation.is_valid():
            return JsonResponse({
                'valid': True, 
                'message': _('Valid invitation code'),
                'expires_at': invitation.expires_at.isoformat(),
                'max_uses': invitation.max_uses,
                'current_uses': invitation.current_uses
            })
        else:
            if invitation.current_uses >= invitation.max_uses:
                message = _('This code has been used up')
            elif invitation.expires_at < timezone.now():
                message = _('This code has expired')
            else:
                message = _('This code is not valid')
            
            return JsonResponse({'valid': False, 'message': message})
    
    except InvitationCode.DoesNotExist:
        return JsonResponse({'valid': False, 'message': _('Invalid code')})
