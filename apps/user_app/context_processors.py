"""
User Context Processors for TubeWhale
Provides user-related context data to templates
"""

from django.contrib.auth.models import AnonymousUser


def user_context(request):
    """
    Add user-related context variables to all templates
    
    Args:
        request: HTTP request object
        
    Returns:
        dict: Context variables for templates
    """
    user = getattr(request, 'user', AnonymousUser())
    
    context = {
        'current_user': user,
        'is_authenticated': user.is_authenticated,
        'user_tier': getattr(user, 'tier', 'basic') if user.is_authenticated else 'basic',
        'user_tier_display': getattr(user, 'get_tier_display_name', lambda: 'Basic')() if user.is_authenticated else 'Basic',
        'can_switch_templates': getattr(user, 'can_switch_templates', lambda: False)() if user.is_authenticated else False,
        'can_customize_templates': getattr(user, 'can_customize_templates', lambda: False)() if user.is_authenticated else False,
        'is_enterprise_user': getattr(user, 'is_enterprise_user', lambda: False)() if user.is_authenticated else False,
    }
    
    # Add user profile data if available
    if hasattr(request, 'user_profile'):
        context['user_profile'] = request.user_profile
    
    return context


def user_preferences_context(request):
    """
    Add user preference context (language, timezone, etc.)
    
    Args:
        request: HTTP request object
        
    Returns:
        dict: User preference context variables
    """
    user = getattr(request, 'user', AnonymousUser())
    
    if not user.is_authenticated:
        return {
            'user_language': 'en',
            'user_timezone': 'UTC',
        }
    
    return {
        'user_language': getattr(user, 'language', 'en'),
        'user_timezone': getattr(user, 'timezone_setting', 'UTC'),
        'user_verified': getattr(user, 'is_verified', False),
    }