"""
Multilingual API Views for TubeWhale
English-First Language Switching and Translation Support
"""

from django.shortcuts import render
from django.http import JsonResponse, HttpResponseRedirect
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.utils import translation
from django.utils.translation import gettext as _
from django.conf import settings
from django.urls import reverse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import json
import logging

from tubewhale_project.multilingual_config import get_language_info, RTL_LANGUAGES

logger = logging.getLogger(__name__)

@require_http_methods(["POST"])
def set_language(request):
    """
    Set user's preferred language
    Enhanced version of Django's built-in set_language view
    """
    next_url = request.POST.get('next', request.GET.get('next', '/'))
    language_code = request.POST.get('language', request.GET.get('lang', settings.LANGUAGE_CODE))
    
    # Validate language code
    valid_languages = [lang[0] for lang in settings.LANGUAGES]
    if language_code not in valid_languages:
        logger.warning(f"Invalid language code attempted: {language_code}")
        language_code = settings.LANGUAGE_CODE
    
    # Activate language for this request
    translation.activate(language_code)
    
    # Create response
    response = HttpResponseRedirect(next_url)
    
    # Set language cookie
    response.set_cookie(
        settings.LANGUAGE_COOKIE_NAME,
        language_code,
        max_age=settings.LANGUAGE_COOKIE_AGE,
        path=settings.LANGUAGE_COOKIE_PATH,
        domain=settings.LANGUAGE_COOKIE_DOMAIN,
        secure=settings.LANGUAGE_COOKIE_SECURE if hasattr(settings, 'LANGUAGE_COOKIE_SECURE') else False,
        httponly=False,  # Allow JavaScript access for dynamic content
        samesite='Lax'
    )
    
    # Store in session as well
    if hasattr(request, 'session'):
        request.session[settings.LANGUAGE_SESSION_KEY] = language_code
    
    logger.info(f"Language changed to {language_code} for user {request.user.id if request.user.is_authenticated else 'anonymous'}")
    
    return response

@api_view(['GET'])
def language_info(request, language_code=None):
    """
    Get language information
    """
    if not language_code:
        language_code = translation.get_language() or settings.LANGUAGE_CODE
    
    # Validate language code
    valid_languages = [lang[0] for lang in settings.LANGUAGES]
    if language_code not in valid_languages:
        return Response({'error': 'Invalid language code'}, status=status.HTTP_400_BAD_REQUEST)
    
    # Get language info
    lang_info = get_language_info(language_code)
    
    return Response({
        'code': language_code,
        'info': lang_info,
        'is_rtl': language_code in RTL_LANGUAGES,
        'is_current': language_code == translation.get_language(),
    })

@api_view(['GET'])
def available_languages(request):
    """
    Get list of available languages with their information
    """
    languages = []
    current_language = translation.get_language() or settings.LANGUAGE_CODE
    
    for lang_code, lang_name in settings.LANGUAGES:
        lang_info = get_language_info(lang_code)
        languages.append({
            'code': lang_code,
            'name': lang_info['name'],
            'native_name': lang_info['native_name'],
            'flag': lang_info['flag'],
            'direction': lang_info['direction'],
            'is_current': lang_code == current_language,
            'is_rtl': lang_code in RTL_LANGUAGES,
        })
    
    return Response({
        'languages': languages,
        'current': current_language,
        'default': settings.LANGUAGE_CODE,
    })

@api_view(['GET'])
def translations(request, language_code):
    """
    Get translation strings for JavaScript
    """
    # Validate language code
    valid_languages = [lang[0] for lang in settings.LANGUAGES]
    if language_code not in valid_languages:
        return Response({'error': 'Invalid language code'}, status=status.HTTP_400_BAD_REQUEST)
    
    # Activate language
    translation.activate(language_code)
    
    # Common translations for JavaScript
    js_translations = {
        # Core interface
        'loading': _('Loading...'),
        'error': _('Error'),
        'success': _('Success'),
        'warning': _('Warning'),
        'info': _('Information'),
        'close': _('Close'),
        'cancel': _('Cancel'),
        'save': _('Save'),
        'delete': _('Delete'),
        'edit': _('Edit'),
        'submit': _('Submit'),
        'search': _('Search'),
        'filter': _('Filter'),
        'refresh': _('Refresh'),
        'back': _('Back'),
        'next': _('Next'),
        'previous': _('Previous'),
        'continue': _('Continue'),
        'finish': _('Finish'),
        
        # Wizard interface
        'wizard_title': _('TubeWhale YouTube Analysis Wizard'),
        'select_tool': _('Select Your Analysis Tool'),
        'choose_role': _('Select Your Professional Role'),
        'configure_analysis': _('Configure Analysis'),
        'start_analysis': _('Start Analysis'),
        'single_video': _('Single Video Analysis'),
        'playlist_analysis': _('Playlist Analysis'),
        'brainstorm_tool': _('Brainstorm Tool'),
        'content_creator': _('Content Creator'),
        'marketing_expert': _('Marketing Expert'),
        'data_analyst': _('Data Analyst'),
        
        # Form elements
        'youtube_url': _('YouTube Video URL or ID'),
        'enter_url': _('Enter the YouTube video URL or video ID'),
        'analysis_depth': _('Analysis Depth'),
        'depth_basic': _('Basic'),
        'depth_comprehensive': _('Comprehensive'),
        'depth_expert': _('Expert'),
        'custom_questions': _('Custom Thinking Questions'),
        'enable_custom': _('Enable custom questions'),
        'question_1': _('Question 1'),
        'question_2': _('Question 2'),
        'question_3': _('Question 3'),
        
        # Status messages
        'analysis_starting': _('Starting analysis...'),
        'analysis_complete': _('Analysis complete!'),
        'analysis_failed': _('Analysis failed. Please try again.'),
        'invalid_url': _('Please enter a valid YouTube URL'),
        'processing': _('Processing your request...'),
        'please_wait': _('Please wait...'),
        
        # Language interface
        'select_language': _('Select Language'),
        'current_language': _('Current Language'),
        'switch_language': _('Switch Language'),
        'language_changed': _('Language changed successfully'),
        
        # Validation
        'field_required': _('This field is required'),
        'invalid_format': _('Invalid format'),
        'url_invalid': _('Please enter a valid URL'),
        'video_not_found': _('Video not found'),
        'access_denied': _('Access denied'),
    }
    
    return Response(js_translations)

@api_view(['POST'])
def set_user_language_preference(request):
    """
    Set authenticated user's language preference
    """
    if not request.user.is_authenticated:
        return Response({'error': 'Authentication required'}, status=status.HTTP_401_UNAUTHORIZED)
    
    language_code = request.data.get('language')
    if not language_code:
        return Response({'error': 'Language code required'}, status=status.HTTP_400_BAD_REQUEST)
    
    # Validate language code
    valid_languages = [lang[0] for lang in settings.LANGUAGES]
    if language_code not in valid_languages:
        return Response({'error': 'Invalid language code'}, status=status.HTTP_400_BAD_REQUEST)
    
    # Update user profile
    try:
        user_profile = request.user.userprofile
        user_profile.preferred_language = language_code
        user_profile.save()
        
        # Activate language for current session
        translation.activate(language_code)
        request.session[settings.LANGUAGE_SESSION_KEY] = language_code
        
        logger.info(f"User {request.user.id} set language preference to {language_code}")
        
        return Response({
            'message': _('Language preference saved successfully'),
            'language': language_code,
            'info': get_language_info(language_code)
        })
        
    except Exception as e:
        logger.error(f"Error setting user language preference: {e}")
        return Response({'error': 'Failed to save language preference'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def language_detection(request):
    """
    Detect user's preferred language from various sources
    """
    detection_results = {}
    
    # From URL parameter
    url_lang = request.GET.get('lang')
    if url_lang:
        detection_results['url'] = url_lang
    
    # From session
    session_lang = request.session.get(settings.LANGUAGE_SESSION_KEY)
    if session_lang:
        detection_results['session'] = session_lang
    
    # From cookie
    cookie_lang = request.COOKIES.get(settings.LANGUAGE_COOKIE_NAME)
    if cookie_lang:
        detection_results['cookie'] = cookie_lang
    
    # From user profile (if authenticated)
    if request.user.is_authenticated:
        try:
            profile_lang = request.user.userprofile.preferred_language
            if profile_lang:
                detection_results['profile'] = profile_lang
        except:
            pass
    
    # From Accept-Language header
    if hasattr(request, 'META') and 'HTTP_ACCEPT_LANGUAGE' in request.META:
        accept_languages = request.META['HTTP_ACCEPT_LANGUAGE']
        # Parse Accept-Language header
        for lang_range in accept_languages.split(','):
            lang_code = lang_range.split(';')[0].strip().lower()
            # Check if we support this language
            for supported_lang, _ in settings.LANGUAGES:
                if lang_code.startswith(supported_lang) or supported_lang.startswith(lang_code):
                    detection_results['browser'] = supported_lang
                    break
            if 'browser' in detection_results:
                break
    
    # Current active language
    current_language = translation.get_language() or settings.LANGUAGE_CODE
    
    return Response({
        'detected': detection_results,
        'current': current_language,
        'default': settings.LANGUAGE_CODE,
        'recommended': detection_results.get('profile') or 
                      detection_results.get('session') or 
                      detection_results.get('cookie') or 
                      detection_results.get('url') or 
                      detection_results.get('browser') or 
                      settings.LANGUAGE_CODE
    })

def language_context_view(request):
    """
    Render language context for JavaScript initialization
    """
    current_language = translation.get_language() or settings.LANGUAGE_CODE
    lang_info = get_language_info(current_language)
    
    # Prepare available languages
    available_languages = []
    for lang_code, lang_name in settings.LANGUAGES:
        lang_data = get_language_info(lang_code)
        available_languages.append({
            'code': lang_code,
            'name': lang_data['name'],
            'native_name': lang_data['native_name'],
            'flag': lang_data['flag'],
            'direction': lang_data['direction'],
            'is_current': lang_code == current_language,
        })
    
    context = {
        'CURRENT_LANGUAGE': current_language,
        'CURRENT_LANGUAGE_INFO': lang_info,
        'AVAILABLE_LANGUAGES': available_languages,
        'LANGUAGE_CODES': [lang[0] for lang in settings.LANGUAGES],
        'IS_RTL': current_language in RTL_LANGUAGES,
        'DEFAULT_LANGUAGE': settings.LANGUAGE_CODE,
    }
    
    # Render as JavaScript
    js_content = f"window.LANGUAGE_CONTEXT = {json.dumps(context, ensure_ascii=False)};"
    
    return JsonResponse({
        'context': context,
        'javascript': js_content
    })

class LanguageMiddleware:
    """
    Enhanced language middleware with better detection
    """
    def __init__(self, get_response):
        self.get_response = get_response
    
    def __call__(self, request):
        # Language detection order: URL param -> User profile -> Session -> Cookie -> Browser -> Default
        language = self.detect_language(request)
        
        if language:
            translation.activate(language)
            request.LANGUAGE_CODE = language
        
        response = self.get_response(request)
        
        # Add language info to response headers
        if hasattr(request, 'LANGUAGE_CODE'):
            response['Content-Language'] = request.LANGUAGE_CODE
            response['X-Language-Info'] = json.dumps(get_language_info(request.LANGUAGE_CODE))
        
        return response
    
    def detect_language(self, request):
        """Detect preferred language from multiple sources"""
        # 1. URL parameter (highest priority)
        url_lang = request.GET.get('lang')
        if url_lang and self.is_valid_language(url_lang):
            return url_lang
        
        # 2. User profile (if authenticated)
        if request.user.is_authenticated:
            try:
                profile_lang = request.user.userprofile.preferred_language
                if profile_lang and self.is_valid_language(profile_lang):
                    return profile_lang
            except:
                pass
        
        # 3. Session
        session_lang = request.session.get(settings.LANGUAGE_SESSION_KEY)
        if session_lang and self.is_valid_language(session_lang):
            return session_lang
        
        # 4. Cookie
        cookie_lang = request.COOKIES.get(settings.LANGUAGE_COOKIE_NAME)
        if cookie_lang and self.is_valid_language(cookie_lang):
            return cookie_lang
        
        # 5. Browser Accept-Language
        if hasattr(request, 'META') and 'HTTP_ACCEPT_LANGUAGE' in request.META:
            browser_lang = self.parse_accept_language(request.META['HTTP_ACCEPT_LANGUAGE'])
            if browser_lang:
                return browser_lang
        
        # 6. Default
        return settings.LANGUAGE_CODE
    
    def is_valid_language(self, language_code):
        """Check if language code is supported"""
        valid_languages = [lang[0] for lang in settings.LANGUAGES]
        return language_code in valid_languages
    
    def parse_accept_language(self, accept_language):
        """Parse Accept-Language header and return best match"""
        for lang_range in accept_language.split(','):
            lang_code = lang_range.split(';')[0].strip().lower()
            # Direct match
            if self.is_valid_language(lang_code):
                return lang_code
            # Language prefix match (e.g., 'en-US' -> 'en')
            lang_prefix = lang_code.split('-')[0]
            if self.is_valid_language(lang_prefix):
                return lang_prefix
        return None