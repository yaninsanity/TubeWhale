"""
Simplified Admin Language Middleware for TubeWhale
Fixes admin logout issues by avoiding session interference
"""

import logging
from django.conf import settings
from django.utils import translation
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)


class AdminLanguageMiddleware(MiddlewareMixin):
    """
    Simplified admin language middleware that doesn't interfere with sessions
    Focuses on language switching without affecting authentication
    """
    
    SUPPORTED_LANGUAGES = dict(settings.LANGUAGES)
    DEFAULT_ADMIN_LANGUAGE = 'en'
    
    def process_request(self, request):
        """
        Simple language detection and activation
        """
        try:
            if self._is_admin_request(request):
                language = self._determine_admin_language(request)
                translation.activate(language)
                request.LANGUAGE_CODE = language
                
                # Store language preference WITHOUT affecting session keys
                if hasattr(request, 'session') and request.session.session_key:
                    request.session['admin_language'] = language
                    
                logger.debug(f"Admin language set to: {language}")
                
        except Exception as e:
            logger.error(f"Language middleware error: {e}")
            translation.activate(self.DEFAULT_ADMIN_LANGUAGE)
    
    def process_response(self, request, response):
        """
        Set language cookie for persistence
        """
        try:
            if self._is_admin_request(request):
                language = getattr(request, 'LANGUAGE_CODE', self.DEFAULT_ADMIN_LANGUAGE)
                
                # Set admin language cookie
                response.set_cookie(
                    'admin_language',
                    language,
                    max_age=86400 * 30,  # 30 days
                    path='/admin/',
                    secure=request.is_secure(),
                    httponly=True,
                    samesite='Lax'
                )
                
        except Exception as e:
            logger.error(f"Error setting language cookie: {e}")
            
        return response
    
    def _is_admin_request(self, request):
        """Check if request is for admin interface"""
        return request.path.startswith('/admin/')
    
    def _determine_admin_language(self, request):
        """
        Determine admin language from multiple sources
        Priority: URL param > Cookie > Session > Browser > Default
        """
        # 1. URL parameter (highest priority)
        url_language = request.GET.get('language')
        if url_language and url_language in self.SUPPORTED_LANGUAGES:
            return url_language
            
        # 2. Admin-specific cookie
        cookie_language = request.COOKIES.get('admin_language')
        if cookie_language and cookie_language in self.SUPPORTED_LANGUAGES:
            return cookie_language
            
        # 3. Session language
        if hasattr(request, 'session'):
            session_language = request.session.get('admin_language')
            if session_language and session_language in self.SUPPORTED_LANGUAGES:
                return session_language
                
        # 4. Browser language (Accept-Language header)
        if hasattr(request, 'META') and 'HTTP_ACCEPT_LANGUAGE' in request.META:
            browser_language = request.META['HTTP_ACCEPT_LANGUAGE'].split(',')[0].strip()
            
            # Map common browser language codes
            language_mapping = {
                'zh-CN': 'zh-hans',
                'zh-TW': 'zh-hant',
                'zh-HK': 'zh-hant',
                'en-US': 'en',
                'en-GB': 'en',
            }
            
            browser_language = language_mapping.get(browser_language, browser_language)
            
            if browser_language in self.SUPPORTED_LANGUAGES:
                return browser_language
                

