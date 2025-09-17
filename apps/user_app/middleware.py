"""
Professional Enterprise-Grade Language Middleware for TubeWhale Admin
Implements AAA-level language switching with industrial best practices
"""

import logging
from django.conf import settings
from django.utils import translation
from django.utils.deprecation import MiddlewareMixin
from django.shortcuts import redirect
from django.urls import reverse

logger = logging.getLogger(__name__)


class AdminLanguageMiddleware(MiddlewareMixin):
    """
    Enterprise-grade admin language middleware with professional implementation
    Features:
    - Automatic language detection and activation
    - Persistent language storage across sessions
    - URL parameter processing with validation
    - Browser language fallback with intelligent mapping
    - Cookie-based persistence for better UX
    - Comprehensive logging for debugging
    """
    
    # 支持的语言列表（从settings.LANGUAGES获取）
    SUPPORTED_LANGUAGES = dict(settings.LANGUAGES)
    
    # 添加Django内部locale代码映射
    SUPPORTED_LANGUAGES.update({
        'zh-Hans': '简体中文',
        'zh-Hant': '繁體中文',
    })
    DEFAULT_ADMIN_LANGUAGE = 'en'
    
    def process_request(self, request):
        """
        Process incoming request and handle language switching
        """
        try:
            if self._is_admin_request(request):
                language = self._determine_admin_language(request)
                self._activate_language(request, language)
                
                # CRITICAL: Store our desired language separately from Django's system
                request.admin_desired_language = language
                request.LANGUAGE_CODE = language
                request.session['django_language'] = language
                
                logger.info(f"🌍 Admin language FORCED to: {language} for path: {request.path}")
                
            else:
                # Handle non-admin requests with standard detection
                language = self._determine_standard_language(request)
                self._activate_language(request, language)
                
        except Exception as e:
            logger.error(f"Language middleware error: {e}")
            # Fallback to default language
            self._activate_language(request, self.DEFAULT_ADMIN_LANGUAGE)
            request.admin_desired_language = self.DEFAULT_ADMIN_LANGUAGE
            request.LANGUAGE_CODE = self.DEFAULT_ADMIN_LANGUAGE
    
    def process_view(self, request, view_func, view_args, view_kwargs):
        """
        Process view - ensure language is still active before view execution
        """
        if self._is_admin_request(request) and hasattr(request, 'admin_desired_language'):
            # Double-check that our language is still active
            current_lang = translation.get_language()
            desired_lang = request.admin_desired_language
            
            if current_lang != desired_lang:
                logger.warning(f"🔥 Language mismatch detected! Current: {current_lang}, Desired: {desired_lang}")
                # Force re-activation
                translation.activate(desired_lang)
                logger.info(f"🔧 Language RE-FORCED to: {desired_lang}")
        
        return None
    
    def process_template_response(self, request, response):
        """
        Process template response - final language activation before template rendering
        """
        logger.info(f"🎯 TEMPLATE PROCESSING called for: {request.path}")
        
        if self._is_admin_request(request) and hasattr(request, 'admin_desired_language'):
            desired_lang = request.admin_desired_language
            current_lang = translation.get_language()
            
            logger.info(f"🎯 TEMPLATE: Admin request detected - Current: {current_lang}, Desired: {desired_lang}")
            
            if current_lang != desired_lang:
                logger.info(f"🎨 TEMPLATE: Re-activating language {desired_lang} (was {current_lang})")
                translation.activate(desired_lang)
                logger.info(f"🎨 TEMPLATE: After activation, language is now: {translation.get_language()}")
            else:
                logger.info(f"🎨 TEMPLATE: Language already correct: {current_lang}")
        else:
            logger.info(f"🎯 TEMPLATE: Not admin request or no admin_desired_language set")
        
        return response
    
    def process_response(self, request, response):
        """
        Process response and set language cookies for persistence
        """
        try:
            if hasattr(request, 'admin_desired_language') and self._is_admin_request(request):
                language = getattr(request, 'admin_desired_language', self.DEFAULT_ADMIN_LANGUAGE)
                
                # CRITICAL: Re-activate language to prevent LocaleMiddleware override
                current_lang = translation.get_language()
                if current_lang != language:
                    logger.info(f"🔧 FINAL language activation: {language} (was {current_lang})")
                    translation.activate(language)
                
                # Set admin-specific language cookie
                response.set_cookie(
                    'admin_language',  # Use our custom admin language cookie
                    language,
                    max_age=86400 * 365,  # 1 year
                    path='/admin/',
                    secure=request.is_secure(),
                    httponly=True,
                    samesite='Lax'
                )
                
                # Also set general language cookie for consistency
                response.set_cookie(
                    'django_language',  # Django's default language cookie
                    language,
                    max_age=86400 * 365,  # 1 year
                    path='/',
                    secure=request.is_secure(),
                    httponly=False,  # Allow JS access for frontend
                    samesite='Lax'
                )
                
                logger.debug(f"Language cookies set: {language}")
                
        except Exception as e:
            logger.error(f"Error setting language cookies: {e}")
            
        return response
    
    def _is_admin_request(self, request):
        """Check if request is for admin interface"""
        return request.path.startswith('/admin/')
    
    def _determine_admin_language(self, request):
        """
        Determine language for admin interface with priority:
        1. URL parameter (?admin_lang=zh-hans)
        2. Session storage
        3. Admin language cookie
        4. General language cookie
        5. Browser Accept-Language header
        6. Default language
        """
        
        # Priority 1: URL parameter (highest priority)
        url_lang = self._get_url_language(request)
        if url_lang:
            # Store in session for future requests
            request.session['admin_language'] = url_lang
            logger.info(f"Language set from URL parameter: {url_lang}")
            return url_lang
        
        # Check if this is a login redirect and extract language from next parameter
        if 'login' in request.path and 'next' in request.GET:
            next_url = request.GET.get('next', '')
            if 'admin_lang=' in next_url:
                import urllib.parse
                # Extract admin_lang from the next URL
                parsed = urllib.parse.parse_qs(urllib.parse.urlparse(next_url).query)
                if 'admin_lang' in parsed:
                    lang = parsed['admin_lang'][0]
                    
                    # 规范化locale代码
                    if lang == 'zh-hans':
                        lang = 'zh-Hans'
                    elif lang == 'zh-hant':
                        lang = 'zh-Hant'
                    
                    if self._is_supported_language(lang):
                        request.session['admin_language'] = lang
                        logger.info(f"Language extracted from login redirect: {lang}")
                        return lang
        
        # Priority 2: Session storage
        session_lang = request.session.get('admin_language')
        if session_lang and self._is_supported_language(session_lang):
            logger.debug(f"Language from session: {session_lang}")
            return session_lang
        
        # Priority 3: Admin language cookie
        admin_cookie_lang = request.COOKIES.get('admin_language')
        if admin_cookie_lang and self._is_supported_language(admin_cookie_lang):
            request.session['admin_language'] = admin_cookie_lang
            logger.debug(f"Language from admin cookie: {admin_cookie_lang}")
            return admin_cookie_lang
        
        # Priority 4: General language cookie
        general_cookie_lang = request.COOKIES.get('django_language')
        if general_cookie_lang and self._is_supported_language(general_cookie_lang):
            logger.debug(f"Language from general cookie: {general_cookie_lang}")
            return general_cookie_lang
        
        # Priority 5: Browser language detection
        browser_lang = self._detect_browser_language(request)
        if browser_lang:
            logger.debug(f"Language from browser: {browser_lang}")
            return browser_lang
        
        # Priority 6: Default fallback
        logger.debug(f"Using default language: {self.DEFAULT_ADMIN_LANGUAGE}")
        return self.DEFAULT_ADMIN_LANGUAGE
    
    def _determine_standard_language(self, request):
        """Determine language for non-admin requests"""
        # Use Django's standard language detection
        return (
            request.session.get('django_language') or
            request.COOKIES.get('django_language') or
            self._detect_browser_language(request) or
            self.DEFAULT_ADMIN_LANGUAGE
        )
    
    def _get_url_language(self, request):
        """Extract and validate language from URL parameter"""
        lang = request.GET.get('admin_lang')
        if lang:
            # 规范化locale代码以匹配Django的内部格式
            if lang == 'zh-hans':
                lang = 'zh-Hans'
            elif lang == 'zh-hant':
                lang = 'zh-Hant'
            
            if self._is_supported_language(lang):
                return lang
        return None
    
    def _is_supported_language(self, language):
        """Check if language is supported"""
        return language in self.SUPPORTED_LANGUAGES
    
    def _detect_browser_language(self, request):
        """
        Detect language from browser Accept-Language header
        with intelligent mapping for Chinese variants
        """
        accept_language = request.META.get('HTTP_ACCEPT_LANGUAGE', '')
        if not accept_language:
            return None
        
        # Parse Accept-Language header
        languages = []
        for lang_item in accept_language.split(','):
            if ';' in lang_item:
                lang = lang_item.split(';')[0].strip()
            else:
                lang = lang_item.strip()
            
            lang = lang.lower()
            
            # Map common language codes to our supported languages
            if lang.startswith('zh'):
                # Chinese variants mapping
                if 'cn' in lang or 'hans' in lang or lang == 'zh':
                    return 'zh-hans'
            elif lang.startswith('ja'):
                return 'ja'
            elif lang.startswith('en'):
                return 'en'
        
        return None
    
    def _activate_language(self, request, language):
        """Activate language and set request attributes with force"""
        if not language:
            language = self.DEFAULT_ADMIN_LANGUAGE
        
        # FORCE activation of Django translation - this is critical!
        translation.activate(language)
        
        # Set multiple request attributes to ensure language persistence
        request.LANGUAGE_CODE = language
        request.session['django_language'] = language
        request.session['admin_language'] = language
        
        # Force set thread-local language to ensure it's used in templates
        from django.utils import translation as trans
        trans.activate(language)
        
        logger.debug(f"Language FORCE activated: {language}, Thread language: {trans.get_language()}")


class EnterpriseLanguageMiddleware(MiddlewareMixin):
    """
    Alternative enterprise-grade middleware that can replace Django's LocaleMiddleware
    for more control over language handling
    """
    
    def process_request(self, request):
        """Handle language detection for entire application"""
        # This would be used if we want to replace Django's LocaleMiddleware
        pass
    
    def process_response(self, request, response):
        # Set language cookie if not admin
        if not request.path.startswith('/admin/') and hasattr(request, 'LANGUAGE_CODE'):
            language = request.LANGUAGE_CODE
            if language != response.cookies.get('language'):
                response.set_cookie(
                    'language', 
                    language, 
                    max_age=settings.SESSION_COOKIE_AGE,
                    httponly=True
                )
        
        return response
