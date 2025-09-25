"""
Language Context Processor for TubeWhale Templates
Provides multilingual support context to all templates
"""

from django.conf import settings
from django.utils import translation
from django.utils.translation import gettext as _
from django.http import HttpRequest
from tubewhale_project.multilingual_config import get_language_info, RTL_LANGUAGES

def language_context(request: HttpRequest) -> dict:
    """
    Add language-related context variables to all templates
    
    Args:
        request: HTTP request object
        
    Returns:
        dict: Context variables for templates
    """
    current_language = translation.get_language() or settings.LANGUAGE_CODE
    
    # Get language information
    lang_info = get_language_info(current_language)
    
    # Prepare available languages with their info
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
            'url': f'?lang={lang_code}' if request else f'/set-language/{lang_code}/',
        })
    
    # Language switching URLs
    current_url = request.get_full_path() if request else '/'
    language_switch_urls = {}
    
    for lang_code, _ in settings.LANGUAGES:
        # Preserve current URL parameters while changing language
        if '?' in current_url:
            base_url, params = current_url.split('?', 1)
            # Remove existing lang parameter
            param_pairs = [p for p in params.split('&') if not p.startswith('lang=')]
            # Add new lang parameter
            param_pairs.append(f'lang={lang_code}')
            language_switch_urls[lang_code] = f"{base_url}?{'&'.join(param_pairs)}"
        else:
            language_switch_urls[lang_code] = f"{current_url}?lang={lang_code}"
    
    # RTL support
    is_rtl = current_language in RTL_LANGUAGES
    
    return {
        # Current language info
        'CURRENT_LANGUAGE': current_language,
        'CURRENT_LANGUAGE_INFO': lang_info,
        'CURRENT_LANGUAGE_NAME': lang_info['native_name'],
        'CURRENT_LANGUAGE_FLAG': lang_info['flag'],
        'CURRENT_LANGUAGE_DIRECTION': lang_info['direction'],
        
        # Language list
        'AVAILABLE_LANGUAGES': available_languages,
        'LANGUAGE_SWITCH_URLS': language_switch_urls,
        
        # RTL support
        'IS_RTL': is_rtl,
        'TEXT_DIRECTION': 'rtl' if is_rtl else 'ltr',
        
        # Common translations
        'LANGUAGE_LABELS': {
            'select_language': 'Select Language',
            'current_language': 'Current Language',
            'switch_language': 'Switch Language',
            'language': 'Language',
            'close': 'Close',
            'select': 'Select',
            'change': 'Change',
        },
        
        # Language codes for JavaScript
        'LANGUAGE_CODES': [lang[0] for lang in settings.LANGUAGES],
        'DEFAULT_LANGUAGE': settings.LANGUAGE_CODE,
        
        # Translation status
        'TRANSLATION_ENABLED': settings.USE_I18N,
        'LOCALIZATION_ENABLED': settings.USE_L10N,
        
        # Regional formatting info
        'CURRENCY_SYMBOL': lang_info.get('currency_symbol', '$'),
        'DATE_FORMAT': lang_info.get('date_format', 'M d, Y'),
        'TIME_FORMAT': lang_info.get('time_format', 'h:i A'),
        'DECIMAL_SEPARATOR': lang_info.get('decimal_separator', '.'),
        'THOUSAND_SEPARATOR': lang_info.get('thousand_separator', ','),
        
        # Template helpers
        'LANGUAGE_TEMPLATE_HELPERS': {
            'get_language_class': lambda: f'lang-{current_language} {"rtl" if is_rtl else "ltr"}',
            'get_direction_class': lambda: 'rtl' if is_rtl else 'ltr',
            'get_language_attributes': lambda: f'lang="{current_language}" dir="{lang_info["direction"]}"',
        }
    }


def get_language_switch_context(request: HttpRequest, target_language: str = None) -> dict:
    """
    Get context for language switching forms/modals
    
    Args:
        request: HTTP request object  
        target_language: Optional target language code
        
    Returns:
        dict: Language switching context
    """
    current_language = translation.get_language() or settings.LANGUAGE_CODE
    
    # If target language specified, validate it
    if target_language:
        valid_languages = [lang[0] for lang in settings.LANGUAGES]
        if target_language not in valid_languages:
            target_language = settings.LANGUAGE_CODE
    
    # Build language options
    language_options = []
    for lang_code, lang_name in settings.LANGUAGES:
        lang_info = get_language_info(lang_code)
        language_options.append({
            'code': lang_code,
            'name': lang_info['name'],
            'native_name': lang_info['native_name'],
            'flag': lang_info['flag'],
            'is_current': lang_code == current_language,
            'is_target': lang_code == target_language,
        })
    
    return {
        'language_options': language_options,
        'current_language': current_language,
        'target_language': target_language,
        'switch_form_action': '/i18n/setlang/',
        'redirect_to': request.get_full_path() if request else '/',
    }


def get_translated_content_context(content_dict: dict, language_code: str = None) -> dict:
    """
    Get translated content for templates
    
    Args:
        content_dict: Dictionary with language-specific content
        language_code: Target language code (defaults to current)
        
    Returns:
        dict: Translated content
    """
    if not language_code:
        language_code = translation.get_language() or settings.LANGUAGE_CODE
    
    # Fallback chain: current language -> English -> first available
    fallback_chain = [
        language_code,
        settings.LANGUAGE_CODE,
        next(iter(content_dict.keys()), None)
    ]
    
    translated_content = {}
    
    for key, content in content_dict.items():
        if isinstance(content, dict):
            # Handle nested dictionaries (e.g., multi-language content)
            for lang in fallback_chain:
                if lang and lang in content:
                    translated_content[key] = content[lang]
                    break
            else:
                # No translation found, use first available
                translated_content[key] = next(iter(content.values()), '')
        else:
            # Direct content
            translated_content[key] = content
    
    return translated_content