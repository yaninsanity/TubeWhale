# TubeWhale Multilingual Configuration
# English-First Multi-Language Support System

import os
from pathlib import Path
from django.utils.translation import gettext_lazy as _

# Supported Languages Configuration
LANGUAGES = [
    ('en', _('English')),           # Primary language (English first)
    ('zh-hans', _('简体中文')),      # Chinese Simplified  
    ('zh-hant', _('繁體中文')),      # Chinese Traditional
    ('ja', _('日本語')),            # Japanese
    ('ko', _('한국어')),            # Korean
    ('es', _('Español')),           # Spanish
    ('fr', _('Français')),          # French
    ('de', _('Deutsch')),           # German
    ('pt', _('Português')),         # Portuguese
    ('ru', _('Русский')),           # Russian
    ('ar', _('العربية')),           # Arabic (RTL support)
    ('hi', _('हिन्दी')),            # Hindi
]

# Language Configuration
LANGUAGE_CODE = 'en'  # Default to English
DEFAULT_LANGUAGE_CODE = 'en'  # Fallback language

# Internationalization Settings
USE_I18N = True          # Enable internationalization
USE_L10N = True          # Enable localization  
USE_TZ = True            # Enable timezone support

# Locale Configuration
LOCALE_PATHS = [
    Path(__file__).parent.parent / 'locale',
    Path(__file__).parent.parent / 'apps' / 'templates_app' / 'locale',
    Path(__file__).parent.parent / 'static' / 'locale',
]

# Format Localization
FORMAT_MODULE_PATH = [
    'tubewhale_project.formats',
]

# Session Language Preference
SESSION_COOKIE_NAME = 'tubewhale_sessionid'
LANGUAGE_SESSION_KEY = 'django_language'
LANGUAGE_COOKIE_NAME = 'django_language'
LANGUAGE_COOKIE_AGE = 365 * 24 * 60 * 60  # 1 year
LANGUAGE_COOKIE_DOMAIN = None
LANGUAGE_COOKIE_PATH = '/'

# Language Detection Order
LANGUAGE_DETECTION_ORDER = [
    'url_parameter',     # ?lang=en
    'session',          # Session preference
    'cookie',           # Cookie preference  
    'accept_language',  # Browser preference
    'default',          # Fallback to English
]

# RTL Language Support
RTL_LANGUAGES = ['ar', 'he', 'fa', 'ur']

def get_language_info(language_code):
    """Get comprehensive language information"""
    language_map = {
        'en': {
            'name': 'English',
            'native_name': 'English',
            'direction': 'ltr',
            'flag': '🇺🇸',
            'encoding': 'utf-8',
            'date_format': 'M d, Y',
            'time_format': 'h:i A',
            'currency_symbol': '$',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'zh-hans': {
            'name': 'Chinese Simplified',
            'native_name': '简体中文',
            'direction': 'ltr',
            'flag': '🇨🇳',
            'encoding': 'utf-8',
            'date_format': 'Y年m月d日',
            'time_format': 'H:i',
            'currency_symbol': '¥',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'zh-hant': {
            'name': 'Chinese Traditional',
            'native_name': '繁體中文',
            'direction': 'ltr',
            'flag': '🇹🇼',
            'encoding': 'utf-8',
            'date_format': 'Y年m月d日',
            'time_format': 'H:i',
            'currency_symbol': 'NT$',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'ja': {
            'name': 'Japanese',
            'native_name': '日本語',
            'direction': 'ltr',
            'flag': '🇯🇵',
            'encoding': 'utf-8',
            'date_format': 'Y年m月d日',
            'time_format': 'H:i',
            'currency_symbol': '¥',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'ko': {
            'name': 'Korean',
            'native_name': '한국어',
            'direction': 'ltr',
            'flag': '🇰🇷',
            'encoding': 'utf-8',
            'date_format': 'Y년 m월 d일',
            'time_format': 'H:i',
            'currency_symbol': '₩',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'es': {
            'name': 'Spanish',
            'native_name': 'Español',
            'direction': 'ltr',
            'flag': '🇪🇸',
            'encoding': 'utf-8',
            'date_format': 'd \\d\\e F \\d\\e Y',
            'time_format': 'H:i',
            'currency_symbol': '€',
            'decimal_separator': ',',
            'thousand_separator': '.',
        },
        'fr': {
            'name': 'French',
            'native_name': 'Français',
            'direction': 'ltr',
            'flag': '🇫🇷',
            'encoding': 'utf-8',
            'date_format': 'd F Y',
            'time_format': 'H:i',
            'currency_symbol': '€',
            'decimal_separator': ',',
            'thousand_separator': ' ',
        },
        'de': {
            'name': 'German',
            'native_name': 'Deutsch',
            'direction': 'ltr',
            'flag': '🇩🇪',
            'encoding': 'utf-8',
            'date_format': 'd. F Y',
            'time_format': 'H:i',
            'currency_symbol': '€',
            'decimal_separator': ',',
            'thousand_separator': '.',
        },
        'pt': {
            'name': 'Portuguese',
            'native_name': 'Português',
            'direction': 'ltr',
            'flag': '🇵🇹',
            'encoding': 'utf-8',
            'date_format': 'd \\d\\e F \\d\\e Y',
            'time_format': 'H:i',
            'currency_symbol': '€',
            'decimal_separator': ',',
            'thousand_separator': '.',
        },
        'ru': {
            'name': 'Russian',
            'native_name': 'Русский',
            'direction': 'ltr',
            'flag': '🇷🇺',
            'encoding': 'utf-8',
            'date_format': 'd F Y \\г.',
            'time_format': 'H:i',
            'currency_symbol': '₽',
            'decimal_separator': ',',
            'thousand_separator': ' ',
        },
        'ar': {
            'name': 'Arabic',
            'native_name': 'العربية',
            'direction': 'rtl',
            'flag': '🇸🇦',
            'encoding': 'utf-8',
            'date_format': 'd F Y',
            'time_format': 'H:i',
            'currency_symbol': 'ر.س',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
        'hi': {
            'name': 'Hindi',
            'native_name': 'हिन्दी',
            'direction': 'ltr',
            'flag': '🇮🇳',
            'encoding': 'utf-8',
            'date_format': 'd F Y',
            'time_format': 'H:i',
            'currency_symbol': '₹',
            'decimal_separator': '.',
            'thousand_separator': ',',
        },
    }
    
    return language_map.get(language_code, language_map['en'])

# Export configuration for easy import
__all__ = [
    'LANGUAGES',
    'LANGUAGE_CODE', 
    'DEFAULT_LANGUAGE_CODE',
    'LOCALE_PATHS',
    'RTL_LANGUAGES',
    'get_language_info',
]