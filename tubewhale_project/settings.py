"""
Enhanced Django Settings with English-First Multilingual Support
Best Practices Implementation for TubeWhale Project
"""

import os
import sys
import logging
from pathlib import Path
from django.utils.translation import gettext_lazy as _

# Build paths inside the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    dotenv_path = BASE_DIR / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        print(f"✅ Loaded .env from {dotenv_path}")
    else:
        print(f"⚠️  No .env file found at {dotenv_path}")
except ImportError:
    print("⚠️  python-dotenv not installed, skipping .env loading")

# Add project root to Python path
sys.path.insert(0, str(BASE_DIR))

# Import multilingual configuration
try:
    from .multilingual_config import (
        LANGUAGES, LANGUAGE_CODE, DEFAULT_LANGUAGE_CODE, 
        LOCALE_PATHS, RTL_LANGUAGES, get_language_info
    )
except ImportError as e:
    print(f"⚠️  Multilingual config import error: {e}")
    # Fallback configuration
    LANGUAGES = [('en', 'English')]
    LANGUAGE_CODE = 'en'
    DEFAULT_LANGUAGE_CODE = 'en'
    LOCALE_PATHS = [BASE_DIR / 'locale']
    RTL_LANGUAGES = ['ar']
    get_language_info = lambda x: {'name': 'English', 'direction': 'ltr'}

# ============== SECURITY CONFIGURATION ==============
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-dev-key-change-in-production')
DEBUG = os.environ.get('DEBUG', 'True').lower() in ('true', '1', 'yes', 'on')

# Allowed hosts
ALLOWED_HOSTS = [
    'localhost', 
    '127.0.0.1', 
    '0.0.0.0',
    'tubewhale.local',
    'testserver',
    os.environ.get('ALLOWED_HOST', ''),
]
ALLOWED_HOSTS = [host for host in ALLOWED_HOSTS if host]

# ============== APPLICATION CONFIGURATION ==============
DJANGO_APPS = [
    'admin_interface',  # Must be before django.contrib.admin
    'colorfield',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes', 
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.humanize',  # For number formatting
]

THIRD_PARTY_APPS = [
    'rest_framework',
    'rest_framework_simplejwt',
    'corsheaders',
    'django_filters',
    'django_extensions',
    'import_export',
    'guardian',
    'drf_spectacular',
    'django_redis',
    'channels',
]

LOCAL_APPS = [
    'apps.user_app',
    'apps.video_app', 
    'apps.dashboard_app',
    'apps.admin_control',
    'apps.api_app',
    'apps.client_app',
    'apps.analysis_app',
    'apps.templates_app',
    'apps.tubewhale_engine',
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# ============== MIDDLEWARE CONFIGURATION ==============
MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    # 'apps.user_app.middleware.AdminLanguageMiddleware',  # Temporarily disabled - causing admin access issues
    'django.middleware.locale.LocaleMiddleware',  # Language support
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'apps.user_app.middleware.UserProfileMiddleware',  # Temporarily disabled
]

# ============== URL CONFIGURATION ==============
ROOT_URLCONF = 'tubewhale_project.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            BASE_DIR / 'templates',
            BASE_DIR / 'apps' / 'templates_app' / 'templates',
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.i18n',  # Internationalization
                'django.template.context_processors.media',
                'django.template.context_processors.static',
                'django.template.context_processors.tz',
                'apps.user_app.context_processors.user_context',
                'apps.templates_app.context_processors.language_context',
            ],
        },
    },
]

WSGI_APPLICATION = 'tubewhale_project.wsgi.application'
ASGI_APPLICATION = 'tubewhale_project.asgi.application'

# ============== DATABASE CONFIGURATION ==============
DATABASES = {
    'default': {
        'ENGINE': os.environ.get('DB_ENGINE', 'django.db.backends.sqlite3'),
        'NAME': os.environ.get('DB_NAME', BASE_DIR / 'tubewhale.db'),
        'USER': os.environ.get('DB_USER', ''),
        'PASSWORD': os.environ.get('DB_PASSWORD', ''),
        'HOST': os.environ.get('DB_HOST', ''),
        'PORT': os.environ.get('DB_PORT', ''),
        'OPTIONS': {
            'charset': 'utf8mb4',
        } if os.environ.get('DB_ENGINE', '').endswith('mysql') else {},
    }
}

# ============== CACHES CONFIGURATION ==============
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/1'),
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'CONNECTION_POOL_KWARGS': {
                'max_connections': 50,
                'retry_on_timeout': True,
            },
        },
        'KEY_PREFIX': 'tubewhale',
        'TIMEOUT': 300,
    },
    'localmem': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
    }
}

# ============== AUTHENTICATION CONFIGURATION ==============
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

AUTH_USER_MODEL = 'user_app.User'

# ============== INTERNATIONALIZATION - ENGLISH-FIRST INTELLIGENT PLATFORM ==============
# Language settings - English-first priority for intelligent platform
LANGUAGES = LANGUAGES
LANGUAGE_CODE = 'en'     # 🇺🇸 PRIMARY: English-first intelligent platform
DEFAULT_LANGUAGE_CODE = 'en'  # Force English as default

# English-first content analysis and template priorities
DEFAULT_TEMPLATE_LANGUAGE = 'en'
CONTENT_ANALYSIS_LANGUAGE = 'en'
ADMIN_LANGUAGE = 'en'
API_DEFAULT_LANGUAGE = 'en'

# Internationalization - English optimized
USE_I18N = True          # Enable internationalization
USE_L10N = True          # Enable localization  
USE_TZ = True            # Enable timezone support

TIME_ZONE = 'UTC'
LOCALE_PATHS = LOCALE_PATHS

# Format localization
FORMAT_MODULE_PATH = [
    'tubewhale_project.formats',
]

# Language session settings
LANGUAGE_SESSION_KEY = 'django_language'
LANGUAGE_COOKIE_NAME = 'django_language'
LANGUAGE_COOKIE_AGE = 365 * 24 * 60 * 60  # 1 year
LANGUAGE_COOKIE_DOMAIN = None
LANGUAGE_COOKIE_PATH = '/'

# ============== STATIC FILES CONFIGURATION ==============
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [
    BASE_DIR / 'static',
    BASE_DIR / 'apps' / 'templates_app' / 'static',
]

STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
]

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# ============== SESSION CONFIGURATION ==============
# Use cache-based sessions for better reliability with admin access
SESSION_ENGINE = 'django.contrib.sessions.backends.cache'
SESSION_CACHE_ALIAS = 'default'
SESSION_COOKIE_NAME = 'sessionid'  # Use Django default name
SESSION_COOKIE_AGE = 86400 * 7  # 7 days (shorter for security)
SESSION_EXPIRE_AT_BROWSER_CLOSE = False
SESSION_SAVE_EVERY_REQUEST = False  # Reduced to improve performance
SESSION_COOKIE_SECURE = False if DEBUG else True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

# CSRF Protection
CSRF_COOKIE_SECURE = False if DEBUG else True
CSRF_COOKIE_HTTPONLY = True
CSRF_COOKIE_SAMESITE = 'Lax'
CSRF_COOKIE_AGE = 86400 * 7  # 7 days

# Admin settings
ADMIN_SESSION_TIMEOUT = 86400 * 7  # 7 days
ADMIN_LANGUAGE_COOKIE_NAME = 'admin_language'
ADMIN_LANGUAGE_COOKIE_AGE = 86400 * 365  # 1 year

# ============== LOGGING CONFIGURATION ==============
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[{levelname}] {asctime} {name} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '[{levelname}] {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'tubewhale.log',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'] if not DEBUG else ['console'],
            'level': 'INFO',
        },
        'tubewhale': {
            'handlers': ['console', 'file'] if not DEBUG else ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': False,
        },
    },
}

# Create logs directory
(BASE_DIR / 'logs').mkdir(exist_ok=True)

# ============== REST FRAMEWORK CONFIGURATION ==============
REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.SessionAuthentication',
        'rest_framework.authentication.TokenAuthentication',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 20,
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
    ],
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
}

# ============== CORS CONFIGURATION ==============
CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:8001",
    "http://127.0.0.1:8001",
]
CORS_ALLOW_CREDENTIALS = True

# ============== CHANNELS CONFIGURATION ==============
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')],
        },
    },
}

# ============== EMAIL CONFIGURATION ==============
EMAIL_BACKEND = 'django.core.mail.backends.' + (
    'console.EmailBackend' if DEBUG else 'smtp.EmailBackend'
)

if not DEBUG:
    EMAIL_HOST = os.environ.get('EMAIL_HOST', 'smtp.gmail.com')
    EMAIL_PORT = int(os.environ.get('EMAIL_PORT', '587'))
    EMAIL_USE_TLS = True
    EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER', '')
    EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD', '')

DEFAULT_FROM_EMAIL = os.environ.get('DEFAULT_FROM_EMAIL', 'noreply@tubewhale.com')

# ============== SECURITY CONFIGURATION ==============
if not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = 'DENY'
    SECURE_HSTS_SECONDS = 31536000  # 1 year
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True

# ============== DEFAULT PRIMARY KEY ==============
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ============== CUSTOM TUBEWHALE CONFIGURATION ==============
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY', '')

# Analysis configuration
MAX_VIDEO_DURATION = int(os.environ.get('MAX_VIDEO_DURATION', '3600'))  # 1 hour
MAX_CONCURRENT_ANALYSIS = int(os.environ.get('MAX_CONCURRENT_ANALYSIS', '5'))

# File upload limits
FILE_UPLOAD_MAX_MEMORY_SIZE = 100 * 1024 * 1024  # 100MB
DATA_UPLOAD_MAX_MEMORY_SIZE = 100 * 1024 * 1024  # 100MB

# ============== CELERY CONFIGURATION ==============
# Celery settings for async task processing - Production optimized
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0'))
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0'))

# Task settings  
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE

# Task execution settings - Optimized for performance
CELERY_TASK_ALWAYS_EAGER = False  # Set to True for synchronous testing
CELERY_TASK_EAGER_PROPAGATES = False
CELERY_TASK_IGNORE_RESULT = False
CELERY_TASK_STORE_EAGER_RESULT = True
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 1800  # 30 minutes max per task
CELERY_TASK_SOFT_TIME_LIMIT = 1500  # 25 minutes soft limit

# Worker settings - Production tuned
CELERY_WORKER_CONCURRENCY = int(os.environ.get('CELERY_WORKER_CONCURRENCY', '4'))
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000
CELERY_WORKER_DISABLE_RATE_LIMITS = False

# Connection settings
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BROKER_CONNECTION_RETRY = True
CELERY_BROKER_CONNECTION_MAX_RETRIES = 10

# Task routing - Performance optimized queues
CELERY_TASK_ROUTES = {
    'apps.tubewhale_engine.tasks.*': {'queue': 'analysis'},
    'apps.video_app.tasks.*': {'queue': 'video_processing'},
    'apps.templates_app.tasks.*': {'queue': 'template_processing'},
}

# Result backend settings
CELERY_RESULT_EXPIRES = 3600  # Results expire after 1 hour
CELERY_RESULT_CACHE_MAX = 10000  # Cache up to 10k results

# Beat scheduler for periodic tasks
CELERY_BEAT_SCHEDULE = {}
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

# ============== LOGGING CONFIGURATION ==============
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': 'logs/tubewhale.log',
            'formatter': 'verbose',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'celery': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'tubewhale': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

# Create logs directory if it doesn't exist
import os
if not os.path.exists('logs'):
    os.makedirs('logs')

# ============== DEVELOPMENT SETTINGS ==============
if DEBUG:
    INTERNAL_IPS = ['127.0.0.1', 'localhost']
    
    try:
        import debug_toolbar
        INSTALLED_APPS.append('debug_toolbar')
        MIDDLEWARE.insert(0, 'debug_toolbar.middleware.DebugToolbarMiddleware')
    except ImportError:
        pass

# Print configuration summary
print(f"🌍 TubeWhale Multilingual Configuration Loaded:")
print(f"   • Default Language: {LANGUAGE_CODE}")
print(f"   • Supported Languages: {len(LANGUAGES)} languages")
print(f"   • Debug Mode: {DEBUG}")
print(f"   • Database: {DATABASES['default']['ENGINE'].split('.')[-1]}")
print(f"   • Cache Backend: {'Redis' if 'redis' in CACHES['default']['BACKEND'] else 'Local'}")