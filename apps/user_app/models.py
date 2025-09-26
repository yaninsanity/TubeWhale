"""
User App Models
Extended User Model with API Key Management
"""

import uuid
import secrets
import string
from datetime import datetime, timedelta
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


class User(AbstractUser):
    """Extended User Model with API Key Management"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Extended profile fields
    phone_number = models.CharField(max_length=20, blank=True, null=True, 
                                  verbose_name=_("Phone Number"), help_text=_("User phone number"))
    avatar = models.ImageField(upload_to='avatars/', blank=True, null=True,
                             verbose_name=_("Avatar"), help_text=_("User avatar image"))
    
    # User preferences
    timezone_setting = models.CharField(
        max_length=50, 
        default='UTC',
        verbose_name=_("Timezone"),
        help_text=_("User timezone setting")
    )
    language = models.CharField(
        max_length=10, 
        default='en', 
        choices=[
            ('en', 'English'),
            ('zh-hans', 'Simplified Chinese'),
            ('zh-hant', 'Traditional Chinese'),
            ('ja', 'Japanese'),
            ('ko', '한국어'),
            ('es', 'Español'),
            ('fr', 'Français'),
            ('de', 'Deutsch'),
            ('pt', 'Português'),
            ('ru', 'Русский'),
        ],
        verbose_name=_("Language"),
        help_text=_("User interface language")
    )
    
    # User tier system for template access
    USER_TIERS = [
        ('basic', _('Basic Tier - Default templates only')),
        ('standard', _('Standard Tier - Template switching')),
        ('premium', _('Premium Tier - Custom AI interactions')),
        ('enterprise', _('Enterprise Tier - Full system access')),
    ]
    
    tier = models.CharField(
        max_length=20,
        choices=USER_TIERS,
        default='basic',
        verbose_name=_("User Tier"),
        help_text=_("Determines template access level")
    )
    
    # Account status and statistics
    is_verified = models.BooleanField(default=False, verbose_name=_("Verified"), 
                                    help_text=_("Email verification status"))
    api_calls_today = models.IntegerField(default=0, verbose_name=_("API Calls Today"), 
                                        help_text=_("Today's API call count"))
    total_api_calls = models.IntegerField(default=0, verbose_name=_("Total API Calls"), 
                                        help_text=_("Total API call count"))
    last_activity = models.DateTimeField(auto_now=True, verbose_name=_("Last Activity"), 
                                       help_text=_("Last activity timestamp"))
    
    # Resolve reverse accessor conflicts
    groups = models.ManyToManyField(
        'auth.Group',
        verbose_name=_('Groups'),
        blank=True,
        help_text=_('The groups this user belongs to.'),
        related_name="tubewhale_user_set",
        related_query_name="tubewhale_user",
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        verbose_name=_('User Permissions'),
        blank=True,
        help_text=_('Specific permissions for this user.'),
        related_name="tubewhale_user_set",
        related_query_name="tubewhale_user",
    )
    
    class Meta:
        db_table = 'users'
        verbose_name = _("User")
        verbose_name_plural = _("User Management")
    
    def __str__(self):
        return f"{self.username} ({self.email})"
    
    def reset_daily_api_calls(self):
        """Reset daily API call counter"""
        self.api_calls_today = 0
        self.save(update_fields=['api_calls_today'])
    
    def increment_api_calls(self):
        """Increment API call counter"""
        self.api_calls_today += 1
        self.total_api_calls += 1
        self.save(update_fields=['api_calls_today', 'total_api_calls'])
        
    def get_active_api_keys(self):
        """Get user's active API keys"""
        from django.db.models import Q
        return self.api_keys.filter(
            Q(is_active=True) & 
            (Q(expires_at__isnull=True) | Q(expires_at__gt=timezone.now()))
        )
    
    def can_switch_templates(self):
        """Check if user can switch between templates"""
        return self.tier in ['standard', 'premium', 'enterprise']
    
    def can_customize_templates(self):
        """Check if user can customize AI interactions"""
        return self.tier in ['premium', 'enterprise']
    
    def is_enterprise_user(self):
        """Check if user has enterprise access"""
        return self.tier == 'enterprise' or self.is_superuser
    
    def get_tier_display_name(self):
        """Get human-readable tier name"""
        if self.is_superuser:
            return _('Enterprise')
        tier_names = {
            'basic': _('Basic'),
            'standard': _('Standard'), 
            'premium': _('Premium'),
            'enterprise': _('Enterprise')
        }
        return tier_names.get(self.tier, self.tier)


class APIKey(models.Model):
    """API Key Model"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='api_keys', 
                           verbose_name=_('User'))
    
    # API Key basic information
    name = models.CharField(max_length=100, verbose_name=_("Name"), 
                          help_text=_("API Key name"))
    key = models.CharField(max_length=128, verbose_name=_("Key"),
                         help_text=_("API key string"))
    prefix = models.CharField(max_length=8, verbose_name=_("Prefix"),
                            help_text=_("Key prefix"))
    
    # Reference to original admin key for shared keys
    original_key_hash = models.CharField(
        max_length=64, 
        blank=True, 
        null=True,
        verbose_name=_("Original Key Hash"),
        help_text=_("Hash of original admin key for reference")
    )
    
    # API Key type and service information
    SERVICE_TYPES = [
        ('openai', _('OpenAI API')),
        ('youtube', _('YouTube API')),
        ('other', _('Other Service')),
    ]
    
    KEY_TYPES = [
        ('user_generated', _('User Generated')),
        ('admin_configured', _('Admin Configured')),
        ('system_default', _('System Default')),
    ]
    
    service_type = models.CharField(
        max_length=20,
        choices=SERVICE_TYPES,
        default='other',
        verbose_name=_("Service Type"),
        help_text=_("Type of service this API key is for")
    )
    
    key_type = models.CharField(
        max_length=20,
        choices=KEY_TYPES,
        default='user_generated',
        verbose_name=_("Key Type"),
        help_text=_("How this API key was created")
    )
    
    # Permissions and restrictions
    is_active = models.BooleanField(default=True, verbose_name=_("Active"),
                                  help_text=_("Whether the key is active"))
    rate_limit = models.IntegerField(default=1000, verbose_name=_("Rate Limit"),
                                   help_text=_("Daily call limit"))
    allowed_ips = models.TextField(blank=True, verbose_name=_("Allowed IPs"),
                                 help_text=_("Allowed IP addresses, comma separated"))
    
    # Time management
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_('Created At'))
    expires_at = models.DateTimeField(null=True, blank=True, verbose_name=_('Expires At'))
    last_used = models.DateTimeField(null=True, blank=True, verbose_name=_('Last Used'))
    
    # Usage statistics
    usage_count = models.IntegerField(default=0, verbose_name=_("Usage Count"),
                                    help_text=_("Number of times used"))
    
    class Meta:
        db_table = 'api_keys'
        verbose_name = _("API Key")
        verbose_name_plural = _("API Key Management")
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.name} ({self.prefix}***)"
    
    def save(self, *args, **kwargs):
        if not self.key:
            # Generate API Key
            full_key = secrets.token_urlsafe(32)
            self.key = full_key
            self.prefix = full_key[:8]
        super().save(*args, **kwargs)
    
    def is_valid(self):
        """Check if API key is valid"""
        if not self.is_active:
            return False
        if self.expires_at and timezone.now() > self.expires_at:
            return False
        return True
    
    def update_usage(self):
        """Update usage statistics"""
        self.usage_count += 1
        self.last_used = timezone.now()
        self.save(update_fields=['usage_count', 'last_used'])


class APIUsageLog(models.Model):
    """API Usage Log Model"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    api_key = models.ForeignKey(APIKey, on_delete=models.CASCADE, related_name='usage_logs', 
                              verbose_name=_('API Key'))
    
    # Request information
    endpoint = models.CharField(max_length=200, verbose_name=_("Endpoint"),
                              help_text=_("API endpoint"))
    method = models.CharField(max_length=10, verbose_name=_("Method"),
                            help_text=_("HTTP method"))
    ip_address = models.GenericIPAddressField(verbose_name=_("IP Address"),
                                            help_text=_("Client IP address"))
    user_agent = models.TextField(blank=True, verbose_name=_("User Agent"),
                                help_text=_("User agent string"))
    
    # Response information
    status_code = models.IntegerField(verbose_name=_("Status Code"),
                                    help_text=_("HTTP status code"))
    response_time = models.FloatField(verbose_name=_("Response Time"),
                                    help_text=_("Response time in seconds"))
    
    # Time information
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name=_('Timestamp'))
    
    class Meta:
        db_table = 'api_usage_logs'
        verbose_name = _("API Usage Log")
        verbose_name_plural = _("API Usage Logs")
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['api_key', '-timestamp']),
            models.Index(fields=['timestamp']),
        ]
    
    def __str__(self):
        return f"{self.api_key.name} - {self.endpoint} ({self.timestamp})"


class InvitationCode(models.Model):
    """
    Invitation Code Model for Beta User Registration
    """
    # Code information
    code = models.CharField(max_length=20, unique=True, verbose_name=_("Invitation Code"),
                          help_text=_("Unique invitation code"))
    
    # Usage tracking
    is_used = models.BooleanField(default=False, verbose_name=_("Used"),
                                help_text=_("Whether this code has been used"))
    used_by = models.ForeignKey('User', on_delete=models.SET_NULL, null=True, blank=True,
                              related_name='used_invitation_codes',
                              verbose_name=_("Used By"),
                              help_text=_("User who used this code"))
    used_at = models.DateTimeField(null=True, blank=True, verbose_name=_("Used At"),
                                 help_text=_("When this code was used"))
    
    # Validity
    expires_at = models.DateTimeField(verbose_name=_("Expires At"),
                                    help_text=_("When this code expires"))
    max_uses = models.IntegerField(default=1, verbose_name=_("Max Uses"),
                                 help_text=_("Maximum number of times this code can be used"))
    current_uses = models.IntegerField(default=0, verbose_name=_("Current Uses"),
                                     help_text=_("Number of times this code has been used"))
    
    # Metadata
    created_by = models.ForeignKey('User', on_delete=models.CASCADE,
                                 related_name='created_invitation_codes',
                                 verbose_name=_("Created By"),
                                 help_text=_("Admin who created this code"))
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("Created At"))
    notes = models.TextField(blank=True, verbose_name=_("Notes"),
                           help_text=_("Additional notes about this invitation code"))
    
    # User restrictions
    allowed_domains = models.TextField(blank=True, verbose_name=_("Allowed Domains"),
                                     help_text=_("Comma-separated list of allowed email domains"))
    
    class Meta:
        db_table = 'invitation_codes'
        verbose_name = _("Invitation Code")
        verbose_name_plural = _("Invitation Codes")
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['code']),
            models.Index(fields=['is_used', 'expires_at']),
            models.Index(fields=['created_at']),
        ]
    
    @staticmethod
    def generate_code(length=10):
        """Generate a random invitation code"""
        characters = string.ascii_uppercase + string.digits
        return ''.join(secrets.choice(characters) for _ in range(length))
    
    @classmethod
    def create_invitation_code(cls, created_by, expires_days=30, max_uses=1, notes="", allowed_domains=""):
        """Create a new invitation code"""
        from django.utils import timezone
        
        code = cls.generate_code()
        expires_at = timezone.now() + timedelta(days=expires_days)
        
        return cls.objects.create(
            code=code,
            expires_at=expires_at,
            max_uses=max_uses,
            created_by=created_by,
            notes=notes,
            allowed_domains=allowed_domains
        )
    
    def is_valid(self):
        """Check if the invitation code is valid"""
        from django.utils import timezone
        
        if self.is_used and self.current_uses >= self.max_uses:
            return False
        if timezone.now() > self.expires_at:
            return False
        return True
    
    def can_be_used_by_email(self, email):
        """Check if this code can be used by given email"""
        if not self.allowed_domains:
            return True
        
        email_domain = email.split('@')[1] if '@' in email else ''
        allowed = [domain.strip() for domain in self.allowed_domains.split(',') if domain.strip()]
        
        return email_domain in allowed
    
    def use_code(self, user):
        """Mark this code as used by a user"""
        from django.utils import timezone
        
        if not self.is_valid():
            raise ValueError(_("Invitation code is not valid"))
        
        self.current_uses += 1
        if self.current_uses >= self.max_uses:
            self.is_used = True
            self.used_by = user
            self.used_at = timezone.now()
        
        self.save()
        return True
    
    def __str__(self):
        status = _("Used") if self.is_used else _("Active")
        return f"{self.code} ({status})"


class UserAnalysisHistory(models.Model):
    """User Analysis History Model"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='analysis_history')
    
    # Analysis basic information
    scenario_type = models.CharField(max_length=50, verbose_name=_("Analysis Scenario"))
    youtube_url = models.URLField(verbose_name=_("YouTube URL"))
    video_title = models.CharField(max_length=500, blank=True, null=True)
    
    # Analysis status
    STATUS_CHOICES = [
        ('pending', _('Pending')),
        ('processing', _('Processing')),
        ('completed', _('Completed')),
        ('failed', _('Failed')),
        ('cancelled', _('Cancelled')),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    
    # Analysis results
    analysis_result = models.JSONField(blank=True, null=True, verbose_name=_("Analysis Result"))
    error_message = models.TextField(blank=True, null=True, verbose_name=_("Error Message"))
    
    # Metadata
    processing_time_seconds = models.PositiveIntegerField(blank=True, null=True)
    tokens_used = models.PositiveIntegerField(blank=True, null=True)
    cost_usd = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    
    class Meta:
        verbose_name = _("User Analysis History")
        verbose_name_plural = _("User Analysis Histories")
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['status', '-created_at']),
        ]
    
    def __str__(self):
        return f"{self.user.username} - {self.scenario_type} - {self.status}"
    
    @property
    def duration_minutes(self):
        """Calculate analysis duration in minutes"""
        if self.processing_time_seconds:
            return round(self.processing_time_seconds / 60, 1)
        return None


class UserProfile(models.Model):
    """Extended User Profile Model"""
    
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
    # API Keys storage
    api_keys = models.JSONField(default=dict, blank=True, help_text=_("User API keys storage"))
    
    # User preferences
    workspace_folder = models.CharField(max_length=500, blank=True, null=True)
    preferred_language = models.CharField(max_length=10, default='en')
    preferred_template = models.CharField(max_length=100, default='default_analysis')
    subscription_tier = models.CharField(max_length=50, default='basic')
    
    # Settings
    auto_analysis = models.BooleanField(default=False)
    email_notifications = models.BooleanField(default=True)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = _("User Profile")
        verbose_name_plural = _("User Profiles")
    
    def __str__(self):
        return f"{self.user.username} Profile"
