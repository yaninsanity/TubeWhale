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
            ('zh-hans', '简体中文'),
            ('zh-hant', '繁體中文'),
            ('ja', '日本語'),
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
        return self.api_keys.filter(
            is_active=True,
            expires_at__gt=timezone.now()
        )


class APIKey(models.Model):
    """API Key Model"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='api_keys', 
                           verbose_name=_('User'))
    
    # API Key basic information
    name = models.CharField(max_length=100, verbose_name=_("Name"), 
                          help_text=_("API Key name"))
    key = models.CharField(max_length=64, unique=True, verbose_name=_("Key"),
                         help_text=_("API key string"))
    prefix = models.CharField(max_length=8, verbose_name=_("Prefix"),
                            help_text=_("Key prefix"))
    
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
