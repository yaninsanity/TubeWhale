from django.db import models
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

User = get_user_model()


class UserTier(models.TextChoices):
    """用户层级定义"""
    BASIC = 'basic', 'Basic (3 Templates)'
    PRO = 'pro', 'Pro (All Templates)'  
    ENTERPRISE = 'enterprise', 'Enterprise (All + Custom)'


class UserProfile(models.Model):
    """用户档案扩展，包含层级信息"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='template_profile')
    tier = models.CharField(
        max_length=20,
        choices=UserTier.choices,
        default=UserTier.BASIC,
        help_text="User access tier for template usage"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Tier limits
    @property
    def max_templates(self):
        """Maximum number of templates this user can access"""
        if self.tier == UserTier.BASIC:
            return 3
        elif self.tier == UserTier.PRO:
            return 999  # Unlimited for built-in templates
        else:  # ENTERPRISE
            return 999  # Unlimited
    
    @property
    def can_customize(self):
        """Whether user can create/edit custom templates"""
        return self.tier == UserTier.ENTERPRISE
    
    @property
    def can_access_all_builtin(self):
        """Whether user can access all built-in templates"""
        return self.tier in [UserTier.PRO, UserTier.ENTERPRISE]
    
    def __str__(self):
        return f"{self.user.username} ({self.get_tier_display()})"
    
    class Meta:
        verbose_name = "User Profile"
        verbose_name_plural = "User Profiles"


class TemplateUsage(models.Model):
    """模板使用记录，用于跟踪和限制"""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    template_id = models.CharField(max_length=100)
    template_name = models.CharField(max_length=200, blank=True)
    usage_count = models.PositiveIntegerField(default=0)
    last_used = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['user', 'template_id']
        verbose_name = "Template Usage"
        verbose_name_plural = "Template Usage Records"
    
    def __str__(self):
        return f"{self.user.username} - {self.template_name} ({self.usage_count} uses)"