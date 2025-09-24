"""
Template Information Models
Detailed information about analysis templates for client selection
"""

from django.db import models
from django.utils.translation import gettext_lazy as _


class TemplateCategory(models.Model):
    """Template categories for organization"""
    
    name = models.CharField(max_length=100, unique=True, verbose_name=_("Category Name"))
    slug = models.SlugField(max_length=100, unique=True, verbose_name=_("URL Slug"))
    description = models.TextField(blank=True, verbose_name=_("Description"))
    icon = models.CharField(max_length=50, default="fas fa-folder", verbose_name=_("Icon Class"))
    color = models.CharField(max_length=7, default="#007bff", verbose_name=_("Color Code"))
    order = models.PositiveIntegerField(default=0, verbose_name=_("Display Order"))
    is_active = models.BooleanField(default=True, verbose_name=_("Is Active"))
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = _("Template Category")
        verbose_name_plural = _("Template Categories")
        ordering = ['order', 'name']
    
    def __str__(self):
        return self.name


class AnalysisTemplate(models.Model):
    """Analysis template information for client interface"""
    
    TIER_CHOICES = [
        ('basic', _('Basic - Available to all users')),
        ('standard', _('Standard - Requires Standard+ tier')),
        ('premium', _('Premium - Requires Premium tier')),
    ]
    
    COMPLEXITY_CHOICES = [
        ('beginner', _('Beginner - Simple analysis')),
        ('intermediate', _('Intermediate - Moderate complexity')),
        ('advanced', _('Advanced - Complex analysis')),
        ('expert', _('Expert - Highly specialized')),
    ]
    
    # Basic Information
    name = models.CharField(max_length=200, verbose_name=_("Template Name"))
    slug = models.SlugField(max_length=200, unique=True, verbose_name=_("URL Slug"))
    short_description = models.CharField(max_length=300, verbose_name=_("Short Description"))
    full_description = models.TextField(verbose_name=_("Full Description"))
    
    # Categorization
    category = models.ForeignKey(TemplateCategory, on_delete=models.CASCADE, 
                               related_name='templates', verbose_name=_("Category"))
    tags = models.CharField(max_length=500, blank=True, 
                          help_text=_("Comma-separated tags"), verbose_name=_("Tags"))
    
    # Access Control
    required_tier = models.CharField(max_length=20, choices=TIER_CHOICES, 
                                   default='basic', verbose_name=_("Required Tier"))
    complexity_level = models.CharField(max_length=20, choices=COMPLEXITY_CHOICES,
                                      default='beginner', verbose_name=_("Complexity Level"))
    
    # Visual Elements
    icon = models.CharField(max_length=50, default="fas fa-chart-bar", verbose_name=_("Icon Class"))
    color = models.CharField(max_length=7, default="#007bff", verbose_name=_("Color Code"))
    preview_image = models.ImageField(upload_to='template_previews/', blank=True, null=True,
                                    verbose_name=_("Preview Image"))
    
    # Technical Information
    estimated_duration = models.CharField(max_length=100, blank=True,
                                        verbose_name=_("Estimated Duration"))
    output_formats = models.CharField(max_length=200, blank=True,
                                    help_text=_("e.g., PDF, CSV, JSON"), 
                                    verbose_name=_("Output Formats"))
    sample_use_cases = models.TextField(blank=True, verbose_name=_("Sample Use Cases"))
    
    # Template Configuration
    template_id = models.CharField(max_length=100, unique=True, 
                                 verbose_name=_("Internal Template ID"))
    parameters_schema = models.JSONField(default=dict, blank=True,
                                       verbose_name=_("Parameters Schema"))
    
    # Status and Metadata
    is_active = models.BooleanField(default=True, verbose_name=_("Is Active"))
    is_featured = models.BooleanField(default=False, verbose_name=_("Is Featured"))
    order = models.PositiveIntegerField(default=0, verbose_name=_("Display Order"))
    usage_count = models.PositiveIntegerField(default=0, verbose_name=_("Usage Count"))
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = _("Analysis Template")
        verbose_name_plural = _("Analysis Templates")
        ordering = ['category__order', 'order', 'name']
    
    def __str__(self):
        return f"{self.name} ({self.category.name})"
    
    def get_tags_list(self):
        """Return tags as a list"""
        if self.tags:
            return [tag.strip() for tag in self.tags.split(',') if tag.strip()]
        return []
    
    def get_tier_badge_class(self):
        """Get CSS class for tier badge"""
        tier_classes = {
            'basic': 'bg-secondary',
            'standard': 'bg-primary',
            'premium': 'bg-warning text-dark'
        }
        return tier_classes.get(self.required_tier, 'bg-secondary')
    
    def get_complexity_badge_class(self):
        """Get CSS class for complexity badge"""
        complexity_classes = {
            'beginner': 'bg-success',
            'intermediate': 'bg-info',
            'advanced': 'bg-warning text-dark',
            'expert': 'bg-danger'
        }
        return complexity_classes.get(self.complexity_level, 'bg-secondary')
    
    def can_access(self, user):
        """Check if user can access this template based on tier"""
        if not user.is_authenticated:
            return False
        
        user_tier = getattr(user, 'tier', 'basic')
        
        tier_hierarchy = ['basic', 'standard', 'premium']
        user_tier_level = tier_hierarchy.index(user_tier) if user_tier in tier_hierarchy else 0
        required_tier_level = tier_hierarchy.index(self.required_tier) if self.required_tier in tier_hierarchy else 0
        
        return user_tier_level >= required_tier_level