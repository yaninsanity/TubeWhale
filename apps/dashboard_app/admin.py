"""
Dashboard App Admin Configuration
Template and category management
"""

from django.contrib import admin
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from .models import TemplateCategory, AnalysisTemplate


@admin.register(TemplateCategory)
class TemplateCategoryAdmin(admin.ModelAdmin):
    """Template Category Admin"""
    
    list_display = ('name', 'slug', 'template_count', 'color_preview', 'is_active', 'order')
    list_filter = ('is_active', 'created_at')
    search_fields = ('name', 'description')
    prepopulated_fields = {'slug': ('name',)}
    ordering = ('order', 'name')
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('name', 'slug', 'description')
        }),
        (_('Appearance'), {
            'fields': ('icon', 'color', 'order')
        }),
        (_('Status'), {
            'fields': ('is_active',)
        }),
    )
    
    def template_count(self, obj):
        """Display number of templates in this category"""
        return obj.templates.count()
    template_count.short_description = _('Templates')
    
    def color_preview(self, obj):
        """Display color preview"""
        return format_html(
            '<span style="display: inline-block; width: 20px; height: 20px; '
            'background-color: {}; border: 1px solid #ccc; border-radius: 3px;"></span>',
            obj.color
        )
    color_preview.short_description = _('Color')


@admin.register(AnalysisTemplate)
class AnalysisTemplateAdmin(admin.ModelAdmin):
    """Analysis Template Admin"""
    
    list_display = ('name', 'category', 'tier_display', 'complexity_display', 
                   'is_featured', 'is_active', 'usage_count', 'order')
    list_filter = ('category', 'required_tier', 'complexity_level', 'is_active', 
                  'is_featured', 'created_at')
    search_fields = ('name', 'short_description', 'tags', 'template_id')
    prepopulated_fields = {'slug': ('name',)}
    ordering = ('category__order', 'order', 'name')
    
    readonly_fields = ('usage_count', 'created_at', 'updated_at')
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('name', 'slug', 'short_description', 'full_description')
        }),
        (_('Categorization'), {
            'fields': ('category', 'tags', 'required_tier', 'complexity_level')
        }),
        (_('Appearance'), {
            'fields': ('icon', 'color', 'preview_image')
        }),
        (_('Technical Details'), {
            'fields': ('template_id', 'estimated_duration', 'output_formats', 
                      'sample_use_cases', 'parameters_schema')
        }),
        (_('Status & Display'), {
            'fields': ('is_active', 'is_featured', 'order')
        }),
        (_('Statistics'), {
            'fields': ('usage_count', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['mark_as_featured', 'mark_as_not_featured', 'activate_templates', 'deactivate_templates']
    
    def tier_display(self, obj):
        """Display tier with badge styling"""
        return format_html(
            '<span class="badge {}">{}</span>',
            obj.get_tier_badge_class(),
            obj.get_required_tier_display()
        )
    tier_display.short_description = _('Tier')
    tier_display.admin_order_field = 'required_tier'
    
    def complexity_display(self, obj):
        """Display complexity with badge styling"""
        return format_html(
            '<span class="badge {}">{}</span>',
            obj.get_complexity_badge_class(),
            obj.get_complexity_level_display()
        )
    complexity_display.short_description = _('Complexity')
    complexity_display.admin_order_field = 'complexity_level'
    
    def mark_as_featured(self, request, queryset):
        """Mark selected templates as featured"""
        updated = queryset.update(is_featured=True)
        self.message_user(request, f'{updated} templates marked as featured.')
    mark_as_featured.short_description = _('Mark selected templates as featured')
    
    def mark_as_not_featured(self, request, queryset):
        """Remove featured status from selected templates"""
        updated = queryset.update(is_featured=False)
        self.message_user(request, f'{updated} templates unmarked as featured.')
    mark_as_not_featured.short_description = _('Remove featured status')
    
    def activate_templates(self, request, queryset):
        """Activate selected templates"""
        updated = queryset.update(is_active=True)
        self.message_user(request, f'{updated} templates activated.')
    activate_templates.short_description = _('Activate selected templates')
    
    def deactivate_templates(self, request, queryset):
        """Deactivate selected templates"""
        updated = queryset.update(is_active=False)
        self.message_user(request, f'{updated} templates deactivated.')
    deactivate_templates.short_description = _('Deactivate selected templates')
    
    class Media:
        css = {
            'all': ('admin/css/template_admin.css',)
        }
        js = ('admin/js/template_admin.js',)