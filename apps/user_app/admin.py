"""
User App Admin Configuration
Enhanced admin interface with English as default
"""

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from django.http import HttpResponse
from django.shortcuts import render
from .models import User, APIKey, APIUsageLog, InvitationCode


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """Enhanced User Admin with English Interface"""
    
    list_display = ('username', 'email', 'first_name', 'last_name', 'tier_display', 'is_verified', 'api_calls_today', 'total_api_calls', 'is_staff', 'date_joined')
    list_filter = ('is_staff', 'is_superuser', 'is_active', 'is_verified', 'tier', 'language', 'date_joined')
    search_fields = ('username', 'first_name', 'last_name', 'email', 'phone_number')
    ordering = ('-date_joined',)
    
    # Enhanced fieldsets with proper English labels
    fieldsets = BaseUserAdmin.fieldsets + (
        (_('User Tier & Access'), {
            'fields': ('tier',),
            'description': _('User access level determines available templates and features')
        }),
        (_('Extended Information'), {
            'fields': ('phone_number', 'avatar', 'timezone_setting', 'language', 'is_verified')
        }),
        (_('API Statistics'), {
            'fields': ('api_calls_today', 'total_api_calls', 'last_activity'),
            'classes': ('collapse',)
        }),
        (_('Invitation'), {
            'fields': ('invitation_code_used',),
            'classes': ('collapse',)
        }),
    )
    
    add_fieldsets = BaseUserAdmin.add_fieldsets + (
        (_('User Tier'), {
            'fields': ('tier',),
            'description': _('Set user access level (Basic/Standard/Premium)')
        }),
        (_('Extended Information'), {
            'fields': ('email', 'phone_number', 'timezone_setting', 'language')
        }),
    )
    
    readonly_fields = ('last_activity', 'date_joined', 'invitation_code_used')
    
    # Enhanced admin actions for better tier management
    actions = ['upgrade_to_standard', 'upgrade_to_premium', 'downgrade_to_basic', 'verify_users', 'export_user_report']
    
    def tier_display(self, obj):
        """Display user tier with badge styling"""
        tier_colors = {
            'basic': 'secondary',
            'standard': 'primary', 
            'premium': 'warning'
        }
        color = tier_colors.get(obj.tier, 'secondary')
        return format_html(
            '<span class="badge bg-{}">{}</span>',
            color,
            obj.get_tier_display_name()
        )
    tier_display.short_description = _('User Tier')
    tier_display.admin_order_field = 'tier'
    
    # Custom admin actions for tier management
    actions = ['upgrade_to_standard', 'upgrade_to_premium', 'downgrade_to_basic', 'verify_users', 'export_user_report']
    
    def upgrade_to_standard(self, request, queryset):
        """Batch upgrade users to Standard tier"""
        updated = queryset.update(tier='standard')
        self.message_user(request, f'{updated} users upgraded to Standard tier.', level='success')
    upgrade_to_standard.short_description = _('🔥 Upgrade selected users to Standard tier')
    
    def upgrade_to_premium(self, request, queryset):
        """Batch upgrade users to Premium tier"""
        updated = queryset.update(tier='premium')
        self.message_user(request, f'{updated} users upgraded to Premium tier.', level='success')
    upgrade_to_premium.short_description = _('💎 Upgrade selected users to Premium tier')
    
    def downgrade_to_basic(self, request, queryset):
        """Batch downgrade users to Basic tier"""
        updated = queryset.update(tier='basic')
        self.message_user(request, f'{updated} users downgraded to Basic tier.', level='warning')
    downgrade_to_basic.short_description = _('⬇️ Downgrade selected users to Basic tier')
    
    def verify_users(self, request, queryset):
        """Batch verify selected users"""
        updated = queryset.update(is_verified=True)
        self.message_user(request, f'{updated} users verified successfully.', level='success')
    verify_users.short_description = _('✅ Verify selected users')
    
    def export_user_report(self, request, queryset):
        """Export user report as CSV"""
        import csv
        from django.http import HttpResponse
        
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="user_report.csv"'
        
        writer = csv.writer(response)
        writer.writerow(['Username', 'Email', 'Tier', 'API Calls Today', 'Total API Calls', 'Date Joined'])
        
        for user in queryset:
            writer.writerow([
                user.username,
                user.email,
                user.get_tier_display_name(),
                user.api_calls_today,
                user.total_api_calls,
                user.date_joined.strftime('%Y-%m-%d')
            ])
        
        return response
    export_user_report.short_description = _('Export selected users report as CSV')
    
    def invitation_code_used(self, obj):
        """Display invitation code used by this user"""
        codes = obj.used_invitation_codes.all()
        if codes:
            return ", ".join([code.code for code in codes])
        return _("None")
    invitation_code_used.short_description = _("Invitation Code Used")
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related().prefetch_related('used_invitation_codes')


@admin.register(APIKey)
class APIKeyAdmin(admin.ModelAdmin):
    """API Key Management"""
    
    list_display = ('name', 'user', 'prefix_display', 'is_active', 'rate_limit', 'usage_count', 'created_at', 'expires_at')
    list_filter = ('is_active', 'created_at', 'expires_at')
    search_fields = ('name', 'user__username', 'user__email', 'prefix')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('user', 'name', 'is_active')
        }),
        (_('Key Information'), {
            'fields': ('key_display', 'prefix'),
            'classes': ('collapse',)
        }),
        (_('Permission Settings'), {
            'fields': ('rate_limit', 'allowed_ips', 'expires_at')
        }),
        (_('Statistics'), {
            'fields': ('usage_count', 'last_used', 'created_at'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ('key_display', 'prefix', 'usage_count', 'last_used', 'created_at')
    
    def prefix_display(self, obj):
        """Display key prefix"""
        return f"{obj.prefix}***"
    prefix_display.short_description = _("Key Prefix")
    
    def key_display(self, obj):
        """Display partial key for identification"""
        if obj.key:
            return f"{obj.key[:8]}{'*' * 24}"
        return "-"
    key_display.short_description = _("API Key")
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user')


@admin.register(APIUsageLog)
class APIUsageLogAdmin(admin.ModelAdmin):
    """API Usage Log Management"""
    
    list_display = ('api_key_name', 'endpoint', 'method', 'status_code', 'response_time', 'ip_address', 'timestamp')
    list_filter = ('method', 'status_code', 'timestamp', 'api_key__name')
    search_fields = ('endpoint', 'api_key__name', 'ip_address')
    ordering = ('-timestamp',)
    
    fieldsets = (
        (_('Request Information'), {
            'fields': ('api_key', 'endpoint', 'method', 'ip_address', 'user_agent')
        }),
        (_('Response Information'), {
            'fields': ('status_code', 'response_time')
        }),
        (_('Timestamps'), {
            'fields': ('timestamp',)
        }),
    )
    
    readonly_fields = ('timestamp',)
    
    def api_key_name(self, obj):
        """Display API key name"""
        return obj.api_key.name
    api_key_name.short_description = _("API Key")
    api_key_name.admin_order_field = 'api_key__name'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('api_key', 'api_key__user')


@admin.register(InvitationCode)
class InvitationCodeAdmin(admin.ModelAdmin):
    """Invitation Code Admin Interface"""
    
    list_display = ('code', 'is_used', 'current_uses', 'max_uses', 'expires_at', 'created_by', 'used_by', 'created_at')
    list_filter = ('is_used', 'expires_at', 'created_at', 'max_uses')
    search_fields = ('code', 'notes', 'created_by__username', 'used_by__username')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Code Information'), {
            'fields': ('code', 'notes')
        }),
        (_('Usage Settings'), {
            'fields': ('max_uses', 'expires_at', 'allowed_domains')
        }),
        (_('Usage Tracking'), {
            'fields': ('is_used', 'current_uses', 'used_by', 'used_at'),
            'classes': ('collapse',)
        }),
        (_('Metadata'), {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ('created_at', 'used_at', 'current_uses')
    
    def save_model(self, request, obj, form, change):
        if not change:  # Creating new object
            obj.created_by = request.user
        super().save_model(request, obj, form, change)
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('created_by', 'used_by')
    
    actions = ['generate_bulk_codes', 'mark_as_expired']
    
    def generate_bulk_codes(self, request, queryset):
        """Generate multiple invitation codes"""
        # This would open a form to generate bulk codes
        # For now, just a simple message
        self.message_user(request, _("Bulk code generation feature coming soon"))
    generate_bulk_codes.short_description = _("Generate bulk invitation codes")
    
    def mark_as_expired(self, request, queryset):
        """Mark selected codes as expired"""
        from datetime import datetime
        count = queryset.update(expires_at=datetime.now())
        self.message_user(request, _(f"Marked {count} codes as expired"))
    mark_as_expired.short_description = _("Mark as expired")
    
    def has_add_permission(self, request):
        """禁止手动添加日志"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """禁止修改日志"""
        return False
