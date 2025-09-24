"""
Enhanced Django Admin Site
提供增强的智能数据收集管理界面
"""

from django.contrib import admin
from django.contrib.admin import AdminSite
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.translation import gettext_lazy as _
from .admin_views import admin_dashboard_summary


class EnhancedAdminSite(AdminSite):
    """增强的管理站点，使用现代化界面和智能代理功能"""
    
    site_header = _('TubeWhale Intelligence Hub')
    site_title = _('TubeWhale Admin')
    index_title = _('智能数据收集平台')
    
    def index(self, request: HttpRequest, extra_context=None) -> HttpResponse:
        """
        使用增强的主页模板，显示实时代理状态和数据指标
        """
        # 获取增强的上下文数据
        context = self._get_enhanced_context(request)
        
        # 合并额外上下文
        if extra_context:
            context.update(extra_context)
            
        return render(request, 'admin/admin_index.html', context)
    
    def _get_enhanced_context(self, request: HttpRequest) -> dict:
        """获取增强主页的上下文数据"""
        from .admin_views import (
            _get_videos_count, _get_active_agents_count, 
            _get_daily_data_points, _get_processing_queue_size,
            _get_agents_status, _get_system_health_summary
        )
        
        return {
            'videos_count': _get_videos_count(),
            'active_agents': _get_active_agents_count(),
            'data_points': _get_daily_data_points(),
            'queue_size': _get_processing_queue_size(),
            'agents_status': _get_agents_status(),
            'system_health': _get_system_health_summary(),
            'site_header': self.site_header,
            'site_title': self.site_title,
            'index_title': self.index_title,
            'has_permission': request.user.is_active and request.user.is_staff,
        }


# 创建增强站点实例
enhanced_admin_site = EnhancedAdminSite(name='enhanced_admin')


# 重新注册所有现有的admin配置到增强站点
def register_all_admins():
    """将所有现有的admin配置注册到增强站点"""
    try:
        # 导入并注册所有admin配置
        from apps.tubewhale_engine.admin import (
            ProcessingJobAdmin, VideoProcessingResultAdmin, 
            TubeWhaleEngineConfigAdmin, CLICommandLogAdmin
        )
        from apps.tubewhale_engine.models import (
            ProcessingJob, VideoProcessingResult, 
            TubeWhaleEngineConfig, CLICommandLog
        )
        
        # 注册到增强站点
        enhanced_admin_site.register(ProcessingJob, ProcessingJobAdmin)
        enhanced_admin_site.register(VideoProcessingResult, VideoProcessingResultAdmin)
        enhanced_admin_site.register(TubeWhaleEngineConfig, TubeWhaleEngineConfigAdmin)
        enhanced_admin_site.register(CLICommandLog, CLICommandLogAdmin)
        
    except ImportError:
        # 如果某些模型不存在，继续注册其他的
        pass
    
    try:
        # 注册其他应用的admin
        from apps.user_app import admin as user_admin
        from apps.templates_app import admin as templates_admin
        from apps.analysis_app import admin as analysis_admin
        from apps.video_app import admin as video_admin
        from apps.api_app import admin as api_admin
        
        # 这里可以添加更多的注册逻辑
        
    except ImportError:
        pass


# 自动注册所有admin配置
register_all_admins()


# Template Information Admin
from .models import TemplateInfo

@admin.register(TemplateInfo)
class TemplateInfoAdmin(admin.ModelAdmin):
    """Template Information Admin for Client Interface"""
    
    list_display = ('title', 'category', 'theme', 'difficulty_level', 'required_tier', 'is_active', 'sort_order')
    list_filter = ('category', 'difficulty_level', 'required_tier', 'is_active')
    search_fields = ('title', 'description', 'theme', 'template_id')
    list_editable = ('is_active', 'sort_order')
    ordering = ['sort_order', 'title']
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('template_id', 'title', 'description', 'theme', 'category')
        }),
        (_('User Experience'), {
            'fields': ('difficulty_level', 'estimated_time', 'required_tier')
        }),
        (_('Features & Use Cases'), {
            'fields': ('features', 'use_cases'),
            'description': _('Enter as JSON lists, e.g., ["Feature 1", "Feature 2"]')
        }),
        (_('Display Settings'), {
            'fields': ('is_active', 'sort_order'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['activate_templates', 'deactivate_templates', 'set_basic_tier', 'set_premium_tier']
    
    def activate_templates(self, request, queryset):
        """Activate selected templates"""
        updated = queryset.update(is_active=True)
        self.message_user(request, f'{updated} templates activated successfully.')
    activate_templates.short_description = _('✅ Activate selected templates')
    
    def deactivate_templates(self, request, queryset):
        """Deactivate selected templates"""
        updated = queryset.update(is_active=False)
        self.message_user(request, f'{updated} templates deactivated.')
    deactivate_templates.short_description = _('❌ Deactivate selected templates')
    
    def set_basic_tier(self, request, queryset):
        """Set templates to basic tier"""
        updated = queryset.update(required_tier='basic')
        self.message_user(request, f'{updated} templates set to Basic tier.')
    set_basic_tier.short_description = _('🆓 Set to Basic tier')
    
    def set_premium_tier(self, request, queryset):
        """Set templates to premium tier"""
        updated = queryset.update(required_tier='premium')
        self.message_user(request, f'{updated} templates set to Premium tier.')
    set_premium_tier.short_description = _('💎 Set to Premium tier')