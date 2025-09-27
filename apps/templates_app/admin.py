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


# Template Management Admin
from .models import (
    TemplateInfo, CustomTemplate, ExpertPrompt, 
    CLIExecution, Job, UserTemplateSelection,
    SystemHotspotEvent
)

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


@admin.register(CustomTemplate)
class CustomTemplateAdmin(admin.ModelAdmin):
    """自定义模板管理"""
    
    list_display = ('template_id', 'name', 'domain', 'template_type', 'version', 'immutable', 'created_at')
    list_filter = ('domain', 'template_type', 'immutable', 'created_at')
    search_fields = ('template_id', 'name', 'description')
    list_editable = ('immutable',)
    ordering = ['template_id']
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        (_('Template Identity'), {
            'fields': ('template_id', 'name', 'domain', 'template_type')
        }),
        (_('Content'), {
            'fields': ('description', 'prompt'),
            'classes': ('wide',)
        }),
        (_('Configuration'), {
            'fields': ('parameters', 'tags', 'version'),
            'description': _('Parameters and tags should be valid JSON')
        }),
        (_('Settings'), {
            'fields': ('immutable',),
            'classes': ('collapse',)
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['make_immutable', 'make_mutable', 'duplicate_template']
    
    def make_immutable(self, request, queryset):
        """Make selected templates immutable"""
        updated = queryset.update(immutable=True)
        self.message_user(request, f'{updated} templates marked as immutable.')
    make_immutable.short_description = _('🔒 Make immutable')
    
    def make_mutable(self, request, queryset):
        """Make selected templates mutable"""
        updated = queryset.update(immutable=False)
        self.message_user(request, f'{updated} templates marked as mutable.')
    make_mutable.short_description = _('🔓 Make mutable')
    
    def duplicate_template(self, request, queryset):
        """Duplicate selected templates"""
        for template in queryset:
            template.pk = None
            template.template_id = f"{template.template_id}_copy"
            template.name = f"{template.name} (Copy)"
            template.immutable = False
            template.save()
        self.message_user(request, f'{queryset.count()} templates duplicated.')
    duplicate_template.short_description = _('📄 Duplicate templates')


@admin.register(ExpertPrompt)
class ExpertPromptAdmin(admin.ModelAdmin):
    """专家提示管理"""
    
    list_display = ('slug', 'role', 'domain', 'title', 'active', 'weight', 'created_at')
    list_filter = ('role', 'domain', 'active', 'created_at')
    search_fields = ('slug', 'title', 'description')
    list_editable = ('active', 'weight')
    ordering = ['weight', 'role', 'domain']
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        (_('Expert Identity'), {
            'fields': ('slug', 'role', 'domain', 'title')
        }),
        (_('Content'), {
            'fields': ('description', 'prompt_intro', 'prompt_outro'),
            'classes': ('wide',)
        }),
        (_('Settings'), {
            'fields': ('active', 'weight'),
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['activate_prompts', 'deactivate_prompts', 'set_high_priority', 'set_low_priority']
    
    def activate_prompts(self, request, queryset):
        """Activate selected prompts"""
        updated = queryset.update(active=True)
        self.message_user(request, f'{updated} expert prompts activated.')
    activate_prompts.short_description = _('✅ Activate prompts')
    
    def deactivate_prompts(self, request, queryset):
        """Deactivate selected prompts"""
        updated = queryset.update(active=False)
        self.message_user(request, f'{updated} expert prompts deactivated.')
    deactivate_prompts.short_description = _('❌ Deactivate prompts')
    
    def set_high_priority(self, request, queryset):
        """Set high priority (weight=10)"""
        updated = queryset.update(weight=10)
        self.message_user(request, f'{updated} prompts set to high priority.')
    set_high_priority.short_description = _('⬆️ Set high priority')
    
    def set_low_priority(self, request, queryset):
        """Set low priority (weight=1000)"""
        updated = queryset.update(weight=1000)
        self.message_user(request, f'{updated} prompts set to low priority.')
    set_low_priority.short_description = _('⬇️ Set low priority')


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    """任务管理"""
    
    list_display = ('id', 'user', 'command', 'template_id', 'status', 'progress', 'created_at', 'finished_at')
    list_filter = ('status', 'command', 'created_at')
    search_fields = ('user__username', 'template_id', 'expert_slug', 'celery_task_id')
    readonly_fields = ('created_at', 'updated_at', 'finished_at', 'celery_task_id', 'execution_id')
    ordering = ['-created_at']
    
    fieldsets = (
        (_('Job Information'), {
            'fields': ('user', 'command', 'template_id', 'expert_slug')
        }),
        (_('Configuration'), {
            'fields': ('variables',),
            'classes': ('wide',)
        }),
        (_('Status'), {
            'fields': ('status', 'progress', 'error'),
        }),
        (_('Execution Details'), {
            'fields': ('celery_task_id', 'execution_id'),
            'classes': ('collapse',)
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at', 'finished_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['cancel_jobs', 'restart_failed_jobs']
    
    def cancel_jobs(self, request, queryset):
        """Cancel selected jobs"""
        from apps.templates_app.models import JobStatus
        updated = queryset.filter(
            status__in=[JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING]
        ).update(status=JobStatus.CANCELED)
        self.message_user(request, f'{updated} jobs cancelled.')
    cancel_jobs.short_description = _('❌ Cancel jobs')
    
    def restart_failed_jobs(self, request, queryset):
        """Restart failed jobs"""
        from apps.templates_app.models import JobStatus
        updated = queryset.filter(status=JobStatus.ERROR).update(
            status=JobStatus.PENDING, 
            error='', 
            progress=0
        )
        self.message_user(request, f'{updated} failed jobs reset to pending.')
    restart_failed_jobs.short_description = _('🔄 Restart failed jobs')


@admin.register(CLIExecution)
class CLIExecutionAdmin(admin.ModelAdmin):
    """CLI执行历史管理"""
    
    list_display = ('id', 'command', 'template_id', 'status', 'returncode', 'duration_ms', 'created_at')
    list_filter = ('command', 'status', 'created_at')
    search_fields = ('command', 'template_id', 'expert_slug')
    readonly_fields = ('created_at', 'updated_at', 'duration_ms', 'returncode')
    ordering = ['-created_at']
    
    fieldsets = (
        (_('Execution Info'), {
            'fields': ('command', 'args', 'template_id', 'expert_slug')
        }),
        (_('Results'), {
            'fields': ('status', 'returncode', 'duration_ms'),
        }),
        (_('Output'), {
            'fields': ('stdout', 'stderr'),
            'classes': ('wide', 'collapse'),
        }),
        (_('Content'), {
            'fields': ('prompt_final',),
            'classes': ('wide', 'collapse'),
        }),
        (_('Metadata'), {
            'fields': ('meta',),
            'classes': ('collapse',)
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['cleanup_old_executions', 'export_executions']
    
    def cleanup_old_executions(self, request, queryset):
        """Clean up old executions (older than 30 days)"""
        from django.utils import timezone
        from datetime import timedelta
        cutoff = timezone.now() - timedelta(days=30)
        deleted_count = queryset.filter(created_at__lt=cutoff).count()
        queryset.filter(created_at__lt=cutoff).delete()
        self.message_user(request, f'{deleted_count} old executions cleaned up.')
    cleanup_old_executions.short_description = _('🧹 Cleanup old executions')
    
    def export_executions(self, request, queryset):
        """Export execution data"""
        # 这里可以实现导出功能
        self.message_user(request, f'{queryset.count()} executions ready for export.')
    export_executions.short_description = _('📤 Export executions')


@admin.register(UserTemplateSelection)
class UserTemplateSelectionAdmin(admin.ModelAdmin):
    """用户模板选择管理"""
    
    list_display = ('user', 'template_id', 'template_name', 'template_type', 'active', 'locked_at')
    list_filter = ('template_type', 'active', 'locked_at')
    search_fields = ('user__username', 'template_id', 'template_name')
    readonly_fields = ('locked_at',)
    ordering = ['-locked_at']
    
    fieldsets = (
        (_('Selection Info'), {
            'fields': ('user', 'template_id', 'template_name', 'template_type')
        }),
        (_('Status'), {
            'fields': ('active', 'locked_at'),
        }),
    )


@admin.register(SystemHotspotEvent)
class SystemHotspotEventAdmin(admin.ModelAdmin):
    """系统热点事件管理"""
    
    list_display = ('resource', 'level', 'peak_percent', 'duration_sec', 'resolved', 'started_at')
    list_filter = ('resource', 'level', 'resolved', 'started_at')
    readonly_fields = ('started_at', 'ended_at', 'duration_sec')
    ordering = ['-started_at']
    
    fieldsets = (
        (_('Event Info'), {
            'fields': ('resource', 'level', 'peak_percent')
        }),
        (_('Status'), {
            'fields': ('resolved', 'started_at', 'ended_at', 'duration_sec'),
        }),
        (_('Metadata'), {
            'fields': ('meta',),
            'classes': ('collapse',)
        }),
    )