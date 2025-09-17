"""
TubeWhale Engine Admin
Django管理界面配置
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from .models import ProcessingJob, VideoProcessingResult, TubeWhaleEngineConfig


@admin.register(ProcessingJob)
class ProcessingJobAdmin(admin.ModelAdmin):
    """处理任务管理界面"""
    
    list_display = [
        'video_id', 'user', 'task_type', 'status_display', 
        'progress_display', 'created_at', 'duration_display', 'actions_display'
    ]
    list_filter = ['status', 'task_type', 'created_at', 'user']
    search_fields = ['video_id', 'user__username', 'user__email']
    readonly_fields = [
        'id', 'created_at', 'started_at', 'completed_at', 
        'duration_display', 'result_preview'
    ]
    
    fieldsets = (
        (_('基本信息'), {
            'fields': ('id', 'user', 'video_id', 'task_type')
        }),
        (_('状态信息'), {
            'fields': ('status', 'progress_percentage', 'current_step', 'error_message')
        }),
        (_('时间信息'), {
            'fields': ('created_at', 'started_at', 'completed_at', 'duration_display')
        }),
        (_('配置和结果'), {
            'fields': ('options', 'result_preview'),
            'classes': ('collapse',)
        }),
    )
    
    def status_display(self, obj):
        """状态显示"""
        color_map = {
            'pending': '#ffc107',
            'processing': '#007bff',
            'completed': '#28a745',
            'failed': '#dc3545',
            'cancelled': '#6c757d'
        }
        color = color_map.get(obj.status, '#000')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, obj.get_status_display()
        )
    status_display.short_description = _('Status')
    
    def progress_display(self, obj):
        """进度显示"""
        if obj.status == 'completed':
            return format_html('<span style="color: green;">✅ 100%</span>')
        elif obj.status == 'failed':
            return format_html('<span style="color: red;">❌ Failed</span>')
        elif obj.status == 'cancelled':
            return format_html('<span style="color: gray;">🛑 Cancelled</span>')
        else:
            return format_html(
                '<div style="background: #e9ecef; border-radius: 3px; width: 100px;">'
                '<div style="background: #007bff; height: 10px; width: {}%; border-radius: 3px;"></div>'
                '</div> {}%',
                obj.progress_percentage, obj.progress_percentage
            )
    progress_display.short_description = _('Progress')
    
    def duration_display(self, obj):
        """持续时间显示"""
        if obj.duration:
            return str(obj.duration)
        return '-'
    duration_display.short_description = _('Duration')
    
    def actions_display(self, obj):
        """操作按钮"""
        actions = []
        
        if obj.status == 'completed' and hasattr(obj, 'processing_result'):
            result_url = reverse('admin:tubewhale_engine_videoprocessingresult_change', 
                               args=[obj.processing_result.id])
            actions.append(f'<a href="{result_url}" style="color: green;">📄 查看结果</a>')
        
        if obj.status in ['pending', 'processing']:
            # 这里可以添加取消任务的链接
            actions.append('<span style="color: orange;">⏸️ 可取消</span>')
        
        return format_html(' | '.join(actions)) if actions else '-'
    actions_display.short_description = _('Actions')
    
    def result_preview(self, obj):
        """结果预览"""
        if obj.result:
            # 显示结果的简要信息
            result_info = []
            if obj.result.get('video_info'):
                result_info.append(f"📹 标题: {obj.result['video_info'].get('title', 'N/A')[:50]}")
            if obj.result.get('transcript'):
                result_info.append(f"📝 字幕长度: {len(obj.result['transcript'])} 字符")
            if obj.result.get('summary'):
                result_info.append(f"📋 摘要长度: {len(obj.result['summary'])} 字符")
            
            return format_html('<br>'.join(result_info)) if result_info else '无详细信息'
        return '无结果'
    result_preview.short_description = _('Result Preview')


@admin.register(VideoProcessingResult)
class VideoProcessingResultAdmin(admin.ModelAdmin):
    """视频处理结果管理界面"""
    
    list_display = [
        'video_id', 'video_title_short', 'job_user', 'language_detected',
        'has_transcript', 'has_summary', 'created_at'
    ]
    list_filter = ['language_detected', 'created_at', 'job__user']
    search_fields = ['video_id', 'video_title', 'channel_name', 'job__user__username']
    readonly_fields = ['job', 'video_id', 'created_at', 'updated_at']
    
    fieldsets = (
        (_('关联信息'), {
            'fields': ('job', 'video_id')
        }),
        (_('视频信息'), {
            'fields': ('video_title', 'video_description', 'video_duration', 
                      'video_thumbnail_url', 'channel_name')
        }),
        (_('处理结果'), {
            'fields': ('transcript_text', 'audio_summary', 'generated_summary')
        }),
        (_('分析结果'), {
            'fields': ('key_topics', 'sentiment_analysis', 'language_detected', 'related_videos')
        }),
        (_('元数据'), {
            'fields': ('processing_metadata', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def video_title_short(self, obj):
        """缩短的视频标题"""
        return obj.video_title[:50] + '...' if len(obj.video_title) > 50 else obj.video_title
    video_title_short.short_description = _('Video Title')
    
    def job_user(self, obj):
        """任务用户"""
        return obj.job.user.username
    job_user.short_description = _('User')
    
    def has_transcript(self, obj):
        """是否有字幕"""
        return '✅' if obj.transcript_text else '❌'
    has_transcript.short_description = _('Transcript')
    
    def has_summary(self, obj):
        """是否有摘要"""
        return '✅' if obj.generated_summary else '❌'
    has_summary.short_description = _('Summary')


@admin.register(TubeWhaleEngineConfig)
class TubeWhaleEngineConfigAdmin(admin.ModelAdmin):
    """TubeWhale引擎配置管理界面"""
    
    list_display = [
        'user', 'default_language', 'auto_summarize', 'audio_quality',
        'email_notifications', 'created_at'
    ]
    list_filter = ['default_language', 'auto_summarize', 'audio_quality', 'email_notifications']
    search_fields = ['user__username', 'user__email']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        (_('用户信息'), {
            'fields': ('user',)
        }),
        (_('API配置'), {
            'fields': ('openai_api_key', 'youtube_api_key'),
            'description': _('API密钥将被加密存储')
        }),
        (_('处理偏好'), {
            'fields': ('default_language', 'auto_summarize', 'audio_quality')
        }),
        (_('通知设置'), {
            'fields': ('email_notifications', 'webhook_url')
        }),
        (_('时间戳'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_form(self, request, obj=None, **kwargs):
        """自定义表单"""
        form = super().get_form(request, obj, **kwargs)
        
        # 隐藏API密钥的实际值
        if obj and obj.openai_api_key:
            form.base_fields['openai_api_key'].help_text = _('当前已设置API密钥（留空保持不变）')
        if obj and obj.youtube_api_key:
            form.base_fields['youtube_api_key'].help_text = _('当前已设置API密钥（留空保持不变）')
        
        return form


# 自定义管理站点标题
admin.site.site_header = _('TubeWhale Engine Administration')
admin.site.site_title = _('TubeWhale Engine')
admin.site.index_title = _('Welcome to TubeWhale Engine Management')
