"""
Video App Admin Configuration
Enhanced admin interface with English labels
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from .models import Video, AnalysisTask, VideoComment, VideoTag


@admin.register(Video)
class VideoAdmin(admin.ModelAdmin):
    """Video Management"""
    
    list_display = ('title', 'youtube_id', 'user', 'status', 'duration_display', 'view_count', 'created_at')
    list_filter = ('status', 'created_at', 'channel_name')
    search_fields = ('title', 'youtube_id', 'channel_name', 'user__username')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('user', 'youtube_id', 'title', 'description', 'url')
        }),
        (_('Video Metadata'), {
            'fields': ('duration', 'view_count', 'like_count', 'comment_count', 'publish_date'),
            'classes': ('collapse',)
        }),
        (_('Channel Information'), {
            'fields': ('channel_name', 'channel_id'),
            'classes': ('collapse',)
        }),
        (_('File Information'), {
            'fields': ('local_file_path', 'file_size', 'downloaded_at'),
            'classes': ('collapse',)
        }),
        (_('Status'), {
            'fields': ('status', 'created_at', 'updated_at'),
        }),
    )
    
    readonly_fields = ('created_at', 'updated_at', 'downloaded_at')
    
    def duration_display(self, obj):
        """Display duration"""
        if obj.duration:
            minutes = obj.duration // 60
            seconds = obj.duration % 60
            return f"{minutes}:{seconds:02d}"
        return "-"
    duration_display.short_description = _("Duration")
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user')


@admin.register(AnalysisTask)
class AnalysisTaskAdmin(admin.ModelAdmin):
    """Analysis Task Management"""
    
    list_display = ('video_title', 'analysis_type', 'expert_domain', 'status', 'progress', 'confidence_score', 'created_at')
    list_filter = ('analysis_type', 'expert_domain', 'status', 'created_at')
    search_fields = ('video__title', 'video__youtube_id', 'user__username')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('video', 'user', 'analysis_type', 'expert_domain', 'analysis_depth')
        }),
        (_('Task Status'), {
            'fields': ('status', 'progress', 'error_message', 'celery_task_id')
        }),
        (_('Result Data'), {
            'fields': ('result_data', 'confidence_score'),
            'classes': ('collapse',)
        }),
        (_('Time Records'), {
            'fields': ('created_at', 'started_at', 'completed_at'),
        }),
    )
    
    readonly_fields = ('created_at', 'started_at', 'completed_at')
    
    def video_title(self, obj):
        """Display video title"""
        return obj.video.title
    video_title.short_description = _("Video Title")
    video_title.admin_order_field = 'video__title'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('video', 'user')


@admin.register(VideoComment)
class VideoCommentAdmin(admin.ModelAdmin):
    """Video Comment Management"""
    
    list_display = ('video_title', 'author', 'text_preview', 'like_count', 'sentiment_score', 'published_at')
    list_filter = ('is_spam', 'published_at', 'collected_at')
    search_fields = ('author', 'text', 'video__title')
    ordering = ('-published_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('video', 'youtube_comment_id', 'author', 'text')
        }),
        (_('Statistics'), {
            'fields': ('like_count', 'reply_count')
        }),
        (_('Analysis Results'), {
            'fields': ('sentiment_score', 'is_spam')
        }),
        (_('Timestamps'), {
            'fields': ('published_at', 'collected_at')
        }),
    )
    
    readonly_fields = ('collected_at',)
    
    def video_title(self, obj):
        """Display video title"""
        return obj.video.title
    video_title.short_description = _("Video Title")
    video_title.admin_order_field = 'video__title'
    
    def text_preview(self, obj):
        """Display comment preview"""
        return obj.text[:100] + "..." if len(obj.text) > 100 else obj.text
    text_preview.short_description = _("Comment Content")
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('video')


@admin.register(VideoTag)
class VideoTagAdmin(admin.ModelAdmin):
    """Video Tag Management"""
    
    list_display = ('video_title', 'name', 'tag_type', 'confidence', 'source', 'created_by', 'created_at')
    list_filter = ('tag_type', 'source', 'created_at')
    search_fields = ('name', 'video__title')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('video', 'name', 'tag_type', 'confidence')
        }),
        (_('Source Information'), {
            'fields': ('source', 'created_by', 'created_at')
        }),
    )
    
    readonly_fields = ('created_at',)
    
    def video_title(self, obj):
        """Display video title"""
        return obj.video.title
    video_title.short_description = _("Video Title")
    video_title.admin_order_field = 'video__title'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('video', 'created_by')
