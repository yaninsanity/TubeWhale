"""
TubeWhale Engine Models
存储TubeWhale处理结果的Django模型
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.contrib.auth import get_user_model
import uuid

User = get_user_model()


class ProcessingJob(models.Model):
    """视频处理任务模型"""
    
    STATUS_CHOICES = [
        ('pending', _('Pending')),
        ('processing', _('Processing')),
        ('completed', _('Completed')),
        ('failed', _('Failed')),
        ('cancelled', _('Cancelled')),
    ]
    
    TASK_TYPE_CHOICES = [
        ('transcript', _('Transcript Only')),
        ('audio', _('Audio Processing')),
        ('full', _('Full Analysis')),
        ('summary', _('Summary Generation')),
        ('search', _('Related Video Search')),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='tubewhale_jobs')
    video_id = models.CharField(max_length=11, help_text=_('YouTube Video ID'))
    task_type = models.CharField(max_length=20, choices=TASK_TYPE_CHOICES, default='full')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # 处理选项
    options = models.JSONField(default=dict, blank=True, help_text=_('Processing options'))
    
    # 时间戳
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # 结果和错误信息
    result = models.JSONField(null=True, blank=True, help_text=_('Processing result'))
    error_message = models.TextField(blank=True, help_text=_('Error message if failed'))
    
    # 进度跟踪
    progress_percentage = models.IntegerField(default=0, help_text=_('Progress percentage (0-100)'))
    current_step = models.CharField(max_length=100, blank=True, help_text=_('Current processing step'))
    
    class Meta:
        verbose_name = _('TubeWhale Processing Job')
        verbose_name_plural = _('TubeWhale Processing Jobs')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'status']),
            models.Index(fields=['video_id', 'status']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"{self.video_id} - {self.get_task_type_display()} ({self.get_status_display()})"
    
    @property
    def is_completed(self):
        return self.status in ['completed', 'failed', 'cancelled']
    
    @property
    def duration(self):
        """处理时长"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None


class VideoProcessingResult(models.Model):
    """视频处理结果模型"""
    
    job = models.OneToOneField(ProcessingJob, on_delete=models.CASCADE, related_name='processing_result')
    video_id = models.CharField(max_length=11, db_index=True)
    
    # 视频基本信息
    video_title = models.CharField(max_length=500, blank=True)
    video_description = models.TextField(blank=True)
    video_duration = models.IntegerField(null=True, blank=True, help_text=_('Duration in seconds'))
    video_thumbnail_url = models.URLField(blank=True)
    channel_name = models.CharField(max_length=200, blank=True)
    
    # 处理结果
    transcript_text = models.TextField(blank=True, help_text=_('Video transcript'))
    audio_summary = models.TextField(blank=True, help_text=_('Audio processing summary'))
    generated_summary = models.TextField(blank=True, help_text=_('AI generated summary'))
    
    # 分析结果
    key_topics = models.JSONField(default=list, blank=True, help_text=_('Extracted key topics'))
    sentiment_analysis = models.JSONField(default=dict, blank=True, help_text=_('Sentiment analysis result'))
    language_detected = models.CharField(max_length=10, blank=True, help_text=_('Detected language code'))
    
    # 相关视频
    related_videos = models.JSONField(default=list, blank=True, help_text=_('Related videos found'))
    
    # 元数据
    processing_metadata = models.JSONField(default=dict, blank=True, help_text=_('Processing metadata'))
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = _('Video Processing Result')
        verbose_name_plural = _('Video Processing Results')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['video_id']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"{self.video_id} - {self.video_title[:50]}"


class TubeWhaleEngineConfig(models.Model):
    """TubeWhale引擎配置模型"""
    
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='tubewhale_config')
    
    # API配置
    openai_api_key = models.CharField(max_length=200, blank=True, help_text=_('OpenAI API Key'))
    youtube_api_key = models.CharField(max_length=200, blank=True, help_text=_('YouTube API Key'))
    
    # 处理偏好
    default_language = models.CharField(max_length=10, default='en', help_text=_('Default processing language'))
    auto_summarize = models.BooleanField(default=True, help_text=_('Auto generate summaries'))
    audio_quality = models.CharField(
        max_length=20, 
        choices=[('low', _('Low')), ('medium', _('Medium')), ('high', _('High'))],
        default='medium'
    )
    
    # 通知设置
    email_notifications = models.BooleanField(default=True, help_text=_('Email notifications'))
    webhook_url = models.URLField(blank=True, help_text=_('Webhook URL for notifications'))
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = _('TubeWhale Engine Config')
        verbose_name_plural = _('TubeWhale Engine Configs')
    
    def __str__(self):
        return f"{self.user.username} - TubeWhale Config"


class CLICommandLog(models.Model):
    """Audit log of CLI commands executed via admin integration.

    Stores sanitized command name, allowed args, execution metadata, truncated outputs,
    and rate limiting correlation fields.
    """
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL, related_name='cli_command_logs')
    command = models.CharField(max_length=100, db_index=True)
    args = models.JSONField(default=list, blank=True)
    return_code = models.IntegerField(null=True, blank=True)
    success = models.BooleanField(default=False)
    stdout_truncated = models.TextField(blank=True)
    stderr_truncated = models.TextField(blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    rate_bucket = models.CharField(max_length=64, blank=True, help_text=_('Rate limiting bucket key'))
    error_flag = models.BooleanField(default=False)
    meta = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = _('CLI Command Log')
        verbose_name_plural = _('CLI Command Logs')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['command', 'created_at']),
            models.Index(fields=['user', 'created_at']),
        ]

    MAX_OUTPUT_CHARS = 4000

    @classmethod
    def truncate(cls, text: str | None):  # type: ignore[override]
        if not text:
            return ''
        if len(text) > cls.MAX_OUTPUT_CHARS:
            return text[:cls.MAX_OUTPUT_CHARS] + f"\n...[truncated {len(text)-cls.MAX_OUTPUT_CHARS} chars]"
        return text

    def __str__(self):  # pragma: no cover
        return f"{self.command} ({'ok' if self.success else 'fail'}) @ {self.created_at:%Y-%m-%d %H:%M:%S}"
