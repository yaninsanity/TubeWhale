"""
TubeWhale Engine Models
存储TubeWhale处理结果的Django模型
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.contrib.auth import get_user_model
from django.utils import timezone
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


# ============================================================================
# ENHANCED ANALYSIS RESULTS STORAGE MODELS
# ============================================================================

class AnalysisJob(models.Model):
    """Store analysis job information and execution results"""
    
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    ANALYSIS_TYPES = [
        ('video', 'Video Analysis'),
        ('playlist', 'Playlist Analysis'),
        ('batch', 'Batch Analysis'),
        ('custom', 'Custom Analysis'),
    ]
    
    # Job identification
    job_id = models.CharField(max_length=32, unique=True, primary_key=True, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    
    # Job configuration
    analysis_type = models.CharField(max_length=20, choices=ANALYSIS_TYPES, default='video')
    expert_role = models.CharField(max_length=50)
    template_id = models.CharField(max_length=50)
    
    # Content information
    content_id = models.CharField(max_length=100)  # Video ID, Playlist ID, etc.
    content_url = models.URLField(max_length=500, blank=True)
    content_title = models.TextField(blank=True)
    content_description = models.TextField(blank=True)
    
    # Job status and timing
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='queued')
    status_message = models.CharField(max_length=200, blank=True, help_text='Current status message')
    progress = models.IntegerField(default=0)  # 0-100
    current_step = models.CharField(max_length=100, blank=True, help_text='Current processing step')
    estimated_time_remaining = models.IntegerField(null=True, blank=True, help_text='Estimated seconds remaining')
    steps_completed = models.IntegerField(default=0, help_text='Number of completed steps')
    total_steps = models.IntegerField(default=5, help_text='Total number of steps')
    
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Execution details
    processing_time_seconds = models.FloatField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    
    # Analysis configuration
    custom_questions = models.JSONField(default=list, blank=True)
    analysis_options = models.JSONField(default=dict, blank=True)
    
    # Task tracking
    celery_task_id = models.CharField(max_length=128, blank=True, default="", db_index=True, help_text="Celery task ID for async processing")
    
    # Client information
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    api_version = models.CharField(max_length=20, default='v1')
    
    class Meta:
        db_table = 'tubewhale_analysis_jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status', 'created_at']),
            models.Index(fields=['analysis_type', 'expert_role']),
            models.Index(fields=['content_id']),
        ]
    
    def save(self, *args, **kwargs):
        if not self.job_id:
            self.job_id = uuid.uuid4().hex[:8]
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.job_id} - {self.analysis_type} ({self.status})"
    
    @property
    def progress_percentage(self):
        """获取进度百分比"""
        if self.total_steps > 0:
            return min(100, (self.steps_completed / self.total_steps) * 100)
        return self.progress
    
    @property
    def estimated_completion_time(self):
        """估算完成时间"""
        from django.utils import timezone
        if self.started_at and self.estimated_time_remaining:
            return timezone.now() + timezone.timedelta(seconds=self.estimated_time_remaining)
        return None
    
    def update_progress(self, step_name, progress=None, steps_completed=None, estimated_remaining=None):
        """更新任务进度"""
        self.current_step = step_name
        if progress is not None:
            self.progress = progress
        if steps_completed is not None:
            self.steps_completed = steps_completed
            self.progress = min(100, (steps_completed / self.total_steps) * 100)
        if estimated_remaining is not None:
            self.estimated_time_remaining = estimated_remaining
        self.save(update_fields=['current_step', 'progress', 'steps_completed', 'estimated_time_remaining'])
    
    def log_execution_step(self, step_name, details=None, execution_time=None, cpu_usage=None, memory_usage=None):
        """记录详细的执行步骤日志"""
        ExecutionLog.objects.create(
            job=self,
            step_name=step_name,
            details=details or {},
            execution_time_ms=execution_time,
            cpu_usage_percent=cpu_usage,
            memory_usage_mb=memory_usage,
            timestamp=timezone.now()
        )
    
    def calculate_precise_eta(self):
        """基于历史数据计算精确的预计完成时间"""
        from django.utils import timezone
        from django.db.models import Avg
        import datetime
        
        if not self.started_at:
            return None
            
        # 获取相同类型任务的历史数据
        similar_jobs = AnalysisJob.objects.filter(
            analysis_type=self.analysis_type,
            expert_role=self.expert_role,
            status='completed',
            processing_time_seconds__isnull=False
        ).order_by('-completed_at')[:10]  # 最近10个相似任务
        
        if similar_jobs:
            avg_time = similar_jobs.aggregate(Avg('processing_time_seconds'))['processing_time_seconds__avg']
            if avg_time:
                elapsed = (timezone.now() - self.started_at).total_seconds()
                progress_ratio = self.progress / 100.0 if self.progress > 0 else 0.1
                estimated_total = elapsed / progress_ratio if progress_ratio > 0 else avg_time
                remaining = max(0, estimated_total - elapsed)
                return int(remaining)
        
        # 如果没有历史数据，基于当前进度估算
        if self.progress > 0:
            elapsed = (timezone.now() - self.started_at).total_seconds()
            estimated_total = elapsed * (100 / self.progress)
            remaining = max(0, estimated_total - elapsed)
            return int(remaining)
        
        return None


class ExecutionLog(models.Model):
    """精确的任务执行日志"""
    
    job = models.ForeignKey(AnalysisJob, on_delete=models.CASCADE, related_name='execution_logs')
    step_name = models.CharField(max_length=200, help_text="执行步骤名称")
    details = models.JSONField(default=dict, help_text="详细执行信息")
    
    # 性能指标
    execution_time_ms = models.IntegerField(null=True, blank=True, help_text="执行时间(毫秒)")
    cpu_usage_percent = models.FloatField(null=True, blank=True, help_text="CPU使用率")
    memory_usage_mb = models.FloatField(null=True, blank=True, help_text="内存使用量(MB)")
    
    # 网络相关
    api_calls_count = models.IntegerField(default=0, help_text="API调用次数")
    data_processed_kb = models.IntegerField(null=True, blank=True, help_text="处理的数据量(KB)")
    
    # 错误和警告
    warnings = models.JSONField(default=list, blank=True, help_text="警告信息")
    errors = models.JSONField(default=list, blank=True, help_text="错误信息")
    
    # 时间戳
    timestamp = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'tubewhale_execution_logs'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['job', 'timestamp']),
            models.Index(fields=['step_name', 'timestamp']),
        ]
    
    def __str__(self):
        return f"{self.job.job_id} - {self.step_name} ({self.timestamp})"


class PerformanceMetrics(models.Model):
    """系统性能监控指标"""
    
    job = models.ForeignKey(AnalysisJob, on_delete=models.CASCADE, related_name='performance_metrics')
    
    # 系统资源
    cpu_percent = models.FloatField(help_text="CPU使用百分比")
    memory_percent = models.FloatField(help_text="内存使用百分比")
    disk_io_read_mb = models.FloatField(default=0, help_text="磁盘读取量(MB)")
    disk_io_write_mb = models.FloatField(default=0, help_text="磁盘写入量(MB)")
    
    # 网络资源
    network_sent_kb = models.FloatField(default=0, help_text="网络发送量(KB)")
    network_recv_kb = models.FloatField(default=0, help_text="网络接收量(KB)")
    
    # 任务特定指标
    api_response_time_ms = models.IntegerField(null=True, blank=True, help_text="API响应时间(毫秒)")
    queue_wait_time_ms = models.IntegerField(null=True, blank=True, help_text="队列等待时间(毫秒)")
    
    # 时间戳
    timestamp = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'tubewhale_performance_metrics'
        ordering = ['-timestamp']
    
    def __str__(self):
        return f"{self.job.job_id} - Metrics ({self.timestamp})"


class AnalysisResult(models.Model):
    """Store detailed analysis results with downloadable content"""
    
    # Link to analysis job
    job = models.OneToOneField(AnalysisJob, on_delete=models.CASCADE, related_name='result')
    
    # Analysis results
    raw_data = models.JSONField(default=dict, help_text="Raw analysis data")
    insights = models.JSONField(default=dict, help_text="Processed insights and recommendations")
    metrics = models.JSONField(default=dict, help_text="Performance metrics and scores")
    
    # Report content
    summary = models.TextField(blank=True, help_text="Executive summary")
    detailed_analysis = models.TextField(blank=True, help_text="Detailed analysis content")
    recommendations = models.TextField(blank=True, help_text="Actionable recommendations")
    
    # Media and attachments
    charts_data = models.JSONField(default=dict, blank=True, help_text="Chart and visualization data")
    screenshots = models.JSONField(default=list, blank=True, help_text="Screenshot URLs")
    
    # Quality scores
    confidence_score = models.FloatField(default=0.0, help_text="Analysis confidence (0-1)")
    completeness_score = models.FloatField(default=0.0, help_text="Data completeness (0-1)")
    
    # Export and download tracking
    download_count = models.IntegerField(default=0)
    last_downloaded_at = models.DateTimeField(null=True, blank=True)
    available_formats = models.JSONField(default=list, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'tubewhale_analysis_results'
        
    def get_formatted_result(self, format_type='json'):
        """Get analysis result in specified format"""
        
        if format_type == 'json':
            return {
                'job_id': self.job.job_id,
                'analysis_type': self.job.analysis_type,
                'expert_role': self.job.expert_role,
                'template_id': self.job.template_id,
                'content_info': {
                    'id': self.job.content_id,
                    'title': self.job.content_title,
                    'url': self.job.content_url,
                },
                'results': {
                    'summary': self.summary,
                    'raw_data': self.raw_data,
                    'insights': self.insights,
                    'metrics': self.metrics,
                    'recommendations': self.recommendations,
                },
                'metadata': {
                    'confidence_score': self.confidence_score,
                    'completeness_score': self.completeness_score,
                    'processing_time': self.job.processing_time_seconds,
                    'created_at': self.created_at.isoformat(),
                    'completed_at': self.job.completed_at.isoformat() if self.job.completed_at else None,
                }
            }
        
        elif format_type == 'markdown':
            return self._generate_markdown_report()
            
        elif format_type == 'html':
            return self._generate_html_report()
            
        return self.raw_data
    
    def increment_download_count(self):
        """Increment download counter and update timestamp"""
        from django.utils import timezone
        self.download_count += 1
        self.last_downloaded_at = timezone.now()
        self.save(update_fields=['download_count', 'last_downloaded_at'])
    
    def __str__(self):
        return f"Result for {self.job.job_id}"
