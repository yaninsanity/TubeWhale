"""
Video App Models
视频应用数据模型 - 视频管理、分析任务、评论标签
"""

import uuid
import os
from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.core.validators import URLValidator
from django.utils.translation import gettext_lazy as _

User = get_user_model()


class Video(models.Model):
    """YouTube视频模型"""
    
    STATUS_CHOICES = [
        ('pending', _('Pending')),
        ('downloading', _('Downloading')),
        ('downloaded', _('Downloaded')),
        ('analyzing', _('Analyzing')),
        ('completed', _('Completed')),
        ('failed', _('Failed')),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='videos', verbose_name=_('User'))
    
    # YouTube视频基础信息
    youtube_id = models.CharField(max_length=50, unique=True, help_text="YouTube视频ID")
    title = models.CharField(max_length=500, help_text="视频标题")
    description = models.TextField(blank=True, help_text="视频描述")
    url = models.URLField(help_text="YouTube视频URL")
    
    # 视频元数据
    duration = models.IntegerField(null=True, blank=True, help_text="视频时长(秒)")
    view_count = models.BigIntegerField(null=True, blank=True, help_text="观看次数")
    like_count = models.BigIntegerField(null=True, blank=True, help_text="点赞数")
    comment_count = models.BigIntegerField(null=True, blank=True, help_text="评论数")
    publish_date = models.DateTimeField(null=True, blank=True, help_text="发布时间")
    
    # 频道信息
    channel_name = models.CharField(max_length=200, blank=True, help_text="频道名称")
    channel_id = models.CharField(max_length=100, blank=True, help_text="频道ID")
    
    # 文件信息
    local_file_path = models.CharField(max_length=500, blank=True, help_text="本地文件路径")
    file_size = models.BigIntegerField(null=True, blank=True, help_text="文件大小(字节)")
    
    # 状态和时间
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', help_text="处理状态")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    downloaded_at = models.DateTimeField(null=True, blank=True, verbose_name='下载时间')
    
    class Meta:
        db_table = 'videos'
        verbose_name = _("Video")
        verbose_name_plural = _("Video Management")
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['youtube_id']),
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['status']),
        ]
    
    def __str__(self):
        return f"{self.title} ({self.youtube_id})"
    
    @property
    def youtube_url(self):
        """生成YouTube URL"""
        return f"https://www.youtube.com/watch?v={self.youtube_id}"
    
    @property
    def file_exists(self):
        """检查本地文件是否存在"""
        if self.local_file_path:
            return os.path.exists(self.local_file_path)
        return False
    
    def get_analysis_tasks(self):
        """获取所有分析任务"""
        return self.analysis_tasks.all()
    
    def get_latest_analysis(self):
        """获取最新的分析任务"""
        return self.analysis_tasks.filter(status='completed').order_by('-completed_at').first()


class AnalysisTask(models.Model):
    """视频分析任务模型"""
    
    STATUS_CHOICES = [
        ('pending', '等待中'),
        ('processing', '处理中'),
        ('completed', '已完成'),
        ('failed', '失败'),
        ('cancelled', '已取消'),
    ]
    
    ANALYSIS_TYPE_CHOICES = [
        ('transcript', '转录分析'),
        ('audio', '音频分析'),
        ('search', '搜索分析'),
        ('summarize', '总结分析'),
        ('comprehensive', '综合分析'),
    ]
    
    EXPERT_DOMAIN_CHOICES = [
        ('general', '通用'),
        ('technology', '科技'),
        ('business', '商业'),
        ('education', '教育'),
        ('entertainment', '娱乐'),
        ('news', '新闻'),
        ('health', '健康'),
        ('finance', '金融'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name='analysis_tasks', verbose_name='视频')
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='analysis_tasks', verbose_name='用户')
    
    # 分析配置
    analysis_type = models.CharField(max_length=20, choices=ANALYSIS_TYPE_CHOICES, help_text="分析类型")
    expert_domain = models.CharField(max_length=20, choices=EXPERT_DOMAIN_CHOICES, default='general', help_text="专家领域")
    analysis_depth = models.IntegerField(default=3, help_text="分析深度(1-5)")
    
    # 任务状态
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', help_text="任务状态")
    progress = models.IntegerField(default=0, help_text="进度百分比")
    error_message = models.TextField(blank=True, help_text="错误信息")
    
    # 结果数据
    result_data = models.JSONField(blank=True, null=True, help_text="分析结果数据")
    confidence_score = models.FloatField(null=True, blank=True, help_text="置信度分数")
    
    # 时间记录
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    started_at = models.DateTimeField(null=True, blank=True, verbose_name='开始时间')
    completed_at = models.DateTimeField(null=True, blank=True, verbose_name='完成时间')
    
    # Celery任务ID
    celery_task_id = models.CharField(max_length=255, blank=True, help_text="Celery任务ID")
    
    class Meta:
        db_table = 'analysis_tasks'
        verbose_name = _("Analysis Task")
        verbose_name_plural = _("Analysis Task Management")
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['video', '-created_at']),
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['status']),
            models.Index(fields=['celery_task_id']),
        ]
    
    def __str__(self):
        return f"{self.video.title} - {self.get_analysis_type_display()}"
    
    def start_processing(self):
        """开始处理"""
        self.status = 'processing'
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at'])
    
    def complete_processing(self, result_data=None, confidence_score=None):
        """完成处理"""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.progress = 100
        if result_data:
            self.result_data = result_data
        if confidence_score:
            self.confidence_score = confidence_score
        self.save(update_fields=['status', 'completed_at', 'progress', 'result_data', 'confidence_score'])
    
    def fail_processing(self, error_message):
        """处理失败"""
        self.status = 'failed'
        self.error_message = error_message
        self.save(update_fields=['status', 'error_message'])


class VideoComment(models.Model):
    """视频评论模型"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name='comments', verbose_name='视频')
    
    # YouTube评论信息
    youtube_comment_id = models.CharField(max_length=100, unique=True, help_text="YouTube评论ID")
    author = models.CharField(max_length=200, help_text="评论作者")
    text = models.TextField(help_text="评论内容")
    like_count = models.IntegerField(default=0, help_text="点赞数")
    reply_count = models.IntegerField(default=0, help_text="回复数")
    
    # 时间信息
    published_at = models.DateTimeField(help_text="发布时间")
    collected_at = models.DateTimeField(auto_now_add=True, verbose_name='收集时间')
    
    # 分析结果
    sentiment_score = models.FloatField(null=True, blank=True, help_text="情感分数(-1到1)")
    is_spam = models.BooleanField(default=False, help_text="是否为垃圾评论")
    
    class Meta:
        db_table = 'video_comments'
        verbose_name = _("Video Comment")
        verbose_name_plural = _("Video Comment Management")
        ordering = ['-published_at']
        indexes = [
            models.Index(fields=['video', '-published_at']),
            models.Index(fields=['youtube_comment_id']),
        ]
    
    def __str__(self):
        return f"{self.author}: {self.text[:50]}..."


class VideoTag(models.Model):
    """视频标签模型"""
    
    TAG_TYPE_CHOICES = [
        ('category', '分类'),
        ('topic', '主题'),
        ('keyword', '关键词'),
        ('emotion', '情感'),
        ('quality', '质量'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name='tags', verbose_name='视频')
    
    # 标签信息
    name = models.CharField(max_length=100, help_text="标签名称")
    tag_type = models.CharField(max_length=20, choices=TAG_TYPE_CHOICES, help_text="标签类型")
    confidence = models.FloatField(default=0.0, help_text="置信度(0-1)")
    
    # 来源信息
    source = models.CharField(max_length=50, default='auto', help_text="标签来源(auto/manual)")
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, verbose_name='创建者')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    
    class Meta:
        db_table = 'video_tags'
        verbose_name = _("Video Tag")
        verbose_name_plural = _("Video Tag Management")
        unique_together = ['video', 'name', 'tag_type']
        indexes = [
            models.Index(fields=['video']),
            models.Index(fields=['name']),
            models.Index(fields=['tag_type']),
        ]
    
    def __str__(self):
        return f"{self.video.title} - {self.name} ({self.get_tag_type_display()})"
