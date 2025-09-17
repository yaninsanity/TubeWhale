"""
Video App Models
视频应用数据模型 - 视频、分析任务、评论、标签
"""

import uuid
from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.core.validators import MinValueValidator, MaxValueValidator

User = get_user_model()


class VideoTag(models.Model):
    """视频标签模型"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=50, unique=True, verbose_name='标签名称')
    description = models.TextField(blank=True, verbose_name='描述')
    color = models.CharField(max_length=7, default='#007bff', verbose_name='颜色')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        verbose_name = '视频标签'
        verbose_name_plural = '视频标签'
        ordering = ['name']
    
    def __str__(self):
        return self.name


class Video(models.Model):
    """视频模型"""
    
    STATUS_CHOICES = [
        ('pending', '待处理'),
        ('downloading', '下载中'),
        ('completed', '已完成'),
        ('failed', '失败'),
        ('processing', '处理中'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    youtube_id = models.CharField(max_length=20, unique=True, verbose_name='YouTube ID')
    youtube_url = models.URLField(verbose_name='YouTube URL')
    title = models.CharField(max_length=200, verbose_name='标题')
    description = models.TextField(blank=True, verbose_name='描述')
    channel_name = models.CharField(max_length=100, blank=True, verbose_name='频道名称')
    duration = models.PositiveIntegerField(null=True, blank=True, verbose_name='时长(秒)')
    view_count = models.PositiveIntegerField(default=0, verbose_name='观看次数')
    like_count = models.PositiveIntegerField(default=0, verbose_name='点赞数')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', verbose_name='状态')
    thumbnail_url = models.URLField(blank=True, verbose_name='缩略图URL')
    uploaded_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='videos', verbose_name='上传者')
    tags = models.ManyToManyField(VideoTag, blank=True, related_name='videos', verbose_name='标签')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        verbose_name = '视频'
        verbose_name_plural = '视频管理'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.title} ({self.get_status_display()})"
    
    def extract_youtube_id(self):
        """从URL提取YouTube ID"""
        import re
        patterns = [
            r'youtube\.com/watch\?v=([^&\n?#]+)',
            r'youtu\.be/([^&\n?#]+)',
            r'youtube\.com/embed/([^&\n?#]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, self.youtube_url)
            if match:
                return match.group(1)
        return None
    
    def save(self, *args, **kwargs):
        """保存时自动提取YouTube ID"""
        if not self.youtube_id and self.youtube_url:
            self.youtube_id = self.extract_youtube_id()
        super().save(*args, **kwargs)


class AnalysisTask(models.Model):
    """分析任务模型"""
    
    STATUS_CHOICES = [
        ('pending', '待处理'),
        ('processing', '处理中'),
        ('completed', '已完成'),
        ('failed', '失败'),
        ('cancelled', '已取消'),
    ]
    
    EXPERT_DOMAIN_CHOICES = [
        ('general', '通用分析'),
        ('technology', '技术'),
        ('business', '商业'),
        ('education', '教育'),
        ('entertainment', '娱乐'),
        ('health', '健康'),
        ('science', '科学'),
        ('politics', '政治'),
    ]
    
    ANALYSIS_DEPTH_CHOICES = [
        ('quick', '快速分析'),
        ('standard', '标准分析'),
        ('deep', '深度分析'),
        ('comprehensive', '全面分析'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name='analysis_tasks', verbose_name='视频')
    requested_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='analysis_tasks', verbose_name='请求者')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', verbose_name='状态')
    expert_domain = models.CharField(max_length=20, choices=EXPERT_DOMAIN_CHOICES, default='general', verbose_name='专家领域')
    analysis_depth = models.CharField(max_length=20, choices=ANALYSIS_DEPTH_CHOICES, default='standard', verbose_name='分析深度')
    progress = models.FloatField(default=0.0, validators=[MinValueValidator(0.0), MaxValueValidator(100.0)], verbose_name='进度')
    results = models.JSONField(default=dict, blank=True, verbose_name='分析结果')
    error_message = models.TextField(blank=True, verbose_name='错误信息')
    started_at = models.DateTimeField(null=True, blank=True, verbose_name='开始时间')
    completed_at = models.DateTimeField(null=True, blank=True, verbose_name='完成时间')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        verbose_name = '分析任务'
        verbose_name_plural = '分析任务管理'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"Analysis of {self.video.title} ({self.get_status_display()})"
    
    def get_duration_minutes(self):
        """获取分析持续时间（分钟）"""
        if self.started_at and self.completed_at:
            duration = self.completed_at - self.started_at
            return duration.total_seconds() / 60
        return None


class VideoComment(models.Model):
    """视频评论模型"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name='comments', verbose_name='视频')
    text = models.TextField(verbose_name='评论内容')
    author = models.CharField(max_length=100, verbose_name='作者')
    like_count = models.PositiveIntegerField(default=0, verbose_name='点赞数')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    
    class Meta:
        verbose_name = '视频评论'
        verbose_name_plural = '视频评论'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"Comment by {self.author} on {self.video.title}"
