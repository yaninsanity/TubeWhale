"""
Analysis App Models
分析应用数据模型 - 分析报告、指标、专家领域
"""

import uuid
from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

User = get_user_model()


class ExpertDomain(models.Model):
    """专家领域模型"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # 基本信息
    name = models.CharField(max_length=100, unique=True, help_text="领域名称")
    code = models.CharField(max_length=20, unique=True, help_text="领域代码")
    description = models.TextField(blank=True, help_text="领域描述")
    
    # 配置信息
    is_active = models.BooleanField(default=True, help_text="是否启用")
    prompt_template = models.TextField(blank=True, help_text="提示词模板")
    analysis_keywords = models.JSONField(default=list, help_text="分析关键词列表")
    
    # 权重和优先级
    priority = models.IntegerField(default=0, help_text="优先级(数值越大优先级越高)")
    confidence_threshold = models.FloatField(default=0.7, help_text="置信度阈值")
    
    # 时间信息
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        db_table = 'expert_domains'
        verbose_name = _("Expert Domain")
        verbose_name_plural = _("Expert Domain Management")
        ordering = ['-priority', 'name']
    
    def __str__(self):
        return f"{self.name} ({self.code})"


class AnalysisReport(models.Model):
    """分析报告模型"""
    
    REPORT_TYPE_CHOICES = [
        ('transcript', '转录报告'),
        ('audio', '音频分析报告'),
        ('search', '搜索分析报告'),
        ('summary', '总结报告'),
        ('comprehensive', '综合分析报告'),
    ]
    
    STATUS_CHOICES = [
        ('draft', '草稿'),
        ('processing', '处理中'),
        ('completed', '已完成'),
        ('reviewed', '已审核'),
        ('published', '已发布'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # 关联信息
    video = models.ForeignKey('video_app.Video', on_delete=models.CASCADE, related_name='analysis_reports', verbose_name='视频')
    analysis_task = models.OneToOneField('video_app.AnalysisTask', on_delete=models.CASCADE, related_name='report', verbose_name='分析任务')
    expert_domain = models.ForeignKey(ExpertDomain, on_delete=models.SET_NULL, null=True, verbose_name='专家领域')
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='analysis_reports', verbose_name='创建者')
    
    # 报告内容
    title = models.CharField(max_length=500, help_text="报告标题")
    report_type = models.CharField(max_length=20, choices=REPORT_TYPE_CHOICES, help_text="报告类型")
    executive_summary = models.TextField(help_text="执行摘要")
    content = models.TextField(help_text="报告正文")
    conclusions = models.TextField(blank=True, help_text="结论")
    recommendations = models.TextField(blank=True, help_text="建议")
    
    # 元数据
    word_count = models.IntegerField(default=0, help_text="字数统计")
    reading_time = models.IntegerField(default=0, help_text="预计阅读时间(分钟)")
    tags = models.JSONField(default=list, help_text="标签列表")
    
    # 质量和评估
    confidence_score = models.FloatField(default=0.0, help_text="整体置信度(0-1)")
    quality_score = models.FloatField(default=0.0, help_text="质量评分(0-1)")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft', help_text="报告状态")
    
    # 时间信息
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    published_at = models.DateTimeField(null=True, blank=True, verbose_name='发布时间')
    
    class Meta:
        db_table = 'analysis_reports'
        verbose_name = _("Analysis Report")
        verbose_name_plural = _("Analysis Report Management")
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['video', '-created_at']),
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['report_type']),
            models.Index(fields=['status']),
        ]
    
    def __str__(self):
        return f"{self.title} ({self.get_report_type_display()})"
    
    def calculate_reading_time(self):
        """计算阅读时间(基于250字/分钟)"""
        if self.word_count > 0:
            self.reading_time = max(1, self.word_count // 250)
        else:
            # 简单估算中文字数
            total_chars = len(self.content) + len(self.executive_summary)
            self.reading_time = max(1, total_chars // 300)
        return self.reading_time
    
    def publish(self):
        """发布报告"""
        self.status = 'published'
        self.published_at = timezone.now()
        self.save(update_fields=['status', 'published_at'])


class AnalysisMetrics(models.Model):
    """分析指标模型"""
    
    METRIC_TYPE_CHOICES = [
        ('accuracy', '准确度'),
        ('relevance', '相关性'),
        ('completeness', '完整性'),
        ('clarity', '清晰度'),
        ('objectivity', '客观性'),
        ('depth', '深度'),
        ('insight', '洞察力'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # 关联信息
    report = models.ForeignKey(AnalysisReport, on_delete=models.CASCADE, related_name='metrics', verbose_name='分析报告')
    
    # 指标信息
    metric_type = models.CharField(max_length=20, choices=METRIC_TYPE_CHOICES, help_text="指标类型")
    name = models.CharField(max_length=100, help_text="指标名称")
    description = models.TextField(blank=True, help_text="指标描述")
    
    # 数值信息
    value = models.FloatField(help_text="指标值")
    min_value = models.FloatField(default=0.0, help_text="最小值")
    max_value = models.FloatField(default=1.0, help_text="最大值")
    unit = models.CharField(max_length=20, blank=True, help_text="单位")
    
    # 计算信息
    calculation_method = models.CharField(max_length=100, blank=True, help_text="计算方法")
    data_source = models.CharField(max_length=100, blank=True, help_text="数据来源")
    
    # 时间信息
    measured_at = models.DateTimeField(default=timezone.now, verbose_name='测量时间')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    
    class Meta:
        db_table = 'analysis_metrics'
        verbose_name = _("Analysis Metric")
        verbose_name_plural = _("Analysis Metric Management")
        unique_together = ['report', 'metric_type', 'name']
        ordering = ['-measured_at']
        indexes = [
            models.Index(fields=['report', 'metric_type']),
            models.Index(fields=['metric_type', 'value']),
        ]
    
    def __str__(self):
        return f"{self.report.title} - {self.name}: {self.value}"
    
    @property
    def normalized_value(self):
        """获取标准化值(0-1)"""
        if self.max_value > self.min_value:
            return (self.value - self.min_value) / (self.max_value - self.min_value)
        return 0.0
    
    @property
    def percentage_value(self):
        """获取百分比值"""
        return self.normalized_value * 100


class AnalysisInsight(models.Model):
    """分析洞察模型"""
    
    INSIGHT_TYPE_CHOICES = [
        ('trend', '趋势'),
        ('pattern', '模式'),
        ('anomaly', '异常'),
        ('correlation', '关联'),
        ('prediction', '预测'),
        ('recommendation', '建议'),
    ]
    
    IMPORTANCE_CHOICES = [
        ('low', '低'),
        ('medium', '中'),
        ('high', '高'),
        ('critical', '关键'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # 关联信息
    report = models.ForeignKey(AnalysisReport, on_delete=models.CASCADE, related_name='insights', verbose_name='分析报告')
    
    # 洞察内容
    title = models.CharField(max_length=200, help_text="洞察标题")
    description = models.TextField(help_text="洞察描述")
    insight_type = models.CharField(max_length=20, choices=INSIGHT_TYPE_CHOICES, help_text="洞察类型")
    importance = models.CharField(max_length=20, choices=IMPORTANCE_CHOICES, default='medium', help_text="重要性")
    
    # 支撑数据
    supporting_data = models.JSONField(default=dict, help_text="支撑数据")
    confidence_level = models.FloatField(default=0.5, help_text="置信水平(0-1)")
    evidence_count = models.IntegerField(default=0, help_text="证据数量")
    
    # 标签和分类
    tags = models.JSONField(default=list, help_text="标签列表")
    category = models.CharField(max_length=100, blank=True, help_text="分类")
    
    # 行动项
    actionable = models.BooleanField(default=False, help_text="是否可执行")
    action_items = models.TextField(blank=True, help_text="行动项")
    
    # 时间信息
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        db_table = 'analysis_insights'
        verbose_name = _("Analysis Insight")
        verbose_name_plural = _("Analysis Insight Management")
        ordering = ['-importance', '-confidence_level', '-created_at']
        indexes = [
            models.Index(fields=['report', '-importance']),
            models.Index(fields=['insight_type', '-confidence_level']),
        ]
    
    def __str__(self):
        return f"{self.report.title} - {self.title}"
