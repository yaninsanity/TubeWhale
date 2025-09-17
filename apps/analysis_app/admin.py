"""
Analysis App Admin Configuration
Enhanced admin interface with English labels
"""

from django.contrib import admin
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from .models import ExpertDomain, AnalysisReport, AnalysisMetrics, AnalysisInsight


@admin.register(ExpertDomain)
class ExpertDomainAdmin(admin.ModelAdmin):
    """Expert Domain Management"""
    
    list_display = ('name', 'code', 'is_active', 'priority', 'confidence_threshold', 'created_at')
    list_filter = ('is_active', 'created_at')
    search_fields = ('name', 'code', 'description')
    ordering = ['-priority', 'name']
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('name', 'code', 'description', 'is_active')
        }),
        (_('Configuration'), {
            'fields': ('prompt_template', 'analysis_keywords'),
            'classes': ('collapse',)
        }),
        (_('Weight Settings'), {
            'fields': ('priority', 'confidence_threshold')
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
        }),
    )
    
    readonly_fields = ('created_at', 'updated_at')


@admin.register(AnalysisReport)
class AnalysisReportAdmin(admin.ModelAdmin):
    """Analysis Report Management"""
    
    list_display = ('title', 'report_type', 'video_title', 'expert_domain', 'status', 'confidence_score', 'created_at')
    list_filter = ('report_type', 'status', 'expert_domain', 'created_at')
    search_fields = ('title', 'video__title', 'content')
    ordering = ('-created_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('video', 'analysis_task', 'expert_domain', 'user', 'title', 'report_type')
        }),
        (_('Report Content'), {
            'fields': ('executive_summary', 'content', 'conclusions', 'recommendations')
        }),
        (_('Metadata'), {
            'fields': ('word_count', 'reading_time', 'tags'),
            'classes': ('collapse',)
        }),
        (_('Quality Assessment'), {
            'fields': ('confidence_score', 'quality_score', 'status')
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at', 'published_at'),
        }),
    )
    
    readonly_fields = ('created_at', 'updated_at', 'published_at')
    
    def video_title(self, obj):
        """Display video title"""
        return obj.video.title
    video_title.short_description = _("Video Title")
    video_title.admin_order_field = 'video__title'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('video', 'expert_domain', 'user', 'analysis_task')


class AnalysisMetricsInline(admin.TabularInline):
    """Analysis Metrics Inline"""
    model = AnalysisMetrics
    extra = 0
    readonly_fields = ('measured_at', 'created_at')


class AnalysisInsightInline(admin.TabularInline):
    """Analysis Insight Inline"""
    model = AnalysisInsight
    extra = 0
    readonly_fields = ('created_at', 'updated_at')


# Add inlines to AnalysisReport admin
AnalysisReportAdmin.inlines = [AnalysisMetricsInline, AnalysisInsightInline]


@admin.register(AnalysisMetrics)
class AnalysisMetricsAdmin(admin.ModelAdmin):
    """Analysis Metrics Management"""
    
    list_display = ('report_title', 'metric_type', 'name', 'value', 'unit', 'normalized_value_display', 'measured_at')
    list_filter = ('metric_type', 'measured_at')
    search_fields = ('name', 'report__title')
    ordering = ('-measured_at',)
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('report', 'metric_type', 'name', 'description')
        }),
        (_('Value Information'), {
            'fields': ('value', 'min_value', 'max_value', 'unit')
        }),
        (_('Calculation Information'), {
            'fields': ('calculation_method', 'data_source'),
            'classes': ('collapse',)
        }),
        (_('Timestamps'), {
            'fields': ('measured_at', 'created_at'),
        }),
    )
    
    readonly_fields = ('created_at',)
    
    def report_title(self, obj):
        """Display report title"""
        return obj.report.title
    report_title.short_description = _("Report Title")
    report_title.admin_order_field = 'report__title'
    
    def normalized_value_display(self, obj):
        """Display normalized value"""
        return f"{obj.normalized_value:.2%}"
    normalized_value_display.short_description = _("Normalized Value")
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('report')


@admin.register(AnalysisInsight)
class AnalysisInsightAdmin(admin.ModelAdmin):
    """Analysis Insight Management"""
    
    list_display = ('title', 'report_title', 'insight_type', 'importance', 'confidence_level', 'actionable', 'created_at')
    list_filter = ('insight_type', 'importance', 'actionable', 'created_at')
    search_fields = ('title', 'description', 'report__title')
    ordering = ('-importance', '-confidence_level', '-created_at')
    
    fieldsets = (
        (_('Basic Information'), {
            'fields': ('report', 'title', 'description', 'insight_type', 'importance')
        }),
        (_('Supporting Data'), {
            'fields': ('supporting_data', 'confidence_level', 'evidence_count'),
            'classes': ('collapse',)
        }),
        (_('Classification'), {
            'fields': ('tags', 'category')
        }),
        (_('Action Items'), {
            'fields': ('actionable', 'action_items')
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
        }),
    )
    
    readonly_fields = ('created_at', 'updated_at')
    
    def report_title(self, obj):
        """Display report title"""
        return obj.report.title
    report_title.short_description = _("Report Title")
    report_title.admin_order_field = 'report__title'
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('report')
