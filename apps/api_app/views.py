"""
API App Views
REST API视图集
"""

from rest_framework import viewsets, status, permissions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from django.contrib.auth import get_user_model

from .serializers import (
    UserSerializer, APIKeySerializer, APIKeyCreateSerializer,
    VideoSerializer, VideoCreateSerializer, AnalysisTaskSerializer, AnalysisTaskCreateSerializer,
    ExpertDomainSerializer, AnalysisReportSerializer, AnalysisMetricsSerializer,
    AnalysisInsightSerializer, VideoCommentSerializer, VideoTagSerializer, APIUsageLogSerializer
)
from .permissions import (
    IsOwner, IsOwnerOrReadOnly, APIKeyPermission, VideoOwnerPermission,
    AnalysisTaskPermission, ReportPermission, AdminOrReadOnly, APIKeyOwnerPermission
)
from apps.user_app.models import APIKey, APIUsageLog
from apps.video_app.models import Video, AnalysisTask, VideoComment, VideoTag
from apps.analysis_app.models import ExpertDomain, AnalysisReport, AnalysisMetrics, AnalysisInsight

User = get_user_model()


class UserViewSet(viewsets.ModelViewSet):
    """用户管理API"""
    
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    search_fields = ['username', 'email', 'first_name', 'last_name']
    ordering_fields = ['date_joined', 'last_activity']
    ordering = ['-date_joined']
    
    def get_queryset(self):
        # 只能查看自己的信息
        return User.objects.filter(id=self.request.user.id)
    
    @action(detail=False, methods=['get'], url_path='me')
    def get_current_user(self, request):
        """获取当前用户信息"""
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)
    
    @action(detail=False, methods=['post'], url_path='reset-daily-calls')
    def reset_daily_calls(self, request):
        """重置每日API调用计数"""
        request.user.reset_daily_api_calls()
        return Response({'message': 'Daily API calls reset successfully'})


class APIKeyViewSet(viewsets.ModelViewSet):
    """API密钥管理API"""
    
    permission_classes = [IsAuthenticated, APIKeyOwnerPermission]
    filter_backends = [DjangoFilterBackend, OrderingFilter]
    filterset_fields = ['is_active']
    ordering_fields = ['created_at', 'last_used']
    ordering = ['-created_at']
    
    def get_queryset(self):
        return APIKey.objects.filter(user=self.request.user)
    
    def get_serializer_class(self):
        if self.action == 'create':
            return APIKeyCreateSerializer
        return APIKeySerializer
    
    def create(self, request, *args, **kwargs):
        """创建API密钥"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        api_key = serializer.save()
        
        # 返回完整的API Key（仅在创建时）
        response_data = APIKeySerializer(api_key).data
        response_data['full_key'] = api_key.key  # 只在创建时返回完整密钥
        
        return Response(response_data, status=status.HTTP_201_CREATED)
    
    @action(detail=True, methods=['post'], url_path='regenerate')
    def regenerate_key(self, request, pk=None):
        """重新生成API密钥"""
        api_key = self.get_object()
        api_key.key = ''  # 清空现有密钥
        api_key.save()  # 保存时会自动生成新密钥
        
        response_data = APIKeySerializer(api_key).data
        response_data['full_key'] = api_key.key
        
        return Response(response_data)


class VideoViewSet(viewsets.ModelViewSet):
    """视频管理API"""
    
    permission_classes = [IsAuthenticated, IsOwner]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['status', 'channel_name']
    search_fields = ['title', 'youtube_id', 'channel_name']
    ordering_fields = ['created_at', 'view_count', 'duration']
    ordering = ['-created_at']
    
    def get_queryset(self):
        return Video.objects.filter(user=self.request.user)
    
    def get_serializer_class(self):
        if self.action == 'create':
            return VideoCreateSerializer
        return VideoSerializer
    
    @action(detail=True, methods=['get'], url_path='analysis-tasks')
    def get_analysis_tasks(self, request, pk=None):
        """获取视频的分析任务"""
        video = self.get_object()
        tasks = video.analysis_tasks.all()
        serializer = AnalysisTaskSerializer(tasks, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['get'], url_path='comments')
    def get_comments(self, request, pk=None):
        """获取视频评论"""
        video = self.get_object()
        comments = video.comments.all()[:100]  # 限制返回数量
        serializer = VideoCommentSerializer(comments, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['get'], url_path='tags')
    def get_tags(self, request, pk=None):
        """获取视频标签"""
        video = self.get_object()
        tags = video.tags.all()
        serializer = VideoTagSerializer(tags, many=True)
        return Response(serializer.data)


class AnalysisTaskViewSet(viewsets.ModelViewSet):
    """分析任务API"""
    
    permission_classes = [IsAuthenticated, AnalysisTaskPermission]
    filter_backends = [DjangoFilterBackend, OrderingFilter]
    filterset_fields = ['status', 'analysis_type', 'expert_domain']
    ordering_fields = ['created_at', 'started_at', 'completed_at']
    ordering = ['-created_at']
    
    def get_queryset(self):
        return AnalysisTask.objects.filter(user=self.request.user).select_related('video')
    
    def get_serializer_class(self):
        if self.action == 'create':
            return AnalysisTaskCreateSerializer
        return AnalysisTaskSerializer
    
    @action(detail=True, methods=['post'], url_path='cancel')
    def cancel_task(self, request, pk=None):
        """取消分析任务"""
        task = self.get_object()
        if task.status in ['pending', 'processing']:
            task.status = 'cancelled'
            task.save()
            return Response({'message': 'Task cancelled successfully'})
        else:
            return Response(
                {'error': 'Cannot cancel task in current status'},
                status=status.HTTP_400_BAD_REQUEST
            )


class ExpertDomainViewSet(viewsets.ReadOnlyModelViewSet):
    """专家领域API（只读）"""
    
    queryset = ExpertDomain.objects.filter(is_active=True)
    serializer_class = ExpertDomainSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [OrderingFilter]
    ordering = ['-priority', 'name']


class AnalysisReportViewSet(viewsets.ReadOnlyModelViewSet):
    """分析报告API"""
    
    permission_classes = [IsAuthenticated, ReportPermission]
    serializer_class = AnalysisReportSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['report_type', 'status', 'expert_domain']
    search_fields = ['title', 'content']
    ordering_fields = ['created_at', 'confidence_score', 'quality_score']
    ordering = ['-created_at']
    
    def get_queryset(self):
        return AnalysisReport.objects.filter(user=self.request.user).select_related(
            'video', 'expert_domain', 'analysis_task'
        )
    
    @action(detail=True, methods=['get'], url_path='metrics')
    def get_metrics(self, request, pk=None):
        """获取报告指标"""
        report = self.get_object()
        metrics = report.metrics.all()
        serializer = AnalysisMetricsSerializer(metrics, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['get'], url_path='insights')
    def get_insights(self, request, pk=None):
        """获取报告洞察"""
        report = self.get_object()
        insights = report.insights.all()
        serializer = AnalysisInsightSerializer(insights, many=True)
        return Response(serializer.data)


class APIUsageLogViewSet(viewsets.ReadOnlyModelViewSet):
    """API使用日志API（只读）"""
    
    permission_classes = [IsAuthenticated]
    serializer_class = APIUsageLogSerializer
    filter_backends = [DjangoFilterBackend, OrderingFilter]
    filterset_fields = ['method', 'status_code', 'api_key__name']
    ordering_fields = ['timestamp', 'response_time']
    ordering = ['-timestamp']
    
    def get_queryset(self):
        # 只能查看自己的API密钥的使用日志
        return APIUsageLog.objects.filter(
            api_key__user=self.request.user
        ).select_related('api_key')[:1000]  # 限制返回数量
