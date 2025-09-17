"""
TubeWhale Engine API Serializers
"""

from rest_framework import serializers
from .models import ProcessingJob, VideoProcessingResult, TubeWhaleEngineConfig


class ProcessingJobSerializer(serializers.ModelSerializer):
    """处理任务序列化器"""
    
    duration = serializers.ReadOnlyField()
    is_completed = serializers.ReadOnlyField()
    
    class Meta:
        model = ProcessingJob
        fields = [
            'id', 'video_id', 'task_type', 'status', 'options',
            'created_at', 'started_at', 'completed_at',
            'progress_percentage', 'current_step', 'error_message',
            'duration', 'is_completed'
        ]
        read_only_fields = [
            'id', 'status', 'created_at', 'started_at', 'completed_at',
            'progress_percentage', 'current_step', 'error_message', 'result'
        ]


class CreateProcessingJobSerializer(serializers.Serializer):
    """创建处理任务序列化器"""
    
    video_id = serializers.CharField(
        max_length=11, 
        min_length=11,
        help_text="YouTube视频ID（11位字符）"
    )
    task_type = serializers.ChoiceField(
        choices=ProcessingJob.TASK_TYPE_CHOICES,
        default='full',
        help_text="任务类型"
    )
    options = serializers.JSONField(
        default=dict,
        required=False,
        help_text="处理选项"
    )
    
    def validate_video_id(self, value):
        """验证YouTube视频ID格式"""
        if not value.isalnum():
            raise serializers.ValidationError("YouTube视频ID必须是11位字母数字字符")
        return value


class BatchProcessingJobSerializer(serializers.Serializer):
    """批量处理任务序列化器"""
    
    video_ids = serializers.ListField(
        child=serializers.CharField(max_length=11, min_length=11),
        min_length=1,
        max_length=10,  # 限制批量数量
        help_text="YouTube视频ID列表（最多10个）"
    )
    task_type = serializers.ChoiceField(
        choices=ProcessingJob.TASK_TYPE_CHOICES,
        default='full'
    )
    options = serializers.JSONField(default=dict, required=False)
    
    def validate_video_ids(self, value):
        """验证视频ID列表"""
        for video_id in value:
            if not video_id.isalnum():
                raise serializers.ValidationError(f"无效的视频ID: {video_id}")
        
        # 检查重复
        if len(set(value)) != len(value):
            raise serializers.ValidationError("视频ID列表包含重复项")
        
        return value


class VideoProcessingResultSerializer(serializers.ModelSerializer):
    """视频处理结果序列化器"""
    
    job_id = serializers.CharField(source='job.id', read_only=True)
    job_status = serializers.CharField(source='job.status', read_only=True)
    
    class Meta:
        model = VideoProcessingResult
        fields = [
            'job_id', 'job_status', 'video_id', 'video_title', 'video_description',
            'video_duration', 'video_thumbnail_url', 'channel_name',
            'transcript_text', 'audio_summary', 'generated_summary',
            'key_topics', 'sentiment_analysis', 'language_detected',
            'related_videos', 'processing_metadata',
            'created_at', 'updated_at'
        ]


class TubeWhaleEngineConfigSerializer(serializers.ModelSerializer):
    """TubeWhale引擎配置序列化器"""
    
    class Meta:
        model = TubeWhaleEngineConfig
        fields = [
            'openai_api_key', 'youtube_api_key', 'default_language',
            'auto_summarize', 'audio_quality', 'email_notifications',
            'webhook_url', 'created_at', 'updated_at'
        ]
        extra_kwargs = {
            'openai_api_key': {'write_only': True},
            'youtube_api_key': {'write_only': True},
        }


class JobStatusSerializer(serializers.Serializer):
    """任务状态查询结果序列化器"""
    
    id = serializers.CharField()
    video_id = serializers.CharField()
    status = serializers.CharField()
    progress_percentage = serializers.IntegerField()
    current_step = serializers.CharField()
    error_message = serializers.CharField(allow_blank=True)
    created_at = serializers.DateTimeField()
    started_at = serializers.DateTimeField(allow_null=True)
    completed_at = serializers.DateTimeField(allow_null=True)
    duration = serializers.CharField(allow_null=True)
