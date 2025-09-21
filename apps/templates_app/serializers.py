from rest_framework import serializers
from .models import CustomTemplate, Job


class CustomTemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomTemplate
        fields = (
            "id",
            "template_id",
            "name",
            "domain",
            "description",
            "prompt",
            "parameters",
            "tags",
            "template_type",
            "version",
            "immutable",
            "created_at",
            "updated_at",
        )
        read_only_fields = ("id", "created_at", "updated_at")


class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = (
            'id', 'command', 'template_id', 'expert_slug', 'variables',
            'status', 'progress', 'error', 'celery_task_id', 'execution_id',
            'created_at', 'updated_at', 'finished_at'
        )
        read_only_fields = (
            'id', 'status', 'progress', 'error', 'celery_task_id', 'execution_id', 'created_at', 'updated_at', 'finished_at'
        )
