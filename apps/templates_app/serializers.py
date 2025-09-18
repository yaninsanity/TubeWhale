from rest_framework import serializers
from .models import CustomTemplate


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
