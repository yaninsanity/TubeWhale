from django.contrib import admin
from .models import CustomTemplate


@admin.register(CustomTemplate)
class CustomTemplateAdmin(admin.ModelAdmin):
    list_display = ("template_id", "name", "domain", "template_type", "version", "updated_at")
    list_filter = ("domain", "template_type")
    search_fields = ("template_id", "name", "description", "tags")
    ordering = ("template_id",)
