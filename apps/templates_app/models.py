from django.db import models


class TemplateDomain(models.TextChoices):
    GENERAL = "general", "General"
    BUSINESS = "business", "Business Intelligence"
    MEDICAL = "medical", "Medical Research"
    TECHNOLOGY = "technology", "Technology Innovation"
    ACADEMIC = "academic", "Academic Research"


class TemplateType(models.TextChoices):
    CORE = "core", "Core"
    DOMAIN = "domain", "Domain"
    CUSTOM = "custom", "Custom"


class CustomTemplate(models.Model):
    template_id = models.SlugField(max_length=128, unique=True)
    name = models.CharField(max_length=255)
    domain = models.CharField(max_length=32, choices=TemplateDomain.choices, default=TemplateDomain.GENERAL)
    description = models.TextField(blank=True, default="")
    prompt = models.TextField()
    parameters = models.JSONField(default=dict, blank=True)
    tags = models.JSONField(default=list, blank=True)
    template_type = models.CharField(max_length=16, choices=TemplateType.choices, default=TemplateType.CUSTOM)
    version = models.CharField(max_length=16, default="1.0.0")
    immutable = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "templates_custom_template"
        ordering = ["template_id"]

    def __str__(self) -> str:
        return f"{self.template_id} ({self.domain})"
