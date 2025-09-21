from django.db import models
from django.utils import timezone
from .tier_models import UserProfile, UserTier, TemplateUsage
from django.contrib.auth import get_user_model
User = get_user_model()


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


class ExpertRole(models.TextChoices):
    GENERAL_ANALYST = "general_analyst", "General Analyst"
    BUSINESS_STRATEGIST = "business_strategist", "Business Strategist"
    MEDICAL_RESEARCHER = "medical_researcher", "Medical Researcher"
    TECHNOLOGY_ARCHITECT = "technology_architect", "Technology Architect"
    ACADEMIC_SCHOLAR = "academic_scholar", "Academic Scholar"
    INVESTOR = "investor", "Investor / Due Diligence"
    POLICY_ANALYST = "policy_analyst", "Policy Analyst"


class ExpertPrompt(models.Model):
    """Domain/expert specific prompt customization layer.

    This allows layering an expert perspective over a base template.
    Resolution precedence (future use):
        1. User-specific override (not yet implemented)
        2. ExpertPrompt (domain + role)
        3. Base template prompt
    """
    slug = models.SlugField(max_length=128, unique=True)
    role = models.CharField(max_length=64, choices=ExpertRole.choices)
    domain = models.CharField(max_length=32, choices=TemplateDomain.choices, default=TemplateDomain.GENERAL)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    prompt_intro = models.TextField(help_text="Optional preface injected before base template prompt", blank=True, default="")
    prompt_outro = models.TextField(help_text="Optional suffix appended after base template prompt", blank=True, default="")
    active = models.BooleanField(default=True)
    weight = models.PositiveIntegerField(default=100, help_text="Ordering / priority (lower = earlier)")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "templates_expert_prompt"
        ordering = ["weight", "role", "domain"]
        indexes = [
            models.Index(fields=["role", "domain", "active"]),
        ]

    def __str__(self) -> str:  # pragma: no cover - simple repr
        return f"{self.slug} ({self.role}/{self.domain})"

    def apply_to(self, base_prompt: str) -> str:
        """Compose final prompt with intro/outro if enabled."""
        parts = []
        if self.prompt_intro.strip():
            parts.append(self.prompt_intro.strip())
        parts.append(base_prompt)
        if self.prompt_outro.strip():
            parts.append(self.prompt_outro.strip())
        return "\n\n".join(parts)


class CLIExecution(models.Model):
    """Persisted record of a CLI tool execution.

    This enables downstream export, analytics, and graph construction. Keeping
    it in templates_app (rather than a new app) keeps related prompt + template
    context colocated while scope remains small.
    """
    STATUS_CHOICES = [
        ("success", "Success"),
        ("error", "Error"),
        ("timeout", "Timeout"),
        ("missing_entry", "Missing Entry"),
    ]
    command = models.CharField(max_length=128)
    args = models.JSONField(default=list, blank=True)
    template_id = models.CharField(max_length=128, blank=True, default="")
    expert_slug = models.CharField(max_length=128, blank=True, default="")
    prompt_final = models.TextField(blank=True, default="")
    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES)
    returncode = models.IntegerField(null=True, blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)
    meta = models.JSONField(default=dict, blank=True, help_text="Arbitrary execution metadata (engine status, user id, etc.)")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "templates_cli_execution"
        indexes = [
            models.Index(fields=["command", "created_at"]),
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["template_id", "expert_slug"]),
        ]
        ordering = ["-created_at"]

    def __str__(self):  # pragma: no cover - repr only
        return f"CLIExecution({self.command} status={self.status} id={self.id})"


# ================= User Template Selection (Tier-based access locking) =================

class UserTemplateSelection(models.Model):
    """Represents a *locked* template a BASIC tier user has chosen.

    For PRO/ENTERPRISE we generally do not persist selections (they have global access)
    but future preference tracking could reuse this table with active=False / tag markers.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="template_selections")
    template_id = models.CharField(max_length=128)
    template_name = models.CharField(max_length=255, blank=True, default="")
    template_type = models.CharField(max_length=32, blank=True, default="")
    active = models.BooleanField(default=True)
    locked_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "templates_user_template_selection"
        unique_together = ["user", "template_id"]
        indexes = [
            models.Index(fields=["user", "active"]),
        ]
        ordering = ["locked_at"]

    def __str__(self):  # pragma: no cover
        return f"Sel({self.user_id}:{self.template_id})"

class SystemHotspotEvent(models.Model):
    """Represents a sustained high resource utilization period.

    Created when CPU or Memory stays above CRITICAL threshold for HOTSPOT_MIN_DURATION_SEC.
    Resolved automatically when metric falls back below WARN threshold.
    """
    RESOURCE_CHOICES = (
        ('cpu','CPU'),
        ('memory','Memory'),
    )
    resource = models.CharField(max_length=16, choices=RESOURCE_CHOICES)
    level = models.CharField(max_length=16)  # 'critical' or 'warn'
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    peak_percent = models.FloatField(default=0)
    duration_sec = models.IntegerField(default=0)
    resolved = models.BooleanField(default=False)
    meta = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ['-started_at']

    def mark_resolved(self):
        if not self.resolved:
            from django.utils import timezone
            self.ended_at = timezone.now()
            self.duration_sec = int((self.ended_at - self.started_at).total_seconds())
            self.resolved = True
            self.save(update_fields=['ended_at','duration_sec','resolved'])


# ================= Job Abstraction (Non-technical execution wrapper) =================

class JobStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    QUEUED = "queued", "Queued"
    RUNNING = "running", "Running"
    SUCCESS = "success", "Success"
    ERROR = "error", "Error"
    CANCELED = "canceled", "Canceled"


class Job(models.Model):
    """High-level user submitted job.

    Wraps an asynchronous CLIExecution invocation with user/template context and
    trackable lifecycle for non-technical consumption. Links to CLIExecution via
    Integer field (execution_id) to avoid import cycles (resolved lazily).
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="jobs")
    command = models.CharField(max_length=128)
    template_id = models.CharField(max_length=128, blank=True, default="")
    expert_slug = models.CharField(max_length=128, blank=True, default="")
    variables = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=16, choices=JobStatus.choices, default=JobStatus.PENDING)
    progress = models.PositiveIntegerField(default=0)
    error = models.TextField(blank=True, default="")
    celery_task_id = models.CharField(max_length=128, blank=True, default="", db_index=True)
    execution_id = models.IntegerField(null=True, blank=True, help_text="FK to CLIExecution when available")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "templates_job"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "created_at"]),
        ]

    def mark_running(self):  # pragma: no cover - trivial state change
        self.status = JobStatus.RUNNING
        self.save(update_fields=["status", "updated_at"])

    def mark_error(self, msg: str):  # pragma: no cover
        self.status = JobStatus.ERROR
        self.error = (msg or "")[:1000]
        self.finished_at = timezone.now()
        self.save(update_fields=["status", "error", "finished_at", "updated_at"])

    def mark_success(self):  # pragma: no cover
        self.status = JobStatus.SUCCESS
        self.finished_at = timezone.now()
        self.save(update_fields=["status", "finished_at", "updated_at"])

    def __str__(self):  # pragma: no cover
        return f"Job({self.id} {self.command} {self.status})"

