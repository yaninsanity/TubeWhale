from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response

from .models import CustomTemplate, Job, JobStatus
from .models import ExpertPrompt
from django.conf import settings
from django.utils import translation
from .serializers import CustomTemplateSerializer, JobSerializer

# Bridge: use enterprise engine for core/domain templates
from service.enterprise_template_engine import TemplateEngine
from service.core.expert_customization import (
    ExpertDomain,
    BusinessIntelligenceTemplate,
    MedicalResearchTemplate,
    TechnologyInnovationTemplate,
)
import os


class CustomTemplateViewSet(viewsets.ModelViewSet):
    queryset = CustomTemplate.objects.all()
    serializer_class = CustomTemplateSerializer
    permission_classes = [IsAuthenticated]

    @action(detail=False, methods=["get"], permission_classes=[AllowAny])
    def domains(self, request):
        engine = TemplateEngine(validation_strict=False)
        return Response({"domains": engine.get_domains()})

    @action(detail=False, methods=["get"], permission_classes=[AllowAny])
    def list_all(self, request):
        engine = TemplateEngine(validation_strict=False)
        # Core + domain from engine
        templates = engine.list_templates(include_metadata=True)
        return Response({"engine_templates": templates})

    @action(detail=False, methods=["get"], permission_classes=[AllowAny])
    def search(self, request):
        q = request.query_params.get("q", "")
        engine = TemplateEngine(validation_strict=False)
        results = engine.search_templates(q)
        return Response({"results": results})

    @action(detail=False, methods=["post"], permission_classes=[AllowAny])
    def compile(self, request):
        """Compile any template (engine or custom) with provided variables"""
        data = request.data
        template_id = data.get("template_id")
        variables = data.get("variables", {})
        if not template_id:
            return Response({"detail": "template_id required"}, status=400)

        engine = TemplateEngine(validation_strict=False)
        try:
            # Try engine first
            if engine.get_template(template_id):
                compiled = engine.compile_template(template_id, variables)
                params = engine.get_template_parameters(template_id)
                return Response({"compiled": compiled, "parameters": params})
        except Exception:
            pass

        # Fallback to DB custom
        try:
            obj = CustomTemplate.objects.get(template_id=template_id)
            compiled = obj.prompt.format(**variables)
            return Response({"compiled": compiled, "parameters": obj.parameters})
        except CustomTemplate.DoesNotExist:
            return Response({"detail": "template not found"}, status=404)
        except KeyError as e:
            return Response({"detail": f"missing variable: {e}"}, status=400)


# ================== Public English-only prompt & template catalog (for CLI & UI) ==================

@api_view(["GET"])
@permission_classes([AllowAny])
def expert_prompts_catalog(request):
    """Return canonical expert prompts (always English).

    Query params:
      active_only=1  -> filter active
    """
    # Force language context to English if setting enabled
    if getattr(settings, 'FORCE_EN_PROMPTS', False):
        translation.activate('en')
    qs = ExpertPrompt.objects.all().order_by('slug')
    if request.query_params.get('active_only') == '1':
        qs = qs.filter(active=True)
    data = [
        {
            'slug': p.slug,
            'role': p.role,
            'domain': p.domain,
            'title': p.title,
            'description': p.description,
            'prompt_intro': p.prompt_intro,
            'prompt_outro': p.prompt_outro,
            'active': p.active,
            'updated_at': p.updated_at,
        } for p in qs
    ]
    return Response({'count': len(data), 'prompts': data})


@api_view(["GET"])
@permission_classes([AllowAny])
def core_templates_catalog(request):
    """Return core summary templates (English)."""
    if getattr(settings, 'FORCE_EN_PROMPTS', False):
        translation.activate('en')
    qs = CustomTemplate.objects.filter(template_type='core').order_by('template_id')
    data = [
        {
            'template_id': t.template_id,
            'name': t.name,
            'domain': t.domain,
            'description': t.description,
            'prompt': t.prompt,
            'tags': t.tags,
            'parameters': t.parameters,
            'template_type': t.template_type,
        } for t in qs
    ]
    return Response({'count': len(data), 'templates': data})


@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    engine = TemplateEngine(validation_strict=False)
    return Response({
        "status": "ok",
        "domains": engine.get_domains(),
        "engine_loaded": True
    })


# ================== Expert system endpoints ==================

_EXPERT_TEMPLATES = {
    ExpertDomain.BUSINESS_INTELLIGENCE.value: BusinessIntelligenceTemplate(),
    ExpertDomain.MEDICAL_RESEARCH.value: MedicalResearchTemplate(),
    ExpertDomain.TECHNOLOGY_INNOVATION.value: TechnologyInnovationTemplate(),
}


@api_view(["GET"])
@permission_classes([AllowAny])
def expert_domains(request):
    return Response({
        "domains": [d.value for d in ExpertDomain]
    })


@api_view(["GET"])
@permission_classes([AllowAny])
def expert_defaults(request):
    domain = request.query_params.get("domain", "")
    tpl = _EXPERT_TEMPLATES.get(domain)
    if not tpl:
        return Response({"detail": "unknown domain"}, status=400)
    qs = tpl.get_default_questions()
    data = {
        "domain": domain,
        "default_questions": [
            {
                "question": q.question,
                "context": q.context,
                "focus_areas": q.focus_areas,
                "expected_format": q.expected_format.value,
                "weight": q.weight,
                "evaluation_criteria": q.evaluation_criteria,
                "follow_up_questions": q.follow_up_questions,
            }
            for q in qs
        ],
        "framework": tpl.get_analysis_framework(),
    }
    return Response(data)


@api_view(["POST"])
@permission_classes([AllowAny])
def expert_compile(request):
    payload = request.data or {}
    domain = payload.get("domain")
    variables = payload.get("variables", {})
    include_questions = bool(payload.get("include_questions", True))
    tpl = _EXPERT_TEMPLATES.get(domain)
    if not tpl:
        return Response({"detail": "unknown domain"}, status=400)
    framework = tpl.get_analysis_framework()
    expert_questions = "\n".join([
        f"- {q.question}" for q in tpl.get_default_questions()
    ]) if include_questions else ""
    variables = {**variables, "expert_questions": expert_questions}
    try:
        compiled = framework.format(**variables)
        return Response({"compiled": compiled})
    except KeyError as e:
        return Response({"detail": f"missing variable: {e}"}, status=400)


# ================== CLI configuration endpoint ==================

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def cli_config(request):
    def split_keys(raw: str) -> list:
        if not raw:
            return []
        # Support comma/semicolon/whitespace as separators
        for sep in ["\n", "\t", ";"]:
            raw = raw.replace(sep, ",")
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return parts

    yt_keys_raw = os.environ.get("YOUTUBE_API_KEYS", "")
    yt_keys = split_keys(yt_keys_raw)

    return Response({
        "YOUTUBE_API_KEYS": yt_keys,
        # Extend with more CLI-configurable items as needed
    })


# ================== Template Selection API (Tier BASIC) ==================
from .access_control import (
    get_active_selections,
    select_template_for_basic,
)
from .tier_utils import get_user_template_stats


@api_view(["GET"])
def template_selection_list(request):
    if not request.user.is_authenticated:
        return Response({"detail": "auth required"}, status=401)
    sels = get_active_selections(request.user)
    stats = get_user_template_stats(request.user)
    return Response({
        "tier": stats.get("tier"),
        "max": stats.get("max_templates"),
        "selected": [
            {
                "template_id": s.template_id,
                "template_name": s.template_name,
                "template_type": s.template_type,
                "locked_at": s.locked_at,
            } for s in sels
        ],
        "remaining": stats.get("remaining_templates"),
    })


@api_view(["POST"])
def template_selection_add(request):
    if not request.user.is_authenticated:
        return Response({"detail": "auth required"}, status=401)
    payload = request.data or {}
    template_id = payload.get("template_id")
    template_name = payload.get("template_name") or template_id
    template_type = payload.get("template_type") or "core"
    if not template_id:
        return Response({"detail": "template_id required"}, status=400)
    # Enforce that only BASIC tier users use this endpoint (others have implicit access)
    from .access_control import get_user_profile
    profile = get_user_profile(request.user)
    from .tier_models import UserTier
    if profile.tier != UserTier.BASIC:
        return Response({"detail": "selection endpoint only for BASIC tier"}, status=400)
    try:
        from django.db import transaction
        with transaction.atomic():
            sel = select_template_for_basic(request.user, template_id, template_name, template_type)
        return Response({"added": sel.template_id})
    except ValueError as e:
        return Response({"detail": str(e)}, status=400)


@api_view(["DELETE"])
def template_selection_remove(request, template_id: str):
    if not request.user.is_authenticated:
        return Response({"detail": "auth required"}, status=401)
    from .models import UserTemplateSelection
    qs = UserTemplateSelection.objects.filter(user=request.user, template_id=template_id, active=True)
    if not qs.exists():
        return Response({"detail": "not found"}, status=404)
    qs.update(active=False)
    return Response({"removed": template_id})


# ================== Job API ==================
from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import CreateModelMixin, ListModelMixin, RetrieveModelMixin
from django.utils import timezone
from django.conf import settings
from celery import current_app
from .tasks import run_job_task


class JobViewSet(CreateModelMixin, ListModelMixin, RetrieveModelMixin, GenericViewSet):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):  # user-level isolation
        return Job.objects.filter(user=self.request.user).order_by('-created_at')

    def perform_create(self, serializer):
        job: Job = serializer.save(user=self.request.user, status=JobStatus.PENDING)
        # Enqueue task; if broker unavailable fallback to inline
        try:
            async_result = run_job_task.delay(job.id)
            job.status = JobStatus.QUEUED
            job.celery_task_id = async_result.id
            job.save(update_fields=['status', 'celery_task_id', 'updated_at'])
        except Exception:
            # Fallback inline execution (synchronous) to ensure usability in degraded mode
            job.status = JobStatus.RUNNING
            job.save(update_fields=['status', 'updated_at'])
            from .tasks import run_job_task as inline_task  # local import to avoid circular
            inline_task.apply(args=[job.id])
            job.refresh_from_db()
        return job

    @action(detail=True, methods=['post'])
    def cancel(self, request, pk=None):
        try:
            job = self.get_queryset().get(pk=pk)
        except Job.DoesNotExist:
            return Response({'detail': 'not found'}, status=404)
        if job.status in (JobStatus.SUCCESS, JobStatus.ERROR, JobStatus.CANCELED):
            return Response({'detail': f'cannot cancel in state {job.status}'}, status=400)
        # Attempt revoke if queued/running
        if job.celery_task_id:
            try:
                current_app.control.revoke(job.celery_task_id, terminate=True)
            except Exception:
                pass
        job.status = JobStatus.CANCELED
        job.finished_at = timezone.now()
        job.save(update_fields=['status', 'finished_at', 'updated_at'])
        return Response({'canceled': True})

    @action(detail=True, methods=['get'])
    def logs(self, request, pk=None):
        """Return stdout/stderr for the linked CLI execution (owner only)."""
        try:
            job = self.get_queryset().get(pk=pk)
        except Job.DoesNotExist:
            return Response({'detail': 'not found'}, status=404)
        if not job.execution_id:
            return Response({'detail': 'execution_pending'}, status=202)
        from .models import CLIExecution
        exec_obj = CLIExecution.objects.filter(id=job.execution_id).first()
        if not exec_obj:
            return Response({'detail': 'execution_missing'}, status=404)
        return Response({
            'job_id': job.id,
            'execution_id': exec_obj.id,
            'command': exec_obj.command,
            'status': job.status,
            'cli_status': exec_obj.status,
            'stdout': exec_obj.stdout,
            'stderr': exec_obj.stderr,
            'returncode': exec_obj.returncode,
            'duration_ms': exec_obj.duration_ms,
        })
