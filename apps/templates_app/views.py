from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response

from .models import CustomTemplate
from .serializers import CustomTemplateSerializer

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
