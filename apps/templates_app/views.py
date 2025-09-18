from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response

from .models import CustomTemplate
from .serializers import CustomTemplateSerializer

# Bridge: use enterprise engine for core/domain templates
from service.enterprise_template_engine import TemplateEngine


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
