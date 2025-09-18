
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from .services import get_tubewhale_engine

@api_view(["GET"])
@permission_classes([AllowAny])
def simple_health(request):
    try:
        engine = get_tubewhale_engine()
        return Response({
            "status": "healthy", 
            "engine_loaded": engine is not None
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        })

