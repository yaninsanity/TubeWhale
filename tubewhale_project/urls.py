"""
TubeWhale Project URLs
Main URL Configuration
"""

from django.contrib import admin
from django.urls import path, include
from apps.templates_app import admin_views as tpl_admin_views
from django.views.generic import RedirectView
from django.conf import settings
from django.conf.urls.static import static
from django.utils.translation import gettext_lazy as _
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
# from apps.templates_app.admin_views import env_config_view, engine_templates_catalog, engine_template_preview
from apps.templates_app.simple_admin_views import (
    template_system_dashboard,
    template_analytics,
    template_import_wizard,
    bulk_template_operations,
    template_ping,
    public_template_index,
    public_template_preview,
    compose_and_run,
    compose_preview,
    cli_runs_export,
    cli_runs_graph,
    cli_runs_async_run,
    cli_runs_async_status,
    cli_runs_overview,
    expert_matrix_view,
    cli_runs_recent,
)
from apps.templates_app.admin_views import (
    cli_db_overview,
    cli_db_table_preview,
    cli_db_table_export,
    engine_templates_catalog,  # needed for base templates reverse('admin-engine-templates')
    engine_template_preview,  # template preview functionality
    expert_prompts_overview,
    env_config_view,  # environment configuration editor
)

urlpatterns = [
    # Anonymous diagnostic ping for template subsystem (placed BEFORE admin site to avoid swallow)
    path('templates-ping/', template_ping, name='public-template-ping'),
    path('templates-public/', public_template_index, name='public-template-index'),
    path('templates-public/<str:template_slug>/', public_template_preview, name='public-template-preview'),
    path('admin/templates/compose-run/', compose_and_run, name='admin-template-compose-run'),
    path('admin/templates/compose-preview/', compose_preview, name='admin-template-compose-preview'),
    path('admin/templates/cli-runs/export/<str:fmt>/', cli_runs_export, name='admin-cli-runs-export'),
    path('admin/templates/cli-runs/graph/', cli_runs_graph, name='admin-cli-runs-graph'),
    path('admin/templates/cli-runs/async-run/', cli_runs_async_run, name='admin-cli-runs-async-run'),
    path('admin/templates/cli-runs/async-status/<str:task_id>/', cli_runs_async_status, name='admin-cli-runs-async-status'),
    path('admin/templates/cli-runs/overview/', cli_runs_overview, name='admin-cli-runs-overview'),
    path('admin/templates/cli-runs/recent/', cli_runs_recent, name='admin-cli-runs-recent'),
    # CLI external DB introspection
    path('admin/templates/cli-db/', cli_db_overview, name='admin-cli-db-overview'),
    path('admin/templates/cli-db/preview/<str:table>/', cli_db_table_preview, name='admin-cli-db-preview'),
    path('admin/templates/cli-db/export/<str:table>/', cli_db_table_export, name='admin-cli-db-export'),
    path('admin/templates/expert-prompts/', expert_prompts_overview, name='admin-expert-prompts'),
    
    # === TUBEWHALE TEMPLATE MANAGEMENT ===
    # Professional template system with enterprise features
    path('admin/templates/', include([
        path('', template_system_dashboard, name='admin-templates-home'),  # 添加根路径处理器
        path('dashboard/', template_system_dashboard, name='admin-template-dashboard'),
        path('ping/', template_ping, name='admin-template-ping'),
        path('catalog/', engine_templates_catalog, name='admin-engine-templates'),
        path('catalog/<str:template_id>/preview/', engine_template_preview, name='admin-engine-template-preview'),
        path('analytics/', template_analytics, name='admin-template-analytics'),
        path('expert-matrix/', expert_matrix_view, name='admin-expert-matrix'),
        path('import/', template_import_wizard, name='admin-template-import-wizard'),
        path('bulk/', bulk_template_operations, name='admin-bulk-operations'),
        # (cli-db routes defined globally to avoid duplication)
        path('expert-prompts/', expert_prompts_overview, name='admin-expert-prompts'),
    ])),
    # Explicit no-slash variant to reduce 404 risk if client omits trailing slash
    path('admin/templates', template_system_dashboard, name='admin-templates-home-noslash'),
    
    # === SYSTEM CONFIGURATION ===
    path('admin/config/', include([
        path('environment/', env_config_view, name='admin-env-config'),
    ])),
    
    # === API ENDPOINTS ===
    # User management and authentication
    path('user/', include('apps.user_app.urls')),
    
    # Core API services
    path('api/', include('apps.api_app.urls')),
    
    # Intelligent Templates API
    path('api/templates/', include('apps.templates_app.urls')),
    # Friendly alias (non-API) for direct browser/manual usage
    path('templates/', include('apps.templates_app.urls')),
    path('admin/system-health/', tpl_admin_views.system_health_overview, name='admin-system-health'),
    path('admin/dashboard-embed/', tpl_admin_views.admin_dashboard_summary, name='admin-dashboard-embed'),
    
    # === TUBEWHALE ENGINE ===
    # Main application engine and CLI integration
    path('', include('apps.tubewhale_engine.urls')),
    
    # === API DOCUMENTATION ===
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    # === DJANGO ADMIN CORE (moved to end so custom /admin/templates/* take precedence) ===
    path('admin/', admin.site.urls),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# Custom admin titles
admin.site.site_header = _("TubeWhale Admin")
admin.site.site_title = _("TubeWhale")
admin.site.index_title = _("Welcome to TubeWhale Management System")
