from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    CustomTemplateViewSet,
    health,
    expert_domains,
    expert_defaults,
    expert_compile,
    cli_config,
    expert_prompts_catalog,
    core_templates_catalog,
)
from .views import JobViewSet
from .views import template_selection_list, template_selection_add, template_selection_remove
from .admin_views import (
    engine_templates_catalog,
    engine_template_preview,
    env_config_view,
    system_health_overview,
    system_health_overview_json,
    system_metrics_json,
    system_metrics_dashboard,
    system_metrics_stream,
    system_metrics_prometheus,
    hotspot_events_overview,
    admin_header_status,
)
from .simple_admin_views import (
    template_system_dashboard,
    bulk_template_operations,
    template_analytics,
    template_import_wizard,
    template_quick_actions,
)
from .cli_integration import (
    cli_dashboard,
    cli_command_executor,
    cli_template_operations,
    cli_template_manager,
)
from ..tubewhale_engine.cli_admin_views import cli_logs

router = DefaultRouter()
router.register(r'custom-templates', CustomTemplateViewSet, basename='custom-templates')
router.register(r'jobs', JobViewSet, basename='jobs')

urlpatterns = [
    path("", include(router.urls)),
    path("health/", health, name="templates-health"),
    # Expert endpoints
    path("expert/domains/", expert_domains, name="expert-domains"),
    path("expert/defaults/", expert_defaults, name="expert-defaults"),
    path("expert/compile/", expert_compile, name="expert-compile"),
    path("expert/catalog/", expert_prompts_catalog, name="expert-prompts-catalog"),
    path("core/catalog/", core_templates_catalog, name="core-templates-catalog"),
    # CLI configuration endpoint (auth)
    path("cli/config/", cli_config, name="cli-config"),
    # Template selection (tier BASIC)
    path("selection/", template_selection_list, name="template-selection-list"),
    path("selection/add/", template_selection_add, name="template-selection-add"),
    path("selection/remove/<str:template_id>/", template_selection_remove, name="template-selection-remove"),
    
    # Simple Admin views
    path('admin/dashboard/', template_system_dashboard, name='admin-template-dashboard'),
    path('admin/analytics/', template_analytics, name='admin-template-analytics'),
    path('admin/import-wizard/', template_import_wizard, name='admin-template-import-wizard'),
    path('admin/bulk-operations/', bulk_template_operations, name='admin-bulk-operations'),
    path('admin/quick-actions/', template_quick_actions, name='admin-template-quick-actions'),
    
    # CLI Integration views
    path('admin/cli/dashboard/', cli_dashboard, name='admin-cli-dashboard'),
    path('admin/cli/execute/', cli_command_executor, name='admin-cli-execute'),
    path('admin/cli/template-operations/', cli_template_operations, name='admin-cli-template-operations'),
    path('admin/cli/template-manager/', cli_template_manager, name='admin-cli-template-manager'),
    path('admin/cli/logs/', cli_logs, name='admin-cli-logs'),
    path('admin/env-config/', env_config_view, name='admin-env-config'),
    path('admin/system/health/', system_health_overview, name='admin-system-health'),
    path('admin/system/health.json', system_health_overview_json, name='admin-system-health-json'),
    path('admin/system/metrics.json', system_metrics_json, name='admin-system-metrics-json'),
    path('admin/system/metrics/', system_metrics_dashboard, name='admin-system-metrics-dashboard'),
    path('admin/system/metrics.stream', system_metrics_stream, name='admin-system-metrics-stream'),
    path('admin/system/metrics.prom', system_metrics_prometheus, name='admin-system-metrics-prometheus'),
    path('admin/system/hotspots/', hotspot_events_overview, name='admin-system-hotspots'),
    path('admin/system/header-status.json', admin_header_status, name='admin-header-status-json'),
]
