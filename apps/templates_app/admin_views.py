from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Dict

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.conf import settings

from dotenv import dotenv_values


ENV_FILE = os.path.join(settings.BASE_DIR, ".env")
BACKUP_PATTERN = ".env.bak_%Y%m%d_%H%M%S"

# Only allow updating these keys via Admin to minimize risk
ALLOWED_KEYS = [
    "KEYWORD",
    "YOUTUBE_API_KEYS",
    "OPENAI_API_KEY",
    "DB_PATH",
    "PERSIST_AGENT_SUMMARIES",
    "FULL_AUDIO_ANALYSIS",
    "DRY_RUN",
    "MAX_N",
    "TOP_K",
    "FILTER_TYPE",
    "CONCURRENCY",
    "PURE_YOUTUBE",
    # Optional CLI remote config
    "CLI_CONFIG_URL",
    "CLI_CONFIG_AUTH",
]


def _read_env_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_env_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _update_env_content(original: str, updates: Dict[str, str]) -> str:
    # Preserve unknown lines and comments; update only ALLOWED_KEYS if present or append at end.
    lines = original.splitlines() if original else []
    kv_index = {k: None for k in ALLOWED_KEYS}
    # Index existing allowed keys
    for idx, line in enumerate(lines):
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        k = line.split("=", 1)[0].strip()
        if k in kv_index and kv_index[k] is None:
            kv_index[k] = idx

    # Apply updates (only for keys in ALLOWED_KEYS)
    for k, v in updates.items():
        if k not in ALLOWED_KEYS:
            continue
        # Normalize YOUTUBE_API_KEYS to newline-separated for readability
        if k == "YOUTUBE_API_KEYS":
            # Accept comma/semicolon/newline/space separated input
            tmp = v.replace("\t", "\n").replace(";", "\n").replace(",", "\n")
            parts = [p.strip() for p in tmp.splitlines() if p.strip()]
            v = "\n".join(parts)
        # Quote values that contain spaces or special chars
        needs_quote = any(ch in v for ch in [" ", "#", "\n"])
        val = f'"{v}"' if needs_quote and not (v.startswith('"') and v.endswith('"')) else v

        assignment = f"{k}={val}"
        if kv_index.get(k) is not None:
            lines[kv_index[k]] = assignment
        else:
            lines.append(assignment)
    return "\n".join(lines) + ("\n" if lines else "")


@staff_member_required
def env_config_view(request: HttpRequest) -> HttpResponse:
    # Load existing values
    env_map = dotenv_values(ENV_FILE) if os.path.exists(ENV_FILE) else {}
    context = {"env": {k: env_map.get(k, "") for k in ALLOWED_KEYS}, "env_path": ENV_FILE}

    if request.method == "POST":
        # Build updates from POST safely
        updates: Dict[str, str] = {}
        for k in ALLOWED_KEYS:
            updates[k] = request.POST.get(k, "").strip()

        original = _read_env_text(ENV_FILE)

        # Backup
        backup_name = datetime.now().strftime(BACKUP_PATTERN)
        backup_path = os.path.join(settings.BASE_DIR, backup_name)
        try:
            if original:
                _write_env_text(backup_path, original)
        except Exception:
            messages.warning(request, f"Failed to create .env backup at {backup_path}")

        # Write new content
        try:
            new_content = _update_env_content(original, updates)
            _write_env_text(ENV_FILE, new_content)
            messages.success(request, ".env updated successfully. Restart may be required to take effect.")
            return redirect(reverse("admin-env-config"))
        except Exception as e:
            messages.error(request, f"Failed to update .env: {e}")

    return render(request, "admin/env_config.html", context)


# ================== Engine Templates Catalog (read-only + import) ==================

"""Admin auxiliary views for environment config and template engine UI.

Pollution Guard Note:
The TemplateEngine is optional. We wrap its import so that if the enterprise
engine module was removed (minimal deployment) these admin endpoints degrade
gracefully instead of raising ImportError.
"""

try:  # Optional dependency
    from service.enterprise_template_engine import TemplateEngine  # type: ignore
except Exception:  # Broad: missing module or any init error
    TemplateEngine = None  # type: ignore
from .models import CustomTemplate, TemplateType
from .tier_utils import filter_accessible_templates, get_user_template_stats, track_template_usage
from . import cli_db
from .models import ExpertPrompt
from .models import UserTemplateSelection

from django.db import connection, models
from django.apps import apps as django_apps
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone
from django.contrib.auth import get_user_model
User = get_user_model()

# ========== Lightweight in-process metrics history (no external TSDB) ==========
# We keep a small ring buffer of recent samples (timestamp, cpu%, mem%).
# This is reset on process restart; sufficient for real-time admin observability.
from collections import deque
import threading
import time

_METRICS_LOCK = threading.Lock()
_METRICS_HISTORY = deque(maxlen=120)  # ~10 minutes at 5s cadence (to be polled by JS)
_HOTSPOT_STATE = {
    'cpu': {'last_over_crit': None, 'active_event_id': None},
    'memory': {'last_over_crit': None, 'active_event_id': None},
}

def _record_metric_sample(cpu_percent: float, mem_percent: float):
    with _METRICS_LOCK:
        _METRICS_HISTORY.append({
            'ts': time.time(),
            'cpu': round(cpu_percent, 2),
            'mem': round(mem_percent, 2),
        })
    _maybe_detect_hotspots(cpu_percent, mem_percent)

def _get_metrics_history():
    with _METRICS_LOCK:
        return list(_METRICS_HISTORY)

def _compute_rolling_averages():
    """Compute rolling averages over 1m/5m/10m windows based on 5s samples.

    Returns dict: {
       '1m': {'cpu': float|None, 'mem': float|None}, ...
    }
    """
    history = _get_metrics_history()
    if not history:
        return {
            '1m': {'cpu': None, 'mem': None},
            '5m': {'cpu': None, 'mem': None},
            '10m': {'cpu': None, 'mem': None},
        }
    now_ts = time.time()
    windows = {
        '1m': 60,
        '5m': 300,
        '10m': 600,
    }
    out = {}
    for label, span in windows.items():
        span_cut = now_ts - span
        samples = [h for h in history if h['ts'] >= span_cut]
        if samples:
            out[label] = {
                'cpu': round(sum(s['cpu'] for s in samples) / len(samples), 2),
                'mem': round(sum(s['mem'] for s in samples) / len(samples), 2),
            }
        else:
            out[label] = {'cpu': None, 'mem': None}
    return out

def _maybe_detect_hotspots(cpu_percent: float, mem_percent: float):
    """Detect sustained critical usage and create/resolve SystemHotspotEvent records.

    Rules:
      - When metric > CRIT threshold: start timer; if duration > HOTSPOT_MIN_DURATION_SEC → create event.
      - While active event: update peak.
      - When metric drops below WARN threshold: resolve event.
    """
    from django.conf import settings as dj_settings
    from .models import SystemHotspotEvent
    now = time.time()
    config = [
        ('cpu', cpu_percent, dj_settings.CPU_WARN_THRESHOLD, dj_settings.CPU_CRIT_THRESHOLD),
        ('memory', mem_percent, dj_settings.MEM_WARN_THRESHOLD, dj_settings.MEM_CRIT_THRESHOLD),
    ]
    for resource, value, warn_thr, crit_thr in config:
        state = _HOTSPOT_STATE[resource]
        # Critical tracking
        if value >= crit_thr:
            if state['last_over_crit'] is None:
                state['last_over_crit'] = now
            # Check if we need to create event
            if state['active_event_id'] is None and (now - state['last_over_crit']) >= dj_settings.HOTSPOT_MIN_DURATION_SEC:
                evt = SystemHotspotEvent.objects.create(resource=resource, level='critical', peak_percent=value, meta={
                    'crit_threshold': crit_thr,
                    'warn_threshold': warn_thr,
                })
                state['active_event_id'] = evt.id
        else:
            state['last_over_crit'] = None

        # Update active event peak
        if state['active_event_id'] is not None:
            try:
                from .models import SystemHotspotEvent as SHE
                evt = SHE.objects.filter(id=state['active_event_id'], resolved=False).first()
                if evt:
                    if value > evt.peak_percent:
                        evt.peak_percent = value
                        evt.save(update_fields=['peak_percent'])
                    # Resolve if value below warn
                    if value < warn_thr:
                        evt.mark_resolved()
                        state['active_event_id'] = None
            except Exception:
                pass

def _gather_system_health(fast: bool = False):
    """Core health snapshot logic (reusable for HTML + JSON)."""
    # Defensive local import so stale container layers lacking top-level import won't 500
    try:  # pragma: no cover - defensive
        from django.db import models as dj_models
    except Exception:  # pragma: no cover
        dj_models = None
    db_engine = connection.settings_dict.get('ENGINE')
    db_name = connection.settings_dict.get('NAME')

    pending_migrations: list[str] = []
    if not fast:
        try:
            executor = MigrationExecutor(connection)
            targets = executor.loader.graph.leaf_nodes()
            plan = executor.migration_plan(targets)
            pending_migrations = [f"{app}.{name}" for app, name in [m[0] for m in plan]]
        except Exception:
            # Migration introspection failure should not break health
            pending_migrations = ["<introspection_failed>"]

    beat_tables = [
        'django_celery_beat_periodictask',
        'django_celery_beat_crontabschedule',
        'django_celery_beat_intervalschedule',
    ]
    existing = set(connection.introspection.table_names())
    beat_status = {t: (t in existing) for t in beat_tables}

    sel_qs = UserTemplateSelection.objects.filter(active=True)
    total_active_selections = sel_qs.count()
    users_with_selections = sel_qs.values('user').distinct().count()
    from .access_control import BASIC_LIMIT, enforce_basic_limit
    over_limit_users = []
    if dj_models is not None:
        try:
            for row in sel_qs.values('user').annotate(c=dj_models.Count('id')):  # type: ignore
                if row['c'] > BASIC_LIMIT:
                    over_limit_users.append({'user_id': row['user'], 'count': row['c']})
        except Exception:
            # Fallback silently if annotation fails (schema drift etc.)
            pass

    # Hard dependency: psutil import failures should have been surfaced at startup in AppConfig.ready().
    import psutil  # type: ignore
    cpu_percent = psutil.cpu_percent(interval=0.05)
    virt = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    try:
        load1, load5, load15 = os.getloadavg()
        load_avg = {'1m': round(load1,2), '5m': round(load5,2), '15m': round(load15,2)}
    except Exception:
        load_avg = None
    try:
        p = psutil.Process()
        proc_rss_mb = round(p.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        proc_rss_mb = None
    sys_metrics = {
        'cpu_percent': cpu_percent,
        'mem_used_mb': round(virt.used / 1024 / 1024, 1),
        'mem_total_mb': round(virt.total / 1024 / 1024, 1),
        'mem_percent': virt.percent,
        'disk_used_gb': round(disk.used / 1024 / 1024 / 1024, 2),
        'disk_total_gb': round(disk.total / 1024 / 1024 / 1024, 2),
        'disk_percent': disk.percent,
        'load_avg': load_avg,
        'proc_rss_mb': proc_rss_mb,
    }
    _record_metric_sample(cpu_percent, virt.percent)

    celery_ok = None
    try:
        from celery import current_app
        insp = current_app.control.inspect(timeout=0.5)
        active = insp.active() if insp else None
        celery_ok = bool(active is not None)
    except Exception:
        celery_ok = False

    # Derive component statuses
    components = []
    components.append({'name': 'database', 'ok': True, 'engine': db_engine})
    components.append({'name': 'celery', 'ok': celery_ok})
    components.append({'name': 'beat_tables', 'ok': all(beat_status.values()), 'details': beat_status})
    components.append({'name': 'template_selection', 'ok': len(over_limit_users) == 0, 'active': total_active_selections})
    overall_ok = all(c.get('ok') for c in components if c.get('ok') is not None)

    return {
        'db_engine': db_engine,
        'db_name': db_name,
        'pending_migrations': pending_migrations,
        'pending_count': len(pending_migrations),
        'beat_status': beat_status,
        'total_active_selections': total_active_selections,
        'users_with_selections': users_with_selections,
        'over_limit_users': over_limit_users,
    'system_metrics': sys_metrics,
        'metrics_history': _get_metrics_history(),
        'rolling_averages': _compute_rolling_averages(),
        'thresholds': {
            'cpu_warn': getattr(settings, 'CPU_WARN_THRESHOLD', 70),
            'cpu_crit': getattr(settings, 'CPU_CRIT_THRESHOLD', 90),
            'mem_warn': getattr(settings, 'MEM_WARN_THRESHOLD', 75),
            'mem_crit': getattr(settings, 'MEM_CRIT_THRESHOLD', 90),
        },
        'celery_ok': celery_ok,
        'components': components,
        'overall_ok': overall_ok,
    }

@staff_member_required
def system_metrics_json(request: HttpRequest) -> JsonResponse:
    """Lightweight polling endpoint returning just changing metrics & history.

    Keeps payload small for high-frequency (5s) polling.
    """
    data = {}
    try:
        base = _gather_system_health(fast=True)
        data = {
            'ts': time.time(),
            'system_metrics': base.get('system_metrics', {}),
            'history': base.get('metrics_history', []),
            'rolling_averages': base.get('rolling_averages', {}),
            'overall_ok': base.get('overall_ok'),
            'components': base.get('components'),
            'thresholds': base.get('thresholds', {}),
        }
    except Exception as e:
        data = {'error': str(e)}
    return JsonResponse(data)

@staff_member_required
def system_metrics_dashboard(request: HttpRequest) -> HttpResponse:
    """Render the real-time metrics dashboard (graphs + polling JS)."""
    return render(request, 'admin/system_metrics_dashboard.html', {})

@staff_member_required
def system_metrics_stream(request: HttpRequest) -> HttpResponse:
    """Server-Sent Events (SSE) stream for real-time metrics.

    Fallback to 400 if client does not accept text/event-stream.
    """
    if request.headers.get('Accept') and 'text/event-stream' not in request.headers['Accept']:
        return HttpResponse('Accept header must include text/event-stream', status=400)

    import json
    from django.utils.timezone import now
    from django.http import StreamingHttpResponse

    def event_stream():  # pragma: no cover (streaming loop)
        last_data = time.time()
        heartbeat_interval = 15  # seconds between keep-alive comments
        data_interval = 5        # seconds between metric payloads
        while True:
            now_ts = time.time()
            try:
                # Send metrics payload each data_interval
                if now_ts - last_data >= data_interval:
                    base = _gather_system_health(fast=True)
                    payload = {
                        'ts': now_ts,
                        'system_metrics': base.get('system_metrics', {}),
                        'overall_ok': base.get('overall_ok'),
                        'rolling_averages': base.get('rolling_averages'),
                        'thresholds': base.get('thresholds'),
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_data = now_ts
                else:
                    # Heartbeat comment every heartbeat_interval to keep proxies open
                    if int(now_ts) % heartbeat_interval == 0:
                        yield f": keep-alive {int(now_ts)}\n\n"
                time.sleep(1)
            except GeneratorExit:
                break
            except Exception:
                # On unexpected error, send minimal error packet then continue and trigger client-side fallback logic
                yield "data: {\"error\": \"metrics_unavailable\"}\n\n"
                time.sleep(data_interval)

    resp = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    resp['Cache-Control'] = 'no-cache'
    return resp

@staff_member_required
def system_metrics_prometheus(request: HttpRequest) -> HttpResponse:
    """Expose primary gauges in Prometheus exposition format (no auth tokens here)."""
    base = _gather_system_health(fast=True)
    m = base.get('system_metrics', {}) or {}
    lines = [
        '# HELP tubewhale_cpu_percent CPU usage percent',
        '# TYPE tubewhale_cpu_percent gauge',
        f"tubewhale_cpu_percent {m.get('cpu_percent','0')}",
        '# HELP tubewhale_memory_percent Memory usage percent',
        '# TYPE tubewhale_memory_percent gauge',
        f"tubewhale_memory_percent {m.get('mem_percent','0')}",
        '# HELP tubewhale_disk_percent Disk usage percent',
        '# TYPE tubewhale_disk_percent gauge',
        f"tubewhale_disk_percent {m.get('disk_percent','0')}",
    ]
    return HttpResponse('\n'.join(lines)+'\n', content_type='text/plain; version=0.0.4')

@staff_member_required
def admin_header_status(request: HttpRequest) -> JsonResponse:
    """Ultra-light status snapshot for header observability panel.

    Returns:
      cpu_percent, mem_percent, hotspot_active_count, last_cli_ts
    """
    try:
        import psutil  # type: ignore
        cpu = psutil.cpu_percent(interval=0.0)
        mem = psutil.virtual_memory().percent
    except Exception:
        cpu = mem = None
    from apps.templates_app.models import SystemHotspotEvent
    hotspot_active = SystemHotspotEvent.objects.filter(resolved=False).count()
    # Lazy import for CLI log model (engine app)
    try:
        from apps.tubewhale_engine.models import CLICommandLog
        last_cli = CLICommandLog.objects.order_by('-created_at').values_list('created_at', flat=True).first()
        last_cli_ts = last_cli.isoformat() if last_cli else None
    except Exception:
        last_cli_ts = None
    return JsonResponse({
        'cpu': cpu,
        'mem': mem,
        'hotspots': hotspot_active,
        'last_cli': last_cli_ts,
    })
@staff_member_required
def hotspot_events_overview(request: HttpRequest) -> HttpResponse:
    from .models import SystemHotspotEvent
    qs = SystemHotspotEvent.objects.all()[:200]
    return render(request, 'admin/hotspot_events_overview.html', {
        'events': qs,
        'count': qs.count(),
    })


@staff_member_required
def system_health_overview(request: HttpRequest) -> HttpResponse:
    fast = request.GET.get('fast') == '1'
    context = _gather_system_health(fast=fast)
    return render(request, 'admin/system_health_overview.html', context)


@staff_member_required
def system_health_overview_json(request: HttpRequest) -> JsonResponse:
    fast = request.GET.get('fast') == '1'
    data = _gather_system_health(fast=fast)
    # Trim large lists for JSON (industrial friendly)
    if len(data.get('pending_migrations', [])) > 50:
        data['pending_migrations'] = data['pending_migrations'][:50] + ['<truncated>']
    return JsonResponse(data)


@staff_member_required
def admin_dashboard_summary(request: HttpRequest) -> HttpResponse:
    """Lightweight JSON-like context for embedding into admin index template.

    Avoids heavy dependencies; uses small queries only.
    """
    from .models import Job, CLIExecution
    recent_jobs = list(Job.objects.order_by('-created_at')[:5].values('id','command','status','created_at'))
    failed_jobs = list(Job.objects.filter(status='error').order_by('-updated_at')[:5].values('id','command','status','error'))
    recent_execs = list(CLIExecution.objects.order_by('-created_at')[:5].values('id','command','status','created_at'))
    total_jobs = Job.objects.count()
    total_execs = CLIExecution.objects.count()
    active_users = User.objects.filter(last_login__gte=timezone.now()-timezone.timedelta(days=7)).count()
    data = {
        'recent_jobs': recent_jobs,
        'failed_jobs': failed_jobs,
        'recent_execs': recent_execs,
        'total_jobs': total_jobs,
        'total_execs': total_execs,
        'active_users_7d': active_users,
    }
    return render(request, 'admin/dashboard_embed.html', data)


@staff_member_required
def engine_templates_catalog(request: HttpRequest) -> HttpResponse:
    """Enhanced template catalog with comprehensive error handling and accessibility."""
    try:
        if TemplateEngine is None:
            messages.warning(request, "Template engine not available in this deployment (minimal mode).")
            return render(request, "admin/engine_templates_enhanced.html", {
                "templates": [],
                "domains": [],
                "types": ["core", "domain", "custom"],
                "q": "",
                "domain_selected": "",
                "type_selected": "",
                "user_stats": None,
                "engine_missing": True,
            })

        try:
            engine = TemplateEngine(validation_strict=False)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to initialize TemplateEngine: {e}")
            messages.error(request, f"Template engine initialization failed: {e}")
            return render(request, "admin/engine_templates_enhanced.html", {
                "templates": [],
                "domains": [],
                "types": ["core", "domain", "custom"],
                "q": "",
                "domain_selected": "",
                "type_selected": "",
                "user_stats": None,
                "engine_error": True,
            })

        # Filters with validation
        q = request.GET.get("q", "").strip()[:100]  # Limit query length
        domain = request.GET.get("domain", "").strip()[:50]  # Limit domain length
        type_filter = request.GET.get("type", "").strip()[:20]  # Limit type length

        try:
            if q:
                engine_templates = engine.search_templates(q)
            else:
                engine_templates = engine.list_templates(
                    domain_filter=domain or None,
                    template_type_filter=type_filter or None,
                    include_metadata=True,
                )
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to retrieve templates: {e}")
            messages.error(request, f"Failed to retrieve templates: {e}")
            engine_templates = []

        # Apply tier-based filtering for non-staff users
        if not request.user.is_staff:
            try:
                engine_templates = filter_accessible_templates(request.user, engine_templates)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Template filtering error: {e}")
                # Continue with unfiltered templates for staff

        # Handle import action with enhanced error handling
        if request.method == "POST":
            template_id = request.POST.get("template_id", "").strip()[:100]  # Limit ID length
            if not template_id:
                messages.error(request, "Template ID is required for import.")
                return redirect(reverse("admin-engine-templates"))
                
            try:
                tpl = engine.get_template(template_id)
                if not tpl:
                    messages.error(request, f"Template '{template_id}' not found in engine")
                else:
                    # If exists, don't duplicate
                    try:
                        exists = CustomTemplate.objects.filter(template_id=template_id).first()
                        if exists:
                            messages.info(request, f"Template '{template_id}' already imported. You can edit it in Custom Templates.")
                        else:
                            CustomTemplate.objects.create(
                                template_id=template_id,
                                name=tpl.get("name", template_id)[:200],  # Limit name length
                                domain=tpl.get("domain", "general")[:100],  # Limit domain length
                                description=tpl.get("description", "")[:1000],  # Limit description
                                prompt=tpl.get("prompt", ""),
                                parameters=tpl.get("parameters", {}),
                                tags=tpl.get("tags", []),
                                template_type=TemplateType.CUSTOM,
                                immutable=False,
                            )
                            messages.success(request, f"Imported '{template_id}' as customizable template.")
                            # Track usage with error handling
                            try:
                                track_template_usage(request.user, template_id, tpl.get("name", template_id))
                            except Exception as track_e:
                                import logging
                                logger = logging.getLogger(__name__)
                                logger.warning(f"Template usage tracking failed: {track_e}")
                    except Exception as db_e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.error(f"Database error during template import: {db_e}")
                        messages.error(request, f"Database error during import: {db_e}")
                return redirect(reverse("admin-engine-templates"))
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Template import failed for {template_id}: {e}")
                messages.error(request, f"Failed to import: {e}")

        # Build list of domains/types for filters with error handling
        try:
            domains = engine.get_domains()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to get domains: {e}")
            domains = []

        types = ["core", "domain", "custom"]

        # Mark which ones already imported with error handling
        try:
            imported_ids = set(CustomTemplate.objects.values_list("template_id", flat=True))
            for item in engine_templates:
                item["imported"] = item.get("id") in imported_ids
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to check imported templates: {e}")
            # Continue without import status

        # Add user stats for tier display with error handling
        try:
            user_stats = get_user_template_stats(request.user)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to get user stats: {e}")
            user_stats = None

        context = {
            "templates": engine_templates,
            "domains": domains,
            "types": types,
            "q": q,
            "domain_selected": domain,
            "type_selected": type_filter,
            "user_stats": user_stats,
        }
        return render(request, "admin/engine_templates_enhanced.html", context)
    
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Unexpected error in engine_templates_catalog: {e}")
        messages.error(request, f"An unexpected error occurred: {e}")
        # Return minimal safe template
        return render(request, "admin/engine_templates_enhanced.html", {
            "templates": [],
            "domains": [],
            "types": ["core", "domain", "custom"],
            "q": "",
            "domain_selected": "",
            "type_selected": "",
            "user_stats": None,
            "unexpected_error": True,
        })
    return render(request, "admin/engine_templates_enhanced.html", context)


@staff_member_required
def engine_template_preview(request: HttpRequest, template_id: str) -> HttpResponse:
    """Enhanced template preview with intelligent variable detection and real-time compilation.

    Features:
    - Automatic variable extraction from template
    - Smart defaults based on variable names
    - Real-time preview updates
    - Error handling and validation
    - Template metadata display
    """
    if TemplateEngine is None:
        messages.error(request, "Template engine not available in this deployment.")
        return redirect(reverse("admin-engine-templates"))

    engine = TemplateEngine(validation_strict=False)
    tpl = engine.get_template(template_id)

    if not tpl:
        messages.error(request, f"Template '{template_id}' not found in engine")
        return redirect(reverse("admin-engine-templates"))

    # Extract variables intelligently
    required_vars = engine.extract_template_variables(template_id)

    # Smart defaults based on variable names
    smart_defaults = {
        'title': 'Sample Video: Advanced Data Analysis Techniques',
        'transcript': 'This comprehensive video explores advanced data analysis techniques including statistical modeling, machine learning algorithms, and data visualization best practices. We cover everything from basic data cleaning to advanced predictive analytics.',
        'duration': '25',
        'language': 'English',
        'author': 'Data Science Expert',
        'topic': 'Advanced Data Analysis',
        'description': 'A detailed exploration of modern data analysis methodologies and their practical applications.',
        'keywords': 'data analysis, machine learning, statistics, visualization',
        'summary': 'This video provides a comprehensive overview of advanced data analysis techniques that are essential for modern data scientists and analysts.',
        'content': 'The main content covers various analytical approaches, from basic statistical methods to advanced machine learning algorithms, with practical examples and real-world applications.',
        'conclusion': 'In conclusion, mastering these data analysis techniques will significantly enhance your ability to extract valuable insights from complex datasets.',
        'date': '2024-01-15',
        'url': 'https://example.com/video/advanced-data-analysis',
        'tags': 'data science, analytics, machine learning',
        'category': 'Education',
        'difficulty': 'Advanced',
        'prerequisites': 'Basic statistics and programming knowledge',
        'objectives': 'Learn advanced data analysis techniques, Understand machine learning algorithms, Apply statistical methods to real data',
        'audience': 'Data scientists, analysts, researchers',
        'format': 'Video tutorial with practical examples',
        'length': '25 minutes',
        'chapters': 'Introduction, Statistical Methods, Machine Learning, Case Studies, Conclusion',
        'resources': 'Dataset files, Code examples, Reference materials',
        'next_steps': 'Practice with real datasets, Explore advanced algorithms, Join data science community',
        'feedback': 'Please provide feedback on this tutorial',
        'contact': 'support@example.com',
        'license': 'Creative Commons Attribution 4.0',
        'version': '1.0',
        'updated': '2024-01-15',
        'status': 'Published',
        'visibility': 'Public',
        'featured': 'Yes',
        'rating': '4.8/5.0',
        'views': '1250',
        'likes': '89',
        'comments': '23',
        'shares': '15'
    }

    # Build variables from POST or smart defaults
    variables = {}
    if request.method == "POST":
        for var in required_vars:
            variables[var] = request.POST.get(var, smart_defaults.get(var, ""))
    else:
        for var in required_vars:
            variables[var] = smart_defaults.get(var, "")

    # Compile template with error handling
    compiled = None
    compile_error = None
    compile_success = False

    try:
        compiled = engine.compile_template(template_id, variables, use_cache=False)
        compile_success = True
    except Exception as e:
        compile_error = str(e)
        # Try with minimal variables if compilation fails
        try:
            minimal_vars = {var: smart_defaults.get(var, f"Sample {var}") for var in required_vars}
            compiled = engine.compile_template(template_id, minimal_vars, use_cache=False)
            compile_success = True
            compile_error = "Compiled with fallback values"
        except Exception as fallback_error:
            compile_error = f"Compilation failed: {str(e)}"

    # Template metadata
    template_metadata = {
        'id': template_id,
        'name': tpl.get('name', template_id),
        'domain': tpl.get('domain', 'general'),
        'description': tpl.get('description', ''),
        'tags': tpl.get('tags', []),
        'parameters': tpl.get('parameters', {}),
        'type': tpl.get('template_type', 'custom'),
        'version': tpl.get('version', '1.0'),
    }

    # Variable analysis
    variable_analysis = []
    for var in required_vars:
        var_info = {
            'name': var,
            'value': variables.get(var, ''),
            'has_default': var in smart_defaults,
            'default_value': smart_defaults.get(var, ''),
            'is_required': True,
            'type_hint': 'text'
        }

        # Try to infer variable type
        if var.lower() in ['duration', 'length', 'views', 'likes', 'comments', 'shares', 'rating']:
            var_info['type_hint'] = 'number'
        elif var.lower() in ['date', 'updated', 'created']:
            var_info['type_hint'] = 'date'
        elif var.lower() in ['url', 'link', 'website']:
            var_info['type_hint'] = 'url'
        elif var.lower() in ['email', 'contact']:
            var_info['type_hint'] = 'email'
        elif 'transcript' in var.lower() or 'content' in var.lower() or 'description' in var.lower():
            var_info['type_hint'] = 'textarea'

        variable_analysis.append(var_info)

    context = {
        "template_id": template_id,
        "template": template_metadata,
        "required_vars": required_vars,
        "variables": variables,
        "variable_analysis": variable_analysis,
        "compiled": compiled,
        "compile_error": compile_error,
        "compile_success": compile_success,
        "smart_defaults": smart_defaults,
        "is_popup": request.GET.get('popup') == '1',
    }

    return render(request, "admin/engine_template_preview_enhanced.html", context)


# ================== CLI External SQLite DB Introspection (read-only) ==================

@staff_member_required
def cli_db_overview(request: HttpRequest) -> HttpResponse:
    """List tables & basic stats for the external CLI DB (if present)."""
    tables = cli_db.list_tables(include_counts=True)
    db_path = cli_db.resolve_cli_db_path()
    missing = db_path is None
    context = {
        "db_path": db_path,
        "tables": tables,
        "missing": missing,
        "max_preview": cli_db.MAX_PREVIEW_ROWS,
        "max_export": cli_db.MAX_EXPORT_ROWS,
    }
    return render(request, "admin/cli_db_overview.html", context)


@staff_member_required
def cli_db_table_preview(request: HttpRequest, table: str) -> HttpResponse:
    limit = int(request.GET.get("limit", 50))
    offset = int(request.GET.get("offset", 0))
    data = cli_db.preview_table(table, limit=limit, offset=offset)
    schema = cli_db.table_schema(table)
    if data.get("error"):
        messages.error(request, f"Preview error: {data['error']}")
    context = {
        "table": table,
        "columns": data.get("columns", []),
        "rows": data.get("rows", []),
        "error": data.get("error"),
        "limit": limit,
        "offset": offset,
        "schema": schema,
        "db_path": cli_db.resolve_cli_db_path(),
        "max_preview": cli_db.MAX_PREVIEW_ROWS,
    }
    return render(request, "admin/cli_db_table_preview.html", context)


@staff_member_required
def cli_db_table_export(request: HttpRequest, table: str) -> HttpResponse:
    fmt = request.GET.get("fmt", "json")
    try:
        limit = int(request.GET.get("limit", cli_db.MAX_EXPORT_ROWS))
    except ValueError:
        limit = cli_db.MAX_EXPORT_ROWS
    result = cli_db.export_table(table, fmt=fmt, limit=limit)
    if result.get("error"):
        messages.error(request, f"Export failed: {result['error']}")
        return redirect(reverse("admin-cli-db-overview"))
    content = result["content"]
    ext = result["ext"]
    mime = result["mime"]
    filename = f"cli_{table}.{ext}"
    resp = HttpResponse(content, content_type=mime)
    resp["Content-Disposition"] = f"attachment; filename={filename}"  # ascii safe
    return resp

# ================== Expert Prompts Overview ==================
@staff_member_required
def expert_prompts_overview(request: HttpRequest) -> HttpResponse:
    prompts = ExpertPrompt.objects.all().order_by('weight')
    missing = prompts.count() == 0
    return render(request, 'admin/expert_prompts_overview.html', {
        'prompts': prompts,
        'missing': missing,
    })


@staff_member_required
def user_template_selections_overview(request: HttpRequest) -> HttpResponse:
    qs = UserTemplateSelection.objects.select_related('user').filter(active=True)
    data = {}
    for sel in qs:
        bucket = data.setdefault(sel.user, [])
        bucket.append(sel)
    rows = [
        {
            'user': u,
            'count': len(sels),
            'template_ids': [s.template_id for s in sels],
            'latest': max(s.locked_at for s in sels) if sels else None,
        }
        for u, sels in data.items()
    ]
    rows.sort(key=lambda r: (-r['count'], str(r['user'])))
    return render(request, 'admin/user_template_selections_overview.html', {
        'rows': rows,
        'total_users': len(rows),
    })
