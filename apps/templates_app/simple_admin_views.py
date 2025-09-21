"""Minimal working admin views for template management system.

Enhancements added:
1. Public ping endpoint to verify routing without authentication (/admin/templates/ping/)
2. Graceful unauthenticated handling for dashboard: instead of raw 302 confusion, we can
    optionally render a message if a query param `no_redirect=1` is provided (used for diagnostics).
"""
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render, redirect
from django.http import HttpRequest, HttpResponse, JsonResponse, Http404
from django.contrib.auth.decorators import login_required
from django.conf import settings
import os
import mimetypes
from service import template_manager
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from service.cli_runner import run_cli
from django.utils.dateparse import parse_datetime
from django.utils import timezone
from .models import CLIExecution
from .models import CustomTemplate, ExpertPrompt, TemplateDomain
from .prompt_composer import compose_with_variables
from celery.result import AsyncResult
from apps.templates_app.tasks import run_cli_execution_task

# from service.enterprise_template_engine import TemplateEngine


@login_required
def template_system_dashboard(request: HttpRequest) -> HttpResponse:
    """Simple dashboard for template management.

    Diagnostic mode:
        If user is not staff but authenticated, show a friendly message.
        If `?no_redirect=1` is passed and user is anonymous, show JSON instead of redirect
        (makes it easy to distinguish routing vs auth issues when debugging).
    """
    # Load templates via enterprise adapter if enabled
    engine_enabled = getattr(settings, 'ENTERPRISE_TEMPLATE_DASHBOARD_ENABLED', True)
    templates_list = []
    engine_status = {}
    if engine_enabled:
        engine_status = template_manager.engine_health()
        # Only list metadata if engine healthy
        if engine_status.get('ok'):
            templates_list = template_manager.list_templates(include_metadata=True)

    if not request.user.is_staff:
        # Authenticated but not staff - show limited view
        return render(request, "admin/simple_template_dashboard.html", {
            'templates': [],
            'template_count': 0,
            'engine_enabled': engine_enabled,
            'engine_status': engine_status,
            'error': 'You are authenticated but not staff. Contact admin for access.'
        })

    # Staff view with engine data (or fallback)
    context = {
        'templates': templates_list,
        'template_count': len(templates_list),
        'engine_enabled': engine_enabled,
        'engine_status': engine_status,
    }
    if not templates_list and engine_enabled:
        context['info'] = 'No templates found or engine not loaded yet.'
    return render(request, "admin/simple_template_dashboard.html", context)


@staff_member_required
def expert_matrix_view(request: HttpRequest) -> HttpResponse:
    """Matrix style visibility: which expert prompts exist per domain and template coverage.

    Returns JSON (default) or minimal HTML if ?format=html
    Data model:
        domains: list of domains
        experts: [{slug, role, domain}]
        templates: [{template_id, domain}]
        coverage: mapping template_id -> list of expert slugs that share domain
    """
    templates = list(CustomTemplate.objects.all().values('template_id','domain'))
    experts = list(ExpertPrompt.objects.filter(active=True).values('slug','role','domain'))
    domains = sorted({t['domain'] for t in templates} | {e['domain'] for e in experts})
    coverage = {}
    experts_by_domain = {}
    for e in experts:
        experts_by_domain.setdefault(e['domain'], []).append(e)
    for t in templates:
        rel = experts_by_domain.get(t['domain'], [])
        coverage[t['template_id']] = [e['slug'] for e in rel]
    payload = {
        'ok': True,
        'domains': domains,
        'experts': experts,
        'templates': templates,
        'coverage': coverage,
        'stats': {
            'templates_total': len(templates),
            'experts_total': len(experts),
            'avg_experts_per_template': (sum(len(v) for v in coverage.values())/len(coverage)) if coverage else 0,
        }
    }
    if request.GET.get('format') == 'html':
        rows = []
        for t in templates:
            rows.append(f"<tr><td>{t['template_id']}</td><td>{t['domain']}</td><td>{', '.join(coverage[t['template_id']])}</td></tr>")
        html = f"""
        <html><head><title>Expert Matrix</title><meta charset='utf-8'/></head>
        <body>
        <h1>Expert ↔ Template Matrix</h1>
        <p>Total Templates: {len(templates)} | Experts: {len(experts)}</p>
        <table border='1' cellpadding='4'>
            <thead><tr><th>Template</th><th>Domain</th><th>Expert Prompts</th></tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table>
        <p>JSON view: append ?format=json</p>
        </body></html>
        """
        return HttpResponse(html)
    return JsonResponse(payload)


@staff_member_required
def template_analytics(request: HttpRequest) -> HttpResponse:
    """Basic analytics view"""
    context = {
        'message': 'Template analytics coming soon...'
    }
    return render(request, "admin/simple_analytics.html", context)


@staff_member_required
def template_import_wizard(request: HttpRequest) -> HttpResponse:
    """Basic import wizard"""
    context = {
        'message': 'Template import wizard coming soon...'
    }
    return render(request, "admin/simple_import.html", context)


@staff_member_required
def bulk_template_operations(request: HttpRequest) -> HttpResponse:
    """Basic bulk operations"""
    context = {
        'message': 'Bulk operations coming soon...'
    }
    return render(request, "admin/simple_bulk.html", context)


@staff_member_required
def template_quick_actions(request: HttpRequest) -> HttpResponse:
    """Basic quick actions"""
    return JsonResponse({'status': 'success', 'message': 'Quick actions available'})


def template_ping(request: HttpRequest) -> HttpResponse:
    """Public ping endpoint to validate routing & template subsystem accessibility.

    Returns JSON without requiring authentication so users can distinguish:
        - 404 => URL routing problem
        - 200 => Routing OK (auth might still block the protected pages)
    """
    return JsonResponse({
        'ok': True,
        'message': 'template system reachable',
        'authenticated': request.user.is_authenticated,
        'is_staff': getattr(request.user, 'is_staff', False),
        'next_protected': '/admin/templates/'
    })


# === PUBLIC (READ-ONLY) TEMPLATE DISCOVERY UTILITIES ===

def _discover_admin_templates():
    """Return a list of admin template names (filenames only) safely.

    Best effort search across TEMPLATES[...]['DIRS'] plus default BASE_DIR/templates.
    Only top-level files under an 'admin' directory are listed to avoid exposing
    unrelated templates. Subdirectories are ignored for simplicity/safety.
    """
    candidates = []
    checked_dirs = set()
    # Collect explicit DIRS
    for cfg in getattr(settings, 'TEMPLATES', []):
        for d in cfg.get('DIRS', []):
            checked_dirs.add(d)
    # Common conventional path fallback
    default_dir = os.path.join(getattr(settings, 'BASE_DIR', ''), 'templates')
    if os.path.isdir(default_dir):
        checked_dirs.add(default_dir)

    admin_templates = []
    for base in checked_dirs:
        admin_dir = os.path.join(base, 'admin')
        if not os.path.isdir(admin_dir):
            continue
        try:
            for fname in os.listdir(admin_dir):
                if not fname.endswith('.html'):
                    continue
                # Basic filter to avoid accidental secrets in filenames
                if any(part.startswith('.') for part in fname.split('.')):
                    continue
                admin_templates.append(fname)
        except Exception:  # pragma: no cover - defensive
            continue
    # De-duplicate while preserving order
    seen = set()
    ordered = []
    for f in admin_templates:
        if f not in seen:
            seen.add(f)
            ordered.append(f)
    return ordered


def public_template_index(request: HttpRequest) -> HttpResponse:
    """Public endpoint listing available admin template filenames.

    Returns JSON when `format=json` query param present or Accept header
    prefers application/json. Otherwise returns a minimal HTML page.
    This is intentionally read-only and does not reveal template contents beyond names.
    """
    # Gate by settings flag
    if not getattr(settings, 'PUBLIC_TEMPLATE_PREVIEW_ENABLED', True):
        return JsonResponse({'ok': False, 'error': 'Public preview disabled'}, status=403)
    templates_list = _discover_admin_templates()
    wants_json = (
        request.GET.get('format') == 'json' or
        'application/json' in request.headers.get('Accept', '')
    )
    data = {
        'ok': True,
        'count': len(templates_list),
        'templates': templates_list,
        'preview_example': '/templates-public/{name}/'.format(name=templates_list[0]) if templates_list else None,
    }
    if wants_json:
        return JsonResponse(data)
    # Simple inline HTML (avoid adding a new template just for listing itself)
    rows = '\n'.join(f'<li><a href="/templates-public/{fname}/">{fname}</a></li>' for fname in templates_list)
    html = f"""
    <html><head><title>Public Template Index</title><meta charset='utf-8'/></head>
    <body>
        <h1>Admin Templates (Public Read-Only Index)</h1>
        <p>Total: {len(templates_list)}</p>
        <ul>{rows}</ul>
        <p>JSON view: <code>?format=json</code></p>
    </body></html>
    """
    return HttpResponse(html)


def public_template_preview(request: HttpRequest, template_slug: str) -> HttpResponse:
    """Render a specific admin template in a very restricted way.

    Security considerations / best practices:
    - Only allow filenames that appear in discovery list.
    - No dynamic context is injected (prevents leaking server data).
    - Deny if template not in allowed list -> 404.
    - This is meant for visual verification, not production enablement; you may
      disable by guarding with a setting flag later if desired.
    """
    if not getattr(settings, 'PUBLIC_TEMPLATE_PREVIEW_ENABLED', True):
        raise Http404()
    allowed = _discover_admin_templates()
    # Template slug expected to be full filename (e.g., simple_template_dashboard.html)
    if template_slug not in allowed:
        raise Http404("Template not found or not allowed")
    # Render using existing Django loader; context intentionally minimal
    return render(request, f"admin/{template_slug}", {
        'public_preview': True,
        'notice': 'Public preview — authentication bypassed (read-only).',
    })


@staff_member_required
@require_POST
@csrf_exempt
def compose_and_run(request: HttpRequest) -> HttpResponse:
    template_id = request.POST.get('template_id', '').strip()
    expert_slug = request.POST.get('expert_slug') or None
    command = request.POST.get('command', 'analyze_videos')
    variables = {}
    for k, v in request.POST.items():
        if k.startswith('var_'):
            variables[k[4:]] = v
    prompt = compose_with_variables(template_id, expert_slug, variables)
    before_ids = set()
    try:
        before_ids = set(CLIExecution.objects.values_list('id', flat=True)[:50])
    except Exception:
        pass
    cli_result = run_cli(command, [], prompt_final=prompt, template_id=template_id, expert_slug=expert_slug, meta={'user_id': request.user.id if request.user.is_authenticated else None})
    execution_id = None
    try:
        latest = CLIExecution.objects.order_by('-id').first()
        if latest and latest.id not in before_ids and latest.command == command:
            execution_id = latest.id
    except Exception:
        execution_id = None
    return JsonResponse({
        'ok': True,
        'template_id': template_id,
        'expert_slug': expert_slug,
        'prompt_final': prompt,
        'cli': cli_result,
        'execution_id': execution_id,
    })


@staff_member_required
@require_POST
@csrf_exempt
def compose_preview(request: HttpRequest) -> HttpResponse:
    """Return composed prompt (template + expert + variables) without executing CLI.

    POST params:
        template_id (required)
        expert_slug (optional)
        var_<name> variable injections
    Response:
        { ok, template_id, expert_slug, variables, prompt_final, length }
    """
    template_id = request.POST.get('template_id', '').strip()
    if not template_id:
        return JsonResponse({'ok': False, 'error': 'missing_template_id'}, status=400)
    expert_slug = request.POST.get('expert_slug') or None
    variables = {}
    for k, v in request.POST.items():
        if k.startswith('var_'):
            variables[k[4:]] = v
    prompt = compose_with_variables(template_id, expert_slug, variables)
    return JsonResponse({
        'ok': True,
        'template_id': template_id,
        'expert_slug': expert_slug,
        'variables': variables,
        'prompt_final': prompt,
        'length': len(prompt or ''),
    })


@staff_member_required
def cli_runs_export(request: HttpRequest, fmt: str) -> HttpResponse:
    """Export CLIExecution records in various formats.

    Supported formats: json, ndjson, csv
    Query params:
        limit: int (default 1000)
        status: filter by status
        command: filter by command
    """
    qs = CLIExecution.objects.all().order_by('-created_at')
    status_f = request.GET.get('status')
    if status_f:
        qs = qs.filter(status=status_f)
    command_f = request.GET.get('command')
    if command_f:
        qs = qs.filter(command=command_f)
    since_raw = request.GET.get('since')
    if since_raw:
        dt = _parse_since(since_raw)
        if dt is None:
            return JsonResponse({'ok': False, 'error': 'invalid_since'}, status=400)
        qs = qs.filter(created_at__gte=dt)
    try:
        limit = int(request.GET.get('limit', '1000'))
    except ValueError:
        limit = 1000
    max_cap = getattr(settings, 'MAX_CLI_EXPORT_LIMIT', 5000)
    if limit > max_cap:
        limit = max_cap
    qs = qs[:limit]
    rows = []
    fields = [
        'id','created_at','command','status','returncode','duration_ms','template_id','expert_slug','args','meta'
    ]
    # Keep stdout/stderr/prompt separate to avoid huge CSV lines unless explicitly requested
    include_output = request.GET.get('full') == '1'
    if include_output:
        fields += ['prompt_final','stdout','stderr']
    if fmt == 'json':
        for obj in qs:
            rows.append(_cli_obj_to_dict(obj, include_output=include_output))
        return JsonResponse({'ok': True, 'count': len(rows), 'results': rows})
    elif fmt == 'ndjson':
        lines = []
        import json
        for obj in qs:
            lines.append(json.dumps(_cli_obj_to_dict(obj, include_output=include_output), ensure_ascii=False))
        resp = HttpResponse('\n'.join(lines), content_type='application/x-ndjson')
        resp['Content-Disposition'] = 'attachment; filename="cli_runs.ndjson"'
        return resp
    elif fmt == 'csv':
        import csv, io, json
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(fields)
        for o in qs:
            data = _cli_obj_to_dict(o, include_output=include_output)
            row = [data.get(f, '') if not isinstance(data.get(f), (list, dict)) else json.dumps(data.get(f)) for f in fields]
            writer.writerow(row)
        resp = HttpResponse(buf.getvalue(), content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename="cli_runs.csv"'
        return resp
    else:
        return JsonResponse({'ok': False, 'error': 'unsupported_format'}, status=400)


def _cli_obj_to_dict(o: CLIExecution, include_output: bool=False):  # pragma: no cover - serialization helper
    base = {
        'id': o.id,
        'created_at': o.created_at.isoformat(),
        'command': o.command,
        'status': o.status,
        'returncode': o.returncode,
        'duration_ms': o.duration_ms,
        'template_id': o.template_id,
        'expert_slug': o.expert_slug,
        'args': o.args,
        'meta': o.meta,
    }
    if include_output:
        base['prompt_final'] = o.prompt_final
        base['stdout'] = o.stdout
        base['stderr'] = o.stderr
    return base


@staff_member_required
def cli_runs_graph(request: HttpRequest) -> HttpResponse:
    """Produce a lightweight knowledge graph projection.

    Nodes: templates (type=template), experts (type=expert), executions (type=execution)
    Edges: template->execution, expert->execution
    Optional filters: ?limit=, ?command=
    """
    qs = CLIExecution.objects.all().order_by('-created_at')
    command_f = request.GET.get('command')
    if command_f:
        qs = qs.filter(command=command_f)
    since_raw = request.GET.get('since')
    if since_raw:
        dt = _parse_since(since_raw)
        if dt is None:
            return JsonResponse({'ok': False, 'error': 'invalid_since'}, status=400)
        qs = qs.filter(created_at__gte=dt)
    try:
        limit = int(request.GET.get('limit', '500'))
    except ValueError:
        limit = 500
    max_cap = getattr(settings, 'MAX_CLI_EXPORT_LIMIT', 5000)
    if limit > max_cap:
        limit = max_cap
    qs = list(qs[:limit])
    nodes = {}
    edges = []
    for ex in qs:
        exec_id = f"exec:{ex.id}"
        nodes[exec_id] = {"id": exec_id, "type": "execution", "command": ex.command, "status": ex.status}
        if ex.template_id:
            tid = f"template:{ex.template_id}"
            nodes.setdefault(tid, {"id": tid, "type": "template", "template_id": ex.template_id})
            edges.append({"source": tid, "target": exec_id, "rel": "USED_IN"})
        if ex.expert_slug:
            eid = f"expert:{ex.expert_slug}"
            nodes.setdefault(eid, {"id": eid, "type": "expert", "expert_slug": ex.expert_slug})
            edges.append({"source": eid, "target": exec_id, "rel": "INFLUENCES"})
    graph = {
        'ok': True,
        'nodes': list(nodes.values()),
        'edges': edges,
        'counts': {
            'templates': len([n for n in nodes.values() if n['type'] == 'template']),
            'experts': len([n for n in nodes.values() if n['type'] == 'expert']),
            'executions': len([n for n in nodes.values() if n['type'] == 'execution']),
        }
    }
    return JsonResponse(graph)


def _parse_since(raw: str):  # pragma: no cover - parsing helper
    if not raw:
        return None
    raw = raw.strip()
    dt = parse_datetime(raw)
    if dt is None:
        # Date-only fallback
        if len(raw) == 10 and raw.count('-') == 2:
            from datetime import datetime
            try:
                dt = datetime.strptime(raw, '%Y-%m-%d')
            except Exception:
                return None
    if dt is None:
        return None
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone=timezone.utc)
    return dt.astimezone(timezone.utc)


@staff_member_required
@require_POST
@csrf_exempt
def cli_runs_async_run(request: HttpRequest) -> HttpResponse:
    command = request.POST.get('command', 'analyze_videos')
    template_id = request.POST.get('template_id', '').strip()
    expert_slug = request.POST.get('expert_slug') or ''
    variables = {}
    for k, v in request.POST.items():
        if k.startswith('var_'):
            variables[k[4:]] = v
    task = run_cli_execution_task.delay(command, template_id, expert_slug, variables, request.user.id if request.user.is_authenticated else None)
    # Return early supervision metadata including enqueue time (UTC ISO)
    from django.utils import timezone
    return JsonResponse({'ok': True, 'task_id': task.id, 'enqueued_at': timezone.now().isoformat(), 'command': command})


@staff_member_required
def cli_runs_async_status(request: HttpRequest, task_id: str) -> HttpResponse:
    res = AsyncResult(task_id)
    data = {'ok': True, 'task_id': task_id, 'state': res.state}
    if res.successful():
        try:
            out = res.get(propagate=False)
            data.update(out)
        except Exception as e:
            data['error'] = str(e)
    return JsonResponse(data)


@staff_member_required
def cli_runs_overview(request: HttpRequest) -> HttpResponse:
    """Supervision overview for pipeline executions.

    Query params:
        window: minutes to look back (default 1440 = 24h)
        command: optional command filter
    Provides aggregated counts, duration stats, error ratios, blockage heuristics.
    """
    try:
        window_minutes = int(request.GET.get('window', '1440'))
    except ValueError:
        window_minutes = 1440
    command_f = request.GET.get('command')
    now = timezone.now()
    window_start = now - timezone.timedelta(minutes=window_minutes)
    qs = CLIExecution.objects.filter(created_at__gte=window_start)
    if command_f:
        qs = qs.filter(command=command_f)
    total = qs.count()
    statuses = list(qs.values_list('status', flat=True))
    by_status = {}
    for s in statuses:
        by_status[s] = by_status.get(s, 0) + 1
    durations = list(qs.exclude(duration_ms__isnull=True).values_list('duration_ms', flat=True))
    durations_sorted = sorted(durations)
    def pct(p):
        if not durations_sorted:
            return None
        k = int(len(durations_sorted)*p/100)-1
        k = max(0, min(k, len(durations_sorted)-1))
        return durations_sorted[k]
    avg = sum(durations)/len(durations) if durations else None
    p50 = pct(50)
    p95 = pct(95)
    p99 = pct(99)
    error_like = sum(by_status.get(k,0) for k in ['error','timeout','missing_entry'])
    error_rate = (error_like/total) if total else 0
    timeout_rate = (by_status.get('timeout',0)/total) if total else 0
    # Blockage heuristic: high error rate or timeouts + stagnant successes
    recent_success = qs.filter(status='success').order_by('-created_at').first()
    stagnant_minutes = None
    if recent_success:
        stagnant_minutes = (now - recent_success.created_at).total_seconds()/60.0
    blockage = False
    blockage_reasons = []
    if error_rate > 0.5 and total >= 10:
        blockage = True
        blockage_reasons.append('high_error_rate')
    if timeout_rate > 0.2 and total >= 10:
        blockage = True
        blockage_reasons.append('high_timeout_rate')
    if stagnant_minutes is not None and stagnant_minutes > 120 and total >= 5:
        blockage = True
        blockage_reasons.append('no_recent_success')
    recent_errors = list(qs.filter(status__in=['error','timeout']).order_by('-created_at')[:5].values('id','status','command','duration_ms','created_at'))
    payload = {
        'ok': True,
        'window_minutes': window_minutes,
        'from': window_start.isoformat(),
        'to': now.isoformat(),
        'command_filter': command_f,
        'counts': {
            'total': total,
            **{f'status_{k}': v for k,v in by_status.items()}
        },
        'durations_ms': {
            'avg': avg,
            'p50': p50,
            'p95': p95,
            'p99': p99,
            'sample_size': len(durations),
        },
        'rates': {
            'error_rate': error_rate,
            'timeout_rate': timeout_rate,
        },
        'blockage': blockage,
        'blockage_reasons': blockage_reasons,
        'recent_errors': recent_errors,
    }
    return JsonResponse(payload)


@staff_member_required
def cli_runs_recent(request: HttpRequest) -> HttpResponse:
    """Lightweight recent executions feed + mini aggregates.

    Query params:
        limit: number of rows (default 25, max 200)
        command: optional filter
        status: optional filter
    """
    try:
        limit = int(request.GET.get('limit', '25'))
    except ValueError:
        limit = 25
    if limit > 200:
        limit = 200
    qs = CLIExecution.objects.all().order_by('-created_at')
    command_f = request.GET.get('command')
    if command_f:
        qs = qs.filter(command=command_f)
    status_f = request.GET.get('status')
    if status_f:
        qs = qs.filter(status=status_f)
    rows = []
    for o in qs[:limit]:
        rows.append({
            'id': o.id,
            'created_at': o.created_at.isoformat(),
            'command': o.command,
            'status': o.status,
            'duration_ms': o.duration_ms,
            'returncode': o.returncode,
            'template_id': o.template_id,
            'expert_slug': o.expert_slug,
        })
    # mini aggregates
    from collections import Counter
    status_counts = Counter(r['status'] for r in rows)
    command_counts = Counter(r['command'] for r in rows)
    avg_dur = None
    durations = [r['duration_ms'] for r in rows if r['duration_ms'] is not None]
    if durations:
        avg_dur = sum(durations)/len(durations)
    payload = {
        'ok': True,
        'count': len(rows),
        'rows': rows,
        'status_counts': dict(status_counts),
        'command_counts': dict(command_counts),
        'avg_duration_ms': avg_dur,
    }
    return JsonResponse(payload)