import subprocess
import os
import time
from typing import List, Dict, Any, Optional

# Django imports with fallback for testing environments
try:
    from django.conf import settings
    from django.utils import timezone
    DJANGO_AVAILABLE = True
except ImportError:
    DJANGO_AVAILABLE = False
    # Fallback settings for testing without Django
    class FallbackSettings:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        CLI_PERSIST_ENABLED = False
    settings = FallbackSettings()

try:
    from apps.templates_app.models import CLIExecution  # type: ignore
except Exception:  # pragma: no cover - model import soft fail (migrations not applied yet)
    CLIExecution = None  # type: ignore

ALLOWED_COMMANDS = {
    'analyze_videos': ['--keyword', '--max-results', '--dry-run'],
    'generate_report': ['--format', '--output'],
    'download_audio': ['--video-id', '--quality'],
    'update_database': ['--backup'],
    'health_check': [],
}

def run_cli(command: str, args: List[str], *, prompt_final: Optional[str] = None,
            template_id: str = "", expert_slug: str = "", meta: Optional[Dict[str, Any]] = None,
            persist: Optional[bool] = None) -> Dict[str, Any]:
    """Execute a whitelisted CLI command.

    Extended to optionally persist execution metadata when settings.CLI_PERSIST_ENABLED is True
    or when persist flag explicitly provided.
    """
    if command not in ALLOWED_COMMANDS:
        result = {'success': False, 'error': 'command_not_allowed'}
        _maybe_persist(result, command, [], prompt_final, template_id, expert_slug, meta, status_override="error")
        return result
    filtered = []
    allow = ALLOWED_COMMANDS[command]
    for a in args:
        if any(a.startswith(k) for k in allow):
            filtered.append(a)
    # Prefer main.py but gracefully fallback to cli.py so the existing CLI isn't forced to rename.
    script_candidates = ['main.py', 'cli.py']
    script_path = None
    for cand in script_candidates:
        candidate_path = os.path.join(settings.BASE_DIR, cand)
        if os.path.exists(candidate_path):
            script_path = candidate_path
            break
    python_path = os.path.join(settings.BASE_DIR, 'venv_tubewhale', 'bin', 'python')
    if not os.path.exists(python_path):
        python_path = 'python'
    if script_path is None:
        result = {'success': False, 'error': 'cli_entry_missing', 'searched': script_candidates}
        _maybe_persist(result, command, filtered, prompt_final, template_id, expert_slug, meta, status_override="missing_entry")
        return result
    # Ensure meta captures which entry script was used without mutating caller-provided dict unexpectedly.
    if meta is None:
        meta_local = {'entry_script': os.path.basename(script_path)}
    else:
        meta_local = {**meta, 'entry_script': os.path.basename(script_path)}
    # 构建命令参数，支持backend模式和prompt_final
    full_cmd = [python_path, script_path]
    
    # 对于所有命令，启用backend模式和JSON输出
    full_cmd.extend(['--backend-mode', '--output-format', 'json'])
    
    # 如果有prompt_final，添加到命令中
    if prompt_final:
        full_cmd.extend(['--prompt-final', prompt_final])
    
    # 将命令转换为对应的参数
    if command == 'analyze_videos':
        # analyze_videos映射到默认的视频分析流程，使用filtered参数
        full_cmd.extend(filtered)
    elif command == 'generate_report':
        # 暂时作为普通模式处理，后续可扩展
        full_cmd.extend(filtered)
    elif command == 'download_audio':
        # 暂时作为普通模式处理，后续可扩展  
        full_cmd.extend(filtered)
    elif command == 'update_database':
        # 暂时作为普通模式处理，后续可扩展
        full_cmd.extend(filtered)
    elif command == 'health_check':
        # 健康检查可以作为config-test处理
        full_cmd.append('--config-test')
    else:
        # 其他命令直接传递过滤后的参数
        full_cmd.extend(filtered)
    env = os.environ.copy()
    env['DJANGO_SETTINGS_MODULE'] = 'tubewhale_project.settings'
    started = time.time()
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=300, cwd=settings.BASE_DIR, env=env)
        payload = {
            'success': result.returncode == 0,
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'command': command,
            'args': filtered,
            'entry_script': os.path.basename(script_path),
        }
        _maybe_persist(payload, command, filtered, prompt_final, template_id, expert_slug, meta_local,
                       status_override="success" if payload['success'] else "error", started=started)
        return payload
    except subprocess.TimeoutExpired:
        payload = {'success': False, 'error': 'timeout', 'command': command, 'args': filtered}
        _maybe_persist(payload, command, filtered, prompt_final, template_id, expert_slug, meta_local, status_override="timeout", started=started)
        return payload
    except Exception as e:
        payload = {'success': False, 'error': str(e), 'command': command, 'args': filtered}
        _maybe_persist(payload, command, filtered, prompt_final, template_id, expert_slug, meta_local, status_override="error", started=started)
        return payload


def _maybe_persist(payload: Dict[str, Any], command: str, args: List[str], prompt_final: Optional[str],
                   template_id: str, expert_slug: str, meta: Optional[Dict[str, Any]],
                   status_override: str, started: Optional[float] = None):  # pragma: no cover - side-effect only
    if not DJANGO_AVAILABLE or CLIExecution is None:
        return
    try:
        if meta is None:
            meta = {}
        enabled = getattr(settings, 'CLI_PERSIST_ENABLED', True)
        if enabled is False:
            return
        duration_ms = None
        if started is not None:
            duration_ms = int((time.time() - started) * 1000)
        CLIExecution.objects.create(
            command=command,
            args=args,
            template_id=template_id or "",
            expert_slug=expert_slug or "",
            prompt_final=prompt_final or "",
            stdout=payload.get('stdout', '')[:65535],
            stderr=payload.get('stderr', '')[:65535],
            status=status_override,
            returncode=payload.get('returncode'),
            duration_ms=duration_ms,
            meta=meta,
        )
    except Exception:
        return
