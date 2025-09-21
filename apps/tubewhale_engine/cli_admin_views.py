"""
CLI Integration Views for TubeWhale Admin
Professional CLI-to-Django bridge following industry best practices
"""
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import subprocess
import json
import os
from typing import Dict, Any, List
from django.core.cache import cache
from .models import CLICommandLog
import time


@staff_member_required
def cli_dashboard(request: HttpRequest) -> HttpResponse:
    """CLI集成仪表板 - 将命令行工具集成到Django管理界面"""
    context = {
        'cli_available': _check_cli_availability(),
        'recent_jobs': _get_recent_cli_jobs(),
        'system_status': _get_system_status(),
    }
    return render(request, "admin/cli_integration/dashboard.html", context)


@staff_member_required
@require_http_methods(["POST"])
@csrf_exempt
def execute_cli_command(request: HttpRequest) -> JsonResponse:
    """安全执行CLI命令的API端点"""
    try:
        command = request.POST.get('command')
        args = request.POST.getlist('args', [])
        
        # 简单速率限制 (每用户/每IP 每分钟最多5次)
        user_part = f"u{request.user.id}" if request.user.is_authenticated else 'anon'
        ip = request.META.get('REMOTE_ADDR', '-')
        bucket_key = f"cli_rate:{user_part}:{ip}:{int(time.time()//60)}"
        current = cache.get(bucket_key, 0)
        RATE_LIMIT = 5
        if current >= RATE_LIMIT:
            return JsonResponse({'success': False, 'error': 'Rate limit exceeded (5/min).'}, status=429)
        cache.incr(bucket_key) if cache.get(bucket_key) is not None else cache.set(bucket_key, 1, 70)

        # 安全命令白名单
        ALLOWED_COMMANDS = {
            'analyze_videos': ['--keyword', '--max-results', '--dry-run'],
            'generate_report': ['--format', '--output'],
            'download_audio': ['--video-id', '--quality'],
            'update_database': ['--backup'],
            'health_check': [],
        }
        
        if command not in ALLOWED_COMMANDS:
            return JsonResponse({
                'success': False,
                'error': f'Command "{command}" not allowed'
            }, status=400)
        
        # 验证参数
        allowed_args = ALLOWED_COMMANDS[command]
        filtered_args = []
        for arg in args:
            if any(arg.startswith(allowed) for allowed in allowed_args):
                filtered_args.append(arg)
        
        # 执行命令
        start = time.time()
        result = _execute_safe_cli_command(command, filtered_args)
        duration_ms = int((time.time() - start) * 1000)

        # Persist audit log
        try:
            CLICommandLog.objects.create(
                user=request.user if request.user.is_authenticated else None,
                command=command,
                args=filtered_args,
                return_code=result.get('returncode'),
                success=result.get('success', False),
                stdout_truncated=CLICommandLog.truncate(result.get('stdout')),
                stderr_truncated=CLICommandLog.truncate(result.get('stderr')),
                duration_ms=duration_ms,
                ip_address=ip,
                rate_bucket=bucket_key,
                error_flag=not result.get('success', False),
                meta={
                    'python': result.get('python_exec'),
                }
            )
        except Exception:
            pass

        return JsonResponse({
            'success': True,
            'result': {
                **result,
                'duration_ms': duration_ms,
                'rate_remaining': max(0, RATE_LIMIT - (current + 1))
            },
            'command': command,
            'args': filtered_args
        })
        
    except Exception as e:
        try:
            CLICommandLog.objects.create(
                user=request.user if request.user.is_authenticated else None,
                command=request.POST.get('command') or '<unknown>',
                args=request.POST.getlist('args', []),
                return_code=-1,
                success=False,
                stdout_truncated='',
                stderr_truncated=CLICommandLog.truncate(str(e)),
                duration_ms=None,
                ip_address=request.META.get('REMOTE_ADDR','-'),
                rate_bucket='error',
                error_flag=True,
                meta={'exception': True}
            )
        except Exception:
            pass
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@staff_member_required
def cli_job_status(request: HttpRequest, job_id: str) -> JsonResponse:
    """查询CLI作业状态"""
    try:
        status = _get_job_status(job_id)
        return JsonResponse({
            'success': True,
            'job_id': job_id,
            'status': status
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@staff_member_required
def cli_logs(request: HttpRequest) -> HttpResponse:
    """CLI执行日志查看"""
    logs = _get_cli_logs()
    context = {
        'logs': logs,
        'log_levels': ['INFO', 'WARNING', 'ERROR', 'DEBUG'],
    }
    return render(request, "admin/cli_integration/logs.html", context)


# === 私有辅助函数 ===

def _check_cli_availability() -> bool:
    """检查CLI工具是否可用"""
    try:
        # 检查主要的TubeWhale CLI脚本
        main_script = os.path.join(settings.BASE_DIR, 'main.py')
        return os.path.exists(main_script)
    except Exception:
        return False


def _get_recent_cli_jobs() -> List[Dict[str, Any]]:
    """获取最近的CLI作业"""
    # 这里可以从数据库或日志文件中获取
    return [
        {
            'id': 'job_001',
            'command': 'analyze_videos',
            'status': 'completed',
            'started_at': '2025-09-17 14:30:00',
            'completed_at': '2025-09-17 14:35:00',
            'result': '成功分析了25个视频'
        },
        {
            'id': 'job_002', 
            'command': 'generate_report',
            'status': 'running',
            'started_at': '2025-09-17 14:40:00',
            'completed_at': None,
            'result': None
        }
    ]


def _get_system_status() -> Dict[str, Any]:
    """获取系统状态"""
    return {
        'python_version': f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
        'django_version': getattr(settings, 'DJANGO_VERSION', 'Unknown'),
        'database_connected': True,  # 可以添加实际的数据库连接检查
        'api_keys_configured': _check_api_keys(),
        'disk_space': _get_disk_space(),
    }


def _check_api_keys() -> Dict[str, bool]:
    """检查API密钥配置"""
    return {
        'youtube_api': bool(getattr(settings, 'YOUTUBE_API_KEY', None)),
        'openai_api': bool(getattr(settings, 'OPENAI_API_KEY', None)),
    }


def _get_disk_space() -> str:
    """获取磁盘空间信息"""
    try:
        import shutil
        total, used, free = shutil.disk_usage(settings.BASE_DIR)
        return f"{free // (1024**3)}GB free / {total // (1024**3)}GB total"
    except Exception:
        return "Unknown"


def _execute_safe_cli_command(command: str, args: List[str]) -> Dict[str, Any]:
    """安全执行CLI命令"""
    try:
        # 构建命令
        cmd_path = os.path.join(settings.BASE_DIR, 'main.py')
        if not os.path.exists(cmd_path):
            raise FileNotFoundError("CLI script not found")
        
        # 使用虚拟环境的Python解释器
        python_path = os.path.join(settings.BASE_DIR, 'venv_tubewhale', 'bin', 'python')
        if not os.path.exists(python_path):
            python_path = 'python'  # 回退到系统Python
        
        full_command = [python_path, cmd_path, command] + args
        
        # 设置环境变量
        env = os.environ.copy()
        env['DJANGO_SETTINGS_MODULE'] = 'tubewhale_project.settings'
        
        # 执行命令 (超时保护)
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            timeout=300,  # 5分钟超时
            env=env,
            cwd=settings.BASE_DIR
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        }
        
    except subprocess.TimeoutExpired:
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': 'Command timed out after 5 minutes',
            'success': False
        }
    except Exception as e:
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': str(e),
            'success': False
        }


def _get_job_status(job_id: str) -> Dict[str, Any]:
    """获取作业状态 (模拟实现)"""
    # 实际实现中，这里应该查询数据库或作业队列
    return {
        'status': 'running',
        'progress': 65,
        'message': '正在处理视频分析...',
        'estimated_completion': '2分钟'
    }


def _get_cli_logs() -> List[Dict[str, Any]]:
    """获取CLI日志"""
    # 实际实现中，这里应该读取日志文件
    return [
        {
            'timestamp': '2025-09-17 14:35:22',
            'level': 'INFO',
            'message': '✅ 成功分析视频: dQw4w9WgXcQ',
            'source': 'analyze_videos'
        },
        {
            'timestamp': '2025-09-17 14:35:20',
            'level': 'INFO', 
            'message': '🔍 开始分析关键词: artificial intelligence',
            'source': 'search_agent'
        },
        {
            'timestamp': '2025-09-17 14:35:18',
            'level': 'WARNING',
            'message': '⚠️ API配额使用率: 85%',
            'source': 'youtube_service'
        }
    ]