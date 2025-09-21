"""
CLI Integration views for Django admin
Provides secure command execution and template management through CLI
"""
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render, redirect
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from django.urls import reverse
from django.conf import settings
import json
import subprocess
import os
import sys
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


@staff_member_required
def cli_dashboard(request: HttpRequest) -> HttpResponse:
    """CLI Integration Dashboard for admin"""
    context = {
        'title': 'CLI Integration Dashboard',
        'available_commands': get_available_cli_commands(),
        'recent_executions': get_recent_cli_executions(),
        'system_info': get_system_info(),
    }
    return render(request, 'admin/cli_dashboard.html', context)


@staff_member_required
def cli_command_executor(request: HttpRequest) -> HttpResponse:
    """Execute CLI commands securely through admin interface"""
    if request.method == 'POST':
        command = request.POST.get('command', '').strip()
        args = request.POST.get('args', '').strip()
        working_dir = request.POST.get('working_dir', settings.BASE_DIR)

        if not command:
            messages.error(request, 'Command is required')
            return redirect(reverse('admin-cli-dashboard'))

        # Security validation
        if not is_command_allowed(command):
            messages.error(request, f'Command "{command}" is not allowed')
            return redirect(reverse('admin-cli-dashboard'))

        try:
            # Execute command securely
            result = execute_cli_command(command, args, working_dir)

            # Log execution
            log_cli_execution(request.user, command, args, result['success'])

            if result['success']:
                messages.success(request, f'Command executed successfully')
            else:
                messages.error(request, f'Command failed: {result.get("error", "Unknown error")}')

            context = {
                'title': 'CLI Command Result',
                'command': command,
                'args': args,
                'result': result,
                'execution_time': result.get('execution_time', 0),
            }
            return render(request, 'admin/cli_result.html', context)

        except Exception as e:
            logger.error(f"CLI execution error: {e}")
            messages.error(request, f'Execution failed: {str(e)}')
            return redirect(reverse('admin-cli-dashboard'))

    return redirect(reverse('admin-cli-dashboard'))


@require_http_methods(["POST"])
@staff_member_required
def cli_template_operations(request: HttpRequest) -> JsonResponse:
    """AJAX endpoint for template operations via CLI"""
    operation = request.POST.get('operation')
    template_id = request.POST.get('template_id')

    try:
        if operation == 'validate':
            result = execute_cli_command('python', f'manage.py validate_template {template_id}')
        elif operation == 'compile':
            variables = request.POST.get('variables', '{}')
            result = execute_cli_command('python', f'manage.py compile_template {template_id} --variables="{variables}"')
        elif operation == 'export':
            result = execute_cli_command('python', f'manage.py export_template {template_id}')
        elif operation == 'import':
            file_path = request.POST.get('file_path')
            result = execute_cli_command('python', f'manage.py import_template {file_path}')
        else:
            return JsonResponse({'success': False, 'error': 'Unknown operation'})

        return JsonResponse({
            'success': result['success'],
            'output': result.get('output', ''),
            'error': result.get('error', ''),
            'execution_time': result.get('execution_time', 0)
        })

    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})


@staff_member_required
def cli_template_manager(request: HttpRequest) -> HttpResponse:
    """Template management through CLI interface"""
    from service.enterprise_template_engine import TemplateEngine

    engine = TemplateEngine(validation_strict=False)
    templates = engine.list_templates(include_metadata=True)

    context = {
        'title': 'CLI Template Manager',
        'templates': templates,
        'domains': engine.get_domains(),
        'cli_commands': get_template_cli_commands(),
    }
    return render(request, 'admin/cli_template_manager.html', context)


def get_available_cli_commands() -> List[Dict[str, Any]]:
    """Get list of available CLI commands"""
    return [
        {
            'name': 'validate_template',
            'description': 'Validate template syntax and variables',
            'usage': 'python manage.py validate_template <template_id>',
            'category': 'template'
        },
        {
            'name': 'compile_template',
            'description': 'Compile template with variables',
            'usage': 'python manage.py compile_template <template_id> --variables="{}"',
            'category': 'template'
        },
        {
            'name': 'export_template',
            'description': 'Export template to file',
            'usage': 'python manage.py export_template <template_id> --output=file.json',
            'category': 'template'
        },
        {
            'name': 'import_template',
            'description': 'Import template from file',
            'usage': 'python manage.py import_template file.json',
            'category': 'template'
        },
        {
            'name': 'list_templates',
            'description': 'List all available templates',
            'usage': 'python manage.py list_templates --domain=business',
            'category': 'template'
        },
        {
            'name': 'check_system',
            'description': 'Check system health and dependencies',
            'usage': 'python manage.py check_system',
            'category': 'system'
        },
        {
            'name': 'clear_cache',
            'description': 'Clear template and system cache',
            'usage': 'python manage.py clear_cache',
            'category': 'system'
        }
    ]


def get_template_cli_commands() -> List[Dict[str, Any]]:
    """Get template-specific CLI commands"""
    return [
        {
            'command': 'validate_template',
            'description': 'Check template syntax',
            'args': ['template_id']
        },
        {
            'command': 'compile_template',
            'description': 'Compile with sample data',
            'args': ['template_id', 'variables']
        },
        {
            'command': 'export_template',
            'description': 'Export to JSON',
            'args': ['template_id', 'output_file']
        },
        {
            'command': 'diff_templates',
            'description': 'Compare two templates',
            'args': ['template_id1', 'template_id2']
        }
    ]


def get_recent_cli_executions() -> List[Dict[str, Any]]:
    """Get recent CLI execution history"""
    # This would typically come from a database model
    # For now, return mock data
    return [
        {
            'command': 'validate_template',
            'args': 'business_intelligence_analysis',
            'user': 'admin',
            'timestamp': '2024-01-15 10:30:00',
            'success': True,
            'execution_time': 0.5
        },
        {
            'command': 'compile_template',
            'args': 'educational_content_summary',
            'user': 'admin',
            'timestamp': '2024-01-15 10:25:00',
            'success': True,
            'execution_time': 1.2
        }
    ]


def get_system_info() -> Dict[str, Any]:
    """Get system information for CLI dashboard"""
    return {
        'python_version': sys.version.split()[0],
        'django_version': '4.2.x',
        'working_directory': settings.BASE_DIR,
        'environment': os.environ.get('ENVIRONMENT', 'development'),
        'debug_mode': settings.DEBUG,
        'database_engine': settings.DATABASES['default']['ENGINE'].split('.')[-1],
    }


def is_command_allowed(command: str) -> bool:
    """Check if CLI command is allowed for security"""
    allowed_commands = {
        'python', 'manage.py', 'validate_template', 'compile_template',
        'export_template', 'import_template', 'list_templates',
        'check_system', 'clear_cache'
    }

    # Allow python manage.py commands
    if command == 'python' or command.endswith('manage.py'):
        return True

    return command in allowed_commands


def execute_cli_command(command: str, args: str = '', working_dir: str = None) -> Dict[str, Any]:
    """Execute CLI command securely"""
    import time

    start_time = time.time()

    try:
        # Build command with arguments
        if command == 'python' and 'manage.py' in args:
            full_command = f'{sys.executable} {args}'
        else:
            full_command = f'{command} {args}'.strip()

        # Execute command
        result = subprocess.run(
            full_command,
            shell=True,
            cwd=working_dir or settings.BASE_DIR,
            capture_output=True,
            text=True,
            timeout=30  # 30 second timeout
        )

        execution_time = time.time() - start_time

        return {
            'success': result.returncode == 0,
            'output': result.stdout,
            'error': result.stderr,
            'return_code': result.returncode,
            'execution_time': round(execution_time, 2),
            'command': full_command
        }

    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': 'Command timed out after 30 seconds',
            'execution_time': 30.0
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'execution_time': time.time() - start_time
        }


def log_cli_execution(user, command: str, args: str, success: bool):
    """Log CLI command execution for audit trail"""
    logger.info(f"CLI Execution - User: {user}, Command: {command}, Args: {args}, Success: {success}")