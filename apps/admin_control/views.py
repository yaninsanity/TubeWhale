"""
Admin Control Panel Views
Central management interface for CLI operations and job control
"""

from django.shortcuts import render, redirect
from django.contrib.admin.views.decorators import staff_member_required
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.utils.translation import gettext_lazy as _
from django.contrib import messages
from django.core.paginator import Paginator
import json
import subprocess
import os
from apps.tubewhale_engine.models import CLICommandLog
from apps.user_app.models import User


@staff_member_required
def admin_cli_control_panel(request):
    """Central CLI Control Panel for Administrators"""
    
    # Get recent CLI executions
    recent_executions = CLICommandLog.objects.select_related('user').order_by('-created_at')[:10]
    
    # Get system statistics
    total_users = User.objects.count()
    tier_stats = {
        'basic': User.objects.filter(tier='basic').count(),
        'standard': User.objects.filter(tier='standard').count(), 
        'premium': User.objects.filter(tier='premium').count(),
    }
    
    # Get recent job statistics
    job_stats = {
        'total_jobs': CLICommandLog.objects.count(),
        'successful_jobs': CLICommandLog.objects.filter(status='completed').count(),
        'failed_jobs': CLICommandLog.objects.filter(status='failed').count(),
        'running_jobs': CLICommandLog.objects.filter(status='running').count(),
    }
    
    context = {
        'title': _('Admin CLI Control Panel'),
        'recent_executions': recent_executions,
        'total_users': total_users,
        'tier_stats': tier_stats,
        'job_stats': job_stats,
    }
    
    return render(request, 'admin_control/cli_control_panel.html', context)


@staff_member_required 
@require_POST
def admin_execute_cli_command(request):
    """Execute CLI command from admin interface"""
    try:
        data = json.loads(request.body)
        command = data.get('command', '').strip()
        
        if not command:
            return JsonResponse({'success': False, 'error': 'Command cannot be empty'})
        
        # Security check - only allow specific commands
        allowed_commands = [
            'tubewhale',
            'python manage.py',
            'celery',
            'docker',
        ]
        
        if not any(command.startswith(cmd) for cmd in allowed_commands):
            return JsonResponse({'success': False, 'error': 'Command not allowed'})
        
        # Log the command execution
        log_entry = CLICommandLog.objects.create(
            user=request.user,
            command=command,
            status='running'
        )
        
        try:
            # Execute the command
            result = subprocess.run(
                command.split(),
                capture_output=True,
                text=True,
                timeout=30,
                cwd='/app'
            )
            
            # Update log with results
            log_entry.status = 'completed' if result.returncode == 0 else 'failed'
            log_entry.output = result.stdout
            log_entry.error_output = result.stderr
            log_entry.return_code = result.returncode
            log_entry.save()
            
            return JsonResponse({
                'success': True,
                'log_id': log_entry.id,
                'output': result.stdout,
                'error': result.stderr,
                'return_code': result.returncode
            })
            
        except subprocess.TimeoutExpired:
            log_entry.status = 'failed'
            log_entry.error_output = 'Command timed out after 30 seconds'
            log_entry.save()
            
            return JsonResponse({
                'success': False,
                'error': 'Command timed out'
            })
            
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})


@staff_member_required
def admin_job_management(request):
    """Job Management Interface"""
    
    # Get all jobs with pagination
    jobs = CLICommandLog.objects.select_related('user').order_by('-created_at')
    paginator = Paginator(jobs, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Filter options
    status_filter = request.GET.get('status')
    user_filter = request.GET.get('user')
    
    if status_filter:
        jobs = jobs.filter(status=status_filter)
    if user_filter:
        jobs = jobs.filter(user__username__icontains=user_filter)
    
    context = {
        'title': _('Job Management'),
        'page_obj': page_obj,
        'status_filter': status_filter,
        'user_filter': user_filter,
        'status_choices': CLICommandLog.STATUS_CHOICES,
    }
    
    return render(request, 'admin_control/job_management.html', context)


@staff_member_required
@require_POST  
def admin_cancel_job(request, job_id):
    """Cancel a running job"""
    try:
        job = CLICommandLog.objects.get(id=job_id)
        
        if job.status == 'running':
            job.status = 'cancelled'
            job.error_output = 'Cancelled by administrator'
            job.save()
            
            messages.success(request, f'Job {job_id} has been cancelled.')
        else:
            messages.warning(request, f'Job {job_id} is not running and cannot be cancelled.')
            
    except CLICommandLog.DoesNotExist:
        messages.error(request, f'Job {job_id} not found.')
    
    return redirect('admin_control:job_management')


@staff_member_required
def admin_system_metrics(request):
    """System Metrics and Health Dashboard"""
    import psutil
    
    # Get system metrics
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    # Get database statistics
    user_count = User.objects.count()
    job_count = CLICommandLog.objects.count()
    
    # Get recent activity
    recent_users = User.objects.filter(last_activity__isnull=False).order_by('-last_activity')[:5]
    recent_jobs = CLICommandLog.objects.order_by('-created_at')[:5]
    
    context = {
        'title': _('System Metrics'),
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'disk_percent': (disk.used / disk.total) * 100,
        'user_count': user_count,
        'job_count': job_count,
        'recent_users': recent_users,
        'recent_jobs': recent_jobs,
    }
    
    return render(request, 'admin_control/system_metrics.html', context)