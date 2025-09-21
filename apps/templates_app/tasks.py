"""Celery tasks for template/CLI execution subsystem."""
from celery import shared_task
from django.utils import timezone
from django.conf import settings
from service.cli_runner import run_cli
from .models import CLIExecution, Job, JobStatus
from .prompt_composer import compose_with_variables

@shared_task(bind=True, max_retries=0)
def run_cli_execution_task(self, command: str, template_id: str = "", expert_slug: str = "", variables: dict | None = None, user_id: int | None = None) -> dict:
    """Asynchronous execution of a CLI command with prompt composition.

    Returns a dict containing execution_id (if persisted) and basic status.
    """
    variables = variables or {}
    prompt = compose_with_variables(template_id, expert_slug, variables)
    # run_cli already persists when enabled
    result = run_cli(
        command,
        [],
        prompt_final=prompt,
        template_id=template_id,
        expert_slug=expert_slug,
        meta={'user_id': user_id, 'async': True, 'task_id': self.request.id, 'task_started_at': getattr(self.request, 'time_start', None)},
    )
    execution_id = None
    try:
        latest = CLIExecution.objects.order_by('-id').first()
        if latest and latest.command == command:
            # Heuristic: match by recent template/expert
            if (template_id and latest.template_id == template_id) or (expert_slug and latest.expert_slug == expert_slug) or True:
                execution_id = latest.id
    except Exception:
        execution_id = None
    return {
        'success': result.get('success', False),
        'execution_id': execution_id,
        'command': command,
    }


@shared_task(bind=True, max_retries=0)
def run_job_task(self, job_id: int):
    """Process a Job: compose prompt, invoke CLI, link execution.

    This is a higher-level orchestration wrapper using the existing run_cli logic.
    """
    try:
        job = Job.objects.get(id=job_id)
    except Job.DoesNotExist:  # pragma: no cover - defensive
        return {'error': 'job_missing'}
    if job.status in (JobStatus.CANCELED, JobStatus.SUCCESS, JobStatus.ERROR):
        return {'status': job.status, 'skipped': True}
    job.status = JobStatus.RUNNING
    job.celery_task_id = self.request.id
    job.save(update_fields=['status', 'celery_task_id', 'updated_at'])
    variables = job.variables or {}
    prompt = compose_with_variables(job.template_id, job.expert_slug, variables) if job.template_id else None
    result = run_cli(
        job.command,
        [],
        prompt_final=prompt,
        template_id=job.template_id,
        expert_slug=job.expert_slug,
        meta={'job_id': job.id, 'user_id': job.user_id, 'task_id': self.request.id, 'async': True},
    )
    # Attempt to link latest execution heuristically
    try:
        latest = CLIExecution.objects.order_by('-id').first()
        if latest and latest.command == job.command:
            job.execution_id = latest.id
    except Exception:
        pass
    if result.get('success'):
        job.status = JobStatus.SUCCESS
        job.progress = 100
        job.finished_at = timezone.now()
        job.save(update_fields=['status', 'progress', 'execution_id', 'finished_at', 'updated_at'])
    else:
        # If canceled mid-run by external request, preserve canceled state
        if job.status != JobStatus.CANCELED:
            job.status = JobStatus.ERROR
        job.error = (result.get('error') or '')[:1000]
        job.finished_at = timezone.now()
        job.save(update_fields=['status', 'error', 'execution_id', 'finished_at', 'updated_at'])
    return {'success': result.get('success', False), 'job_id': job.id, 'execution_id': job.execution_id}
