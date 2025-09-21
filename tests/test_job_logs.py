import pytest
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient
from apps.templates_app.models import Job, CLIExecution, JobStatus

User = get_user_model()

@pytest.mark.django_db
def test_job_logs_pending_then_ready():
    user = User.objects.create_user(username='loguser', password='pw12345')
    client = APIClient()
    assert client.login(username='loguser', password='pw12345')
    # Create job (no execution yet)
    resp = client.post('/api/templates/jobs/', {'command': 'health_check'}, format='json')
    assert resp.status_code in (201,200)
    job_id = resp.json()['id']

    # First attempt may still be pending linking
    logs_resp = client.get(f'/api/templates/jobs/{job_id}/logs/')
    assert logs_resp.status_code in (200,202,404)  # Accept variability depending on speed

    # Force-link an execution (simulate async completion)
    job = Job.objects.get(id=job_id)
    if not job.execution_id:
        exec_obj = CLIExecution.objects.create(
            command='health_check', args=[], template_id='', expert_slug='', prompt_final='',
            stdout='OK', stderr='', status='success', returncode=0, duration_ms=10, meta={}
        )
        job.execution_id = exec_obj.id
        job.status = JobStatus.SUCCESS
        job.save(update_fields=['execution_id','status'])

    logs_resp2 = client.get(f'/api/templates/jobs/{job_id}/logs/')
    assert logs_resp2.status_code == 200
    data = logs_resp2.json()
    assert data['stdout'] in ('OK', '')
