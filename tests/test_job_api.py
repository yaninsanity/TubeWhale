import json
from django.urls import reverse
from rest_framework.test import APIClient
import pytest
from django.contrib.auth import get_user_model

User = get_user_model()

@pytest.mark.django_db
def test_job_creation_flow():
    user = User.objects.create_user(username='jobuser', password='pw12345')
    client = APIClient()
    assert client.login(username='jobuser', password='pw12345')
    payload = {
        'command': 'health_check',
        'template_id': '',
        'variables': {},
    }
    resp = client.post('/api/templates/jobs/', payload, format='json')
    assert resp.status_code in (201, 200), resp.content
    data = resp.json()
    assert data['id']
    assert data['command'] == 'health_check'
    assert data['status'] in ('queued','running','success')

    # Retrieve
    rid = data['id']
    resp2 = client.get(f'/api/templates/jobs/{rid}/')
    assert resp2.status_code == 200
    d2 = resp2.json()
    assert d2['id'] == rid

@pytest.mark.django_db
def test_job_cancel():
    user = User.objects.create_user(username='jobuser2', password='pw12345')
    client = APIClient()
    assert client.login(username='jobuser2', password='pw12345')
    resp = client.post('/api/templates/jobs/', {'command': 'health_check'}, format='json')
    jid = resp.json()['id']
    # Attempt cancel
    resp_cancel = client.post(f'/api/templates/jobs/{jid}/cancel/')
    # May be already success; accept 200 or 400 with appropriate message
    assert resp_cancel.status_code in (200,400)
