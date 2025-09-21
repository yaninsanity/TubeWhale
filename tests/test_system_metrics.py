import json
from django.urls import reverse

def test_system_metrics_endpoint_admin_client(db, client, django_user_model):
    # Create staff user
    User = django_user_model
    user = User.objects.create_user(username='admin', password='pass', is_staff=True, is_superuser=True)
    client.login(username='admin', password='pass')
    url = reverse('admin-system-metrics-json')
    resp = client.get(url)
    assert resp.status_code == 200
    data = resp.json()
    assert 'system_metrics' in data
    metrics = data['system_metrics']
    # psutil is a hard dependency now: core keys must exist and be sane
    assert 'cpu_percent' in metrics and 0 <= metrics['cpu_percent'] <= 100
    assert 'mem_percent' in metrics and 0 <= metrics['mem_percent'] <= 100
    assert 'disk_percent' in metrics and 0 <= metrics['disk_percent'] <= 100
    # History should be a list
    assert isinstance(data.get('history', []), list)
