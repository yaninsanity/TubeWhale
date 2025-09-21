import time
from django.urls import reverse
from django.conf import settings
from django.utils import timezone


def test_prometheus_endpoint_admin(db, client, django_user_model):
    User = django_user_model
    user = User.objects.create_user(username='admin2', password='pass', is_staff=True, is_superuser=True)
    client.login(username='admin2', password='pass')
    url = reverse('admin-system-metrics-prometheus')
    resp = client.get(url)
    assert resp.status_code == 200
    body = resp.content.decode()
    assert 'tubewhale_cpu_percent' in body
    assert 'tubewhale_memory_percent' in body


def test_hotspot_event_simulation(db, client, django_user_model, monkeypatch):
    # We will monkeypatch psutil to force high CPU/memory readings across multiple calls
    User = django_user_model
    user = User.objects.create_user(username='admin3', password='pass', is_staff=True, is_superuser=True)
    client.login(username='admin3', password='pass')

    class FakeVirt:
        used=50*1024*1024
        total=100*1024*1024
        percent=95
    class FakeDisk:
        used=1
        total=2
        percent=10
    class FakeProcess:
        def memory_info(self):
            class X: rss=25*1024*1024
            return X()
    class FakePsutil:
        _first=True
        @staticmethod
        def cpu_percent(interval=0.0):
            return 96
        @staticmethod
        def virtual_memory():
            return FakeVirt
        @staticmethod
        def disk_usage(path):
            return FakeDisk
        class Process(FakeProcess):
            pass
    monkeypatch.setitem(globals(), 'psutil', FakePsutil)

    # Reduce hotspot duration threshold for test speed
    monkeypatch.setattr(settings, 'HOTSPOT_MIN_DURATION_SEC', 0)

    metrics_url = reverse('admin-system-metrics-json')
    resp = client.get(metrics_url)
    assert resp.status_code == 200

    from apps.templates_app.models import SystemHotspotEvent
    # After one call event should exist because duration threshold 0
    assert SystemHotspotEvent.objects.filter(resource='cpu').exists() or SystemHotspotEvent.objects.filter(resource='memory').exists()
