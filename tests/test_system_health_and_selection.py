import pytest
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient
from apps.templates_app.tier_models import UserProfile, UserTier

User = get_user_model()


@pytest.mark.django_db
def test_system_health_json_basic():
    user = User.objects.create_superuser(username='adminx', password='x', email='a@b.c')
    client = APIClient()
    assert client.login(username='adminx', password='x')
    r = client.get('/api/templates/admin/system/health.json')
    assert r.status_code == 200
    data = r.json()
    # Minimal expected keys
    for key in ['db_engine', 'db_name', 'beat_status', 'celery_ok']:
        assert key in data
    assert 'overall_ok' in data

    # Alias path should also work
    r2 = client.get('/templates/admin/system/health.json?fast=1')
    assert r2.status_code == 200
    assert 'overall_ok' in r2.json()


@pytest.mark.django_db
def test_selection_add_restricted_to_basic():
    user = User.objects.create_user(username='pro_user', password='x')
    UserProfile.objects.create(user=user, tier=UserTier.PRO)
    client = APIClient()
    assert client.login(username='pro_user', password='x')
    # API path
    r = client.post('/api/templates/selection/add/', {
        'template_id': 'tpl_x',
        'template_name': 'Tpl X',
        'template_type': 'core'
    }, format='json')
    assert r.status_code == 400
    assert 'only for BASIC' in r.json()['detail']
    # Alias path
    r_alias = client.post('/templates/selection/add/', {
        'template_id': 'tpl_y',
        'template_name': 'Tpl Y',
        'template_type': 'core'
    }, format='json')
    assert r_alias.status_code == 400