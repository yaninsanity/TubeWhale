import pytest
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient

from apps.templates_app.tier_models import UserProfile, UserTier
from apps.templates_app.models import UserTemplateSelection
from apps.templates_app.access_control import BASIC_LIMIT

User = get_user_model()


@pytest.mark.django_db
def test_basic_user_selection_limit():
    user = User.objects.create_user(username='basic1', password='x')
    profile = UserProfile.objects.create(user=user, tier=UserTier.BASIC)
    client = APIClient()
    assert client.login(username='basic1', password='x')

    # Add up to BASIC_LIMIT
    for i in range(BASIC_LIMIT):
        r = client.post('/templates/selection/add/', {
            'template_id': f'tpl_{i}',
            'template_name': f'Template {i}',
            'template_type': 'core'
        }, format='json')
        assert r.status_code == 200, r.json()
    assert UserTemplateSelection.objects.filter(user=user, active=True).count() == BASIC_LIMIT

    # Attempt 4th should fail
    r = client.post('/templates/selection/add/', {
        'template_id': 'tpl_over',
        'template_name': 'Template Over',
        'template_type': 'core'
    }, format='json')
    assert r.status_code == 400
    data = r.json()
    assert 'limit' in data['detail'].lower()


@pytest.mark.django_db
def test_pro_user_unrestricted_builtin(monkeypatch):
    user = User.objects.create_user(username='pro1', password='x')
    UserProfile.objects.create(user=user, tier=UserTier.PRO)
    client = APIClient()
    assert client.login(username='pro1', password='x')

    # PRO should not need selections; listing endpoint may show empty (no selections) but access logic should allow builtin templates
    # Simulate template filtering using tier_utils.filter_accessible_templates
    from apps.templates_app.tier_utils import filter_accessible_templates
    sample_templates = [
        {'id': 'core_a', 'template_type': 'core'},
        {'id': 'domain_b', 'template_type': 'domain'},
    ]
    filtered = filter_accessible_templates(user, sample_templates)
    assert len(filtered) == 2


@pytest.mark.django_db
def test_basic_becomes_pro_then_downgrade_prunes():
    user = User.objects.create_user(username='chg', password='x')
    profile = UserProfile.objects.create(user=user, tier=UserTier.BASIC)
    client = APIClient()
    assert client.login(username='chg', password='x')

    # Fill selections
    for i in range(BASIC_LIMIT):
        r = client.post('/templates/selection/add/', {
            'template_id': f's_{i}',
            'template_name': f'S {i}',
            'template_type': 'core'
        }, format='json')
        assert r.status_code == 200

    # Upgrade to PRO, then add another selection artificially (should still allow but BASIC logic no longer enforced)
    profile.tier = UserTier.PRO
    profile.save()
    # Add selection beyond limit (as PRO no restriction)
    UserTemplateSelection.objects.create(user=user, template_id='extra', template_name='Extra', template_type='core')
    assert UserTemplateSelection.objects.filter(user=user, active=True).count() == BASIC_LIMIT + 1

    # Downgrade back to BASIC -> signal should prune to BASIC_LIMIT
    profile.tier = UserTier.BASIC
    profile.save()
    assert UserTemplateSelection.objects.filter(user=user, active=True).count() == BASIC_LIMIT