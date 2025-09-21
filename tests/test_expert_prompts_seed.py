import pytest
from django.apps import apps

@pytest.mark.django_db
def test_expert_prompts_seeded():
    ExpertPrompt = apps.get_model('templates_app', 'ExpertPrompt')
    slugs = {
        'expert_core_insights',
        'expert_action_playbook',
        'expert_risk_and_improvements',
    }
    existing = set(ExpertPrompt.objects.filter(slug__in=slugs).values_list('slug', flat=True))
    missing = slugs - existing
    assert not missing, f"Missing seeded expert prompts: {missing}"
