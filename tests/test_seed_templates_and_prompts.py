import pytest
from django.core import management
from apps.templates_app.models import ExpertPrompt, CustomTemplate


@pytest.mark.django_db
def test_seed_templates_and_prompts_idempotent():
    management.call_command('seed_templates_and_prompts')
    first_prompts = list(ExpertPrompt.objects.order_by('slug').values_list('slug', flat=True))
    first_templates = list(CustomTemplate.objects.filter(template_id__startswith='summary_').values_list('template_id', flat=True))
    assert len(first_prompts) >= 9
    assert 'ep_business_value_map' in first_prompts
    assert len(first_templates) >= 3
    # Call again to ensure no duplicates
    management.call_command('seed_templates_and_prompts')
    second_prompts = list(ExpertPrompt.objects.order_by('slug').values_list('slug', flat=True))
    second_templates = list(CustomTemplate.objects.filter(template_id__startswith='summary_').values_list('template_id', flat=True))
    assert first_prompts == second_prompts
    assert first_templates == second_templates