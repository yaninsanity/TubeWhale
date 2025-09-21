from typing import Optional, Dict, Any
from django.core.exceptions import ObjectDoesNotExist
from .models import ExpertPrompt, CustomTemplate
from service import template_manager


def load_base_prompt(template_id: str) -> Optional[str]:
    tpl = template_manager.get_template(template_id)
    if tpl and 'prompt' in tpl:
        return tpl['prompt']
    db_tpl = CustomTemplate.objects.filter(template_id=template_id).first()
    if db_tpl:
        return db_tpl.prompt
    return None


def compose_prompt(template_id: str, expert_slug: Optional[str] = None) -> Optional[str]:
    base = load_base_prompt(template_id)
    if not base:
        return None
    if not expert_slug:
        return base
    try:
        expert = ExpertPrompt.objects.get(slug=expert_slug, active=True)
    except ObjectDoesNotExist:
        return base
    return expert.apply_to(base)


def compose_with_variables(template_id: str, expert_slug: Optional[str], variables: Dict[str, Any]) -> Optional[str]:
    final_prompt = compose_prompt(template_id, expert_slug)
    if not final_prompt:
        return None
    try:
        return final_prompt.format(**variables)
    except Exception:
        return final_prompt
