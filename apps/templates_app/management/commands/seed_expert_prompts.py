from django.core.management.base import BaseCommand
from django.db import transaction
from apps.templates_app.models import ExpertPrompt, TemplateDomain, ExpertRole

SEED_DEFS = {
    "expert_core_insights": {
        "role": ExpertRole.BUSINESS_STRATEGIST,
        "domain": TemplateDomain.GENERAL,
        "title": "核心洞察与价值主张 / Core Insights & Value Proposition",
        "description": "Programmatic seed refresh (see migration 0005).",
        "prompt_intro": "你现在是资深战略分析师...",
        "prompt_outro": "严格 JSON 输出。",
        "weight": 10,
    },
    "expert_action_playbook": {
        "role": ExpertRole.GENERAL_ANALYST,
        "domain": TemplateDomain.GENERAL,
        "title": "可执行策略与实践 / Actionable Strategies & Playbook",
        "description": "Actionable tactics extraction.",
        "prompt_intro": "你是运营与增长策略顾问...",
        "prompt_outro": "输出 JSON。",
        "weight": 20,
    },
    "expert_risk_and_improvements": {
        "role": ExpertRole.POLICY_ANALYST,
        "domain": TemplateDomain.GENERAL,
        "title": "风险限制与改进 / Risks & Improvement Opportunities",
        "description": "Risk & improvement extraction.",
        "prompt_intro": "你是风险与合规分析师...",
        "prompt_outro": "输出 JSON。",
        "weight": 30,
    },
}

class Command(BaseCommand):
    help = "Seed or refresh core ExpertPrompt records."

    def add_arguments(self, parser):
        parser.add_argument('--force', action='store_true', help='Overwrite existing prompts')

    @transaction.atomic
    def handle(self, *args, **options):
        force = options.get('force')
        created = 0
        updated = 0
        for slug, data in SEED_DEFS.items():
            obj, exists = ExpertPrompt.objects.get_or_create(slug=slug, defaults=data)
            if exists:
                created += 1
            else:
                if force:
                    for k, v in data.items():
                        setattr(obj, k, v)
                    obj.save()
                    updated += 1
        self.stdout.write(self.style.SUCCESS(f"Seed complete. created={created} updated={updated} force={force}"))
