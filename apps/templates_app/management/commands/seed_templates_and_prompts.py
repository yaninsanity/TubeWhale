from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

from apps.templates_app.models import ExpertPrompt, CustomTemplate, TemplateType

# Canonical 9 expert prompts (fully English) INCLUDING reconciliation of the original 3 seeded migration slugs
EXPERT_PROMPTS = [
    # Updated version of original 'expert_core_insights'
    {
        'slug': 'expert_core_insights',
        'role': 'business_strategist',
        'domain': 'general',
        'title': 'Core Insights & Value Proposition',
        'description': 'Extract core topics, problem statements, proposed solutions, differentiators, and direct evidence quotes.',
        'prompt_intro': 'You are a senior strategic analyst. Use only transcript-grounded facts to extract insight clusters.',
        'prompt_outro': 'JSON keys: core_topics, problem_statements, proposed_solutions, unique_value_elements, evidence_quotes[{quote,timestamp}]'
    },
    # Updated version of original 'expert_action_playbook'
    {
        'slug': 'expert_action_playbook',
        'role': 'general_analyst',
        'domain': 'general',
        'title': 'Actionable Strategies & Playbook',
        'description': 'Extract actionable tactics, frameworks, tool recommendations, common mistakes.',
        'prompt_intro': 'You are an execution-focused analyst. Only include concrete, explicitly mentioned tactics or steps.',
        'prompt_outro': 'JSON: {"tactics":[{name,category,steps,expected_outcome,prerequisites,risk_notes}],"frameworks_or_models":[],"tool_recommendations":[],"common_mistakes_mentioned":[]}'
    },
    # Updated version of original 'expert_risk_and_improvements'
    {
        'slug': 'expert_risk_and_improvements',
        'role': 'policy_analyst',
        'domain': 'general',
        'title': 'Risks & Improvement Opportunities',
        'description': 'Identify risks, assumptions, limitations, improvement opportunities, missing perspectives.',
        'prompt_intro': 'You are a risk analyst. Only acknowledge what is stated or clearly implied.',
        'prompt_outro': 'JSON: {"risks":[{type,detail,mitigation}],"assumptions":[],"limitations":[],"improvement_opportunities":[],"missing_perspectives":[]}'
    },
    # New additions
    {
        'slug': 'ep_business_value_map',
        'role': 'business_strategist',
        'domain': 'business',
        'title': 'Business Value Mapping',
        'description': 'Map value propositions, target segments, positioning, monetization signals.',
        'prompt_intro': 'You are a market positioning analyst – extract structured business value elements.',
        'prompt_outro': 'JSON: {"value_props":[],"segments":[],"positioning":[],"monetization_signals":[],"evidence":[]}'
    },
    {
        'slug': 'ep_medical_evidence_matrix',
        'role': 'medical_researcher',
        'domain': 'medical',
        'title': 'Evidence & Claim Matrix',
        'description': 'Map claims to evidence and uncertainty.',
        'prompt_intro': 'You are a clinical reviewer. Do not infer beyond provided content.',
        'prompt_outro': 'JSON: {"claims":[{claim,evidence,uncertainty}],"gaps":[],"cautions":[]}'
    },
    {
        'slug': 'ep_technology_innovation_landscape',
        'role': 'technology_architect',
        'domain': 'technology',
        'title': 'Innovation & Architecture Signals',
        'description': 'Extract patterns, tools, constraints, performance concerns, evolution paths.',
        'prompt_intro': 'You are a systems architect extracting concrete architectural signals.',
        'prompt_outro': 'JSON: {"patterns":[],"tools":[],"constraints":[],"performance_concerns":[],"evolution_paths":[]}'
    },
    {
        'slug': 'ep_academic_rigor_assessment',
        'role': 'academic_scholar',
        'domain': 'academic',
        'title': 'Academic Rigor Assessment',
        'description': 'Evaluate methodology, source quality, limitations, open questions, future work.',
        'prompt_intro': 'You are a peer reviewer focusing on rigor and clarity.',
        'prompt_outro': 'JSON: {"methodology":[],"sources_quality":[],"limitations":[],"open_questions":[],"future_work":[]}'
    },
    {
        'slug': 'ep_investor_due_diligence_scan',
        'role': 'investor',
        'domain': 'business',
        'title': 'Investor Due Diligence Scan',
        'description': 'Extract traction signals, moat indicators, execution risks, team signals, market dynamics.',
        'prompt_intro': 'You are an investment analyst performing a fast structured scan.',
        'prompt_outro': 'JSON: {"traction_signals":[],"moat_indicators":[],"execution_risks":[],"team_signals":[],"market_dynamics":[]}'
    },
    {
        'slug': 'ep_operational_play_risks',
        'role': 'general_analyst',
        'domain': 'general',
        'title': 'Operational Execution Risks',
        'description': 'Find blockers, resource constraints, coordination risks, mitigations.',
        'prompt_intro': 'You are an operations diagnostics reviewer.',
        'prompt_outro': 'JSON: {"blockers":[],"resource_constraints":[],"coordination_risks":[],"mitigations":[]}'
    },
]

CORE_TEMPLATES = [
    # Minimal representative customizable base templates
    {
        'template_id': 'summary_compact_v1',
        'name': 'Compact Summary V1',
        'domain': 'general',
        'description': 'Produce a compact structured summary with key points and insights.',
        'prompt': 'Summarize the transcript focusing on key factual points, decisions, metrics, and actionable insights. Avoid fluff.',
        'parameters': {},
        'tags': ['summary','core'],
        'template_type': TemplateType.CORE,
    },
    {
        'template_id': 'summary_extended_v1',
        'name': 'Extended Summary V1',
        'domain': 'general',
        'description': 'Extended structured summary including context, progression, and implications.',
        'prompt': 'Provide an extended structured summary with sections: context, progression, key_insights, implications, open_questions.',
        'parameters': {},
        'tags': ['summary','extended'],
        'template_type': TemplateType.CORE,
    },
    {
        'template_id': 'summary_bullet_highlight_v1',
        'name': 'Highlight Bullets V1',
        'domain': 'general',
        'description': 'Bullet highlight extraction for rapid scanning.',
        'prompt': 'Extract 8-15 high-signal bullets. Each bullet: <category>: <fact>. No duplication.',
        'parameters': {},
        'tags': ['summary','bullets'],
        'template_type': TemplateType.CORE,
    },
]

class Command(BaseCommand):
    help = "Seed (or reconcile) expert prompts and core summary templates in English (idempotent). Use --force to overwrite regardless of differences."

    def add_arguments(self, parser):
        parser.add_argument(
            '--force', action='store_true', default=False,
            help='Force overwrite all canonical prompt/template fields even if unchanged.'
        )

    def handle(self, *args, **options):
        force = options.get('force', False)
        created_prompts = 0
        updated_prompts = 0
        created_templates = 0
        updated_templates = 0
        now = timezone.now()

        with transaction.atomic():
            # Expert Prompts reconciliation
            existing_prompts = {p.slug: p for p in ExpertPrompt.objects.all()}
            for item in EXPERT_PROMPTS:
                obj = existing_prompts.get(item['slug'])
                fields = {
                    'role': item['role'],
                    'domain': item['domain'],
                    'title': item['title'],
                    'description': item['description'],
                    'prompt_intro': item['prompt_intro'],
                    'prompt_outro': item['prompt_outro'],
                    'active': True,
                }
                if obj:
                    dirty = force
                    if force:
                        for k,v in fields.items():
                            setattr(obj, k, v)
                    else:
                        for k,v in fields.items():
                            if getattr(obj, k) != v:
                                setattr(obj, k, v)
                                dirty = True
                    if dirty:
                        obj.save()
                        updated_prompts += 1
                else:
                    ExpertPrompt.objects.create(slug=item['slug'], **fields)
                    created_prompts += 1

            # Core templates reconciliation
            existing_templates = {t.template_id: t for t in CustomTemplate.objects.all()}
            for item in CORE_TEMPLATES:
                obj = existing_templates.get(item['template_id'])
                fields = {
                    'name': item['name'],
                    'domain': item['domain'],
                    'description': item['description'],
                    'prompt': item['prompt'],
                    'parameters': item['parameters'],
                    'tags': item['tags'],
                    'template_type': item['template_type'],
                }
                if obj:
                    dirty = force
                    if force:
                        for k,v in fields.items():
                            setattr(obj, k, v)
                    else:
                        for k,v in fields.items():
                            if getattr(obj, k) != v:
                                setattr(obj, k, v)
                                dirty = True
                    if dirty:
                        obj.save()
                        updated_templates += 1
                else:
                    CustomTemplate.objects.create(template_id=item['template_id'], **fields)
                    created_templates += 1

        self.stdout.write(self.style.SUCCESS(
            f"Prompts created={created_prompts} updated={updated_prompts}; Templates created={created_templates} updated={updated_templates}"))