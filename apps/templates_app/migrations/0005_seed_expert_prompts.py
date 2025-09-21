from django.db import migrations

SEEDS = [
    {
        "slug": "expert_core_insights",
        "role": "business_strategist",
        "domain": "general",
        "title": "核心洞察与价值主张 / Core Insights & Value Proposition",
        "description": (
            "提炼视频的核心主题、问题与价值主张；English mirrored. JSON fields: core_topics, problem_statements, "
            "proposed_solutions, unique_value_elements, evidence_quotes."
        ),
        "prompt_intro": (
            "你现在是资深战略分析师。请基于视频逐字稿，从中提炼核心问题与价值主张，避免空洞。\n"
            "You are a senior strategic analyst. Extract only grounded insights—no hallucinations."
        ),
        "prompt_outro": (
            "严格输出 JSON: {\\n  \"core_topics\": [],\n  \"problem_statements\": [],\n  \"proposed_solutions\": [],\n  \"unique_value_elements\": [],\n  \"evidence_quotes\": [{\"quote\": \"\", \"timestamp\": \"\"}]\n}\n"
            "Missing fields use empty arrays. No extra commentary."
        ),
        "weight": 10,
    },
    {
        "slug": "expert_action_playbook",
        "role": "general_analyst",
        "domain": "general",
        "title": "可执行策略与实践 / Actionable Strategies & Playbook",
        "description": (
            "Extract actionable tactics/frameworks/tools/mistakes. JSON: tactics[{name,category,steps,expected_outcome,prerequisites,risk_notes}],"
            " frameworks_or_models, tool_recommendations, common_mistakes_mentioned"
        ),
        "prompt_intro": (
            "你是运营与增长策略顾问。抽取视频中可执行实践并按类型分类。\n"
            "You are an execution-focused growth strategist. Extract explicit actionable methods only."
        ),
        "prompt_outro": (
            "输出 JSON: {\\n  \"tactics\": [{\"name\": \"\", \"category\": \"acquisition|retention|monetization|optimization|other\", \"steps\": [], \"expected_outcome\": \"\", \"prerequisites\": [], \"risk_notes\": []}],\n  \"frameworks_or_models\": [],\n  \"tool_recommendations\": [],\n  \"common_mistakes_mentioned\": []\n}\n"
            "Arrays may be empty. No explanation text."
        ),
        "weight": 20,
    },
    {
        "slug": "expert_risk_and_improvements",
        "role": "policy_analyst",
        "domain": "general",
        "title": "风险限制与改进 / Risks & Improvement Opportunities",
        "description": (
            "Identify risks, assumptions, limitations, improvement_opportunities, missing_perspectives."
        ),
        "prompt_intro": (
            "你是风险与合规分析师。仅基于内容识别风险/限制/假设/改进机会，禁止臆测。\n"
            "You are a risk & policy analyst. Only use transcript-grounded facts."
        ),
        "prompt_outro": (
            "输出 JSON: {\\n  \"risks\": [{\"type\": \"operational|market|technical|regulatory|other\", \"detail\": \"\", \"mitigation\": \"\"}],\n  \"assumptions\": [],\n  \"limitations\": [],\n  \"improvement_opportunities\": [],\n  \"missing_perspectives\": []\n}\nNo extra commentary."
        ),
        "weight": 30,
    },
]

def seed_expert_prompts(apps, schema_editor):
    ExpertPrompt = apps.get_model('templates_app', 'ExpertPrompt')
    existing = {e.slug for e in ExpertPrompt.objects.all()}
    to_create = []
    for row in SEEDS:
        if row['slug'] in existing:
            continue
        to_create.append(ExpertPrompt(**row, active=True))
    if to_create:
        ExpertPrompt.objects.bulk_create(to_create)


def unseed_expert_prompts(apps, schema_editor):
    ExpertPrompt = apps.get_model('templates_app', 'ExpertPrompt')
    slugs = [s['slug'] for s in SEEDS]
    ExpertPrompt.objects.filter(slug__in=slugs).delete()

class Migration(migrations.Migration):
    dependencies = [
        ('templates_app', '0004_cliexecution'),
    ]

    operations = [
        migrations.RunPython(seed_expert_prompts, reverse_code=unseed_expert_prompts),
    ]
