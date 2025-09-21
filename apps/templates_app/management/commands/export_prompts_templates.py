import json
from django.core.management.base import BaseCommand
from apps.templates_app.models import ExpertPrompt, CustomTemplate


class Command(BaseCommand):
    help = "Export expert prompts and core/custom templates to JSON (stdout or file)."

    def add_arguments(self, parser):
        parser.add_argument('--file', '-f', help='Output file path (if omitted, prints to stdout)')
        parser.add_argument('--indent', type=int, default=2, help='JSON indentation (default 2)')
        parser.add_argument('--include-inactive', action='store_true', help='Include inactive expert prompts')
        parser.add_argument('--all-templates', action='store_true', help='Include all template types (default core only)')

    def handle(self, *args, **opts):
        include_inactive = opts['include_inactive']
        all_templates = opts['all_templates']
        indent = opts['indent']
        outfile = opts.get('file')

        ep_qs = ExpertPrompt.objects.all().order_by('slug')
        if not include_inactive:
            ep_qs = ep_qs.filter(active=True)
        prompts = [
            {
                'slug': p.slug,
                'role': p.role,
                'domain': p.domain,
                'title': p.title,
                'description': p.description,
                'prompt_intro': p.prompt_intro,
                'prompt_outro': p.prompt_outro,
                'active': p.active,
                'updated_at': p.updated_at.isoformat() if p.updated_at else None,
            } for p in ep_qs
        ]

        tmpl_qs = CustomTemplate.objects.all().order_by('template_id')
        if not all_templates:
            tmpl_qs = tmpl_qs.filter(template_type='core')
        templates = [
            {
                'template_id': t.template_id,
                'name': t.name,
                'domain': t.domain,
                'description': t.description,
                'prompt': t.prompt,
                'tags': t.tags,
                'parameters': t.parameters,
                'template_type': t.template_type,
            } for t in tmpl_qs
        ]

        payload = {
            'expert_prompts': prompts,
            'templates': templates,
            'counts': {
                'expert_prompts': len(prompts),
                'templates': len(templates),
            }
        }

        data = json.dumps(payload, indent=indent, ensure_ascii=False)
        if outfile:
            with open(outfile, 'w', encoding='utf-8') as f:
                f.write(data)
            self.stdout.write(self.style.SUCCESS(f"Exported to {outfile}"))
        else:
            self.stdout.write(data)