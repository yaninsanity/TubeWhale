from django.core.management.base import BaseCommand
from django_celery_beat.models import PeriodicTask, IntervalSchedule
import json

DEFAULT_TASKS = [
    {
        'name': 'Cleanup Old Video Analysis Tasks',
        'task': 'apps.video_app.tasks.cleanup_old_analysis_tasks',
        'every': 30,
        'period': 'minutes',
        'args': [],
        'kwargs': {},
    },
]

PERIOD_MAP = {
    'seconds': IntervalSchedule.SECONDS,
    'minutes': IntervalSchedule.MINUTES,
    'hours': IntervalSchedule.HOURS,
}

class Command(BaseCommand):
    help = 'Seed baseline periodic tasks for Celery Beat (idempotent).'

    def add_arguments(self, parser):
        parser.add_argument('--force', action='store_true', help='Recreate tasks even if they exist')

    def handle(self, *args, **options):
        force = options['force']
        created = 0
        updated = 0
        for spec in DEFAULT_TASKS:
            schedule, _ = IntervalSchedule.objects.get_or_create(
                every=spec['every'],
                period=PERIOD_MAP[spec['period']],
            )
            payload = {
                'interval': schedule,
                'task': spec['task'],
                'args': json.dumps(spec['args']),
                'kwargs': json.dumps(spec['kwargs']),
                'enabled': True,
            }
            existing = PeriodicTask.objects.filter(name=spec['name']).first()
            if existing and not force:
                self.stdout.write(self.style.SUCCESS(f"✓ Exists: {spec['name']}"))
                continue
            if existing and force:
                for k, v in payload.items():
                    setattr(existing, k, v)
                existing.save()
                updated += 1
                self.stdout.write(self.style.WARNING(f"↻ Updated: {spec['name']}"))
            else:
                PeriodicTask.objects.create(name=spec['name'], **payload)
                created += 1
                self.stdout.write(self.style.SUCCESS(f"+ Created: {spec['name']}"))
        self.stdout.write(self.style.NOTICE(f"Done. created={created} updated={updated}"))
