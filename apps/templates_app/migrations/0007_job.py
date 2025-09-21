from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings

class Migration(migrations.Migration):
    dependencies = [
        ('templates_app', '0006_usertemplateselection'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('command', models.CharField(max_length=128)),
                ('template_id', models.CharField(blank=True, default='', max_length=128)),
                ('expert_slug', models.CharField(blank=True, default='', max_length=128)),
                ('variables', models.JSONField(blank=True, default=dict)),
                ('status', models.CharField(choices=[('pending','Pending'),('queued','Queued'),('running','Running'),('success','Success'),('error','Error'),('canceled','Canceled')], max_length=16, default='pending')),
                ('progress', models.PositiveIntegerField(default=0)),
                ('error', models.TextField(blank=True, default='')),
                ('celery_task_id', models.CharField(blank=True, default='', max_length=128, db_index=True)),
                ('execution_id', models.IntegerField(null=True, blank=True, help_text='FK to CLIExecution when available')),  # denormalized to avoid circular
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('finished_at', models.DateTimeField(null=True, blank=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='jobs', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'templates_job',
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='job',
            index=models.Index(fields=['status','created_at'], name='job_status_created_idx'),
        ),
    ]
