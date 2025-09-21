from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings

class Migration(migrations.Migration):
    dependencies = [
        ('templates_app', '0005_seed_expert_prompts'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='UserTemplateSelection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('template_id', models.CharField(max_length=128)),
                ('template_name', models.CharField(blank=True, default='', max_length=255)),
                ('template_type', models.CharField(blank=True, default='', max_length=32)),
                ('active', models.BooleanField(default=True)),
                ('locked_at', models.DateTimeField(auto_now_add=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='template_selections', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'templates_user_template_selection',
                'ordering': ['locked_at'],
            },
        ),
        migrations.AddIndex(
            model_name='usertemplateselection',
            index=models.Index(fields=['user', 'active'], name='templates__user_id_9f4f2c_idx'),
        ),
        migrations.AlterUniqueTogether(
            name='usertemplateselection',
            unique_together={('user', 'template_id')},
        ),
    ]
