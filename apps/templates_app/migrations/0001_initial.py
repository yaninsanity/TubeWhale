from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='CustomTemplate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('template_id', models.SlugField(max_length=128, unique=True)),
                ('name', models.CharField(max_length=255)),
                ('domain', models.CharField(choices=[('general', 'General'), ('business', 'Business Intelligence'), ('medical', 'Medical Research'), ('technology', 'Technology Innovation'), ('academic', 'Academic Research')], default='general', max_length=32)),
                ('description', models.TextField(blank=True, default='')),
                ('prompt', models.TextField()),
                ('parameters', models.JSONField(blank=True, default=dict)),
                ('tags', models.JSONField(blank=True, default=list)),
                ('template_type', models.CharField(choices=[('core', 'Core'), ('domain', 'Domain'), ('custom', 'Custom')], default='custom', max_length=16)),
                ('version', models.CharField(default='1.0.0', max_length=16)),
                ('immutable', models.BooleanField(default=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'templates_custom_template',
                'ordering': ['template_id'],
            },
        ),
    ]
