from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('templates_app', '0003_expertprompt'),
    ]

    operations = [
        migrations.CreateModel(
            name='CLIExecution',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('command', models.CharField(max_length=128)),
                ('args', models.JSONField(blank=True, default=list)),
                ('template_id', models.CharField(blank=True, default='', max_length=128)),
                ('expert_slug', models.CharField(blank=True, default='', max_length=128)),
                ('prompt_final', models.TextField(blank=True, default='')),
                ('stdout', models.TextField(blank=True, default='')),
                ('stderr', models.TextField(blank=True, default='')),
                ('status', models.CharField(choices=[('success', 'Success'), ('error', 'Error'), ('timeout', 'Timeout'), ('missing_entry', 'Missing Entry')], max_length=32)),
                ('returncode', models.IntegerField(blank=True, null=True)),
                ('duration_ms', models.IntegerField(blank=True, null=True)),
                ('meta', models.JSONField(blank=True, default=dict, help_text='Arbitrary execution metadata (engine status, user id, etc.)')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'templates_cli_execution',
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='cliexecution',
            index=models.Index(fields=['command', 'created_at'], name='templates_c_command_a74b23_idx'),
        ),
        migrations.AddIndex(
            model_name='cliexecution',
            index=models.Index(fields=['status', 'created_at'], name='templates_c_status_caeaf2_idx'),
        ),
        migrations.AddIndex(
            model_name='cliexecution',
            index=models.Index(fields=['template_id', 'expert_slug'], name='templates_c_template_05b40e_idx'),
        ),
    ]
