from django.db import migrations, models

class Migration(migrations.Migration):
    dependencies = [
        ('templates_app', '0002_userprofile_templateusage'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExpertPrompt',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('slug', models.SlugField(max_length=128, unique=True)),
                ('role', models.CharField(choices=[('general_analyst', 'General Analyst'), ('business_strategist', 'Business Strategist'), ('medical_researcher', 'Medical Researcher'), ('technology_architect', 'Technology Architect'), ('academic_scholar', 'Academic Scholar'), ('investor', 'Investor / Due Diligence'), ('policy_analyst', 'Policy Analyst')], max_length=64)),
                ('domain', models.CharField(choices=[('general', 'General'), ('business', 'Business Intelligence'), ('medical', 'Medical Research'), ('technology', 'Technology Innovation'), ('academic', 'Academic Research')], default='general', max_length=32)),
                ('title', models.CharField(max_length=255)),
                ('description', models.TextField(blank=True, default='')),
                ('prompt_intro', models.TextField(blank=True, default='')),
                ('prompt_outro', models.TextField(blank=True, default='')),
                ('active', models.BooleanField(default=True)),
                ('weight', models.PositiveIntegerField(default=100)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'templates_expert_prompt',
                'ordering': ['weight', 'role', 'domain'],
            },
        ),
        migrations.AddIndex(
            model_name='expertprompt',
            index=models.Index(fields=['role', 'domain', 'active'], name='templates_a_role_do_5f25cc_idx'),
        ),
    ]
