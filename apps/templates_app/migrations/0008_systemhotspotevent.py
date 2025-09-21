from django.db import migrations, models

class Migration(migrations.Migration):
    dependencies = [
        ('templates_app', '0007_job'),
    ]

    operations = [
        migrations.CreateModel(
            name='SystemHotspotEvent',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('resource', models.CharField(choices=[('cpu', 'CPU'), ('memory', 'Memory')], max_length=16)),
                ('level', models.CharField(max_length=16)),
                ('started_at', models.DateTimeField(auto_now_add=True)),
                ('ended_at', models.DateTimeField(blank=True, null=True)),
                ('peak_percent', models.FloatField(default=0)),
                ('duration_sec', models.IntegerField(default=0)),
                ('resolved', models.BooleanField(default=False)),
                ('meta', models.JSONField(blank=True, default=dict)),
            ],
            options={
                'ordering': ['-started_at'],
            },
        ),
    ]
