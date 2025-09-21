from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings
import uuid


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='ProcessingJob',
            fields=[
                ('id', models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, serialize=False)),
                ('video_id', models.CharField(max_length=11, help_text='YouTube Video ID')),
                ('task_type', models.CharField(max_length=20, choices=[('transcript', 'Transcript Only'), ('audio', 'Audio Processing'), ('full', 'Full Analysis'), ('summary', 'Summary Generation'), ('search', 'Related Video Search')], default='full')),
                ('status', models.CharField(max_length=20, choices=[('pending', 'Pending'), ('processing', 'Processing'), ('completed', 'Completed'), ('failed', 'Failed'), ('cancelled', 'Cancelled')], default='pending')),
                ('options', models.JSONField(default=dict, blank=True, help_text='Processing options')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('started_at', models.DateTimeField(null=True, blank=True)),
                ('completed_at', models.DateTimeField(null=True, blank=True)),
                ('result', models.JSONField(null=True, blank=True, help_text='Processing result')),
                ('error_message', models.TextField(blank=True, help_text='Error message if failed')),
                ('progress_percentage', models.IntegerField(default=0, help_text='Progress percentage (0-100)')),
                ('current_step', models.CharField(max_length=100, blank=True, help_text='Current processing step')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tubewhale_jobs', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'TubeWhale Processing Job',
                'verbose_name_plural': 'TubeWhale Processing Jobs',
                'ordering': ['-created_at'],
            },
        ),
        migrations.CreateModel(
            name='TubeWhaleEngineConfig',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('openai_api_key', models.CharField(blank=True, max_length=200, help_text='OpenAI API Key')),
                ('youtube_api_key', models.CharField(blank=True, max_length=200, help_text='YouTube API Key')),
                ('default_language', models.CharField(default='en', max_length=10, help_text='Default processing language')),
                ('auto_summarize', models.BooleanField(default=True, help_text='Auto generate summaries')),
                ('audio_quality', models.CharField(choices=[('low', 'Low'), ('medium', 'Medium'), ('high', 'High')], default='medium', max_length=20)),
                ('email_notifications', models.BooleanField(default=True, help_text='Email notifications')),
                ('webhook_url', models.URLField(blank=True, help_text='Webhook URL for notifications')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='tubewhale_config', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'TubeWhale Engine Config',
                'verbose_name_plural': 'TubeWhale Engine Configs',
            },
        ),
        migrations.CreateModel(
            name='VideoProcessingResult',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('video_id', models.CharField(db_index=True, max_length=11)),
                ('video_title', models.CharField(blank=True, max_length=500)),
                ('video_description', models.TextField(blank=True)),
                ('video_duration', models.IntegerField(null=True, blank=True, help_text='Duration in seconds')),
                ('video_thumbnail_url', models.URLField(blank=True)),
                ('channel_name', models.CharField(blank=True, max_length=200)),
                ('transcript_text', models.TextField(blank=True, help_text='Video transcript')),
                ('audio_summary', models.TextField(blank=True, help_text='Audio processing summary')),
                ('generated_summary', models.TextField(blank=True, help_text='AI generated summary')),
                ('key_topics', models.JSONField(default=list, blank=True, help_text='Extracted key topics')),
                ('sentiment_analysis', models.JSONField(default=dict, blank=True, help_text='Sentiment analysis result')),
                ('language_detected', models.CharField(blank=True, max_length=10, help_text='Detected language code')),
                ('related_videos', models.JSONField(default=list, blank=True, help_text='Related videos found')),
                ('processing_metadata', models.JSONField(default=dict, blank=True, help_text='Processing metadata')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('job', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='processing_result', to='tubewhale_engine.processingjob')),
            ],
            options={
                'verbose_name': 'Video Processing Result',
                'verbose_name_plural': 'Video Processing Results',
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='processingjob',
            index=models.Index(fields=['user', 'status'], name='tubewhale_e_user_id_0a9f5d_idx'),
        ),
        migrations.AddIndex(
            model_name='processingjob',
            index=models.Index(fields=['video_id', 'status'], name='tubewhale_e_video_i_9a89d6_idx'),
        ),
        migrations.AddIndex(
            model_name='processingjob',
            index=models.Index(fields=['created_at'], name='tubewhale_e_created_339e1e_idx'),
        ),
        migrations.AddIndex(
            model_name='videoprocessingresult',
            index=models.Index(fields=['video_id'], name='tubewhale_e_video_i_81a9d2_idx'),
        ),
        migrations.AddIndex(
            model_name='videoprocessingresult',
            index=models.Index(fields=['created_at'], name='tubewhale_e_created_3bcb0f_idx'),
        ),
    ]
