"""
TubeWhale Engine Celery Tasks
异步处理TubeWhale视频分析任务
"""

import asyncio
import logging
from datetime import datetime
from celery import shared_task
from django.utils import timezone
from django.contrib.auth import get_user_model

from .models import ProcessingJob, VideoProcessingResult
from .services import get_tubewhale_engine

User = get_user_model()
logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def process_video_task(self, job_id: str):
    """
    异步处理视频任务
    
    Args:
        job_id: ProcessingJob的UUID
    """
    try:
        # 获取任务
        job = ProcessingJob.objects.get(id=job_id)
        
        # 更新状态为处理中
        job.status = 'processing'
        job.started_at = timezone.now()
        job.current_step = 'Initializing TubeWhale Engine'
        job.progress_percentage = 5
        job.save()
        
        # 获取TubeWhale引擎
        engine = get_tubewhale_engine()
        
        if not engine.is_available():
            raise Exception("TubeWhale Engine is not available")
        
        # 更新进度
        job.current_step = 'Processing video'
        job.progress_percentage = 20
        job.save()
        
        # 执行异步处理
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                engine.process_video(job.video_id, job.options)
            )
        finally:
            loop.close()
        
        # 检查处理结果
        if not result.get('success', False):
            raise Exception(result.get('error', 'Unknown processing error'))
        
        # 更新进度
        job.current_step = 'Saving results'
        job.progress_percentage = 80
        job.save()
        
        # 保存处理结果
        _save_processing_result(job, result)
        
        # 完成任务
        job.status = 'completed'
        job.completed_at = timezone.now()
        job.current_step = 'Completed'
        job.progress_percentage = 100
        job.result = result
        job.save()
        
        logger.info(f"✅ Video processing job {job_id} completed successfully")
        
        # 发送通知（如果配置了）
        _send_completion_notification(job)
        
        return {
            'success': True,
            'job_id': str(job_id),
            'video_id': job.video_id,
            'duration': str(job.duration) if job.duration else None
        }
        
    except ProcessingJob.DoesNotExist:
        logger.error(f"❌ Processing job {job_id} not found")
        return {'success': False, 'error': 'Job not found'}
        
    except Exception as e:
        logger.error(f"❌ Video processing job {job_id} failed: {e}")
        
        try:
            job = ProcessingJob.objects.get(id=job_id)
            job.status = 'failed'
            job.completed_at = timezone.now()
            job.error_message = str(e)
            job.current_step = f'Failed: {str(e)}'
            job.save()
        except:
            pass
        
        # 重试逻辑
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Retrying job {job_id} (attempt {self.request.retries + 1})")
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        
        return {
            'success': False,
            'job_id': str(job_id),
            'error': str(e)
        }


@shared_task
def cleanup_old_jobs():
    """清理旧的处理任务（超过30天）"""
    from datetime import timedelta
    
    cutoff_date = timezone.now() - timedelta(days=30)
    old_jobs = ProcessingJob.objects.filter(
        created_at__lt=cutoff_date,
        status__in=['completed', 'failed', 'cancelled']
    )
    
    count = old_jobs.count()
    old_jobs.delete()
    
    logger.info(f"🧹 Cleaned up {count} old processing jobs")
    return {'cleaned_jobs': count}


@shared_task
def batch_process_videos(video_ids: list, user_id: int, options: dict = None):
    """
    批量处理多个视频
    
    Args:
        video_ids: YouTube视频ID列表
        user_id: 用户ID
        options: 处理选项
    """
    try:
        user = User.objects.get(id=user_id)
        options = options or {}
        
        jobs = []
        for video_id in video_ids:
            # 创建处理任务
            job = ProcessingJob.objects.create(
                user=user,
                video_id=video_id,
                task_type=options.get('task_type', 'full'),
                options=options
            )
            jobs.append(job)
            
            # 启动异步任务
            process_video_task.delay(str(job.id))
        
        logger.info(f"🚀 Started batch processing {len(jobs)} videos for user {user.username}")
        
        return {
            'success': True,
            'job_ids': [str(job.id) for job in jobs],
            'video_count': len(video_ids)
        }
        
    except User.DoesNotExist:
        logger.error(f"❌ User {user_id} not found for batch processing")
        return {'success': False, 'error': 'User not found'}
        
    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}")
        return {'success': False, 'error': str(e)}


def _save_processing_result(job: ProcessingJob, result: dict):
    """保存处理结果到数据库"""
    try:
        # 创建或更新VideoProcessingResult
        processing_result, created = VideoProcessingResult.objects.get_or_create(
            job=job,
            defaults={
                'video_id': job.video_id,
            }
        )
        
        # 更新结果数据
        video_info = result.get('video_info', {})
        processing_result.video_title = video_info.get('title', '')
        processing_result.video_description = video_info.get('description', '')
        processing_result.video_duration = video_info.get('duration')
        processing_result.video_thumbnail_url = video_info.get('thumbnail_url', '')
        processing_result.channel_name = video_info.get('channel_name', '')
        
        # 处理结果
        processing_result.transcript_text = result.get('transcript', '')
        processing_result.audio_summary = result.get('audio_summary', '')
        processing_result.generated_summary = result.get('summary', '')
        
        # 分析结果
        processing_result.related_videos = result.get('related_videos', [])
        processing_result.language_detected = result.get('language', '')
        
        # 元数据
        processing_result.processing_metadata = {
            'processed_at': timezone.now().isoformat(),
            'options_used': job.options,
            'processing_duration': str(job.duration) if job.duration else None
        }
        
        processing_result.save()
        
        logger.info(f"✅ Saved processing result for job {job.id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save processing result for job {job.id}: {e}")


def _send_completion_notification(job: ProcessingJob):
    """发送任务完成通知"""
    try:
        # 检查用户是否启用了通知
        if hasattr(job.user, 'tubewhale_config'):
            config = job.user.tubewhale_config
            
            if config.email_notifications:
                # TODO: 发送邮件通知
                logger.info(f"📧 Would send email notification to {job.user.email}")
            
            if config.webhook_url:
                # TODO: 发送Webhook通知
                logger.info(f"🔗 Would send webhook notification to {config.webhook_url}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send notification for job {job.id}: {e}")
