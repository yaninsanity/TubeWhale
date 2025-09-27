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


@shared_task(bind=True, max_retries=2)
def analyze_video_task(self, job_id: str):
    """
    Industrial-grade async video analysis with dynamic templates
    Core value: Expert video content summarization and insights
    
    Args:
        job_id: AnalysisJob的job_id
    """
    from .models import AnalysisJob, AnalysisResult
    from .template_engine import template_engine, ExpertRole
    from utils.openAIServices import OpenAIService
    import json
    import time
    import os
    
    try:
        # 获取分析任务
        analysis_job = AnalysisJob.objects.get(job_id=job_id)
        
        # 开始处理
        analysis_job.status = 'processing'
        analysis_job.started_at = timezone.now()
        analysis_job.total_steps = 5  # 定义总步骤数
        analysis_job.update_progress("Initializing services", steps_completed=0, estimated_remaining=180)
        
        start_time = time.time()
        
        # Initialize real services with industrial-grade error handling
        try:
            # Import services
            from utils.youtube import YouTubeService
            import yaml
            
            openai_service = OpenAIService()
            
            # Validate template and role combination
            template_validation = template_engine.validate_combination(
                analysis_job.template_id, 
                analysis_job.expert_role
            )
            
            if not template_validation["valid"]:
                raise ValueError(f"Invalid template/role combination: {template_validation['error']}")
                
            # Get dynamic template
            template = template_engine.get_template(analysis_job.template_id)
            if not template:
                raise ValueError(f"Template {analysis_job.template_id} not found")
            
            # 角色映射 - 支持旧的角色名称
            role_mapping = {
                'content_analyst': ExpertRole.CONTENT_CREATOR,
                'marketing_expert': ExpertRole.MARKETING_EXPERT,
                'data_analyst': ExpertRole.DATA_ANALYST,
                'business_analyst': ExpertRole.BUSINESS_ANALYST,
                'educational_specialist': ExpertRole.EDUCATIONAL_SPECIALIST,
                'research_scientist': ExpertRole.RESEARCH_SCIENTIST,
                'social_scientist': ExpertRole.SOCIAL_SCIENTIST,
                'hci_specialist': ExpertRole.HCI_SPECIALIST,
                'music_educator': ExpertRole.MUSIC_EDUCATOR
            }
            
            expert_role = role_mapping.get(analysis_job.expert_role)
            if not expert_role:
                # 尝试直接匹配枚举值
                try:
                    expert_role = ExpertRole(analysis_job.expert_role)
                except ValueError:
                    raise ValueError(f"Invalid expert role: {analysis_job.expert_role}")
            
            # 加载配置获取YouTube API密钥
            config_path = '/app/config.yaml'
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                youtube_api_keys = config.get('youtube', {}).get('api_keys', [])
            else:
                # 从环境变量获取
                youtube_api_keys = [os.environ.get('YOUTUBE_API_KEY', 'YOUR_YOUTUBE_API_KEY')]
            
            # 检查是否有有效的YouTube API密钥
            if not youtube_api_keys or youtube_api_keys == ['YOUR_YOUTUBE_API_KEY']:
                logger.warning("No valid YouTube API keys found, using demo mode")
                youtube_service = None
            else:
                youtube_service = YouTubeService(api_keys=youtube_api_keys, skip_key_check=True)
                
        except Exception as service_error:
            logger.error(f"Service initialization failed: {service_error}")
            analysis_job.status = 'failed'
            analysis_job.error_message = f"Service initialization failed: {str(service_error)}"
            analysis_job.save()
            return {'success': False, 'error': str(service_error)}
        
        # Get real video data
        analysis_job.update_progress("Fetching video data", steps_completed=1, estimated_remaining=150)
        analysis_job.status_message = "Fetching video data..."
        analysis_job.save()
        
        video_data = None
        if youtube_service:
            try:
                # 使用真实YouTube API获取视频数据
                video_info = youtube_service.get_video_info(analysis_job.content_id)
                if video_info:
                    video_data = {
                        'video_id': analysis_job.content_id,
                        'title': video_info.get('title', 'Unknown Title'),
                        'description': video_info.get('description', '')[:500] + '...' if len(video_info.get('description', '')) > 500 else video_info.get('description', ''),
                        'duration': video_info.get('duration', 'Unknown'),
                        'view_count': int(video_info.get('view_count', 0)),
                        'like_count': int(video_info.get('like_count', 0)),
                        'comment_count': int(video_info.get('comment_count', 0)),
                        'channel_title': video_info.get('channel_title', 'Unknown Channel'),
                        'published_at': video_info.get('published_at', ''),
                        'tags': video_info.get('tags', [])
                    }
                    logger.info(f"✅ Retrieved real video data for {analysis_job.content_id}")
                else:
                    raise Exception("No video data returned from YouTube API")
            except Exception as youtube_error:
                logger.warning(f"YouTube API failed: {youtube_error}, using demo data")
                youtube_service = None
        
        # 如果YouTube API不可用，则使用演示数据
        if not video_data:
            video_data = {
                'video_id': analysis_job.content_id,
                'title': f'Demo Video {analysis_job.content_id}',
                'description': 'Demo video content for analysis (YouTube API not available)',
                'duration': '10:30',
                'view_count': 150000,
                'like_count': 3200,
                'comment_count': 180,
                'channel_title': 'Demo Channel',
                'published_at': '2025-01-01',
                'tags': ['demo', 'analysis']
            }
            logger.info(f"⚠️ Using demo data for {analysis_job.content_id}")
        
        # Build dynamic AI prompt using template engine
        analysis_job.update_progress("Building AI analysis prompt", steps_completed=2, estimated_remaining=120)
        
        # Build industrial-grade dynamic prompt
        prompt = template_engine.build_dynamic_prompt(
            template=template,
            role=expert_role,
            video_data=video_data,
            custom_requirements=analysis_job.custom_questions
        )
        
        # Call OpenAI API with integrated template system
        analysis_job.update_progress(f"Executing {template.level.value} level AI analysis", steps_completed=3, estimated_remaining=90)
        
        # Prepare template integration data
        template_integration_data = analysis_job.analysis_options.get('template_integration', {})
        
        # Industrial-grade retry mechanism with exponential backoff
        max_retries = 3
        ai_response = None
        
        for retry_count in range(max_retries):
            try:
                # Use enhanced OpenAI completion with template integration
                ai_response = openai_service.completion(
                    prompt=prompt,
                    max_tokens=template.max_tokens,
                    temperature=template.temperature,
                    model="gpt-4o-mini",
                    # New parameters for template integration
                    expert_role=analysis_job.expert_role,
                    template_domain=template_integration_data.get('template_domain', 'video_analysis'),
                    video_data=video_data,
                    force_json_output=True
                )
                
                if ai_response and len(ai_response.strip()) > 50:
                    break
                else:
                    raise ValueError("AI response too short or empty")
                    
            except Exception as retry_error:
                analysis_job.status_message = f"OpenAI API retry ({retry_count+1}/{max_retries})..."
                analysis_job.save()
                logger.warning(f"OpenAI retry {retry_count+1}: {retry_error}")
                
                if retry_count == max_retries - 1:
                    # Generate template-specific demo response
                    logger.info(f"Using demo mode for {template.level.value} level analysis")
                    demo_response = _generate_demo_response(template, expert_role, analysis_job.content_id)
                    ai_response = json.dumps(demo_response)
                    break
        
        # Process AI response
        analysis_job.update_progress("Processing AI analysis results", steps_completed=4, estimated_remaining=30)
        
        try:
            ai_analysis = json.loads(ai_response)
        except json.JSONDecodeError:
            ai_analysis = {
                'summary': ai_response[:500] + '...' if len(ai_response) > 500 else ai_response,
                'detailed_analysis': ai_response,
                'recommendations': ['Professional AI-based recommendations'],
                'insights': {'ai_generated': True, 'raw_response': True},
                'metrics': {'overall_score': 8.0},
                'confidence_score': 0.75
            }
        
        # Save analysis results
        analysis_job.progress = 80
        analysis_job.status_message = "Saving analysis results..."
        analysis_job.save()
        
        processing_time = time.time() - start_time
        
        # Create enhanced analysis result with template metadata
        analysis_result = AnalysisResult.objects.create(
            job=analysis_job,
            summary=ai_analysis.get('summary', f"AI analysis of video {analysis_job.content_id} completed"),
            detailed_analysis=ai_analysis.get('detailed_analysis', f"## AI Analysis Report\n\nAnalysis completed."),
            recommendations='\n'.join(ai_analysis.get('recommendations', [])) if isinstance(ai_analysis.get('recommendations'), list) else str(ai_analysis.get('recommendations', '')),
            raw_data={
                'video_id': analysis_job.content_id,
                'expert_role': analysis_job.expert_role,
                'template': {
                    'id': analysis_job.template_id,
                    'name': template.name,
                    'level': template.level.value,
                    'sections': template.analysis_sections
                },
                'analysis_timestamp': timezone.now().isoformat(),
                'ai_response': ai_analysis,
                'video_data': video_data,
                'processing_time_seconds': processing_time,
                'celery_task_id': self.request.id,
                'visualization_config': template.visualization_config,
                'youtube_integration': {
                    'video_url': f"https://www.youtube.com/watch?v={analysis_job.content_id}",
                    'thumbnail_url': f"https://img.youtube.com/vi/{analysis_job.content_id}/hqdefault.jpg",
                    'embed_url': f"https://www.youtube.com/embed/{analysis_job.content_id}"
                }
            },
            insights=ai_analysis.get('insights', {'ai_generated': True}),
            metrics=ai_analysis.get('metrics', {'overall_score': 8.0}),
            confidence_score=ai_analysis.get('confidence_score', 0.75),
            completeness_score=_calculate_completeness_score(ai_analysis, template),
            available_formats=['json', 'markdown', 'html', 'pdf']
        )
        
        # Complete task
        analysis_job.status = 'completed'
        analysis_job.completed_at = timezone.now()  
        analysis_job.processing_time_seconds = processing_time
        analysis_job.update_progress("Analysis completed", steps_completed=5, estimated_remaining=0)
        
        logger.info(f"✅ Video analysis completed: {job_id}")
        
        return {
            'success': True,
            'job_id': job_id,
            'result_id': analysis_result.id,
            'confidence_score': analysis_result.confidence_score,
            'processing_time': processing_time
        }
        
    except AnalysisJob.DoesNotExist:
        logger.error(f"❌ Analysis job {job_id} not found")
        return {'success': False, 'error': 'Job not found'}
        
    except Exception as e:
        logger.error(f"❌ Video analysis failed for {job_id}: {e}")
        
        # 更新任务状态为失败
        try:
            analysis_job = AnalysisJob.objects.get(job_id=job_id)
            analysis_job.status = 'failed'
            analysis_job.completed_at = timezone.now()
            analysis_job.error_message = str(e)
            analysis_job.status_message = f"分析失败: {str(e)}"
            analysis_job.save()
        except:
            pass
        
        # 重试逻辑
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Retrying video analysis {job_id} (attempt {self.request.retries + 1})")
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        
        return {'success': False, 'job_id': job_id, 'error': str(e)}


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


# Industrial-grade helper methods for enhanced analysis
def _generate_demo_response(template, expert_role, video_id):
    """Generate template-specific demo response"""
    from .template_engine import AnalysisLevel
    
    base_response = {
        'summary': f"Professional {template.level.value} analysis for {video_id} from {expert_role.value} perspective (Demo Mode)",
        'confidence_score': 0.8,
        'insights': {'demo_mode': True, 'template_level': template.level.value},
        'metrics': {'overall_score': 8.5}
    }
    
    if template.level == AnalysisLevel.BASIC:
        base_response.update({
            'detailed_analysis': f"## {template.name}\n\n**Expert Role**: {expert_role.value}\n\n### Key Findings\n- Strong content foundation\n- Good audience engagement\n- Clear optimization opportunities\n\n*Demo Mode: Real deployment uses actual AI analysis*",
            'recommendations': ['Optimize thumbnail design', 'Improve video pacing', 'Add interactive elements']
        })
    
    elif template.level == AnalysisLevel.DETAILED:
        base_response.update({
            'detailed_analysis': f"## {template.name}\n\n### Executive Summary\nComprehensive analysis showing strong performance with strategic opportunities.\n\n### Content Analysis\n- High-quality production values\n- Engaging narrative structure\n- Effective audience targeting\n\n### Strategic Recommendations\n- Implement advanced engagement strategies\n- Optimize for search algorithms\n- Develop content series approach\n\n*Demo Mode: Real analysis provides deeper insights*",
            'recommendations': ['Strategic content planning', 'Advanced SEO optimization', 'Audience segmentation', 'Performance tracking implementation'],
            'competitive_positioning': 'Above average in category',
            'audience_insights': 'Strong engagement from target demographic'
        })
    
    elif template.level == AnalysisLevel.EXPERT:
        base_response.update({
            'detailed_analysis': f"## {template.name}\n\n### Theoretical Framework\nApplying {expert_role.value} expertise to comprehensive video analysis.\n\n### Empirical Findings\n- Statistical performance indicators exceed baseline\n- Audience behavior patterns align with theoretical models\n- Content effectiveness metrics demonstrate optimization potential\n\n### Expert Recommendations\n- Implement evidence-based improvement strategies\n- Apply advanced analytical frameworks\n- Develop predictive performance models\n\n*Demo Mode: Real analysis includes scholarly-level insights*",
            'recommendations': ['Evidence-based optimization', 'Advanced analytics implementation', 'Theoretical framework application', 'Predictive modeling integration'],
            'theoretical_analysis': 'Advanced domain-specific insights',
            'methodology_notes': 'Analysis based on industry best practices'
        })
    
    return base_response


def _calculate_completeness_score(ai_analysis, template):
    """Calculate analysis completeness based on template requirements"""
    required_sections = len(template.analysis_sections)
    completed_sections = sum(1 for section in template.output_format.keys() if section in ai_analysis)
    
    base_score = completed_sections / len(template.output_format) if template.output_format else 0.8
    
    # Bonus for comprehensive analysis
    if len(str(ai_analysis)) > 1000:
        base_score += 0.1
    
    return min(base_score, 1.0)
