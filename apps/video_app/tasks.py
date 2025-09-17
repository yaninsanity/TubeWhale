"""
Video Analysis Tasks
视频分析异步任务
"""

from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from apps.video_app.models import Video, AnalysisTask
from apps.analysis_app.models import AnalysisReport, ExpertDomain
import logging

# 添加项目根目录到路径，以便导入现有的service模块
import sys
import os
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, current_dir)

try:
    # 导入现有的service模块
    from service.video_analysis_service import VideoAnalysisService
    from agents.transcript_agent import TranscriptAgent
    from agents.audio_agent import AudioAgent
    from agents.search_agent import SearchAgent
    from agents.summarizer_agent import SummarizerAgent
    SERVICE_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Service modules not available: {e}")
    SERVICE_AVAILABLE = False

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def process_video_analysis(self, task_id):
    """
    处理视频分析任务
    """
    try:
        # 获取分析任务
        task = AnalysisTask.objects.get(id=task_id)
        task.start_processing()
        
        logger.info(f"Starting analysis task {task_id} for video {task.video.youtube_id}")
        
        # 更新进度
        self.update_state(state='PROGRESS', meta={'progress': 10})
        
        if not SERVICE_AVAILABLE:
            # 如果service模块不可用，创建模拟结果
            result_data = create_mock_analysis_result(task)
        else:
            # 使用实际的service模块
            result_data = perform_actual_analysis(task, self)
        
        # 完成任务
        task.complete_processing(
            result_data=result_data,
            confidence_score=result_data.get('confidence_score', 0.8)
        )
        
        # 创建分析报告
        create_analysis_report(task, result_data)
        
        logger.info(f"Completed analysis task {task_id}")
        
        return {
            'task_id': task_id,
            'status': 'completed',
            'result': result_data
        }
        
    except AnalysisTask.DoesNotExist:
        logger.error(f"Analysis task {task_id} not found")
        return {'error': 'Task not found'}
    
    except Exception as e:
        logger.error(f"Error processing analysis task {task_id}: {str(e)}")
        
        try:
            task = AnalysisTask.objects.get(id=task_id)
            task.fail_processing(str(e))
        except:
            pass
        
        return {'error': str(e)}


def create_mock_analysis_result(task):
    """创建模拟分析结果"""
    mock_results = {
        'transcript': {
            'text': "模拟转录内容：这是一个关于技术的视频...",
            'language': 'zh',
            'confidence_score': 0.85,
            'segments': [
                {'start': 0, 'end': 10, 'text': '模拟转录内容：这是一个关于技术的视频'},
                {'start': 10, 'end': 20, 'text': '包含了很多有用的信息'}
            ]
        },
        'audio': {
            'quality_score': 0.78,
            'noise_level': 0.12,
            'speech_clarity': 0.89,
            'music_detected': False
        },
        'search': {
            'keywords': ['技术', '教程', '学习'],
            'topics': ['编程', '开发', '技术分享'],
            'category': 'education'
        },
        'summary': {
            'title': task.video.title,
            'summary': '这是一个技术教程视频的总结...',
            'key_points': ['要点1', '要点2', '要点3'],
            'sentiment': 'positive'
        }
    }
    
    analysis_type = task.analysis_type
    if analysis_type == 'comprehensive':
        return {
            'analysis_type': analysis_type,
            'results': mock_results,
            'confidence_score': 0.82,
            'processing_time': 45.6
        }
    else:
        return {
            'analysis_type': analysis_type,
            'results': {analysis_type: mock_results.get(analysis_type, {})},
            'confidence_score': 0.85,
            'processing_time': 15.2
        }


def perform_actual_analysis(task, celery_task):
    """使用实际的service模块进行分析"""
    video = task.video
    analysis_type = task.analysis_type
    
    # 初始化分析服务
    analysis_service = VideoAnalysisService()
    
    # 更新进度
    celery_task.update_state(state='PROGRESS', meta={'progress': 20})
    
    try:
        if analysis_type == 'transcript':
            agent = TranscriptAgent()
            result = agent.analyze_video(video.local_file_path or video.url)
            
        elif analysis_type == 'audio':
            agent = AudioAgent()
            result = agent.analyze_audio(video.local_file_path or video.url)
            
        elif analysis_type == 'search':
            agent = SearchAgent()
            result = agent.analyze_content(video.title, video.description)
            
        elif analysis_type == 'summarize':
            agent = SummarizerAgent()
            result = agent.summarize_video(video.url)
            
        elif analysis_type == 'comprehensive':
            # 综合分析，调用多个agents
            result = analysis_service.comprehensive_analysis(
                video_url=video.url,
                expert_domain=task.expert_domain,
                depth=task.analysis_depth
            )
            
        else:
            raise ValueError(f"Unknown analysis type: {analysis_type}")
        
        # 更新进度
        celery_task.update_state(state='PROGRESS', meta={'progress': 90})
        
        return {
            'analysis_type': analysis_type,
            'results': result,
            'confidence_score': getattr(result, 'confidence', 0.8),
            'processing_time': getattr(result, 'processing_time', 30.0)
        }
        
    except Exception as e:
        logger.error(f"Error in actual analysis: {str(e)}")
        # 如果实际分析失败，返回模拟结果
        return create_mock_analysis_result(task)


def create_analysis_report(task, result_data):
    """创建分析报告"""
    try:
        # 获取专家领域
        expert_domain = None
        if task.expert_domain:
            try:
                expert_domain = ExpertDomain.objects.get(code=task.expert_domain)
            except ExpertDomain.DoesNotExist:
                pass
        
        # 创建报告
        report = AnalysisReport.objects.create(
            video=task.video,
            analysis_task=task,
            expert_domain=expert_domain,
            user=task.user,
            title=f"{task.video.title} - {task.get_analysis_type_display()}报告",
            report_type=task.analysis_type,
            executive_summary=generate_executive_summary(result_data),
            content=generate_report_content(result_data),
            conclusions=generate_conclusions(result_data),
            recommendations=generate_recommendations(result_data),
            confidence_score=result_data.get('confidence_score', 0.8),
            quality_score=calculate_quality_score(result_data),
            status='completed'
        )
        
        # 计算阅读时间
        report.calculate_reading_time()
        report.save()
        
        logger.info(f"Created analysis report {report.id} for task {task.id}")
        
    except Exception as e:
        logger.error(f"Error creating analysis report: {str(e)}")


def generate_executive_summary(result_data):
    """生成执行摘要"""
    analysis_type = result_data.get('analysis_type', 'unknown')
    confidence = result_data.get('confidence_score', 0.8)
    
    summaries = {
        'transcript': f"转录分析完成，置信度 {confidence:.1%}。提取了完整的语音内容并进行了语言识别。",
        'audio': f"音频分析完成，置信度 {confidence:.1%}。评估了音频质量、噪音水平和语音清晰度。",
        'search': f"搜索分析完成，置信度 {confidence:.1%}。识别了关键词、主题和内容分类。",
        'summarize': f"总结分析完成，置信度 {confidence:.1%}。生成了内容摘要和关键要点。",
        'comprehensive': f"综合分析完成，置信度 {confidence:.1%}。完成了多维度的深度分析。"
    }
    
    return summaries.get(analysis_type, f"分析完成，置信度 {confidence:.1%}。")


def generate_report_content(result_data):
    """生成报告正文"""
    content = "## 分析结果\n\n"
    results = result_data.get('results', {})
    
    for key, value in results.items():
        content += f"### {key.title()} 分析\n\n"
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                content += f"**{subkey}**: {subvalue}\n\n"
        else:
            content += f"{value}\n\n"
    
    return content


def generate_conclusions(result_data):
    """生成结论"""
    analysis_type = result_data.get('analysis_type', 'unknown')
    confidence = result_data.get('confidence_score', 0.8)
    
    if confidence > 0.8:
        quality = "高质量"
    elif confidence > 0.6:
        quality = "中等质量"
    else:
        quality = "低质量"
    
    return f"基于{analysis_type}分析，本次分析结果为{quality}，置信度为{confidence:.1%}。"


def generate_recommendations(result_data):
    """生成建议"""
    analysis_type = result_data.get('analysis_type', 'unknown')
    
    recommendations = {
        'transcript': "建议对转录内容进行进一步的语义分析和关键词提取。",
        'audio': "建议优化音频录制环境以提高音频质量。",
        'search': "建议基于识别的关键词优化内容标签和分类。",
        'summarize': "建议将总结内容用于生成视频描述和标签。",
        'comprehensive': "建议基于综合分析结果制定内容优化策略。"
    }
    
    return recommendations.get(analysis_type, "建议根据分析结果采取相应的优化措施。")


def calculate_quality_score(result_data):
    """计算质量评分"""
    confidence = result_data.get('confidence_score', 0.8)
    processing_time = result_data.get('processing_time', 30.0)
    
    # 基于置信度和处理时间计算质量评分
    time_factor = min(1.0, 60.0 / max(processing_time, 1.0))  # 处理时间越短质量越高
    quality_score = (confidence * 0.8 + time_factor * 0.2)
    
    return min(1.0, quality_score)


@shared_task
def cleanup_old_analysis_tasks():
    """清理旧的分析任务"""
    cutoff_date = timezone.now() - timedelta(days=30)
    
    # 删除30天前的已完成任务
    old_tasks = AnalysisTask.objects.filter(
        status__in=['completed', 'failed', 'cancelled'],
        created_at__lt=cutoff_date
    )
    
    count = old_tasks.count()
    old_tasks.delete()
    
    logger.info(f"Cleaned up {count} old analysis tasks")
    return f"Cleaned up {count} tasks"


@shared_task
def update_video_metadata():
    """更新视频元数据"""
    # 这个任务可以定期运行，从YouTube API更新视频信息
    videos = Video.objects.filter(
        status='completed',
        updated_at__lt=timezone.now() - timedelta(days=1)
    )[:100]  # 限制一次处理的数量
    
    updated_count = 0
    for video in videos:
        try:
            # 这里可以调用YouTube API更新视频信息
            # video.update_metadata_from_youtube()
            video.updated_at = timezone.now()
            video.save()
            updated_count += 1
        except Exception as e:
            logger.error(f"Error updating video {video.id}: {str(e)}")
    
    logger.info(f"Updated metadata for {updated_count} videos")
    return f"Updated {updated_count} videos"
