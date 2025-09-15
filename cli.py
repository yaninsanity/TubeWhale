#!/usr/bin/env python3
"""
主流程：处理多个视频
本模块负责：
  1. 初始化各项服务（数据库、YouTube、OpenAI、各 Agent）
  2. 调用搜索、转录、评论、音频分析、摘要标准化等 Agent，完成视频信息提取
  3. 异步处理多个视频，支持用户输入"exit"退出并生成处理报告
"""

import os
import sys
import json
import logging
import asyncio
import traceback
from datetime import datetime
import multiprocessing
import argparse
import ssl

# 内部模块
from utils.async_database import AsyncDatabase  # 异步数据库包装器
from utils.database import Database              # 同步数据库，由 AsyncDatabase 包装
from agents.search_agent import SearchAgent
from agents.transcript_agent import run_transcript
from agents.summarizer_agent import gpt_summarizer_agent  # 异步 LLM 摘要接口
from agents.audio_agent import AudioProcessingAgent
from agents.standardizer_agent import StandardizerAgent
from utils.youtube import YouTubeService
from utils.helper import print_startup_banner
from utils.config import Config

from dotenv import load_dotenv
load_dotenv()

# 全局HTTP连接池配置
_http_session = None
_ssl_context = None

def get_optimized_ssl_context():
    """获取优化的SSL上下文"""
    global _ssl_context
    if _ssl_context is None:
        _ssl_context = ssl.create_default_context()
        _ssl_context.check_hostname = False
        _ssl_context.verify_mode = ssl.CERT_NONE
        # 优化SSL性能
        _ssl_context.options |= ssl.OP_NO_COMPRESSION
    return _ssl_context

async def get_http_session():
    """获取全局HTTP连接池"""
    global _http_session
    if _http_session is None:
        import aiohttp
        connector = aiohttp.TCPConnector(
            limit=100,  # 总连接池大小
            limit_per_host=30,  # 每个主机的连接数
            ttl_dns_cache=300,  # DNS缓存5分钟
            use_dns_cache=True,
            ssl=get_optimized_ssl_context(),
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'TubeWhale/1.0'}
        )
    return _http_session

async def cleanup_http_session():
    """清理HTTP连接池"""
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

# -------------------------------------------------------------------------------
# 配置多进程启动方式
# -------------------------------------------------------------------------------
try:
    multiprocessing.set_start_method("fork", force=True)
except RuntimeError:
    pass

# -------------------------------------------------------------------------------
# 工业级Logger配置：动态级别、格式化输出、性能监控
# -------------------------------------------------------------------------------
def get_logger(level: str = "INFO", log_file: str = None, name: str = "TubeWhale"):
    """
    配置工业级logger with enhanced features:
    - 动态日志级别配置
    - 美观的控制台输出格式
    - 自动日志文件管理
    - 性能友好的配置
    """
    logger = logging.getLogger(name)
    
    # 设置日志级别
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO, 
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger.setLevel(log_levels.get(level.upper(), logging.INFO))
    
    # 避免重复添加handler
    if not logger.handlers:
        # 增强的formatter with colors for console
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler with dynamic level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logger.level)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler with enhanced naming and rotation
        if log_file or level != "ERROR":  # Always create file unless error-only mode
            os.makedirs('logs', exist_ok=True)
            
            if log_file:
                log_filename = log_file
            else:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                log_filename = os.path.join('logs', f'tubewhale_{timestamp}.log')
            
            file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)  # File always gets full detail
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            
            logger.info(f"Logging to file: {log_filename}")
    
    return logger

# 全局变量将在main中初始化

# -------------------------------------------------------------------------------
# 智能并发控制：动态调整、资源监控、防护机制
# -------------------------------------------------------------------------------
class ConcurrencyManager:
    """智能并发管理器，支持动态调整和资源保护"""
    
    def __init__(self, initial_concurrency: int = 3, max_concurrency: int = 10):
        self.initial_concurrency = max(1, min(initial_concurrency, max_concurrency))
        self.max_concurrency = max_concurrency
        self.current_concurrency = self.initial_concurrency
        self.semaphore = asyncio.Semaphore(self.initial_concurrency)
        self._task_times = []
        self._start_time = None
        self._memory_threshold = 150 * 1024 * 1024  # 150MB 内存阈值
        
    def get_memory_usage(self):
        """获取当前内存使用情况"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss
        except ImportError:
            return 0
    
    def should_reduce_concurrency(self):
        """判断是否应该降低并发度"""
        current_memory = self.get_memory_usage()
        return current_memory > self._memory_threshold
        
    async def acquire(self):
        """获取并发槽位，带性能监控和内存保护"""
        # 内存保护机制
        if self.should_reduce_concurrency():
            import gc
            gc.collect()  # 强制垃圾回收
            
        self._start_time = asyncio.get_event_loop().time()
        await self.semaphore.acquire()
        
    def release(self):
        """释放并发槽位，记录性能数据"""
        if self._start_time:
            task_time = asyncio.get_event_loop().time() - self._start_time
            self._task_times.append(task_time)
            # 保留最近100个任务的时间数据
            if len(self._task_times) > 100:
                self._task_times = self._task_times[-50:]
        self.semaphore.release()
        
    def get_avg_task_time(self) -> float:
        """获取平均任务执行时间"""
        return sum(self._task_times) / len(self._task_times) if self._task_times else 0
        
    def get_performance_stats(self) -> dict:
        """获取性能统计信息"""
        return {
            "current_concurrency": self.current_concurrency,
            "max_concurrency": self.max_concurrency,
            "avg_task_time": self.get_avg_task_time(),
            "total_tasks": len(self._task_times),
            "performance_score": min(1.0, 30.0 / max(self.get_avg_task_time(), 1.0))  # 30秒为基准
        }

# 创建全局并发管理器
concurrency_manager = None
logger = None

# -------------------------------------------------------------------------------
# CLI 参数解析 - 工业级参数管理和验证
# -------------------------------------------------------------------------------
def parse_cli_arguments():
    """解析和验证CLI参数，提供更好的用户体验和错误处理"""
    parser = argparse.ArgumentParser(
        prog="tubewhale",
        description="🐋 TubeWhale - AI-powered YouTube video analysis and summarization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py                                    # Use default configuration
  python cli.py --keyword "machine learning"      # Override search keyword
  python cli.py --dry-run                         # Test run without API calls
  python cli.py --pure-youtube --top-k 10         # YouTube-only mode with 10 videos
  python cli.py --concurrency 5 --audio           # High concurrency with audio analysis
  python cli.py --verbose                         # Detailed logging output

Environment Variables:
  KEYWORD                 - Search keyword (required)
  YOUTUBE_API_KEYS        - YouTube API keys (comma-separated)
  OPENAI_API_KEY         - OpenAI API key
  CONCURRENCY            - Number of concurrent video processing tasks
  
For more information, see: https://github.com/yaninsanity/TubeWhale
        """
    )
    
    # Core processing options
    processing_group = parser.add_argument_group('Processing Options')
    processing_group.add_argument(
        "--keyword", "-k", 
        type=str, 
        help="Search keyword (overrides KEYWORD env variable)"
    )
    processing_group.add_argument(
        "--top-k", "-n", 
        type=int, 
        help="Number of videos to process (overrides TOP_K env variable)"
    )
    processing_group.add_argument(
        "--concurrency", "-c", 
        type=int, 
        choices=range(1, 11), 
        metavar="1-10",
        help="Number of concurrent tasks (1-10, overrides CONCURRENCY env variable)"
    )
    
    # Feature toggles
    features_group = parser.add_argument_group('Feature Control')
    features_group.add_argument(
        "--pure-youtube", 
        action="store_true", 
        help="Pure YouTube mode: disable AI expansion and audio analysis for faster processing"
    )
    features_group.add_argument(
        "--audio", "--full-audio-analysis", 
        dest="audio_analysis",
        action="store_true", 
        help="Enable full audio analysis (requires OpenAI API key, increases processing time)"
    )
    features_group.add_argument(
        "--no-persist", 
        action="store_true", 
        help="Disable database persistence (useful for testing)"
    )
    
    # Execution modes
    execution_group = parser.add_argument_group('Execution Modes')
    execution_group.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Dry run mode: validate configuration and test workflow without API calls or database writes"
    )
    execution_group.add_argument(
        "--config-test", 
        action="store_true", 
        help="Test configuration and exit (validates API keys and settings)"
    )
    
    # Output and debugging
    output_group = parser.add_argument_group('Output and Debugging')
    output_group.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging (DEBUG level)"
    )
    output_group.add_argument(
        "--quiet", "-q", 
        action="store_true", 
        help="Quiet mode: minimal output (ERROR level only)"
    )
    output_group.add_argument(
        "--log-file", 
        type=str, 
        help="Custom log file path (default: auto-generated in logs/ directory)"
    )
    
    args = parser.parse_args()
    
    # Validate argument combinations
    if args.quiet and args.verbose:
        parser.error("--quiet and --verbose cannot be used together")
    
    if args.pure_youtube and args.audio_analysis:
        parser.error("--pure-youtube and --audio cannot be used together (pure mode disables audio analysis)")
    
    if args.concurrency and args.concurrency > 5 and args.audio_analysis:
        logger.warning("High concurrency (>5) with audio analysis may cause rate limiting or memory issues")
    
    return args

# -------------------------------------------------------------------------------
# 异步监听退出命令
# -------------------------------------------------------------------------------
async def listen_for_exit(exit_event: asyncio.Event):
    logger.info("Type 'exit' and press Enter to gracefully stop processing new videos.")
    user_input = await asyncio.to_thread(input, "")
    if user_input.strip().lower() == "exit":
        logger.info("Exit command received. New videos will not be scheduled; waiting for current tasks to finish.")
        exit_event.set()

# -------------------------------------------------------------------------------
# 单个视频处理流程（异步调用各 Agent 接口）
# -------------------------------------------------------------------------------
async def process_single_video(video, keyword, async_db: AsyncDatabase, persist_summaries,
                               full_audio_analysis, dry_run, youtube_service, openai_service, standardizer_agent,
                               logger=None, concurrency_manager=None):
    video_id = video['video_id']
    step = ""
    try:
        # Dry run模式下使用简化的并发控制
        if not dry_run and concurrency_manager:
            await concurrency_manager.acquire()
        
        try:
            # Step 1: 获取视频元数据
            step = "fetch_metadata"
            logger.info(f"[{video_id}] Fetching metadata.")
            if not dry_run:
                video_metadata = youtube_service.fetch_video_metadata(video_id)
            else:
                # 使用更真实的mock数据，基于传入的video数据
                video_metadata = {
                    "video_id": video_id,
                    "title": video.get('title', f"Mock title for {video_id}"),
                    "description": video.get('description', f"Mock description for {video_id} about {keyword}"),
                    "publish_date": video.get('published_at', "2025-01-01"),
                    "channel_id": "mock_channel",
                    "view_count": video.get('view_count', 1000),
                    "like_count": video.get('like_count', 100),
                    "comment_count": 50,
                    "snippet": {
                        "title": video.get('title', f"Mock title for {video_id}"),
                        "description": video.get('description', f"Mock description for {video_id}"),
                        "publishedAt": video.get('published_at', "2025-01-01T00:00:00Z"),
                        "channelTitle": video.get('channel_title', "Mock Channel"),
                        "tags": [keyword, "tutorial", "mock"],
                        "categoryId": "27",  # Education
                        "defaultAudioLanguage": "en",
                        "defaultLanguage": "en"
                    },
                    "contentDetails": {
                        "duration": video.get('duration', "PT5M0S"),
                        "dimension": "2d",
                        "definition": "hd",
                        "caption": "false",
                        "licensedContent": False
                    }
                }
                logger.info(f"[{video_id}] 🎭 Using mock metadata for dry run")
                
            if video_metadata and not dry_run and persist_summaries:
                await async_db.store_video_metadata(video_metadata)
            video['metadata'] = video_metadata

            # Step 2: 获取 transcript 及 LLM 生成摘要
            step = "fetch_transcript"
            logger.info(f"[{video_id}] Fetching transcript.")
            if not dry_run:
                transcript = await run_transcript(video_id, openai_service, youtube_service, logger=logger, dry_run=dry_run)
            else:
                # Dry run: 生成mock transcript
                transcript = f"This is a mock transcript for {video_id} about {keyword}. " \
                           f"The video discusses various aspects of {keyword} and provides " \
                           f"educational content. Duration: 5 minutes. Quality: High definition."
                logger.info(f"[{video_id}] 🎭 Using mock transcript for dry run")
                
            if transcript:
                video['transcript'] = transcript
                if not dry_run:
                    prompt_text = transcript
                    llm_summary = await gpt_summarizer_agent(transcript, service=openai_service, logger=logger, dry_run=dry_run)
                    video['llm_summary'] = llm_summary

                    # —— 立即存一份到 transcripts 表 —— 
                    if persist_summaries:
                        await async_db.store_transcript_summary(
                            video_id,
                            transcript,
                            llm_summary
                        )

                    await async_db.store_ai_interaction(
                        input_data={"prompt": prompt_text},
                        output_data={"response": llm_summary},
                        interaction_type="transcript_summary",
                        tokens_used=openai_service.total_prompt_tokens,
                        cost=openai_service.total_cost
                    )
                else:
                    # Dry run: 生成mock LLM summary
                    video['llm_summary'] = f"Mock LLM summary for {video_id}: This video about {keyword} " \
                                         f"covers key concepts and provides valuable insights. " \
                                         f"Main topics include introduction, explanation, and examples. " \
                                         f"Suitable for beginners and intermediate learners."
                    logger.info(f"[{video_id}] 🎭 Using mock LLM summary for dry run")
                    
                video['summary_source'] = 'transcript'
                logger.info(f"[{video_id}] Transcript and LLM summary obtained.")
            else:
                logger.info(f"[{video_id}] No transcript available.")
                
            # Step 3: 异步获取评论（并行执行）
            step = "fetch_comments"
            logger.info(f"[{video_id}] Fetching comments asynchronously.")
            try:
                if not dry_run:
                    loop = asyncio.get_running_loop()
                    comments = await loop.run_in_executor(None, youtube_service.fetch_all_comments, video_id)
                else:
                    # Dry run: 生成更真实的mock评论
                    comments = [
                        {
                            "comment_id": f"mock_comment_1_{video_id}", 
                            "author": "MockUser1", 
                            "text": f"Great tutorial about {keyword}! Very helpful and well explained.", 
                            "like_count": 15,
                            "publish_time": "2025-01-01T10:00:00Z", 
                            "viewer_rating": "none", 
                            "moderation_status": "published", 
                            "parent_id": None
                        },
                        {
                            "comment_id": f"mock_comment_2_{video_id}", 
                            "author": "TestViewer", 
                            "text": f"Thanks for this {keyword} explanation. Could you make more videos like this?", 
                            "like_count": 8,
                            "publish_time": "2025-01-01T12:30:00Z", 
                            "viewer_rating": "none", 
                            "moderation_status": "published", 
                            "parent_id": None
                        },
                        {
                            "comment_id": f"mock_comment_3_{video_id}", 
                            "author": "LearnerABC", 
                            "text": f"Perfect for beginners in {keyword}! Easy to follow.", 
                            "like_count": 23,
                            "publish_time": "2025-01-01T15:45:00Z", 
                            "viewer_rating": "none", 
                            "moderation_status": "published", 
                            "parent_id": None
                        }
                    ]
                    logger.info(f"[{video_id}] 🎭 Using mock comments for dry run")
                    
                video['comments'] = comments
                if comments and not dry_run and persist_summaries:
                    await async_db.store_comments(video_id, comments)
                logger.info(f"[{video_id}] {len(comments)} comments fetched.")
            except Exception as e:
                logger.error(f"[{video_id}] Error fetching comments: {e}")
                video['comments'] = None

            # Step 4: 音频分析（可选）
            if full_audio_analysis:
                step = "audio_analysis"
                logger.info(f"[{video_id}] Audio analysis enabled.")
                # 1) Whisper 整段转录
                try:
                    transcript_audio = await run_transcript(video_id, openai_service, youtube_service, logger=logger, dry_run=dry_run)
                except Exception as e:
                    logger.error(f"[{video_id}] Whisper transcription failed: {e}")
                    transcript_audio = None

                # 2) LLM 摘要
                audio_summary = None
                if transcript_audio:
                    try:
                        audio_summary = await gpt_summarizer_agent(
                            transcript_audio,
                            service=openai_service,
                            logger=logger,
                            dry_run=dry_run
                        )
                    except Exception as e:
                        logger.error(f"[{video_id}] Audio summary generation failed: {e}")

                # 3) 回退：逐块处理
                if not audio_summary:
                    agent = AudioProcessingAgent(
                        openai_service=openai_service,
                        youtube_service=youtube_service,
                        logger=logger,
                        dry_run=dry_run
                    )
                    try:
                        audio_summary = await agent.process_video_audio(
                            video_id=video_id,
                            topic=keyword,
                            metadata={"title": video_metadata.get("title", "")}
                        )
                    except Exception as e:
                        logger.error(f"[{video_id}] Chunked audio analysis failed: {e}")

                if audio_summary:
                    video['audio_summary'] = audio_summary
                    video['summary_source'] = video.get('summary_source', '') + ', audio'
                    logger.info(f"[{video_id}] Audio summary obtained.")
                else:
                    logger.error(f"[{video_id}] Audio analysis produced no summary.")
            else:
                logger.info(f"[{video_id}] Audio analysis disabled.")

            # Step 5: 标准化摘要
            step = "standardize_summary"
            logger.info(f"[{video_id}] Standardizing summary.")
            if video.get('llm_summary'):
                try:
                    standardized_summary = await standardizer_agent.standardize(video['llm_summary'])
                    video['standardized_summary'] = standardized_summary
                    logger.info(f"[{video_id}] Standardized LLM summary: {standardized_summary}")
                except Exception as e:
                    logger.error(f"[{video_id}] LLM summary standardization error: {e}")
                    video['standardized_summary'] = video.get('llm_summary', '')
            if video.get('audio_summary'):
                try:
                    standardized_audio = await standardizer_agent.standardize(video['audio_summary'])
                    video['standardized_audio_summary'] = standardized_audio
                    logger.info(f"[{video_id}] Standardized audio summary: {standardized_audio}")
                except Exception as e:
                    logger.error(f"[{video_id}] Audio summary standardization error: {e}")
                    video['standardized_audio_summary'] = video.get('audio_summary', '')
            summary_text = (
                video.get('standardized_audio_summary')
                or video.get('standardized_summary')
                or video.get('llm_summary', '')
            )
            video['final_summary'] = summary_text

            # Step 6: 更新数据库记录
            step = "update_metadata"
            if not dry_run and persist_summaries and async_db:
                video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                await async_db.update_video_metadata(
                    video_id,
                    video.get('llm_summary', ''),
                    video.get('transcript', ''),
                    json.dumps(video.get('audio_summary', {})) if video.get('audio_summary') else None
                )
                logger.info(f"[{video_id}] Metadata updated in DB.")

        finally:
            # 确保释放并发资源
            if not dry_run and concurrency_manager:
                concurrency_manager.release()
            
    except Exception as e:
        logger.error(f"[{video_id}] Error at step {step}: {e}")
        logger.debug(traceback.format_exc())

# -------------------------------------------------------------------------------
# 主流程：处理多个视频，支持异步退出且仅完成已调度视频
# -------------------------------------------------------------------------------
async def process_videos(keyword, top_k, youtube_service, openai_api_key, db_path,
                         persist_summaries, full_audio_analysis, dry_run, max_n, pure_youtube=False,
                         logger=None, concurrency_manager=None):
    logger.info("Starting video processing pipeline.")
    if dry_run:
        logger.info("Dry run mode: external API calls and DB writes are skipped.")
        logger.info("🧪 Creating mock data for testing...")

    async_db = AsyncDatabase(Database(db_path, logger=logger)) if not dry_run else None
    
    # Dry run模式下不需要监听退出命令
    if not dry_run:
        exit_event = asyncio.Event()
        exit_listener = asyncio.create_task(listen_for_exit(exit_event))
    else:
        exit_event = None
        exit_listener = None

    processed_videos = []
    tasks = []

    try:
        from utils.openAIServices import OpenAIService
        openai_service = OpenAIService(api_key=openai_api_key, logger=logger)
        standardizer_agent = StandardizerAgent(openai_service=openai_service, logger=logger, dry_run=dry_run)
        
        if dry_run:
            # Dry run模式：使用mock数据
            logger.info("🎭 Using mock video data for dry run")
            refined = []
            for i in range(min(top_k, 3)):  # 最多3个mock视频
                mock_video = {
                    'video_id': f'mock_video_{i+1}',
                    'title': f'Mock Video {i+1}: {keyword} Tutorial',
                    'description': f'This is a mock video description for testing {keyword} processing.',
                    'channel_title': f'Mock Channel {i+1}',
                    'published_at': '2025-01-01T00:00:00Z',
                    'view_count': 1000 * (i+1),
                    'like_count': 100 * (i+1),
                    'duration': '00:05:00',
                    'weight': 1.0 - (i * 0.1)
                }
                refined.append(mock_video)
        else:
            # 正常模式：真实搜索
            search_agent = SearchAgent(youtube_service, openai_service=openai_service, logger=logger)
            aggregated = await search_agent.aggregate_search(keyword)
            if asyncio.iscoroutine(aggregated):
                aggregated = await aggregated
            deduped = search_agent.deduplicate_results(aggregated)
            top_n = top_k if pure_youtube else top_k * max_n
            refined = search_agent.refine_results(deduped, top_n=top_n)
            
        logger.info(f"Selected {len(refined)} videos of keyword {keyword} for processing.")

        # 懒加载 tqdm 仅在需要时导入
        if not dry_run:
            from tqdm import tqdm
            progress_bar = tqdm(refined, desc="Processing Videos")
        else:
            progress_bar = refined

        for video in progress_bar:
            if exit_event and exit_event.is_set():
                logger.info("Exit command detected. Stopping scheduling of new videos.")
                break
            task = asyncio.create_task(process_single_video(
                video, keyword, async_db, persist_summaries,
                full_audio_analysis, dry_run, youtube_service, openai_service, standardizer_agent,
                logger, concurrency_manager
            ))
            tasks.append(task)
            processed_videos.append(video)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        logger.debug(traceback.format_exc())
    finally:
        if async_db:
            await async_db.close()
        if exit_listener:
            await exit_listener

        report = {
            "processed_videos": len(processed_videos),
            "video_ids": [video.get('video_id') for video in processed_videos],
            "total_prompt_tokens": openai_service.total_prompt_tokens if openai_service else 0,
            "total_completion_tokens": openai_service.total_completion_tokens if openai_service else 0,
            "total_cost": openai_service.total_cost if openai_service else 0.0,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        os.makedirs('logs', exist_ok=True)
        report_file = os.path.join("logs", f"report_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json")
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
        logger.info(f"Processing report generated: {report_file}")
        logger.info("Video processing pipeline completed.")

# -------------------------------------------------------------------------------
# 主入口
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    print_startup_banner()
    args = parse_cli_arguments()
    
    # 初始化增强的logger
    log_level = "DEBUG" if args.verbose else ("ERROR" if args.quiet else "INFO")
    logger = get_logger(level=log_level, log_file=getattr(args, 'log_file', None))
    
    # 初始化智能并发管理器
    base_concurrency = int(os.getenv("CONCURRENCY", "3"))
    if hasattr(args, 'concurrency') and args.concurrency:
        base_concurrency = args.concurrency
    concurrency_manager = ConcurrencyManager(initial_concurrency=base_concurrency)
    
    logger.info("🐋 TubeWhale initialization complete")
    logger.info(f"Configuration: log_level={log_level}, concurrency={base_concurrency}")
    
    # 配置测试模式
    if hasattr(args, 'config_test') and args.config_test:
        logger.info("🔧 Running configuration test...")
        test_passed = True
        
        # 测试API密钥
        try:
            config_obj = Config(vars(args))
            if not config_obj.YOUTUBE_API_KEYS:
                logger.error("❌ YouTube API keys not configured")
                test_passed = False
            else:
                logger.info(f"✅ YouTube API keys: {len(config_obj.YOUTUBE_API_KEYS)} key(s) found")
            
            if not config_obj.OPENAI_API_KEY and not getattr(args, 'pure_youtube', False):
                logger.error("❌ OpenAI API key not configured (required for AI features)")
                test_passed = False
            else:
                logger.info("✅ OpenAI API key: configured")
                
            if test_passed:
                logger.info("🎉 Configuration test passed! All required settings are valid.")
            else:
                logger.error("❌ Configuration test failed. Please check your .env file.")
                
        except Exception as e:
            logger.error(f"❌ Configuration test failed: {e}")
            test_passed = False
            
        sys.exit(0 if test_passed else 1)

    try:
        config_obj = Config(vars(args))
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Use --config-test to diagnose configuration issues")
        sys.exit(1)

    keyword = config_obj.KEYWORD
    openai_api_key = config_obj.OPENAI_API_KEY
    db_path = config_obj.DB_PATH
    persist_summaries = config_obj.PERSIST_AGENT_SUMMARIES
    full_audio_analysis = config_obj.FULL_AUDIO_ANALYSIS
    dry_run = config_obj.DRY_RUN
    max_n = config_obj.MAX_N
    top_k = config_obj.TOP_K
    pure_youtube = config_obj.PURE_YOUTUBE
    
    # CLI参数覆盖配置文件设置（工业级配置优先级）
    if hasattr(args, 'keyword') and args.keyword:
        keyword = args.keyword
        logger.info(f"CLI override: keyword = {keyword}")
    
    if hasattr(args, 'top_k') and args.top_k:
        top_k = args.top_k
        logger.info(f"CLI override: top_k = {top_k}")
    
    if hasattr(args, 'audio_analysis') and args.audio_analysis:
        full_audio_analysis = True
        logger.info("CLI override: full_audio_analysis enabled")
    
    if hasattr(args, 'no_persist') and args.no_persist:
        persist_summaries = False
        logger.info("CLI override: persist_summaries disabled")
    
    if hasattr(args, 'dry_run') and args.dry_run:
        dry_run = True
        logger.info("CLI override: dry_run enabled")
    
    if hasattr(args, 'pure_youtube') and args.pure_youtube:
        pure_youtube = True
        logger.info("CLI override: pure_youtube enabled")

    if pure_youtube:
        logger.info("Pure YouTube mode enabled: forcing max_n=1 and disabling audio analysis.")
        max_n = 1
        full_audio_analysis = False

    logger.info("Starting video processing script with parameters:")
    logger.info(f"  keyword = {keyword}")
    logger.info(f"  top_k = {top_k}")
    logger.info(f"  YouTube API keys count = {len(config_obj.YOUTUBE_API_KEYS)}")
    logger.info(f"  openai_api_key = {'SET' if openai_api_key else 'NOT SET'}")
    logger.info(f"  db_path = {db_path}")
    logger.info(f"  persist_agent_summaries = {persist_summaries}")
    logger.info(f"  full_audio_analysis = {full_audio_analysis}")
    logger.info(f"  dry_run = {dry_run}")
    logger.info(f"  max_n = {max_n}")
    logger.info(f"  concurrency = {concurrency_manager.current_concurrency}")
    logger.info(f"  pure_youtube = {pure_youtube}")

    if not config_obj.YOUTUBE_API_KEYS or (not pure_youtube and not openai_api_key):
        logger.error("Required API keys not found. Please check your .env file.")
        sys.exit(1)
    if not keyword:
        logger.error("No keyword provided in configuration.")
        sys.exit(1)

    try:
        youtube_service = YouTubeService(config_obj.YOUTUBE_API_KEYS, logger=logger)
    except Exception as e:
        logger.error(f"Error initializing YouTubeService: {e}")
        sys.exit(1)
    logger.info(f"YouTubeService initialized with {len(config_obj.YOUTUBE_API_KEYS)} key(s).")

    try:
        asyncio.run(
            process_videos(
                keyword=keyword,
                top_k=top_k,
                youtube_service=youtube_service,
                openai_api_key=openai_api_key,
                db_path=db_path,
                persist_summaries=persist_summaries,
                full_audio_analysis=full_audio_analysis,
                dry_run=dry_run,
                max_n=max_n,
                pure_youtube=pure_youtube,
                logger=logger,
                concurrency_manager=concurrency_manager
            )
        )
        
        # 显示性能报告
        if concurrency_manager:
            perf_stats = concurrency_manager.get_performance_stats()
            logger.info("📊 Performance Summary:")
            logger.info(f"  • Concurrency used: {perf_stats['current_concurrency']}")
            logger.info(f"  • Total tasks processed: {perf_stats['total_tasks']}")
            logger.info(f"  • Average task time: {perf_stats['avg_task_time']:.2f}s")
            logger.info(f"  • Performance score: {perf_stats['performance_score']:.2f}")
            
            if perf_stats['avg_task_time'] > 60:
                logger.warning("⚠️  High average task time detected. Consider:")
                logger.warning("   - Reducing concurrency if network-bound")
                logger.warning("   - Checking API rate limits")
                logger.warning("   - Disabling audio analysis for faster processing")
                
        logger.info("🎉 Script finished successfully.")
        
    except KeyboardInterrupt:
        logger.warning("🛑 Process interrupted by user")
        if concurrency_manager:
            stats = concurrency_manager.get_performance_stats()
            logger.info(f"📊 Partial results: {stats['total_tasks']} tasks completed")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
        
    finally:
        # 清理全局资源
        try:
            asyncio.run(cleanup_http_session())
        except Exception as e:
            logger.warning(f"Warning during cleanup: {e}")
        
        # 强制垃圾回收
        import gc
        gc.collect()
        logger.info("🧹 Resources cleaned up successfully")
