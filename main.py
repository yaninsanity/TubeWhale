#!/usr/bin/env python3
import os
import sys
import json
import logging
import asyncio
import traceback
from datetime import datetime
from tqdm import tqdm

# 内部模块
from utils.async_database import AsyncDatabase  # 异步数据库包装器
from utils.database import Database              # 同步数据库，由 AsyncDatabase 包装
from agents.search_agent import SearchAgent
from agents.transcript_agent import fetch_transcript
from agents.summarizer_agent import gpt_summarizer_agent  # 异步 LLM 摘要接口
from agents.audio_agent import AudioProcessingAgent
from agents.standardizer_agent import StandardizerAgent
from utils.youtube import YouTubeService
from utils.helper import print_startup_banner

from dotenv import load_dotenv
load_dotenv()

from utils.config import Config

# -------------------------------------------------------------------------------
# 配置日志
# -------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if not os.path.exists('logs'):
    os.makedirs('logs')
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
log_filename = os.path.join('logs', f'{timestamp}.log')
file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logging.getLogger().addHandler(file_handler)

# -------------------------------------------------------------------------------
# 全局信号量（控制并发数，从配置中读取）
# -------------------------------------------------------------------------------
try:
    concurrency = int(os.getenv("CONCURRENCY", "3"))
except Exception:
    concurrency = 3
semaphore = asyncio.Semaphore(concurrency)

# -------------------------------------------------------------------------------
# CLI 参数解析
# -------------------------------------------------------------------------------
import argparse
def parse_cli_arguments():
    parser = argparse.ArgumentParser(
        description="YouTube Summarization Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--pure_youtube", action="store_true", help="Pure YouTube mode: disable AI expansion and audio analysis.")
    parser.add_argument("--dry_run", action="store_true", help="Dry run mode: skip external API calls and DB writes.")
    return parser.parse_args()

# -------------------------------------------------------------------------------
# 异步监听退出命令
# -------------------------------------------------------------------------------
async def listen_for_exit(exit_event: asyncio.Event):
    logging.info("Type 'exit' and press Enter to gracefully stop processing new videos.")
    user_input = await asyncio.to_thread(input, "")
    if user_input.strip().lower() == "exit":
        logging.info("Exit command received. New videos will not be processed; waiting for current tasks to finish.")
        exit_event.set()

# -------------------------------------------------------------------------------
# 单个视频处理流程（异步调用各 Agent 接口）
# -------------------------------------------------------------------------------
async def process_single_video(video, keyword, async_db: AsyncDatabase, persist_summaries,
                               full_audio_analysis, dry_run, youtube_service, openai_service, standardizer_agent):
    video_id = video['video_id']
    step = ""
    try:
        async with semaphore:
            # Step 1: 获取视频元数据
            step = "fetch_metadata"
            logging.info(f"[{video_id}] Fetching metadata. 😊")
            if not dry_run:
                video_metadata = youtube_service.fetch_video_metadata(video_id)
            else:
                video_metadata = {
                    "video_id": video_id,
                    "title": f"Dummy title for {video_id} [dry_run]",
                    "description": "Dummy description.",
                    "publish_date": "2025-01-01",
                    "channel_id": "dummy_channel",
                    "view_count": 999,
                    "like_count": 999,
                    "comment_count": 999,
                    "snippet": {
                        "title": f"Dummy title for {video_id}",
                        "description": "Dummy description.",
                        "publishedAt": "2025-01-01",
                        "channelTitle": "dummy_channel",
                        "tags": [],
                        "categoryId": "N/A",
                        "defaultAudioLanguage": "en",
                        "defaultLanguage": "en"
                    },
                    "contentDetails": {
                        "duration": "N/A",
                        "dimension": "2d",
                        "definition": "hd",
                        "caption": "false",
                        "licensedContent": False
                    }
                }
            if video_metadata and not dry_run and persist_summaries:
                await async_db.store_video_metadata(video_metadata)
            video['metadata'] = video_metadata

            # Step 2: 获取 transcript 及 LLM 生成摘要
            step = "fetch_transcript"
            logging.info(f"[{video_id}] Fetching transcript. 😊")
            if not dry_run:
                try:
                    transcript = await fetch_transcript(youtube_service, video_id)
                except Exception as e:
                    logging.error(f"[{video_id}] Transcript fetch error: {e}")
                    transcript = None
            else:
                transcript = "Dummy transcript in dry_run."
            if transcript:
                video['transcript'] = transcript
                if not dry_run:
                    prompt_text = transcript  # 可按需要构造更复杂的 prompt
                    llm_summary = await gpt_summarizer_agent(transcript, openai_service=openai_service)
                    video['llm_summary'] = llm_summary
                    await async_db.store_ai_interaction(
                        input_data={"prompt": prompt_text},
                        output_data={"response": llm_summary},
                        interaction_type="transcript_summary",
                        tokens_used=openai_service.total_prompt_tokens,
                        cost=openai_service.total_cost
                    )
                else:
                    video['llm_summary'] = "Dummy LLM summary in dry_run."
                video['summary_source'] = 'transcript'
                logging.info(f"[{video_id}] Transcript and LLM summary obtained. 😊")
            else:
                logging.info(f"[{video_id}] No transcript available. 🌼")

            # Step 3: 异步获取评论（并行执行）
            step = "fetch_comments"
            logging.info(f"[{video_id}] Fetching comments asynchronously. 😊")
            try:
                if not dry_run:
                    loop = asyncio.get_running_loop()
                    comments = await loop.run_in_executor(None, youtube_service.fetch_all_comments, video_id)
                else:
                    comments = [
                        {"comment_id": "dummy1", "author": "Dummy user", "text": "Dummy comment.", "like_count": 0,
                         "publish_time": "2025-01-01", "viewer_rating": "none", "moderation_status": "published", "parent_id": None},
                        {"comment_id": "dummy2", "author": "Tester", "text": "Another dummy comment.", "like_count": 0,
                         "publish_time": "2025-01-01", "viewer_rating": "none", "moderation_status": "published", "parent_id": None}
                    ]
                video['comments'] = comments
                if comments and not dry_run and persist_summaries:
                    await async_db.store_comments(video_id, comments)
                logging.info(f"[{video_id}] {len(comments)} comments fetched. 😊")
            except Exception as e:
                logging.error(f"[{video_id}] Error fetching comments: {e}")
                video['comments'] = None

            # Step 4: 音频分析（可选）
            if full_audio_analysis:
                step = "audio_analysis"
                logging.info(f"[{video_id}] Audio analysis enabled. 😊")
                audio_agent = AudioProcessingAgent(openai_service=openai_service, youtube_service=youtube_service)
                if not dry_run:
                    audio_summary = await audio_agent.download_audio(video_id)
                else:
                    audio_summary = "Dummy audio summary in dry_run."
                if audio_summary:
                    video['audio_summary'] = audio_summary
                    video['summary_source'] = video.get('summary_source', '') + ', audio'
                    logging.info(f"[{video_id}] Audio summary obtained. 😊")
                else:
                    logging.error(f"[{video_id}] Audio summarization failed.")
            else:
                logging.info(f"[{video_id}] Audio analysis disabled.")

            # Step 5: 标准化摘要（统一使用 standardizer_agent）
            step = "standardize_summary"
            logging.info(f"[{video_id}] Standardizing summary. 😊")
            if video.get('llm_summary'):
                try:
                    standardized_summary = await standardizer_agent.standardize(video['llm_summary'])
                    video['standardized_summary'] = standardized_summary
                    logging.info(f"[{video_id}] Standardized LLM summary: {standardized_summary}")
                except Exception as e:
                    logging.error(f"[{video_id}] LLM summary standardization error: {e}")
                    video['standardized_summary'] = video.get('llm_summary', '')
            if video.get('audio_summary'):
                try:
                    standardized_audio = await standardizer_agent.standardize(video['audio_summary'])
                    video['standardized_audio_summary'] = standardized_audio
                    logging.info(f"[{video_id}] Standardized audio summary: {standardized_audio}")
                except Exception as e:
                    logging.error(f"[{video_id}] Audio summary standardization error: {e}")
                    video['standardized_audio_summary'] = video.get('audio_summary', '')
            summary_text = video.get('standardized_audio_summary') or video.get('standardized_summary') or video.get('llm_summary', '')
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
                logging.info(f"[{video_id}] Metadata updated in DB. 😊")

    except Exception as e:
        logging.error(f"[{video_id}] Error at step {step}: {e}")
        logging.debug(traceback.format_exc())

# -------------------------------------------------------------------------------
# 主流程：处理多个视频，支持异步退出且只完成当前视频
# -------------------------------------------------------------------------------
async def process_videos(keyword, top_k, youtube_service, openai_api_key, db_path,
                         persist_summaries, full_audio_analysis, dry_run, max_n, pure_youtube=False):
    logging.info("Starting video processing pipeline.")
    if dry_run:
        logging.info("Dry run mode: skipping external API calls and DB writes.")
    # 构造异步数据库包装对象（非 dry_run 模式下）
    async_db = AsyncDatabase(Database(db_path)) if not dry_run else None

    # 创建退出事件并启动监听任务
    exit_event = asyncio.Event()
    exit_listener = asyncio.create_task(listen_for_exit(exit_event))

    processed_videos = []  # 保存处理过的视频信息
    tasks = []

    try:
        from utils.openAIServices import OpenAIService
        openai_service = OpenAIService(openai_api_key)
        standardizer_agent = StandardizerAgent(openai_service=openai_service)
        search_agent = SearchAgent(youtube_service, openai_service=openai_service)
        aggregated = search_agent.aggregate_search(keyword)
        deduped = search_agent.deduplicate_results(aggregated)
        top_n = top_k if pure_youtube else top_k * max_n
        refined = search_agent.refine_results(deduped, top_n=top_n)
        logging.info(f"Selected {len(refined)} videos for processing.")

        for video in tqdm(refined, desc="Processing Videos"):
            if exit_event.is_set():
                logging.info("Exit command detected. Stopping scheduling of new videos.")
                break
            task = asyncio.create_task(process_single_video(
                video, keyword, async_db, persist_summaries,
                full_audio_analysis, dry_run, youtube_service, openai_service, standardizer_agent
            ))
            tasks.append(task)
            processed_videos.append(video)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        logging.debug(traceback.format_exc())
    finally:
        if async_db:
            async_db.close()
        await exit_listener

        # 生成处理报告
        report = {
            "processed_videos": len(processed_videos),
            "video_ids": [video.get('video_id') for video in processed_videos],
            "total_prompt_tokens": openai_service.total_prompt_tokens if openai_service else 0,
            "total_completion_tokens": openai_service.total_completion_tokens if openai_service else 0,
            "total_cost": openai_service.total_cost if openai_service else 0.0,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        report_file = os.path.join("logs", f"report_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json")
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
        logging.info(f"Processing report generated: {report_file}")
        logging.info("Video processing pipeline completed.")

# -------------------------------------------------------------------------------
# 主入口
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    print_startup_banner()
    args = parse_cli_arguments()

    try:
        config_obj = Config(vars(args))
    except Exception as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)

    # 提取配置
    keyword = config_obj.KEYWORD
    openai_api_key = config_obj.OPENAI_API_KEY
    db_path = config_obj.DB_PATH
    persist_summaries = config_obj.PERSIST_AGENT_SUMMARIES
    full_audio_analysis = config_obj.FULL_AUDIO_ANALYSIS
    dry_run = config_obj.DRY_RUN
    max_n = config_obj.MAX_N
    top_k = config_obj.TOP_K
    pure_youtube = config_obj.PURE_YOUTUBE

    if pure_youtube:
        logging.info("Pure YouTube mode enabled: forcing max_n=1 and disabling audio analysis.")
        max_n = 1
        full_audio_analysis = False

    logging.info("Starting video processing script with parameters:")
    logging.info(f"  keyword = {keyword}")
    logging.info(f"  top_k = {top_k}")
    logging.info(f"  YouTube API keys count = {len(config_obj.YOUTUBE_API_KEYS)}")
    logging.info(f"  openai_api_key = {'SET' if openai_api_key else 'NOT SET'}")
    logging.info(f"  db_path = {db_path}")
    logging.info(f"  persist_agent_summaries = {persist_summaries}")
    logging.info(f"  full_audio_analysis = {full_audio_analysis}")
    logging.info(f"  dry_run = {dry_run}")
    logging.info(f"  max_n = {max_n}")
    logging.info(f"  concurrency = {concurrency}")
    logging.info(f"  pure_youtube = {pure_youtube}")

    if not config_obj.YOUTUBE_API_KEYS or (not pure_youtube and not openai_api_key):
        logging.error("Required API keys not found. Please check your .env file.")
        sys.exit(1)
    if not keyword:
        logging.error("No keyword provided in configuration.")
        sys.exit(1)

    try:
        youtube_service = YouTubeService(config_obj.YOUTUBE_API_KEYS)
    except Exception as e:
        logging.error(f"Error initializing YouTubeService: {e}")
        sys.exit(1)
    logging.info(f"YouTubeService initialized with {len(config_obj.YOUTUBE_API_KEYS)} key(s).")

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
                pure_youtube=pure_youtube
            )
        )
        logging.info("Script finished successfully.")
    except Exception as e:
        logging.error(f"Pipeline execution error: {e}")
        logging.debug(traceback.format_exc())
        sys.exit(1)
