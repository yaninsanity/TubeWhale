import asyncio
import logging
import os
import sys
import json
import traceback
from datetime import datetime

from dotenv import load_dotenv
from tqdm import tqdm

# === 你的项目内部模块 ===
from utils.database import init_db, store_video_metadata, store_comments, update_video_metadata
from agents.search_agent import multiagent_search
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent, chunk_text_by_tokens
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_summary
from agents.standardizer_agent import standardizer_agent
from utils.youtube_fetcher import fetch_all_comments, fetch_video_metadata
from utils.helper import retry, print_startup_banner
import openai

# -------------------------------------------------------------------------------
# 加载环境变量
# -------------------------------------------------------------------------------
load_dotenv()

# -------------------------------------------------------------------------------
# 初始化日志
# -------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 创建 logs 目录及文件日志
if not os.path.exists('logs'):
    os.makedirs('logs')
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
log_filename = os.path.join('logs', f'{timestamp}.log')
file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logging.getLogger().addHandler(file_handler)

# 全局信号量（默认值，稍后由 CLI 参数覆盖）
semaphore = None

# -------------------------------------------------------------------------------
# 命令行参数解析函数
# -------------------------------------------------------------------------------
def parse_cli_arguments():
    """
    解析命令行参数，可用于覆盖 .env 中的默认值。
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="YouTube Summarization Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--keyword", type=str, help="基础搜索关键词；若不提供则使用 .env 中 KEYWORD")
    parser.add_argument("--top_k", type=int, help="每个关键词变体要抓取的视频数量，默认从 .env 中读取 TOP_K")
    parser.add_argument("--filter_type", type=str, help="过滤和排序搜索结果的方式（如 view_count）")
    parser.add_argument("--youtube_api_key", type=str, help="YouTube Data API Key；若不提供则从 .env 中读取 YOUTUBE_API_KEY")
    parser.add_argument("--openai_api_key", type=str, help="OpenAI API Key；若不提供则从 .env 中读取 OPENAI_API_KEY")
    parser.add_argument("--db_path", type=str, help="数据库路径；默认从 .env 中读取 DB_PATH")
    parser.add_argument("--persist_agent_summaries", action="store_true", help="是否持久化存储 agent 结果；若加上此标志则为 True")
    parser.add_argument("--no_persist_agent_summaries", action="store_true", help="若加上此标志则显式设为 False（优先级高于 --persist_agent_summaries）")
    parser.add_argument("--full_audio_analysis", action="store_true", help="是否对音频进行完整分析；若加上此标志则为 True")
    parser.add_argument("--no_full_audio_analysis", action="store_true", help="若加上此标志则显式设为 False（优先级高于 --full_audio_analysis）")
    parser.add_argument("--dry_run", action="store_true", help="若加上此标志，则跳过外部 API 并不写数据库")
    parser.add_argument("--no_dry_run", action="store_true", help="显式设 dry_run 为 False（优先级高于 --dry_run）")
    parser.add_argument("--max_n", type=int, help="多智能体生成多少个关键词变体；默认从 .env 中读取 MAX_N")
    parser.add_argument("--concurrency", type=int, help="并发任务数量；优先级高于 .env 中的 CONCURRENCY")

    return parser.parse_args()

# -------------------------------------------------------------------------------
# 带重试机制的异步函数
# -------------------------------------------------------------------------------
@retry(max_retries=3, delay=2)
async def fetch_transcript_with_retry(video_id):
    try:
        return await fetch_transcript(video_id)
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video {video_id}: {e}")
        raise

@retry(max_retries=3, delay=2)
async def summarize_with_retry(transcript):
    try:
        summary = await gpt_summarizer_agent(transcript)
        logging.info("Summary generated successfully.")
        return summary
    except Exception as e:
        logging.error(f"Error during summarization: {e}")
        return None

# -------------------------------------------------------------------------------
# 单个视频的处理逻辑
# -------------------------------------------------------------------------------
async def process_single_video(
    video,
    openai_api_key,
    keyword,
    conn,
    persist_agent_summaries,
    full_audio_analysis,
    dry_run,
    youtube_api_key
):
    video_id = video['video_id']
    step = ""
    try:
        async with semaphore:
            # Step 1: Fetch and store video metadata
            step = "fetch_metadata"
            logging.info(f"Fetching metadata for video {video_id}.")

            if not dry_run:
                video_metadata = fetch_video_metadata(video_id, youtube_api_key)
            else:
                # 若是 dry_run，可自行决定要不要生成 fake metadata
                video_metadata = {
                    "video_id": video_id,
                    "title": f"Dummy title for {video_id} [dry_run]",
                    "description": "No real metadata fetched in dry_run.",
                    "publish_date": "2025-01-01",
                    "channel_id": "dummy_channel_id",
                    "view_count": 999,
                    "like_count": 999,
                    "comment_count": 999
                }

            if video_metadata and not dry_run and persist_agent_summaries:
                store_video_metadata(conn, video_metadata)

            # Step 2: Fetch transcript or audio summary
            step = "fetch_transcript_or_audio"
            logging.info(f"Attempting to fetch transcript for video {video_id}.")
            transcript = None
            if not dry_run:
                try:
                    transcript = await fetch_transcript_with_retry(video_id)
                except Exception:
                    transcript = None
            else:
                transcript = "Dummy transcript for dry_run."

            if transcript:
                video['transcript'] = transcript
                if not dry_run:
                    video['llm_summary'] = await summarize_with_retry(transcript)
                else:
                    video['llm_summary'] = "This is a dummy LLM summary in dry_run."
                video['summary_source'] = 'transcript'
                logging.info(f"Transcript and LLM summary generated for video {video_id}.")
            else:
                logging.info(f"No transcript available for video {video_id}.")

            # 若启用 full_audio_analysis，则进行音频分析
            if full_audio_analysis:
                logging.info(f'Full audio analysis enabled: {full_audio_analysis}')
                logging.info(f"Attempting audio summarization for video ID: {video_id}.")
                if not dry_run:
                    summary = await transcribe_audio_to_summary(video_id, keyword, video_metadata)
                else:
                    summary = "Dummy audio summary for dry_run."

                if summary:
                    video['audio_summary'] = summary
                    if 'summary_source' in video:
                        video['summary_source'] += ', audio'
                    else:
                        video['summary_source'] = 'audio'
                    logging.info(f"Audio summary generated successfully for video {video_id}.")
                    logging.info(f'Video audio summary collected: {summary}')
                else:
                    logging.error(f"No audio summary available for video {video_id}. Skipping audio summarization.")
            else:
                logging.info(f"Full audio analysis is disabled, skipping audio summarization for video {video_id}.")

            # Step 3: Fetch and store comments
            step = "fetch_comments"
            try:
                if not dry_run:
                    comments = fetch_all_comments(video_id, youtube_api_key)
                    logging.info(f"Fetched {len(comments)} comments for video ID: {video_id}")
                else:
                    # dry_run 模式下生成一些 dummy 评论
                    comments = [
                        {"author": "Dummy user", "text": "This is a dummy comment for dry_run."},
                        {"author": "Tester", "text": "Another dummy comment."}
                    ]
                    logging.info(f"Fetched {len(comments)} dummy comments for video {video_id} in dry_run.")
            except Exception as e:
                logging.error(f"Error fetching comments for video {video_id}: {e}")
                comments = None

            if comments and not dry_run and persist_agent_summaries:
                store_comments(conn, video_id, comments)
                logging.info(f"Comments stored for video ID: {video_id}")

            # Step 4: Ensure weighted_score
            video['weighted_score'] = video.get('weighted_score', 0)

            # Step 5: Standardize summary and analyze metadata
            step = "standardize_summary_metadata"
            logging.info(f"Standardizing summary and metadata for video {video_id}")
            summary_text = None

            # 优先对 llm_summary 做标准化
            if 'llm_summary' in video and video['llm_summary']:
                standardized_results = None
                try:
                    standardized_results = await standardizer_agent(video['llm_summary'])
                except Exception as e:
                    logging.error(f"Error standardizing LLM summary for video {video_id}: {e}")

                if standardized_results:
                    video['standardized_summary'] = standardized_results
                    logging.info(f"Standardization completed for LLM summary of video {video_id}.")
                    summary_text = video['standardized_summary']
                else:
                    logging.error(f"Standardization failed for LLM summary of video {video_id}. Using original summary.")
                    video['standardized_summary'] = video['llm_summary']
                    summary_text = video['llm_summary']
                logging.info(f'[STEP 5] Standard agent summary (LLM): {summary_text}')

            # 如果有音频分析结果，也进行标准化
            if 'audio_summary' in video and video['audio_summary']:
                standardized_audio = None
                try:
                    standardized_audio = await standardizer_agent(video['audio_summary'])
                except Exception as e:
                    logging.error(f"Error standardizing audio summary for video {video_id}: {e}")

                if standardized_audio:
                    # 最终写回 standardized_summary，以最新一个为准
                    video['standardized_summary'] = standardized_audio
                    logging.info(f"Standardization completed for audio summary of video {video_id}.")
                    summary_text = video['standardized_summary']
                else:
                    logging.error(f"Standardization failed for audio summary of video {video_id}. Using original audio summary.")
                    video['standardized_summary'] = video['audio_summary']
                    summary_text = video['audio_summary']
                logging.info(f'[STEP 5] Standard agent summary (Audio): {summary_text}')
            else:
                logging.info(f"No separate audio_summary to standardize for video {video_id} (may be normal).")

            # Step 6: Store final metadata into the database
            if not dry_run and persist_agent_summaries and conn:
                video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Add timestamp

                # 打印调试信息
                if 'llm_summary' in video:
                    logging.info(f"LLM Summary: {video['llm_summary']}")
                if 'audio_summary' in video:
                    logging.info(f"Audio Summary: {video['audio_summary']}")

                # 序列化 audio_summary
                audio_summary_serialized = json.dumps(video.get('audio_summary', {})) if 'audio_summary' in video else None

                # 调用 update_video_metadata 并传递 audio_summary
                update_video_metadata(
                    conn,
                    video_id,
                    video.get('llm_summary', ''),
                    video.get('transcript', ''),
                    audio_summary_serialized
                )
                logging.info(f"Metadata updated in the database for video {video_id}.")
    except Exception as e:
        logging.error(f'Error during processing video {video_id}, Exception: {e}')
        logging.debug(traceback.format_exc())

# -------------------------------------------------------------------------------
# 主流程：处理多个视频
# -------------------------------------------------------------------------------
async def process_videos(
    keyword,
    top_k,
    filter_type,
    youtube_api_key,
    openai_api_key,
    db_path,
    persist_agent_summaries,
    full_audio_analysis,
    dry_run,
    max_n
):
    logging.info("Starting video processing pipeline.")

    if dry_run:
        logging.info("Running in dry_run mode: No API calls will be made, and no data will be persisted.")

    # 连接数据库，若 dry_run 则不连接
    conn = init_db(db_path) if not dry_run else None

    try:
        step = "brainstorm_keywords"
        # Step 1: Brainstorm and search keyword variations
        logging.info(f"Brainstorming {max_n} keyword variations.")
        generated_keywords, search_results = await multiagent_search(
            base_keyword=keyword,
            max_n=max_n,
            top_k=top_k,
            youtube_api_key=youtube_api_key,
            openai_api_key=openai_api_key,
            conn=conn,
            dry_run=dry_run
        )

        # 若返回空，在 dry_run 时可自行构造一些 dummy 结果
        if not search_results or not search_results.get("videos"):
            if dry_run:
                logging.info("No search results from multiagent_search, creating dummy search results for dry_run.")
                search_results = {
                    "videos": [
                        {"video_id": f"dummy_video_{i}", "weighted_score": 0}
                        for i in range(top_k)
                    ]
                }
                generated_keywords = [f"{keyword} dummy variation {i}" for i in range(max_n)]
            else:
                raise Exception(f"No search results {search_results} returned from YouTube API.")

        # 输出生成的关键词
        logging.info(f"Generated keywords: {generated_keywords}")

        step = "filter_search_results"
        # Step 2: Filter valid search results
        valid_videos = search_results.get('videos', [])
        if not valid_videos:
            logging.error("No valid search results found with videos.")
            return

        # 不调用 Critic Agent，直接处理全部视频
        ranked_videos = valid_videos

        logging.info(f"Total videos to process: {len(ranked_videos)}")

        # 并发处理所有视频
        tasks = [
            process_single_video(
                video,
                openai_api_key,
                keyword,
                conn,
                persist_agent_summaries,
                full_audio_analysis,
                dry_run,
                youtube_api_key
            )
            for video in tqdm(ranked_videos, desc="Processing Videos")
        ]

        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Pipeline failed at step {step}: {e}")
        logging.debug(traceback.format_exc())
    finally:
        if conn:
            conn.close()
        logging.info("Video processing pipeline completed.")

# -------------------------------------------------------------------------------
# 打印启动横幅
# -------------------------------------------------------------------------------


# -------------------------------------------------------------------------------
# 入口
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    # 打印启动横幅
    print_startup_banner()

    # 先尝试获取命令行参数（可选）
    args = parse_cli_arguments()

    # ---------------------------------------------------------------------------
    # 1) 从 .env 中读取默认值
    # ---------------------------------------------------------------------------
    keyword_env = os.getenv("KEYWORD")
    youtube_api_key_env = os.getenv("YOUTUBE_API_KEY")
    openai_api_key_env = os.getenv("OPENAI_API_KEY")
    db_path_env = os.getenv("DB_PATH", "youtube_summaries.db")
    persist_env = os.getenv("PERSIST_AGENT_SUMMARIES", "true").lower() == "true"
    full_audio_env = os.getenv("FULL_AUDIO_ANALYSIS", "true").lower() == "true"
    dry_run_env = os.getenv("DRY_RUN", "false").lower() == "true"
    max_n_env = int(os.getenv("MAX_N", "5"))
    top_k_env = int(os.getenv("TOP_K", "3"))
    filter_type_env = os.getenv("FILTER_TYPE", "view_count")
    concurrency_env = int(os.getenv("CONCURRENCY", "1"))

    # ---------------------------------------------------------------------------
    # 2) 若命令行指定了就覆盖，否则用 .env 的值
    # ---------------------------------------------------------------------------
    keyword = args.keyword if args.keyword else keyword_env
    youtube_api_key = args.youtube_api_key if args.youtube_api_key else youtube_api_key_env
    openai_api_key = args.openai_api_key if args.openai_api_key else openai_api_key_env
    db_path = args.db_path if args.db_path else db_path_env

    # persist_agent_summaries 有两个互斥参数：--persist_agent_summaries / --no_persist_agent_summaries
    if args.no_persist_agent_summaries:
        persist_agent_summaries = False
    elif args.persist_agent_summaries:
        persist_agent_summaries = True
    else:
        persist_agent_summaries = persist_env

    # full_audio_analysis 同理
    if args.no_full_audio_analysis:
        full_audio_analysis = False
    elif args.full_audio_analysis:
        full_audio_analysis = True
    else:
        full_audio_analysis = full_audio_env

    # dry_run 同理
    if args.no_dry_run:
        dry_run = False
    elif args.dry_run:
        dry_run = True
    else:
        dry_run = dry_run_env

    # top_k 和 max_n
    top_k = args.top_k if args.top_k is not None else top_k_env
    max_n = args.max_n if args.max_n is not None else max_n_env

    if args.filter_type:
        filter_type = args.filter_type
    else:
        filter_type = filter_type_env

    # 并发数：先看 CLI 参数，否则从 .env 读取
    concurrency = args.concurrency if args.concurrency is not None else concurrency_env

    # 设置全局信号量
    semaphore = asyncio.Semaphore(concurrency)

    # ---------------------------------------------------------------------------
    # 3) 打印最终使用的参数，便于调试
    # ---------------------------------------------------------------------------
    logging.info("Starting the video processing script with the following parameters:")
    logging.info(f"  keyword = {keyword}")
    logging.info(f"  top_k = {top_k}")
    logging.info(f"  filter_type = {filter_type}")
    logging.info(f"  youtube_api_key = {'SET' if youtube_api_key else 'NOT SET'}")
    logging.info(f"  openai_api_key = {'SET' if openai_api_key else 'NOT SET'}")
    logging.info(f"  db_path = {db_path}")
    logging.info(f"  persist_agent_summaries = {persist_agent_summaries}")
    logging.info(f"  full_audio_analysis = {full_audio_analysis}")
    logging.info(f"  dry_run = {dry_run}")
    logging.info(f"  max_n = {max_n}")
    logging.info(f"  concurrency = {concurrency}")

    # 若关键 key 缺失则退出
    if not youtube_api_key or not openai_api_key:
        logging.error("API keys not found. Make sure .env is set correctly or pass via CLI.")
        sys.exit(1)
    if not keyword:
        logging.error("No keyword found. Provide KEYWORD in .env or --keyword in CLI.")
        sys.exit(1)

    # ---------------------------------------------------------------------------
    # 4) 执行异步主流程
    # ---------------------------------------------------------------------------
    try:
        asyncio.run(
            process_videos(
                keyword=keyword,
                top_k=top_k,
                filter_type=filter_type,
                youtube_api_key=youtube_api_key,
                openai_api_key=openai_api_key,
                db_path=db_path,
                persist_agent_summaries=persist_agent_summaries,
                full_audio_analysis=full_audio_analysis,
                dry_run=dry_run,
                max_n=max_n
            )
        )
        logging.info("Script execution finished successfully.")
    except Exception as e:
        logging.error(f"An error occurred while running the pipeline: {e}")
        logging.debug(traceback.format_exc())
        sys.exit(1)
