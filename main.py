import asyncio
import logging
import os
import sys
import json
import traceback
from datetime import datetime

from tqdm import tqdm

# === Your internal modules ===
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
# Import the global configuration from utils/config.py
# -------------------------------------------------------------------------------
from utils.config import Config

# -------------------------------------------------------------------------------
# Initialize logging
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

# Global semaphore (will be set later from CLI or config)
semaphore = None

# -------------------------------------------------------------------------------
# CLI Arguments Parser
# -------------------------------------------------------------------------------
def parse_cli_arguments():
    """
    Parse command-line arguments, which can override the defaults in the .env file.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="YouTube Summarization Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--keyword", type=str, help="Base search keyword; if not provided, uses KEYWORD from .env")
    parser.add_argument("--top_k", type=int, help="Number of videos to retrieve per keyword variation (default from TOP_K in .env)")
    parser.add_argument("--filter_type", type=str, help="Filtering/sorting method for search results (e.g., view_count)")
    parser.add_argument("--youtube_api_key", type=str, help="YouTube Data API Key; if not provided, read from .env")
    parser.add_argument("--openai_api_key", type=str, help="OpenAI API Key; if not provided, read from .env")
    parser.add_argument("--db_path", type=str, help="Path to the database; default from .env")
    parser.add_argument("--persist_agent_summaries", action="store_true", help="Persist agent results; if set, then True")
    parser.add_argument("--no_persist_agent_summaries", action="store_true", help="Explicitly set persist_agent_summaries to False")
    parser.add_argument("--full_audio_analysis", action="store_true", help="Enable full audio analysis; if set, then True")
    parser.add_argument("--no_full_audio_analysis", action="store_true", help="Explicitly set full_audio_analysis to False")
    parser.add_argument("--dry_run", action="store_true", help="If set, skip external API calls and do not write to database")
    parser.add_argument("--no_dry_run", action="store_true", help="Explicitly set dry_run to False")
    parser.add_argument("--max_n", type=int, help="Number of keyword variations to generate; default from .env")
    parser.add_argument("--concurrency", type=int, help="Number of concurrent tasks; overrides CONCURRENCY in .env")
    # ★ New: Pure YouTube mode flag. When enabled, the system uses only the base keyword without AI expansion and disables audio analysis.
    parser.add_argument("--pure_youtube", action="store_true", help="Pure YouTube mode: use only the base keyword (disable AI expansion and audio analysis).")

    return parser.parse_args()

# -------------------------------------------------------------------------------
# Retry-decorated async functions
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
# Process a single video
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

            # Step 3: Audio analysis (only if full_audio_analysis is enabled)
            if full_audio_analysis:
                logging.info(f"Full audio analysis enabled: {full_audio_analysis}")
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
                    logging.info(f"Video audio summary collected: {summary}")
                else:
                    logging.error(f"No audio summary available for video {video_id}. Skipping audio summarization.")
            else:
                logging.info(f"Audio analysis is disabled, skipping audio summarization for video {video_id}.")

            # Step 4: Fetch and store comments
            step = "fetch_comments"
            try:
                if not dry_run:
                    comments = fetch_all_comments(video_id, youtube_api_key)
                    logging.info(f"Fetched {len(comments)} comments for video ID: {video_id}")
                else:
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
                logging.info(f"Comments stored for video {video_id}.")

            # Step 5: Ensure weighted_score field exists
            video['weighted_score'] = video.get('weighted_score', 0)

            # Step 6: Standardize summary and analyze metadata
            step = "standardize_summary_metadata"
            logging.info(f"Standardizing summary and metadata for video {video_id}")
            summary_text = None

            # Prioritize standardizing the LLM summary
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
                logging.info(f"[STEP 5] Standard agent summary (LLM): {summary_text}")

            # Also standardize audio summary if available
            if 'audio_summary' in video and video['audio_summary']:
                standardized_audio = None
                try:
                    standardized_audio = await standardizer_agent(video['audio_summary'])
                except Exception as e:
                    logging.error(f"Error standardizing audio summary for video {video_id}: {e}")

                if standardized_audio:
                    video['standardized_summary'] = standardized_audio
                    logging.info(f"Standardization completed for audio summary of video {video_id}.")
                    summary_text = video['standardized_summary']
                else:
                    logging.error(f"Standardization failed for audio summary for video {video_id}. Using original audio summary.")
                    video['standardized_summary'] = video['audio_summary']
                    summary_text = video['audio_summary']
                logging.info(f"[STEP 5] Standard agent summary (Audio): {summary_text}")
            else:
                logging.info(f"No separate audio_summary to standardize for video {video_id} (may be normal).")

            # Step 7: Store final metadata into the database
            if not dry_run and persist_agent_summaries and conn:
                video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Add timestamp

                # Debug info
                if 'llm_summary' in video:
                    logging.info(f"LLM Summary: {video['llm_summary']}")
                if 'audio_summary' in video:
                    logging.info(f"Audio Summary: {video['audio_summary']}")

                # Serialize audio summary if exists
                audio_summary_serialized = json.dumps(video.get('audio_summary', {})) if 'audio_summary' in video else None

                update_video_metadata(
                    conn,
                    video_id,
                    video.get('llm_summary', ''),
                    video.get('transcript', ''),
                    audio_summary_serialized
                )
                logging.info(f"Metadata updated in the database for video {video_id}.")
    except Exception as e:
        logging.error(f"Error during processing video {video_id}, Exception: {e}")
        logging.debug(traceback.format_exc())

# -------------------------------------------------------------------------------
# Main pipeline: Process multiple videos
# -------------------------------------------------------------------------------
# Added new parameter pure_youtube to control whether to use pure YouTube mode.
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
    max_n,
    pure_youtube=False  # New: Pure YouTube mode flag
):
    logging.info("Starting video processing pipeline.")

    if dry_run:
        logging.info("Running in dry_run mode: No API calls or database writes will be performed.")

    # Connect to the database if not in dry run mode
    conn = init_db(db_path) if not dry_run else None

    try:
        step = "brainstorm_keywords"
        logging.info(f"Brainstorming {max_n} keyword variations.")

        # Pass the pure_youtube flag to multiagent_search
        generated_keywords, search_results = await multiagent_search(
            base_keyword=keyword,
            max_n=max_n,
            top_k=top_k,
            youtube_api_key=youtube_api_key,
            openai_api_key=openai_api_key,
            conn=conn,
            dry_run=dry_run,
            pure_youtube=pure_youtube
        )

        if not search_results or not search_results.get("videos"):
            if dry_run:
                logging.info("No search results from multiagent_search; constructing dummy search results for dry_run.")
                search_results = {
                    "videos": [
                        {"video_id": f"dummy_video_{i}", "weighted_score": 0}
                        for i in range(top_k)
                    ]
                }
                generated_keywords = [f"{keyword} dummy variation {i}" for i in range(max_n)]
            else:
                raise Exception(f"No search results returned from YouTube API: {search_results}")

        logging.info(f"Generated keywords: {generated_keywords}")

        step = "filter_search_results"
        valid_videos = search_results.get("videos", [])
        if not valid_videos:
            logging.error("No valid search results found with videos.")
            return

        # In pure YouTube mode, we only process TOP_K videos (since max_n is forced to 1)
        if pure_youtube:
            top_n = min(top_k, len(valid_videos))
        else:
            top_n = min(top_k * max_n, len(valid_videos))
        logging.info(f"Selecting top {top_n} videos from {len(valid_videos)} collected.")

        selected_videos = valid_videos[:top_n]

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
            for video in tqdm(selected_videos, desc="Processing Videos")
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
# Main entry point
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    # Print startup banner
    print_startup_banner()

    args = parse_cli_arguments()

    # Load configuration via the Config module (merging .env and CLI parameters)
    try:
        config_obj = Config(args)
    except Exception as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)

    # Use configuration values from config_obj
    keyword = config_obj.KEYWORD
    youtube_api_key = config_obj.YOUTUBE_API_KEY
    openai_api_key = config_obj.OPENAI_API_KEY
    db_path = config_obj.DB_PATH
    persist_agent_summaries = config_obj.PERSIST_AGENT_SUMMARIES
    full_audio_analysis = config_obj.FULL_AUDIO_ANALYSIS
    dry_run = config_obj.DRY_RUN
    max_n = config_obj.MAX_N
    top_k = config_obj.TOP_K
    filter_type = config_obj.FILTER_TYPE
    concurrency = config_obj.CONCURRENCY
    pure_youtube = config_obj.PURE_YOUTUBE

    semaphore = asyncio.Semaphore(concurrency)

    # In pure YouTube mode, force max_n to 1 and disable audio analysis
    if pure_youtube:
        logging.info("Pure YouTube mode enabled: overriding max_n to 1 and disabling audio analysis to reduce cost.")
        max_n = 1
        full_audio_analysis = False

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
    logging.info(f"  pure_youtube = {pure_youtube}")

    if not youtube_api_key or (not pure_youtube and not openai_api_key):
        logging.error("API keys not found. Make sure .env is set correctly or pass via CLI.")
        sys.exit(1)
    if not keyword:
        logging.error("No keyword found. Provide KEYWORD in .env or --keyword in CLI.")
        sys.exit(1)

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
                max_n=max_n,
                pure_youtube=pure_youtube  # Pass pure YouTube mode flag
            )
        )
        logging.info("Script execution finished successfully.")
    except Exception as e:
        logging.error(f"An error occurred while running the pipeline: {e}")
        logging.debug(traceback.format_exc())
        sys.exit(1)
