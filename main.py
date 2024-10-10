import asyncio
import logging
import os
import json  # 导入 json 模块
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
from utils.database import init_db, store_video_metadata, store_comments, update_video_metadata
from agents.search_agent import multiagent_search
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent, chunk_text_by_tokens
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_summary
from agents.standardizer_agent import standardizer_agent
from utils.youtube_fetcher import fetch_all_comments, fetch_video_metadata
from utils.helper import retry
import openai  # 确保导入 openai

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retry mechanism wrapper for fetching transcripts
@retry(max_retries=3, delay=2)
async def fetch_transcript_with_retry(video_id):
    try:
        return await fetch_transcript(video_id)
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video {video_id}: {e}")
        raise

# Retry mechanism wrapper for summarization
@retry(max_retries=3, delay=2)
async def summarize_with_retry(transcript):
    try:
        summary = await gpt_summarizer_agent(transcript)
        logging.info("Summary generated successfully.")
        return summary
    except Exception as e:
        logging.error(f"Error during summarization: {e}")
        return None

# Process a single video and store metadata, comments, etc.
async def process_single_video(video, openai_api_key, keyword, conn, persist_agent_summaries, full_audio_analysis, dry_run, youtube_api_key):
    video_id = video['video_id']
    step = ""
    try:
        # Step 1: Fetch and store video metadata
        step = "fetch_metadata"
        logging.info(f"Fetching metadata for video {video_id}.")
        video_metadata = fetch_video_metadata(video_id, youtube_api_key)

        if video_metadata and not dry_run and persist_agent_summaries:
            store_video_metadata(conn, video_metadata)  # Ensure metadata is stored

        # Step 2: Fetch transcript or audio summary
        step = "fetch_transcript_or_audio"
        try:
            transcript = await fetch_transcript_with_retry(video_id)
        except Exception:
            transcript = None  # Ensure transcript is None if fetching fails

        if transcript:
            video['transcript'] = transcript
            video['llm_summary'] = await summarize_with_retry(transcript)  # Summarization step
            video['summary_source'] = 'transcript'
            logging.info(f"Transcript and LLM summary generated for video {video_id}.")
        else:
            logging.info(f"No transcript available for video {video_id}.")

        if full_audio_analysis:
            logging.info(f'Full audio analysis enabled: {full_audio_analysis}')
            logging.info(f"Attempting audio summarization for video ID: {video_id}.")
            summary = await transcribe_audio_to_summary(video_id, keyword, video_metadata)

            if summary:
                video['audio_summary'] = summary
                # 如果存在LLM summary，也可以合并summary_source
                if 'summary_source' in video:
                    video['summary_source'] += ', audio'
                else:
                    video['summary_source'] = 'audio'
                logging.info(f"Audio summary generated successfully for video {video_id}.")
                logging.info(f'Video audio summary collected: {summary}')
            else:
                logging.error(f"No audio summary available for video {video_id}. Skipping audio summarization.")
                # 不返回，允许继续处理 transcript
        else:
            logging.info(f"Full audio analysis is disabled, skipping audio summarization for video {video_id}.")

        # Step 3: Fetch and store comments
        step = "fetch_comments"
        try:
            comments = fetch_all_comments(video_id, youtube_api_key)
            logging.info(f"Fetched {len(comments)} comments for video ID: {video_id}")
        except Exception as e:
            logging.error(f"Error fetching comments for video {video_id}: {e}")
            comments = None  # Continue even if comments fetching fails

        if comments and not dry_run and persist_agent_summaries:
            store_comments(conn, video_id, comments)
            logging.info(f"Comments stored for video ID: {video_id}")

        # Step 4: Ensure weighted_score exists
        video['weighted_score'] = video.get('weighted_score', 0)

        # Step 5: Standardize summary and analyze metadata
        step = "standardize_summary_metadata"
        logging.info(f"Standardizing summary and metadata for video {video_id}")
        summary = None
        if 'llm_summary' in video and video['llm_summary']:
            standardized_results = await standardizer_agent(video['llm_summary'])

            if standardized_results:
                video['standardized_summary'] = standardized_results
                logging.info(f"Standardization completed for video {video_id}.")
                summary = video['standardized_summary']
            else:
                logging.error(f"Standardization failed for video {video_id}. Using original summary.")
                video['standardized_summary'] = video['llm_summary']
                summary = video['llm_summary']
            logging.info(f'[STEP 5] Standard agent summary: {summary}')
        
        if 'audio_summary' in video and video['audio_summary']:
            standardized_results = await standardizer_agent(video['audio_summary'])

            if standardized_results:
                video['standardized_summary'] = standardized_results
                logging.info(f"Standardization completed for audio summary of video {video_id}.")
                summary = video['standardized_summary']
            else:
                logging.error(f"Standardization failed for audio summary of video {video_id}. Using original audio summary.")
                video['standardized_summary'] = video['audio_summary']
                summary = video['audio_summary']
            logging.info(f'[STEP 5] Standard agent summary: {summary}')
        else:
            logging.error(f"No summary available to standardize for video {video_id}.")

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
                audio_summary_serialized,  # 序列化后的 audio_summary
            )
            logging.info(f"Metadata updated in the database for video {video_id}.")
    except Exception as e: 
        logging.error(f'Error during procee video {video_id}, Exception:{e}')
    # Main function to process multiple videos
async def process_videos(keyword, top_k, filter_type, youtube_api_key, openai_api_key, db_path, persist_agent_summaries, full_audio_analysis, dry_run, max_n):
    logging.info("Starting video processing pipeline.")

    if dry_run:
        logging.info("Running in dry_run mode: No API calls will be made, and no data will be persisted.")

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

        if not search_results:
            raise Exception("No search results {search_results} returned from YouTube API.")

        # For logging purposes, output the generated keywords
        logging.info(f"Generated keywords: {generated_keywords}")

        step = "filter_search_results"
        # Step 2: Filter valid search results
        valid_videos = search_results.get('videos', [])
        if not valid_videos:
            logging.error("No valid search results found with videos.")
            return

        # **移除Critic Agent的调用**
        # step = "critic_agent_ranking"
        # # Step 3: Rank videos using critic agent
        # logging.info("Starting critic agent to rank videos.")
        # ranked_videos = await critic_agent(valid_videos, openai_api_key, conn=conn)
        # if not ranked_videos:
        #     logging.error("Critic agent did not return valid video rankings.")
        #     return

        # **直接处理所有有效视频**
        ranked_videos = valid_videos  # 不进行排名，仅处理所有视频

        logging.info(f"Total videos to process: {len(ranked_videos)}")

        # Process all videos
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
            ) for video in tqdm(ranked_videos, desc="Processing Videos")
        ]

        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Pipeline failed at step {step}: {e}")
        logging.exception(e)
    finally:
        if conn:
            conn.close()
        logging.info("Video processing pipeline completed.")

# Main entry point
if __name__ == "__main__":
    import os
    from datetime import datetime

    # Set up logging to file
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Generate filename based on current timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    log_filename = os.path.join('logs', f'{timestamp}.log')

    # Create file handler which logs messages
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the handler to the root logger
    logging.getLogger().addHandler(file_handler)

    # Load environment variables
    load_dotenv()
    keyword=os.getenv("KEYWORD")
    youtube_api_key = os.getenv("YOUTUBE_API_KEY") 
    openai_api_key = os.getenv("OPENAI_API_KEY")
    persist_agent_summaries = os.getenv("PERSIST_AGENT_SUMMARIES", "true").lower() == "true"
    full_audio_analysis = os.getenv("FULL_AUDIO_ANALYSIS", "true").lower() == "true"
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    max_n = int(os.getenv("MAX_N", "5"))
    top_k = int(os.getenv("TOP_K", "3"))
    filter_type = os.getenv("FILTER_TYPE", "view_count")
    db_path = "youtube_summaries.db"

    if not youtube_api_key or not openai_api_key:
        logging.error("API keys not found. Make sure .env file is set correctly and contains both YOUTUBE_API_KEY and OPENAI_API_KEY.")
        raise ValueError("API keys are required")
    else:
        logging.info("Starting the video processing script...")
        try:
            asyncio.run(process_videos(
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
            ))
        except Exception as e:
            logging.error(f"An error occurred while running the pipeline: {e}")
            logging.exception(e)
        logging.info("Script execution finished.")
