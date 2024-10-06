import asyncio
import logging
import os
from tqdm import tqdm
from dotenv import load_dotenv
from utils.database import init_db, store_video_summary, store_keyword_analysis
from agents.search_agent import multiagent_search
from agents.critic_agent import critic_agent
from agents.transcript_agent import process_video_transcript
from agents.summarization_agent import gpt_summarizer_agent
from agents.standardizer_agent import standardizer_agent
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_summary
from datetime import datetime
from agents.transcript_agent import fetch_transcript

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retry decorator to handle retries with exponential backoff
def retry(max_retries=3, delay=2, backoff_factor=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries} in {_delay} seconds...")
                    await asyncio.sleep(_delay)
                    _delay *= backoff_factor  # Increase delay exponentially
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# Retry-enhanced fetch transcript function (asynchronous version)
@retry(max_retries=3, delay=2)
async def fetch_transcript_with_retry(video_id):
    try:
        # Assuming fetch_transcript is an async function that requires awaiting
        transcript = await fetch_transcript(video_id)
        return transcript
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video {video_id}: {e}")
        raise

# Process single video function for concurrency
async def process_single_video(video, openai_api_key, keyword, conn, persist_agent_summaries, full_audio_analysis, dry_run):
    video_id = video['video_id']
    try:
        # Step 1: Fetch transcript or summarize audio
        transcript = await fetch_transcript_with_retry(video_id)
        if transcript:
            video['transcript'] = transcript
            video['summary'] = await summarize_with_retry(transcript, openai_api_key)
            video['summary_source'] = 'transcript'
        elif full_audio_analysis:
            logging.info(f"No transcript found, attempting audio summarization for {video_id}.")
            summary = await transcribe_audio_to_summary(video_id, openai_api_key, keyword)
            if summary:
                video['summary'] = summary
                video['summary_source'] = 'audio'
            else:
                logging.error(f"Failed to process video {video_id}: No transcript or audio found.")
                return

        # Ensure weighted_score is not None
        video['weighted_score'] = video.get('weighted_score', 0)  # Default to 0 if None

        # Store results if required
        if not dry_run and persist_agent_summaries and conn:
            video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            store_video_summary(conn, video)

    except Exception as e:
        logging.error(f"Error processing video {video_id}: {e}")

# Retry-enhanced summarization function
@retry(max_retries=3, delay=2)
async def summarize_with_retry(transcript, api_key):
    return await gpt_summarizer_agent(transcript, api_key)

# Main video processing pipeline
async def process_videos(keyword, agent_count, top_k, filter_type, youtube_api_key, openai_api_key, db_path, persist_agent_summaries, full_audio_analysis, dry_run, max_n):
    logging.info("Starting video processing pipeline.")
    
    if dry_run:
        logging.info("Running in dry_run mode: No API calls will be made, and no data will be persisted.")
    
    conn = init_db(db_path) if not dry_run else None
    failed_videos = []  # Keep track of videos that failed processing

    try:
        # Step 1: Brainstorm and search keyword variations
        logging.info(f"Brainstorming {max_n} keyword variations with {agent_count} agents.")
        search_results = await multiagent_search(keyword, agent_count, max_n, top_k, youtube_api_key, openai_api_key, dry_run)
        
        if not search_results:
            raise Exception("No search results returned from YouTube API.")

        # Step 2: Filter out valid results from search
        valid_search_results = {kw: data for kw, data in search_results.items() if data['videos']}

        if not valid_search_results:
            logging.error("No valid search results found with videos.")
            return

        # Step 3: Use critic agent to rank the keywords based on metadata
        logging.info("Starting critic agent to rank topics.")
        best_keyword, keyword_rankings = await critic_agent(valid_search_results, openai_api_key)

        if not best_keyword:
            logging.error("Critic agent did not return a valid best keyword.")
            return

        if not dry_run:
            store_keyword_analysis(conn, keyword_rankings)

        # Step 4: Process videos under the best keyword
        filtered_videos = filter_videos(valid_search_results[best_keyword]['videos'], filter_type)
        tasks = [process_single_video(video, openai_api_key, keyword, conn, persist_agent_summaries, full_audio_analysis, dry_run) for video in tqdm(filtered_videos, desc="Processing Videos")]

        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        if conn:
            conn.close()
        if failed_videos:
            logging.warning(f"Failed to process the following videos: {failed_videos}")
        logging.info("Video processing pipeline completed.")

# Main entry point
if __name__ == "__main__":
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    persist_agent_summaries = bool(os.getenv("PERSIST_AGENT_SUMMARIES", "true").lower() == "true")
    full_audio_analysis = bool(os.getenv("FULL_AUDIO_ANALYSIS", "false").lower() == "true")
    dry_run = bool(os.getenv("DRY_RUN", "false").lower() == "true")
    max_n = int(os.getenv("MAX_N", "10"))

    if not youtube_api_key or not openai_api_key:
        logging.error("API keys not found. Make sure .env file is set correctly and contains both YOUTUBE_API_KEY and OPENAI_API_KEY.")
        raise ValueError("API keys are required")
    else:
        logging.info("Starting the video processing script...")
        try:
            asyncio.run(process_videos(
                keyword="virginia fishing",
                agent_count=5,
                top_k=5,
                filter_type="relevance",
                youtube_api_key=youtube_api_key,
                openai_api_key=openai_api_key,
                db_path="youtube_summaries.db",
                persist_agent_summaries=persist_agent_summaries,
                full_audio_analysis=full_audio_analysis,
                dry_run=dry_run,
                max_n=max_n
            ))
        except Exception as e:
            logging.error(f"An error occurred while running the pipeline: {e}")
        logging.info("Script execution finished.")
