import asyncio
import logging
import os
from tqdm import tqdm
from dotenv import load_dotenv
from utils.database import init_db, store_video_metadata, store_keyword_analysis, store_comments, store_ai_interaction
from agents.search_agent import multiagent_search
from agents.critic_agent import critic_agent
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_summary
from agents.standardizer_agent import standardizer_agent  # Import standardizer agent
from datetime import datetime
from utils.youtube_fetcher import fetch_all_comments, fetch_video_metadata

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
                    _delay *= backoff_factor
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# Fetch video transcript with retries
@retry(max_retries=3, delay=2)
async def fetch_transcript_with_retry(video_id):
    try:
        transcript = await fetch_transcript(video_id)
        return transcript
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video {video_id}: {e}")
        raise

# Process a single video using agents and storing metadata, transcript, comments
async def process_single_video(video, openai_api_key, keyword, conn, persist_agent_summaries, full_audio_analysis, dry_run, youtube_api_key):
    video_id = video['video_id']
    try:
        step = "fetch_metadata"
        # Step 1: Fetch video metadata and store
        logging.info(f"Fetching metadata for video {video_id} using YouTube API.")
        video_metadata = fetch_video_metadata(video_id, youtube_api_key)  # Pass the API key here
        if video_metadata and not dry_run and persist_agent_summaries:
            store_video_metadata(conn, video_metadata)
        
        step = "fetch_transcript_or_audio"
        # Step 2: Fetch transcript or summarize audio
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

        step = "fetch_comments"
        # Step 3: Fetch comments and store them
        comments = None
        try:
            comments = fetch_all_comments(video_id, youtube_api_key)
        except Exception as e:
            logging.error(f"Error fetching comments for video ID {video_id}: {e}")

        # Proceed with storing comments if they are available
        if comments and not dry_run and persist_agent_summaries:
            store_comments(conn, video_id, comments)
            
        step = "final_store_metadata"
        # Ensure weighted_score is not None
        video['weighted_score'] = video.get('weighted_score', 0)

        # Step 4: Standardize summary and metadata
        logging.info(f"Standardizing summary and metadata for video {video_id}")
        standardized_results = await standardizer_agent(video['summary'], video_metadata, openai_api_key)
        if standardized_results:
            video['standardized_summary'] = standardized_results['standardized_summary']
            video['metadata_analysis'] = standardized_results['metadata_analysis']

        # Store results if required
        if not dry_run and persist_agent_summaries and conn:
            video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            store_video_metadata(conn, video)
    except Exception as e:
        logging.error(f"Error processing video {video_id}: {e}")

# Summarization function with retries
@retry(max_retries=3, delay=2)
async def summarize_with_retry(transcript, api_key):
    return await gpt_summarizer_agent(transcript, api_key)

# Main processing pipeline for videos
async def process_videos(keyword, agent_count, top_k, filter_type, youtube_api_key, openai_api_key, db_path, persist_agent_summaries, full_audio_analysis, dry_run, max_n):
    logging.info("Starting video processing pipeline.")
    
    if dry_run:
        logging.info("Running in dry_run mode: No API calls will be made, and no data will be persisted.")
    
    conn = init_db(db_path) if not dry_run else None

    try:
        step = "brainstorm_keywords"
        # Step 1: Brainstorm and search keyword variations
        logging.info(f"Brainstorming {max_n} keyword variations with {agent_count} agents.")
        search_results = await multiagent_search(keyword, agent_count, max_n, top_k, youtube_api_key, openai_api_key, dry_run)
        
        if not search_results:
            raise Exception("No search results returned from YouTube API.")
        
        step = "filter_search_results"
        # Step 2: Filter valid search results
        valid_search_results = {kw: data for kw, data in search_results.items() if data['videos']}
        if not valid_search_results:
            logging.error("No valid search results found with videos.")
            return

        step = "critic_agent_ranking"
        # Step 3: Critic agent to rank keywords
        logging.info("Starting critic agent to rank topics.")
        best_keyword, keyword_rankings = await critic_agent(valid_search_results, openai_api_key)
        if not best_keyword:
            logging.error("Critic agent did not return a valid best keyword.")
            return

        step = "process_videos"
        # Step 4: Process videos under the best keyword
        filtered_videos = filter_videos(valid_search_results[best_keyword]['videos'], filter_type)
        tasks = [process_single_video(video, openai_api_key, keyword, conn, persist_agent_summaries, full_audio_analysis, dry_run, youtube_api_key) for video in tqdm(filtered_videos, desc="Processing Videos")]

        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        if conn:
            conn.close()
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
