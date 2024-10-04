import asyncio
import logging
import os
from tqdm import tqdm
from dotenv import load_dotenv
from utils.database import init_db, store_video_summary, store_keyword_analysis
from agents.search_agent import multiagent_search
from agents.critic_agent import critic_agent
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent
from agents.standardizer_agent import standardizer_agent
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_text
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retry decorator to handle retries
def retry(max_retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries}...")
                    await asyncio.sleep(delay)
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# Main video processing pipeline
async def process_videos(keyword, agent_count, top_k, filter_type, youtube_api_key, openai_api_key, db_path):
    logging.info("Starting video processing pipeline.")
    conn = init_db(db_path)

    try:
        # Step 1: Brainstorm and search keyword variations
        search_results = multiagent_search(keyword, agent_count, top_k, youtube_api_key, openai_api_key)
        best_keyword, keyword_rankings = await critic_agent(search_results, openai_api_key)
        store_keyword_analysis(conn, keyword_rankings)

        # Step 2: Process videos
        filtered_videos = filter_videos(search_results[best_keyword], filter_type)
        for video in filtered_videos:
            video_id = video['video_id']
            try:
                transcript = await fetch_transcript_with_retry(video_id)
                if transcript:
                    video['transcript'] = transcript
                    video['summary'] = await summarize_with_retry(transcript, openai_api_key)
                    video['summary_source'] = 'transcript'
                else:
                    # Try audio transcription
                    audio_path = await download_audio(video_id)
                    if audio_path:
                        transcript = await transcribe_audio(audio_path)
                        video['transcript'] = transcript
                        video['summary'] = await summarize_with_retry(transcript, openai_api_key)
                        video['summary_source'] = 'audio'
                    else:
                        logging.error(f"Failed to process video {video_id}: No transcript or audio found")
                        continue

                # Store video info in DB
                video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                store_video_summary(conn, video)

            except Exception as e:
                logging.error(f"Error processing video {video_id}: {e}")

    finally:
        conn.close()
        logging.info("Video processing pipeline completed.")

# Retry-enhanced fetch transcript function
@retry(max_retries=3, delay=2)
async def fetch_transcript_with_retry(video_id):
    return fetch_transcript(video_id)

# Retry-enhanced summarization function
@retry(max_retries=3, delay=2)
async def summarize_with_retry(transcript, api_key):
    return await gpt_summarizer_agent(transcript, api_key)

# Retry-enhanced standardization function
@retry(max_retries=3, delay=2)
async def standardize_with_retry(summary, metadata, api_key):
    return await standardizer_agent(summary, metadata, api_key)

if __name__ == "__main__":
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not youtube_api_key or not openai_api_key:
        logging.error("API keys not found. Make sure .env file is set correctly and contains both YOUTUBE_API_KEY and OPENAI_API_KEY.")
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
                db_path="youtube_summaries.db"
            ))
        except Exception as e:
            logging.error(f"An error occurred while running the pipeline: {e}")
        logging.info("Script execution finished.")
