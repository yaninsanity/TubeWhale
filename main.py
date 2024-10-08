import asyncio
import logging
import os
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime
from utils.database import init_db, store_video_metadata, store_comments, update_video_metadata
from agents.search_agent import multiagent_search
from agents.critic_agent import critic_agent
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent, chunk_text_by_tokens
from agents.filter_agent import filter_videos
from agents.audio_agent import transcribe_audio_to_summary
from agents.standardizer_agent import standardizer_agent
from utils.youtube_fetcher import fetch_all_comments, fetch_video_metadata
from utils.helper import retry

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize OpenAI async client (assuming it's initialized within your agents)
# Note: If the client is initialized in agents, you don't need to initialize it here

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
        transcript = await fetch_transcript_with_retry(video_id)

        if transcript:
            video['transcript'] = transcript
            video['llm_summary'] = await summarize_with_retry(transcript)  # Summarization step
            video['summary_source'] = 'transcript'
        elif full_audio_analysis:
            logging.info(f"No transcript found, attempting audio summarization for {video_id}.")
            summary = await transcribe_audio_to_summary(video_id, openai_api_key, keyword)

            if summary:
                video['audio_summary'] = summary
                video['summary_source'] = 'audio'
                logging.info(f"Audio summary generated successfully for video {video_id}.")
            else:
                logging.error(f"No transcript or audio available for video {video_id}. Skipping.")
                return  # Exit early if no transcript or audio
        else:
            logging.error(f"No transcript available for video {video_id}, and full_audio_analysis is disabled. Skipping.")
            return

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
        if 'llm_summary' in video and video['llm_summary']:
            standardized_results = await standardizer_agent(video['llm_summary'])

            if standardized_results:
                video['standardized_summary'] = standardized_results
                logging.info(f"Standardization completed for video {video_id}.")
            else:
                logging.error(f"Standardization failed for video {video_id}. Using original summary.")
                video['standardized_summary'] = video['llm_summary']
        else:
            logging.error(f"No summary available to standardize for video {video_id}.")

        # Step 6: Store final metadata into the database
        if not dry_run and persist_agent_summaries and conn:
            video['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Add timestamp
            logging.info(f"llm summary:{(video['llm_summary'])}")
            update_video_metadata(
                conn,
                video_id,
                video.get('llm_summary', ''),
                video.get('transcript', ''),
                video.get('audio_summary', ''),
            )
            logging.info(f"Metadata updated in the database for video {video_id}.")

    except Exception as e:
        logging.error(f"Error processing video {video_id} at step {step}: {e}")

# Main function to process multiple videos
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
        # Step 3: Rank keywords using critic agent
        logging.info("Starting critic agent to rank topics.")
        best_keyword, keyword_rankings = await critic_agent(valid_search_results, openai_api_key)
        if not best_keyword:
            logging.error("Critic agent did not return a valid best keyword.")
            return

        step = "process_videos"
        # Step 4: Process videos under the best keyword
        filtered_videos = filter_videos(valid_search_results[best_keyword]['videos'], filter_type)
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
            ) for video in tqdm(filtered_videos, desc="Processing Videos")
        ]

        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Pipeline failed at step {step}: {e}")
    finally:
        if conn:
            conn.close()
        logging.info("Video processing pipeline completed.")

# Main entry point
if __name__ == "__main__":
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    persist_agent_summaries = os.getenv("PERSIST_AGENT_SUMMARIES", "true").lower() == "true"
    full_audio_analysis = os.getenv("FULL_AUDIO_ANALYSIS", "false").lower() == "true"
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
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
