import asyncio
import logging
import os
from dotenv import load_dotenv  # 加载 dotenv 库
from tqdm import tqdm
from utils.database import init_db, store_video_summary
from agents.search_agent import multiagent_search
from agents.transcript_agent import fetch_transcript
from agents.summarization_agent import gpt_summarizer_agent
from agents.critic_agent import critic_agent
from agents.standardizer_agent import standardizer_agent
from agents.filter_agent import filter_videos

# Load environment variables from .env file
load_dotenv()  # 这会加载 .env 文件中的变量

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Main video processing pipeline
async def process_videos(keyword, agent_count, top_k, filter_type, youtube_api_key, openai_api_key, db_path):
    logging.info("Starting video processing pipeline.")
    
    # Step 1: Initialize database
    try:
        conn = init_db(db_path)
        logging.info(f"Database initialized at {db_path}.")
    except Exception as e:
        logging.error(f"Failed to initialize the database: {e}")
        return
    
    # Step 2: Multi-agent search
    try:
        logging.info(f"Starting multi-agent search for keyword: {keyword}")
        videos = multiagent_search(keyword, agent_count, top_k, youtube_api_key)
        logging.info(f"Found {len(videos)} videos after multi-agent search.")
    except Exception as e:
        logging.error(f"Error during multi-agent search: {e}")
        return

    # Step 3: Filter videos
    try:
        logging.info("Filtering videos.")
        filtered_videos = filter_videos(videos, filter_type)
        logging.info(f"{len(filtered_videos)} videos remain after filtering.")
    except Exception as e:
        logging.error(f"Error while filtering videos: {e}")
        return

    # Step 4: Process each video
    for video in tqdm(filtered_videos, desc="Processing Videos"):
        video_id = video['video_id']
        try:
            logging.info(f"Processing video: {video['title']} (ID: {video_id})")

            # Step 5: Fetch transcript
            transcript = fetch_transcript(video_id)
            if transcript:
                logging.info(f"Transcript fetched for video ID {video_id}")
                video['transcript'] = transcript

                # Step 6: Summarize transcript
                summary = await gpt_summarizer_agent(transcript, openai_api_key)
                logging.info(f"Summary generated for video ID {video_id}")
                video['summary'] = summary

                # Step 7: Critique summary
                critique = await critic_agent(summary, openai_api_key)
                logging.info(f"Critique for video ID {video_id}: {critique}")

                # Step 8: Standardize summary
                enriched_summary = await standardizer_agent(summary, openai_api_key)
                logging.info(f"Standardized summary for video ID {video_id}")
                video['summary'] = enriched_summary

                # Step 9: Store video info in database
                store_video_summary(conn, video)
                logging.info(f"Video ID {video_id} stored in the database.")
            else:
                logging.error(f"No transcript found for video ID {video_id}")
        except Exception as e:
            logging.error(f"Error processing video {video_id}: {e}")

    # Close the database connection
    conn.close()
    logging.info("Video processing pipeline completed.")

if __name__ == "__main__":
    # 加载环境变量
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not youtube_api_key or not openai_api_key:
        logging.error("API keys not found. Make sure .env file is set correctly and contains both YOUTUBE_API_KEY and OPENAI_API_KEY.")
    else:
        logging.info("Starting the video processing script...")
        try:
            asyncio.run(process_videos(
                keyword="virginia fishing",
                agent_count=3,
                top_k=5,
                filter_type="relevance",
                youtube_api_key=youtube_api_key,  # 从环境变量中读取
                openai_api_key=openai_api_key,    # 从环境变量中读取
                db_path="youtube_summaries.db"
            ))
        except Exception as e:
            logging.error(f"An error occurred while running the pipeline: {e}")
        logging.info("Script execution finished.")
