import os
import sqlite3
import asyncio
from dotenv import load_dotenv
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from openai import AsyncOpenAI
from langchain_openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
from tqdm import tqdm
import logging

#
#ADD TAG

# Load environment variables
load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
DB_PATH = 'youtube_summaries.db'
SEARCH_QUERY = 'Virginia fishing'
MAX_RESULTS = 10  # Number of videos to retrieve

# Initialize OpenAI async client
async_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize YouTube API service
def get_youtube_service():
    return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# Function to search YouTube videos
def search_youtube_videos(query, max_results=10):
    youtube = get_youtube_service()
    request = youtube.search().list(
        q=query,
        part='snippet',
        type='video',
        maxResults=max_results,
        order='relevance'
    )
    response = request.execute()
    return [{'video_id': item['id']['videoId'], 'title': item['snippet']['title']} for item in response['items']]

# Initialize SQLite database
def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            long_text TEXT NOT NULL,
            summary TEXT NOT NULL
        )
    ''')
    conn.commit()
    return conn

# Check if a video exists in the database
def video_exists(conn, video_id):
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM videos WHERE video_id = ?', (video_id,))
    return cursor.fetchone() is not None

# Store video summary and long text in the database
def store_video_summary(conn, video_id, title, long_text, summary):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO videos (video_id, title, long_text, summary) VALUES (?, ?, ?, ?)', 
                   (video_id, title, long_text, summary))
    conn.commit()

# Fetch transcript from YouTube
def fetch_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
        return " ".join([item['text'] for item in transcript])
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video ID {video_id}: {e}")
        return None

# Function to chunk large text into manageable pieces
def chunk_text(text, max_length=128000):
    words = text.split()
    return [" ".join(words[i:i+max_length]) for i in range(0, len(words), max_length)]

# Summarization agent using LangChain with OpenAI
async def gpt_summarizer_agent(long_text):
    memory = ConversationBufferMemory()
    chunks = chunk_text(long_text)
    llm = OpenAI(client=async_client)

    # Sequential process to summarize text chunk by chunk
    summary_prompt = ChatPromptTemplate.from_template("Please summarize the following text: {text}")
    
    summaries = []
    for chunk in chunks:
        prompt = summary_prompt.format(text=chunk)
        summary = await llm.ainvoke(prompt)
        summaries.append(summary)
    return " ".join(summaries)

# Critic agent using LangChain to critique the summary
async def critic_agent(summary):
    llm = OpenAI(client=async_client)
    critique_prompt = ChatPromptTemplate.from_template("Please critique the following summary: {summary}")
    prompt = critique_prompt.format(summary=summary)
    critique = await llm.ainvoke(prompt)
    return critique

# Standardizer agent to extract structured information from the summary and store in long_text field
async def standardizer_agent(summary):
    llm = OpenAI(client=async_client)
    
    standardize_prompt = ChatPromptTemplate.from_template(
        '''
        Standardize and enrich the following summary. Include additional structured information where available:
        - Season
        - Location (e.g., lake, river)
        - Fish types
        - Lure or bait used
        - Any additional fishing tips
        
        Return this enriched information in the same text block for consistency.
        Summary: {summary}
        '''
    )
    
    prompt = standardize_prompt.format(summary=summary)
    
    try:
        standardized_data = await llm.ainvoke(prompt)
        return standardized_data
    except Exception as e:
        logging.error(f"Error in standardizing summary: {e}")
        return None

# Main function to process videos
async def process_videos():
    # Initialize database
    conn = init_db(DB_PATH)

    # Search for new videos
    videos = search_youtube_videos(SEARCH_QUERY, MAX_RESULTS)

    # Initialize progress bar
    progress_bar = tqdm(total=len(videos), desc="Processing Videos")

    for video in videos:
        video_id = video['video_id']
        title = video['title']

        logging.info(f"Processing Video ID: {video_id}, Title: {title}")

        # Check if video is already processed
        if video_exists(conn, video_id):
            logging.info(f"Video ID {video_id} already exists in the database. Skipping.")
            progress_bar.update(1)
            continue

        # Fetch transcript (long text)
        long_text = fetch_transcript(video_id)
        if long_text:
            # Step 1: Summarization agent
            summary = await gpt_summarizer_agent(long_text)
            if not summary:
                logging.error(f"Failed to summarize Video ID {video_id}. Skipping.")
                progress_bar.update(1)
                continue

            # Step 2: Critic agent
            critique = await critic_agent(summary)
            if not critique:
                logging.error(f"Failed to critique summary for Video ID {video_id}. Skipping.")
                progress_bar.update(1)
                continue

            # Step 3: Standardizer agent to enrich summary
            enriched_summary = await standardizer_agent(summary)
            if not enriched_summary:
                logging.error(f"Failed to standardize summary for Video ID {video_id}. Skipping.")
                progress_bar.update(1)
                continue

            # Store the long text, enriched summary, and other information in the database
            store_video_summary(conn, video_id, title, long_text, enriched_summary)
            logging.info(f"Stored enriched summary and long text for Video ID {video_id} in the database.")

        # Update progress bar
        progress_bar.update(1)

    # Close the progress bar
    progress_bar.close()
    conn.close()
    logging.info("Processing complete.")

if __name__ == "__main__":
    # Run the video processing loop
    asyncio.run(process_videos())

