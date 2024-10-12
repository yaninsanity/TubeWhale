# search_agent.py

import logging
import asyncio
from openai import AsyncOpenAI
from googleapiclient.errors import HttpError
from utils.youtube_api import get_youtube_service
from utils.database import store_ai_interaction
from datetime import datetime
from ssl import SSLError
from concurrent.futures import ThreadPoolExecutor
from asyncio import Semaphore
from aiolimiter import AsyncLimiter
import json
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Load YouTube API key
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
if not youtube_api_key:
    logging.error("YouTube API key not found. Please set YOUTUBE_API_KEY in your environment variables.")
    sys.exit(1)

# Load OpenAI API key
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    logging.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
    sys.exit(1)

# Initialize OpenAI client
aclient = AsyncOpenAI(api_key=openai_api_key)

# Initialize single YouTube Data API client
youtube_service = get_youtube_service(youtube_api_key)

# Initialize ThreadPoolExecutor with a limited number of workers to prevent excessive concurrency
executor = ThreadPoolExecutor(max_workers=3)  # Adjust based on your system

# Semaphore to limit concurrency
semaphore = Semaphore(3)  # Adjust based on your system and API rate limits

# Rate limiter: e.g., max 15 requests per second
rate_limiter = AsyncLimiter(max_rate=15, time_period=1)

# Flag to indicate if quota is exceeded
quota_exceeded = False

# Retry decorator with exponential backoff
def retry(max_retries=3, delay=2, backoff_factor=2, exceptions=(Exception,)):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    # Special handling for HttpError
                    if isinstance(e, HttpError):
                        error_content = e.content.decode('utf-8') if e.content else 'No content'
                        if 'quotaExceeded' in str(e):
                            logging.error(f"Quota exceeded: {error_content}")
                            raise e  # Stop further processing
                    if attempt == max_retries:
                        logging.error(f"Error in {func.__name__}: {e}. Exceeded maximum retries.")
                        raise
                    else:
                        logging.warning(f"Error in {func.__name__}: {e}. Retrying {attempt}/{max_retries} after {current_delay} seconds...")
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff_factor
        return wrapper
    return decorator

@retry(max_retries=3, delay=2, backoff_factor=2, exceptions=(Exception,))
async def keyword_generator_agent(base_keyword, max_n, api_key, conn=None):
    """
    Generate keyword variations using OpenAI's API.

    Parameters:
        base_keyword (str): The base keyword for generating variations.
        max_n (int): Maximum number of keyword variations to generate.
        api_key (str): OpenAI API key.
        conn (optional): Database connection object.

    Returns:
        list: List of generated keyword variations.
    """
    logging.info(f"Generating up to {max_n} variations for base keyword: '{base_keyword}'")

    try:
        # Define system prompt and user message
        messages = [
            {"role": "system", "content": (
                "You are a domain expert specializing in professional problem-solving and brainstorming. "
                "Generate relevant and highly accurate keyword variations for the domain-specific keyword provided."
            )},
            {"role": "user", "content": (
                f"Generate up to {max_n} relevant keyword variations for the base keyword '{base_keyword}' "
                f"to search for high topic-related YouTube videos. Provide each keyword on a separate line without numbering."
            )}
        ]

        logging.info(f"Sending prompt to OpenAI API for keyword generation.")

        response = await aclient.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            max_tokens=150,
            temperature=0.7
        )

        content = response.choices[0].message.content.strip()
        generated_keywords = content.split("\n")
        generated_keywords = list(set(filter(None, (kw.strip() for kw in generated_keywords))))
        generated_keywords = generated_keywords[:max_n]

        logging.info(f"Generated {len(generated_keywords)} keyword variations: {generated_keywords}")

        # Record AI interaction to the database if connection is provided
        if conn:
            store_ai_interaction(
                conn,
                "\n".join([msg['content'] for msg in messages]),  # Input
                "\n".join(generated_keywords),  # Output
                "keyword_generation",  # Interaction type
                datetime.utcnow()  # Timestamp
            )
            logging.info(f"AI interaction of type 'keyword_generation' stored successfully.")

        return generated_keywords

    except Exception as e:
        logging.error(f"Error generating keywords with OpenAI: {e}")
        logging.exception(e)
        return [base_keyword]  # Fallback to base keyword in case of error

@retry(max_retries=3, delay=5, backoff_factor=2, exceptions=(SSLError, asyncio.TimeoutError))
async def search_youtube_videos(keyword, youtube_api_key, top_k, timeout=30):
    """
    Search YouTube for videos matching the given keyword.

    Parameters:
        keyword (str): The search keyword.
        youtube_api_key (str): YouTube Data API key.
        top_k (int): Maximum number of videos to retrieve.
        timeout (int): Timeout for each API call in seconds.

    Returns:
        list: List of video details dictionaries.
    """
    global quota_exceeded
    if quota_exceeded:
        logging.error("Quota has been exceeded. Skipping further YouTube searches.")
        return []

    async with semaphore, rate_limiter:
        youtube = youtube_service
        logging.info(f"Fetching videos for keyword: '{keyword}' with top_k={top_k}")

        videos = []
        next_page_token = None
        fetched_videos = 0
        max_results_per_page = 50  # YouTube API maximum results per page

        while fetched_videos < top_k and not quota_exceeded:
            results = min(max_results_per_page, top_k - fetched_videos)

            def make_search_request():
                return youtube.search().list(
                    part="snippet",
                    q=keyword,
                    maxResults=results,
                    type='video',
                    videoEmbeddable='true',
                    videoSyndicated='true',
                    pageToken=next_page_token
                ).execute()

            try:
                search_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, make_search_request),
                    timeout=timeout
                )
            except asyncio.TimeoutError as e:
                logging.warning(f"Timeout during search request for keyword '{keyword}': {e}")
                raise e  # Will be caught by retry decorator
            except SSLError as e:
                logging.error(f"SSL error during search request for keyword '{keyword}': {e}")
                raise e  # Will be caught by retry decorator
            except HttpError as e:
                error_content = e.content.decode('utf-8') if e.content else 'No content'
                if 'quotaExceeded' in str(e):
                    logging.error(f"Quota exceeded for YouTube API during search for keyword '{keyword}': {error_content}")
                    quota_exceeded = True
                    raise e  # Stop further processing
                elif 'videoNotFound' in str(e):
                    logging.error(f"One or more videos not found for keyword '{keyword}': {error_content}")
                    return videos
                else:
                    logging.error(f"HTTP Error during search for keyword '{keyword}': {error_content}")
                    return videos
            except Exception as e:
                logging.error(f"Unexpected error during search for keyword '{keyword}': {e}")
                logging.exception(e)
                return videos  # Return current videos even if an unexpected error occurs

            # Parse search response
            for item in search_response.get('items', []):
                video_id = item['id'].get('videoId', '')
                if video_id:
                    video_data = {
                        'video_id': video_id,
                        'title': item['snippet'].get('title', 'N/A'),
                        'description': item['snippet'].get('description', 'N/A'),
                        'publish_time': item['snippet'].get('publishedAt', 'N/A'),
                        'channel_title': item['snippet'].get('channelTitle', 'N/A')
                    }
                    videos.append(video_data)
                    fetched_videos += 1

                    if fetched_videos >= top_k:
                        break

            logging.info(f"Retrieved {len(videos)} videos so far for keyword: '{keyword}'")

            # Check for next page
            next_page_token = search_response.get('nextPageToken')
            if not next_page_token:
                logging.info("No more pages available.")
                break

        if not videos:
            logging.warning(f"No videos found for keyword: '{keyword}'")

        return videos

@retry(max_retries=3, delay=5, backoff_factor=2, exceptions=(SSLError, asyncio.TimeoutError))
async def get_videos_statistics(youtube_api_key, video_ids, timeout=30):
    """
    Fetch statistics for a list of YouTube video IDs.

    Parameters:
        youtube_api_key (str): YouTube Data API key.
        video_ids (list): List of video IDs.
        timeout (int): Timeout for each API call in seconds.

    Returns:
        dict: Mapping of video IDs to their statistics.
    """
    global quota_exceeded
    if quota_exceeded:
        logging.error("Quota has been exceeded. Skipping fetching video statistics.")
        return {}

    async with semaphore, rate_limiter:
        youtube = youtube_service
        logging.info(f"Fetching statistics for {len(video_ids)} videos.")

        statistics_map = {}
        batch_size = 50  # YouTube API limit per request

        for i in range(0, len(video_ids), batch_size):
            batch_ids = video_ids[i:i + batch_size]

            def make_videos_request():
                return youtube.videos().list(
                    part="statistics,contentDetails",
                    id=",".join(batch_ids)
                ).execute()

            try:
                videos_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, make_videos_request),
                    timeout=timeout
                )
            except asyncio.TimeoutError as e:
                logging.warning(f"Timeout during videos.list request for batch {batch_ids}: {e}")
                raise e  # Will be caught by retry decorator
            except SSLError as e:
                logging.error(f"SSL error during videos.list request for batch {batch_ids}: {e}")
                raise e  # Will be caught by retry decorator
            except HttpError as e:
                error_content = e.content.decode('utf-8') if e.content else 'No content'
                if 'quotaExceeded' in str(e):
                    logging.error(f"Quota exceeded for YouTube API during videos.list request: {error_content}")
                    quota_exceeded = True
                    raise e  # Stop further processing
                elif 'videoNotFound' in str(e):
                    logging.error(f"One or more videos not found during videos.list request: {error_content}")
                    continue  # Skip this batch
                else:
                    logging.error(f"HTTP Error during videos.list request: {error_content}")
                    continue  # Skip this batch
            except Exception as e:
                logging.error(f"Unexpected error during videos.list request for batch {batch_ids}: {e}")
                logging.exception(e)
                continue  # Skip this batch

            # Parse videos_response
            for video in videos_response.get('items', []):
                vid = video.get('id')
                statistics = video.get('statistics', {})
                content_details = video.get('contentDetails', {})
                try:
                    statistics_map[vid] = {
                        'view_count': int(statistics.get('viewCount', 0)),
                        'like_count': int(statistics.get('likeCount', 0)),
                        'comment_count': int(statistics.get('commentCount', 0)),
                        'duration': content_details.get('duration', 'N/A')
                    }
                except ValueError as ve:
                    logging.error(f"ValueError while parsing statistics for video '{vid}': {ve}")
                except Exception as ex:
                    logging.error(f"Unexpected error while parsing statistics for video '{vid}': {ex}")

        logging.info(f"Fetched statistics for {len(statistics_map)} videos.")
        return statistics_map

def aggregate_video_metadata(videos):
    """
    Aggregate metadata from a list of videos.

    Parameters:
        videos (list): List of video dictionaries with metadata.

    Returns:
        dict: Aggregated metadata including total and average views, likes, and comments.
    """
    logging.info("Aggregating video metadata.")

    if not videos:
        logging.warning("No videos available for aggregation.")
        return {
            'total_views': 0,
            'total_likes': 0,
            'total_comments': 0,
            'average_views': 0,
            'average_likes': 0,
            'average_comments': 0
        }

    total_views = sum(video.get('view_count', 0) for video in videos)
    total_likes = sum(video.get('like_count', 0) for video in videos)
    total_comments = sum(video.get('comment_count', 0) for video in videos)
    num_videos = len(videos)

    aggregated_metadata = {
        'total_views': total_views,
        'total_likes': total_likes,
        'total_comments': total_comments,
        'average_views': total_views // num_videos if num_videos > 0 else 0,
        'average_likes': total_likes // num_videos if num_videos > 0 else 0,
        'average_comments': total_comments // num_videos if num_videos > 0 else 0
    }

    logging.info(f"Aggregated metadata: {aggregated_metadata}")
    return aggregated_metadata

async def multiagent_search(base_keyword, max_n, top_k, youtube_api_key, openai_api_key, conn=None, dry_run=False):
    """
    Perform a multi-agent search by generating keyword variations and searching YouTube for videos.

    Parameters:
        base_keyword (str): The base keyword for generating variations.
        max_n (int): Maximum number of keyword variations to generate.
        top_k (int): Maximum number of videos to retrieve per keyword.
        youtube_api_key (str): YouTube Data API key.
        openai_api_key (str): OpenAI API key.
        conn (optional): Database connection object.
        dry_run (bool): If True, skip API calls and data persistence.

    Returns:
        tuple: (generated_keywords, final_search_results)
    """
    logging.info(f"Starting multi-agent search for keyword: '{base_keyword}'")

    if dry_run:
        logging.info("Dry run mode enabled. Skipping API calls.")
        return [], {}

    # Step 1: Generate keyword variations using OpenAI
    generated_keywords = await keyword_generator_agent(base_keyword, max_n, openai_api_key, conn)

    if not generated_keywords:
        logging.error("No keywords generated.")
        return [], {}

    search_results = {}
    all_videos = []

    # Step 2: Perform YouTube searches concurrently
    tasks = [search_youtube_videos(keyword, youtube_api_key, top_k) for keyword in generated_keywords]

    # Gather results with exception handling
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for idx, result in enumerate(results):
        keyword = generated_keywords[idx]
        if isinstance(result, Exception):
            logging.error(f"Error during YouTube search for keyword '{keyword}': {result}")
            continue
        if result:
            search_results[keyword] = {'videos': result}
            all_videos.extend(result)

    logging.info(f"Search completed for {len(search_results)} keywords.")
    logging.info(f"Total videos collected: {len(all_videos)}")

    if not all_videos:
        logging.error("No videos collected from search.")
        return generated_keywords, {}

    # Step 3: Fetch metadata for all collected videos
    video_ids = list(set(video['video_id'] for video in all_videos))
    statistics_map = await get_videos_statistics(youtube_api_key, video_ids)

    # Attach metadata to each video
    for video in all_videos:
        video_id = video['video_id']
        metadata = statistics_map.get(video_id, {})
        video['view_count'] = metadata.get('view_count', 0)
        video['like_count'] = metadata.get('like_count', 0)
        video['comment_count'] = metadata.get('comment_count', 0)
        video['duration'] = metadata.get('duration', 'N/A')

    # Step 4: Sort videos by view count in descending order
    sorted_videos = sorted(all_videos, key=lambda x: x.get('view_count', 0), reverse=True)

    # Select top N videos
    top_n = min(top_k * max_n, len(sorted_videos))
    selected_videos = sorted_videos[:top_n]

    logging.info(f"Selected top {top_n} videos after ranking.")

    # Step 5: Aggregate metadata
    aggregated_metadata = aggregate_video_metadata(selected_videos)

    final_search_results = {
        'videos': selected_videos,
        'aggregated_metadata': aggregated_metadata
    }

    return generated_keywords, final_search_results

# Example of how to run the async function
if __name__ == "__main__":
    # Example usage
    async def main():
        base_keyword = "Arizona Fishing"
        max_n = 35  # Adjust based on your needs
        top_k = 25  # Adjust based on your API quota
        youtube_api_key = youtube_api_key  # From .env
        openai_api_key = openai_api_key  # From .env
        conn = None  # Replace with your database connection if needed

        try:
            generated_keywords, final_search_results = await multiagent_search(
                base_keyword, max_n, top_k, youtube_api_key, openai_api_key, conn, dry_run=False
            )

            print("Generated Keywords:", generated_keywords)
            print("Final Search Results:", json.dumps(final_search_results, indent=4, ensure_ascii=False))
        except HttpError as e:
            logging.error(f"HTTP Error encountered in main: {e}")
        except SSLError as e:
            logging.error(f"SSL Error encountered in main: {e}")
        except Exception as e:
            logging.error(f"Unexpected error encountered in main: {e}")

    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script terminated due to an unexpected error: {e}")
