# search_agent.py

import logging
import asyncio
from openai import OpenAI
from googleapiclient.errors import HttpError
from utils.youtube_api import get_youtube_service
from utils.database import store_ai_interaction
from datetime import datetime
from ssl import SSLError  # Import SSLError for specific SSL exception handling

# Initialize the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# Initialize ThreadPoolExecutor with a limited number of workers to prevent excessive concurrency
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=3)  # Adjust as needed

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
        # Initialize OpenAI client
        client = OpenAI(api_key=api_key)
        
        prompt = (
            f"You are a domain expert specializing in professional problem-solving and brainstorming. "
            f"Generate {max_n} relevant and highly accurate keyword variations for the domain-specific keyword '{base_keyword}' "
            f"to search for high topic-related YouTube videos. Provide each keyword on a separate line without numbering."
        )
        
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Sending prompt to OpenAI API: {prompt}")
        
        # Perform synchronous API call to OpenAI
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.7
        )
        
        content = response.choices[0].message.content.strip()
        generated_keywords = content.split("\n")
        generated_keywords = list(set(filter(None, (kw.strip() for kw in generated_keywords))))
        
        logging.info(f"Generated {len(generated_keywords)} keyword variations: {generated_keywords}")
        
        # Limit the number of keywords to max_n
        generated_keywords = generated_keywords[:max_n]
        
        # Record AI interaction to the database if connection is provided
        if conn:
            store_ai_interaction(
                conn,
                prompt,                     # Input
                "\n".join(generated_keywords),  # Output
                "keyword_generation",      # Interaction type
                start_time                 # Timestamp
            )
        
        return generated_keywords
    
    except Exception as e:
        logging.error(f"Error generating keywords with OpenAI: {e}")
        logging.exception(e)
        return [base_keyword]  # Fallback to base keyword in case of error

async def search_youtube_videos(keyword, youtube_api_key, top_k, max_retries=3, timeout=30):
    """
    Search YouTube for videos matching the given keyword.

    Parameters:
        keyword (str): The search keyword.
        youtube_api_key (str): YouTube Data API key.
        top_k (int): Maximum number of videos to retrieve.
        max_retries (int): Maximum number of retry attempts.
        timeout (int): Timeout for each API call in seconds.

    Returns:
        list: List of video details dictionaries.
    """
    youtube = get_youtube_service(youtube_api_key)
    logging.info(f"Fetching videos for keyword: '{keyword}' with top_k={top_k}")
    
    videos = []
    next_page_token = None
    fetched_videos = 0
    max_results_per_page = 50  # YouTube API maximum results per page
    
    while fetched_videos < top_k:
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
        
        # Implement retry mechanism with exponential backoff
        for attempt in range(1, max_retries + 1):
            try:
                search_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, make_search_request),
                    timeout=timeout
                )
                break  # Successful request, exit retry loop
            except asyncio.TimeoutError:
                logging.warning(f"Timeout during search request for keyword '{keyword}', attempt {attempt}/{max_retries}")
            except SSLError as e:
                logging.error(f"SSL error during search request for keyword '{keyword}': {e}")
            except HttpError as e:
                error_content = e.content.decode('utf-8') if e.content else 'No content'
                if 'quotaExceeded' in str(e):
                    logging.error(f"Quota exceeded for YouTube API during search for keyword '{keyword}': {error_content}")
                    return videos  # Cannot continue
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
            
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff
                logging.info(f"Waiting for {wait_time} seconds before retrying search request for keyword '{keyword}'")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Failed to fetch search results for keyword '{keyword}' after {max_retries} attempts.")
                return videos  # Return videos collected so far
        
        else:
            # All retries failed, skip to next keyword
            logging.error(f"All retries failed for keyword '{keyword}'. Skipping to next keyword.")
            return videos
        
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

async def get_videos_statistics(youtube_api_key, video_ids, max_retries=3, timeout=30):
    """
    Fetch statistics for a list of YouTube video IDs.

    Parameters:
        youtube_api_key (str): YouTube Data API key.
        video_ids (list): List of video IDs.
        max_retries (int): Maximum number of retry attempts.
        timeout (int): Timeout for each API call in seconds.

    Returns:
        dict: Mapping of video IDs to their statistics.
    """
    youtube = get_youtube_service(youtube_api_key)
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
        
        # Implement retry mechanism with exponential backoff
        for attempt in range(1, max_retries + 1):
            try:
                videos_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, make_videos_request),
                    timeout=timeout
                )
                break  # Successful request, exit retry loop
            except asyncio.TimeoutError:
                logging.warning(f"Timeout during videos.list request for batch {batch_ids}, attempt {attempt}/{max_retries}")
            except SSLError as e:
                logging.error(f"SSL error during videos.list request for batch {batch_ids}: {e}")
            except HttpError as e:
                error_content = e.content.decode('utf-8') if e.content else 'No content'
                if 'quotaExceeded' in str(e):
                    logging.error(f"Quota exceeded for YouTube API during videos.list request: {error_content}")
                    return statistics_map  # Cannot continue
                elif 'videoNotFound' in str(e):
                    logging.error(f"One or more videos not found during videos.list request: {error_content}")
                    break  # Skip this batch
                else:
                    logging.error(f"HTTP Error during videos.list request: {error_content}")
                    break  # Skip this batch
            except Exception as e:
                logging.error(f"Unexpected error during videos.list request for batch {batch_ids}: {e}")
                logging.exception(e)
                break  # Skip this batch
            
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff
                logging.info(f"Waiting for {wait_time} seconds before retrying videos.list request for batch {batch_ids}")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Failed to fetch video statistics for batch {batch_ids} after {max_retries} attempts.")
        
        else:
            # All retries failed, skip to next batch
            logging.error(f"All retries failed for batch {batch_ids}. Skipping to next batch.")
            continue
        
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

def fetch_videos_metadata(video_ids, youtube_api_key):
    """
    Fetch metadata for multiple YouTube videos.

    Parameters:
        video_ids (list): List of YouTube video IDs.
        youtube_api_key (str): YouTube Data API key.

    Returns:
        dict: Mapping of video IDs to their metadata.
    """
    youtube = get_youtube_service(youtube_api_key)
    logging.info(f"Fetching metadata for {len(video_ids)} videos.")
    
    all_video_metadata = {}
    
    # YouTube API limits the number of video IDs to 50 per request
    chunk_size = 50
    for i in range(0, len(video_ids), chunk_size):
        chunk_ids = video_ids[i:i+chunk_size]
        try:
            request = youtube.videos().list(
                part="statistics",
                id=",".join(chunk_ids)
            )
            response = request.execute()
            for item in response.get('items', []):
                video_id = item['id']
                stats = item.get('statistics', {})
                metadata = {
                    'view_count': int(stats.get('viewCount', 0)),
                    'like_count': int(stats.get('likeCount', 0)),
                    'comment_count': int(stats.get('commentCount', 0))
                }
                all_video_metadata[video_id] = metadata
        except HttpError as e:
            logging.error(f"HTTP Error while fetching metadata for video IDs {chunk_ids}: {e}")
        except SSLError as e:
            logging.error(f"SSL error while fetching metadata for video IDs {chunk_ids}: {e}")
        except Exception as e:
            logging.error(f"Failed to fetch metadata for video IDs {chunk_ids}: {e}")
    
    logging.info(f"Fetched metadata for {len(all_video_metadata)} videos.")
    return all_video_metadata

# Example of how to run the async function
if __name__ == "__main__":
    # Example usage
    async def main():
        base_keyword = "Virginia fishing"
        max_n = 5
        top_k = 3
        youtube_api_key = "YOUR_YOUTUBE_API_KEY"  # Replace with your actual YouTube API key
        openai_api_key = "YOUR_OPENAI_API_KEY"    # Replace with your actual OpenAI API key
        conn = None  # Replace with your database connection if needed
        
        generated_keywords, final_search_results = await multiagent_search(
            base_keyword, max_n, top_k, youtube_api_key, openai_api_key, conn, dry_run=False
        )
        
        print("Generated Keywords:", generated_keywords)
        print("Final Search Results:", final_search_results)
    
    asyncio.run(main())
