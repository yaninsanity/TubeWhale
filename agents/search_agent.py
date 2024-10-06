import logging
import asyncio
from googleapiclient.errors import HttpError
from utils.youtube_api import get_youtube_service
from langchain_openai import OpenAI

# Initialize OpenAI API
def get_openai_service(api_key):
    return OpenAI(api_key=api_key)

# Multi-agent search (Asynchronous) with metadata aggregation
async def multiagent_search(base_keyword, agent_count, max_n, top_k, youtube_api_key, openai_api_key, dry_run=False):
    logging.info(f"Starting multi-agent search with {agent_count} agents for keyword: {base_keyword}")
    
    if dry_run:
        logging.info("Dry run mode - skipping API calls.")
        return {}

    # Step 1: Generate variations of keywords using OpenAI
    generated_keywords = await keyword_generator_agent(base_keyword, max_n, openai_api_key)

    # Ensure the agent count does not exceed the number of generated keywords
    agent_count = min(agent_count, len(generated_keywords))

    search_results = {}

    # Step 2: Use asyncio.gather for parallel YouTube searches
    tasks = []
    for idx in range(agent_count):
        keyword = generated_keywords[idx]
        tasks.append(search_and_aggregate(keyword, top_k, youtube_api_key))
    
    # Run the tasks concurrently
    results = await asyncio.gather(*tasks)

    # Collect search results
    for idx, result in enumerate(results):
        if result:
            search_results[generated_keywords[idx]] = result
    
    logging.info(f"Search completed for {len(search_results)} keywords.")
    return search_results

# Agent keyword brainstorming using OpenAI's LLM
async def keyword_generator_agent(base_keyword, max_n, api_key):
    logging.info(f"Generating {max_n} variations for base keyword: {base_keyword}")
    llm = get_openai_service(api_key)
    prompt = f"Generate {max_n} relevant variations of the keyword '{base_keyword}' for a YouTube video search."

    try:
        response = await llm.agenerate([prompt])
        generated_keywords = response.generations[0][0].text.split("\n")
        generated_keywords = list(set(filter(None, [kw.strip() for kw in generated_keywords])))
        logging.info(f"Generated {len(generated_keywords)} keyword variations.")
        return generated_keywords
    except Exception as e:
        logging.error(f"Error generating keywords with OpenAI: {e}")
        return [base_keyword]  # 如果失败，返回基础关键字以确保不中断流程

# YouTube video search function
async def search_youtube_videos(keyword, youtube_api_key, top_k):
    youtube = get_youtube_service(youtube_api_key)
    logging.info(f"Fetching videos for keyword: {keyword}")

    try:
        request = youtube.search().list(part="snippet", q=keyword, maxResults=top_k)
        response = request.execute()  # 注意，这里是同步方法，不能用 await

        videos = []
        for item in response['items']:
            video_data = {
                'video_id': item['id'].get('videoId', ''),
                'title': item['snippet'].get('title', 'N/A'),
                'description': item['snippet'].get('description', 'N/A'),
                'publish_time': item['snippet'].get('publishedAt', 'N/A'),
                'channel_title': item['snippet'].get('channelTitle', 'N/A')
            }
            videos.append(video_data)

        logging.info(f"Retrieved {len(videos)} videos for keyword: {keyword}")
        return videos
    except HttpError as e:
        if 'quotaExceeded' in str(e):
            logging.error(f"Quota exceeded for YouTube API during search for keyword '{keyword}': {e}")
        else:
            logging.error(f"Error fetching videos for keyword '{keyword}': {e}")
        return []

# Function to fetch and aggregate video metadata
def aggregate_video_metadata(videos, youtube_api_key):
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

    video_metadata_list = []
    total_views = 0
    total_likes = 0
    total_comments = 0

    for video in videos:
        video_id = video['video_id']
        metadata = fetch_video_metadata(video_id, youtube_api_key)

        if metadata:
            video_metadata_list.append(metadata)
            total_views += metadata['view_count']
            total_likes += metadata['like_count']
            total_comments += metadata['comment_count']

    num_videos = len(video_metadata_list)
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

# Fetch metadata for a single video
def fetch_video_metadata(video_id, youtube_api_key):
    try:
        youtube = get_youtube_service(youtube_api_key)
        request = youtube.videos().list(part="snippet,statistics", id=video_id)
        response = request.execute()  # 同步方法调用

        if not response['items']:
            logging.warning(f"No metadata found for video ID {video_id}")
            return None

        video_data = response['items'][0]
        return {
            'view_count': int(video_data['statistics'].get('viewCount', 0)),
            'like_count': int(video_data['statistics'].get('likeCount', 0)),
            'comment_count': int(video_data['statistics'].get('commentCount', 0))
        }
    except Exception as e:
        logging.error(f"Failed to fetch metadata for video ID {video_id}: {e}")
        return None

# 主流程：提取视频并聚合元数据
async def search_and_aggregate(keyword, top_k, youtube_api_key):
    # Step 1: Search YouTube videos
    videos = await search_youtube_videos(keyword, youtube_api_key, top_k)
    if not videos:
        logging.warning(f"No videos found for keyword: {keyword}")
        return None

    # Step 2: Aggregate metadata for the found videos
    aggregated_metadata = aggregate_video_metadata(videos, youtube_api_key)
    
    return {
        'videos': videos,
        'aggregated_metadata': aggregated_metadata
    }
