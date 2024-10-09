# search_agent.py

import logging
import asyncio
from openai import OpenAI
from googleapiclient.errors import HttpError
from utils.youtube_api import get_youtube_service
from utils.database import store_ai_interaction
from datetime import datetime


# Multi-agent search (Asynchronous) with metadata aggregation
async def multiagent_search(base_keyword, max_n, top_k, youtube_api_key, openai_api_key, conn=None, dry_run=False):
    logging.info(f"Starting multi-agent search for keyword: {base_keyword}")
    if dry_run:
        logging.info("Dry run mode - skipping API calls.")
        return [], {}

    # Step 1: Generate variations of keywords using OpenAI
    generated_keywords = await keyword_generator_agent(base_keyword, max_n, openai_api_key, conn)

    if not generated_keywords:
        logging.error("No keywords generated.")
        return [], {}

    search_results = {}

    # Step 2: Use asyncio.gather for parallel YouTube searches
    tasks = []
    for keyword in generated_keywords:
        tasks.append(search_youtube_videos(keyword, youtube_api_key, top_k))

    # Run the tasks concurrently
    results = await asyncio.gather(*tasks)

    # Collect search results
    all_videos = []
    for idx, videos in enumerate(results):
        if videos:
            keyword = generated_keywords[idx]
            search_results[keyword] = {'videos': videos}
            all_videos.extend(videos)

    logging.info(f"Search completed for {len(search_results)} keywords.")
    logging.info(f"Total videos collected: {len(all_videos)}")

    if not all_videos:
        logging.error("No videos collected from search.")
        return generated_keywords, {}

    # Now, fetch metadata for all videos
    video_ids = list(set([video['video_id'] for video in all_videos]))
    all_video_metadata = fetch_videos_metadata(video_ids, youtube_api_key)

    # Now attach metadata to videos
    videos_with_metadata = []
    for video in all_videos:
        video_id = video['video_id']
        metadata = all_video_metadata.get(video_id, {})
        video['view_count'] = metadata.get('view_count', 0)
        video['like_count'] = metadata.get('like_count', 0)
        video['comment_count'] = metadata.get('comment_count', 0)
        videos_with_metadata.append(video)

    # Now sort the videos by view count (or any other criteria)
    sorted_videos = sorted(videos_with_metadata, key=lambda x: x['view_count'], reverse=True)

    # Now select top N videos
    total_videos = len(sorted_videos)
    top_n = min(top_k * max_n, total_videos)
    selected_videos = sorted_videos[:top_n]

    logging.info(f"Selected top {top_n} videos after ranking.")

    # Prepare the final search results structure
    final_search_results = {
        'videos': selected_videos,
        'aggregated_metadata': aggregate_video_metadata(selected_videos, youtube_api_key, conn)
    }

    return generated_keywords, final_search_results

# Agent keyword brainstorming using OpenAI's LLM
async def keyword_generator_agent(base_keyword, max_n, api_key, conn=None):
    logging.info(f"Generating up to {max_n} variations for base keyword: {base_keyword}")
      # 设置 OpenAI API 密钥
    client = OpenAI(api_key=api_key)  
    prompt = (
        f"You are a domain expert specializing in professional problem-solving. "
        f"Generate {max_n} relevant and highly accurate keyword variations for the domain-specific keyword '{base_keyword}' "
        f"to search for professional YouTube videos. Provide each keyword on a separate line."
    )

    try:
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Sending prompt to OpenAI API: {prompt}")

        # 使用同步的 OpenAI API 调用
        response = client.chat.completions.create(model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=150,
        temperature=0.7)

        content = response.choices[0].message.content.strip()
        generated_keywords = content.split("\n")
        generated_keywords = list(set(filter(None, [kw.strip() for kw in generated_keywords])))
        logging.info(f"Generated {len(generated_keywords)} keyword variations: {generated_keywords}")

        # 限制关键词数量不超过 max_n
        generated_keywords = generated_keywords[:max_n]

        # 将 AI 交互记录到数据库
        if conn:
            store_ai_interaction(
                conn,
                prompt,    # 输入
                "\n".join(generated_keywords),   # 输出
                "keyword_generation",  # 交互类型
                start_time  # 时间戳
            )

        return generated_keywords
    except Exception as e:
        logging.error(f"Error generating keywords with OpenAI: {e}")
        return [base_keyword]  # 在出错时回退到基础关键词

# YouTube video search function
async def search_youtube_videos(keyword, youtube_api_key, top_k):
    youtube = get_youtube_service(youtube_api_key)
    logging.info(f"Fetching videos for keyword: {keyword}")

    try:
        request = youtube.search().list(
            part="snippet",
            q=keyword,
            maxResults=top_k,
            type='video'
        )
        response = request.execute()

        videos = []
        for item in response.get('items', []):
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

# Fetch metadata for multiple videos
def fetch_videos_metadata(video_ids, youtube_api_key):
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
        except Exception as e:
            logging.error(f"Failed to fetch metadata for video IDs {chunk_ids}: {e}")

    logging.info(f"Fetched metadata for {len(all_video_metadata)} videos.")
    return all_video_metadata

# Function to fetch and aggregate video metadata
def aggregate_video_metadata(videos, youtube_api_key, conn=None):
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
