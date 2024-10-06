import logging
from utils.youtube_api import get_youtube_service  # 使用 utils/youtube_api.py 中的统一服务
import time

# 重试机制，确保 API 稳定性
def retry(max_retries=3, delay=2, backoff_factor=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}, retrying {retries + 1}/{max_retries}...")
                    time.sleep(current_delay)
                    retries += 1
                    current_delay *= backoff_factor
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# 使用 YouTube 服务从 API 中抓取视频评论
@retry(max_retries=5, delay=2)
def fetch_video_comments(video_id):
    youtube = get_youtube_service()  # 通过 utils 提供的统一服务获取客户端
    logging.info(f"Fetching comments for video ID: {video_id}")
    comments = []
    
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100,
        textFormat='plainText'
    )

    while request:
        try:
            response = request.execute()

            # 提取评论数据
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'like_count': comment.get('likeCount', 0),
                    'publish_time': comment['publishedAt']
                })

            # 如果存在下一页，则继续请求
            if 'nextPageToken' in response:
                request = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    textFormat='plainText',
                    pageToken=response['nextPageToken']
                )
            else:
                request = None

        except Exception as e:
            logging.error(f"Failed to fetch comments for video ID {video_id}: {e}")
            break

    logging.info(f"Fetched {len(comments)} comments for video ID: {video_id}")
    return comments
