import logging
import time
from youtube_api import get_youtube_service  # 使用 utils 提供的统一服务获取YouTube客户端
from database import store_comments, store_video_metadata, init_db  # 引用存储评论、视频Metadata的函数和数据库初始化

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

# 获取视频的 Metadata，包括语言、字幕、国家代码
@retry(max_retries=5, delay=2)
def fetch_video_metadata(video_id):
    youtube = get_youtube_service()  # 获取YouTube客户端
    logging.info(f"Fetching metadata for video ID: {video_id}")
    
    request = youtube.videos().list(
        part='snippet,statistics,contentDetails',  # 添加 contentDetails 和 snippet
        id=video_id
    )
    
    response = request.execute()
    
    if not response['items']:
        logging.error(f"No metadata found for video ID {video_id}")
        return None
    
    video_info = response['items'][0]
    snippet = video_info.get('snippet', {})
    stats = video_info.get('statistics', {})
    content_details = video_info.get('contentDetails', {})

    video_metadata = {
        'id': video_id,
        'snippet': snippet,
        'contentDetails': content_details,
        'statistics': stats,
        'view_count': stats.get('viewCount', 0),
        'like_count': stats.get('likeCount', 0),
        'comment_count': stats.get('commentCount', 0)
    }
    
    return video_metadata

# 抓取所有评论，包括回复
@retry(max_retries=5, delay=2)
def fetch_all_comments(video_id):
    youtube = get_youtube_service()  # 通过 utils 提供的统一服务获取YouTube客户端
    logging.info(f"Fetching comments for video ID: {video_id}")
    all_comments = []
    
    # Step 1: 获取顶层评论
    request = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id,
        maxResults=100,
        textFormat='plainText'
    )

    while request:
        try:
            response = request.execute()

            # 提取顶层评论和子评论
            for item in response['items']:
                top_comment = item['snippet']['topLevelComment']
                top_comment_snippet = top_comment['snippet']
                top_comment_id = top_comment['id']

                # 添加顶层评论
                all_comments.append({
                    'comment_id': top_comment_id,  # 顶层评论的唯一ID
                    'author': top_comment_snippet['authorDisplayName'],
                    'text': top_comment_snippet['textDisplay'],  # 改为comment_text
                    'like_count': top_comment_snippet.get('likeCount', 0),
                    'viewer_rating': top_comment_snippet.get('viewerRating', 'none'),  # 添加 viewerRating 字段
                    'moderation_status': top_comment_snippet.get('moderationStatus', 'published'),  # 添加 moderationStatus 字段
                    'publish_time': top_comment_snippet['publishedAt'],
                    'parent_id': None  # 顶层评论没有父ID
                })

                # 如果有子评论，提取子评论
                if 'replies' in item and 'comments' in item['replies']:
                    for reply in item['replies']['comments']:
                        reply_comment = reply['snippet']
                        reply_comment_id = reply['id']

                        # 添加子评论
                        all_comments.append({
                            'comment_id': reply_comment_id,  # 子评论的唯一ID
                            'author': reply_comment['authorDisplayName'],
                            'text': reply_comment['textDisplay'],  # 改为comment_text
                            'like_count': reply_comment.get('likeCount', 0),
                            'viewer_rating': reply_comment.get('viewerRating', 'none'),  # 添加 viewerRating 字段
                            'moderation_status': reply_comment.get('moderationStatus', 'published'),  # 添加 moderationStatus 字段
                            'publish_time': reply_comment['publishedAt'],
                            'parent_id': top_comment_id  # 父ID为顶层评论的ID
                        })

            # 检查是否存在下一页评论
            if 'nextPageToken' in response:
                logging.info(f"Next page token found, fetching more comments...")
                request = youtube.commentThreads().list(
                    part='snippet,replies',
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

    logging.info(f"Fetched {len(all_comments)} comments for video ID: {video_id}")
    return all_comments


# 主函数
if __name__ == "__main__":
    import sqlite3

    logging.basicConfig(level=logging.INFO)

    # 设置视频ID进行测试
    video_id = "E5YsxMQzjds"  # 替换为实际的视频ID

    # 初始化数据库连接
    db_path = 'youtube_test.db'  # 数据库路径
    conn = sqlite3.connect(db_path)

    # 初始化数据库（确保表已创建）
    init_db(db_path)

    # 获取视频 Metadata 并存储
    video_metadata = fetch_video_metadata(video_id)
    if video_metadata:
        store_video_metadata(conn, video_metadata)

    # 获取视频的所有评论
    comments = fetch_all_comments(video_id)

    # 将评论存入数据库
    store_comments(conn, video_id, comments)  # 调用 database.py 中的 store_comments

    # 关闭数据库连接
    conn.close()
    logging.info("Script execution finished.")
