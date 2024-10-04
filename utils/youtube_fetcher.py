import logging
from googleapiclient.discovery import build

# 获取 YouTube 服务
def get_youtube_service(api_key):
    return build('youtube', 'v3', developerKey=api_key)

# 从 YouTube API 抓取视频评论，支持分页
def fetch_video_comments(youtube, video_id):
    logging.info(f"Fetching comments for video ID: {video_id}")
    comments = []
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100,  # 可以根据需求调整
        textFormat='plainText'
    )

    while request:
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

        # 检查是否有下一页
        if 'nextPageToken' in response:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100,  # 可以根据需求调整
                textFormat='plainText',
                pageToken=response['nextPageToken']
            )
        else:
            request = None

    logging.info(f"Fetched {len(comments)} comments for video ID: {video_id}")
    return comments
