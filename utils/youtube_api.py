import os
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging

# 加载环境变量
load_dotenv()

# 统一使用全局变量缓存 YouTube 服务实例
youtube_service = None

# 获取 YouTube 服务实例
def get_youtube_service(api_key=None):
    global youtube_service
    if youtube_service is None:
        if not api_key:
            api_key = os.getenv('YOUTUBE_API_KEY')
        if not api_key:
            raise ValueError("YouTube API key not found in environment variables.")
        
        logging.info("Initializing YouTube service.")
        youtube_service = build('youtube', 'v3', developerKey=api_key)
    return youtube_service
