import os
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging

load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

def get_youtube_service():
    logging.info("Initializing YouTube API service.")
    return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
