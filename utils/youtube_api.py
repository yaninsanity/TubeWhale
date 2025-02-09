import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import logging
import ssl

# Load environment variables
load_dotenv()

# Global variable to cache the YouTube service instance
youtube_service = None

def get_youtube_service(api_key=None):
    """
    Initializes and returns the YouTube API service instance.
    """
    global youtube_service
    if youtube_service is None:
        if not api_key:
            api_key = os.getenv('YOUTUBE_API_KEY')
        if not api_key:
            raise ValueError("YouTube API key not found in environment variables.")

        logging.info("Initializing YouTube service.")
        try:
            # Explicitly create a default SSL context for secure connections
            context = ssl.create_default_context()
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

            # Create the service using the API key
            youtube_service = build('youtube', 'v3', developerKey=api_key)
            logging.info("YouTube service initialized successfully.")
        except HttpError as e:
            logging.error(f"Failed to initialize YouTube service: {e}")
            raise e
        except Exception as e:
            logging.error(f"Unexpected error during YouTube service initialization: {e}")
            raise e

    return youtube_service