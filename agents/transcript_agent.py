from youtube_transcript_api import YouTubeTranscriptApi
import logging

def fetch_transcript(video_id):
    logging.info(f"Fetching transcript for video ID: {video_id}")
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
        return " ".join([item['text'] for item in transcript])
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video ID {video_id}: {e}")
        return None
