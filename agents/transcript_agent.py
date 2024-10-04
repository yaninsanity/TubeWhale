from youtube_transcript_api import YouTubeTranscriptApi
import logging
import yt_dlp
import whisper
import os
import logging

def download_audio(video_url, video_id, download_dir="downloads"):
    os.makedirs(download_dir, exist_ok=True)
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(download_dir, f'{video_id}.mp3'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': True
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            logging.info(f"Downloading audio for video ID: {video_id}")
            ydl.download([video_url])
            audio_file_path = os.path.join(download_dir, f'{video_id}.mp3')
            return audio_file_path
        except Exception as e:
            logging.error(f"Failed to download audio for video ID {video_id}: {e}")
            return None

def transcribe_audio(audio_file_path):
    model = whisper.load_model("base")
    try:
        logging.info(f"Transcribing audio file: {audio_file_path}")
        result = model.transcribe(audio_file_path)
        return result['text']
    except Exception as e:
        logging.error(f"Failed to transcribe audio file {audio_file_path}: {e}")
        return None


def fetch_transcript(video_id):
    logging.info(f"Fetching transcript for video ID: {video_id}")
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
        return " ".join([item['text'] for item in transcript])
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video ID {video_id}: {e}")
        return None
