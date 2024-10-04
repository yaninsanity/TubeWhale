import os
import logging
import whisper
from youtube_dl import YoutubeDL

# Function to download audio from YouTube video and transcribe it using Whisper
async def transcribe_audio_to_text(video_id):
    try:
        # Ensure downloads directory exists
        os.makedirs('downloads', exist_ok=True)
        
        logging.info(f"Downloading audio for video ID: {video_id}")
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'downloads/{video_id}.%(ext)s',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'quiet': True
        }

        with YoutubeDL(ydl_opts) as ydl:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            ydl.download([video_url])

        audio_path = f'downloads/{video_id}.mp3'
        
        # Load the Whisper model
        logging.info(f"Transcribing audio to text using Whisper for video ID: {video_id}")
        model = whisper.load_model("base")
        result = model.transcribe(audio_path)
        
        # Clean up the downloaded audio file
        os.remove(audio_path)

        return result['text']
    
    except Exception as e:
        logging.error(f"Failed to transcribe audio for video ID {video_id}: {e}")
        return None
