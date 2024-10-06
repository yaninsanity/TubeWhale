import os
import logging
import whisper
from youtube_dl import YoutubeDL
from utils.database import store_transcript_summary
import openai

# Retry decorator for handling retries on download or transcription failure
def retry(max_retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries}...")
                    await asyncio.sleep(delay)
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# Function to download audio from YouTube video
@retry(max_retries=3, delay=5)
async def download_audio(video_id):
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
        return audio_path
    
    except Exception as e:
        logging.error(f"Failed to download audio for video ID {video_id}: {e}")
        return None

# Function to transcribe audio to text using Whisper model
async def transcribe_audio(audio_path):
    model = whisper.load_model("base")
    try:
        logging.info(f"Transcribing audio file: {audio_path}")
        result = model.transcribe(audio_path)
        return result['text']
    except Exception as e:
        logging.error(f"Failed to transcribe audio file {audio_path}: {e}")
        return None

# 使用 YouTubeTranscriptApi 获取视频转录
async def fetch_transcript(video_id):
    """
    Fetch transcript from the YouTube API or another source.
    This is a placeholder function. You would replace this with actual logic to fetch the transcript.
    """
    # Logic to interact with the YouTube API or third-party transcript provider
    # For example, using youtube_transcript_api:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join([entry['text'] for entry in transcript])
    except Exception as e:
        logging.error(f"Failed to fetch transcript for video {video_id}: {e}")
        return None


# Function to interpret the transcript with OpenAI's LLM
async def interpret_transcript(transcript, openai_api_key, topic):
    try:
        openai.api_key = openai_api_key
        
        # Define system role and task prompt
        system_role_prompt = (
            f"You are an expert system focused on analyzing audio transcripts in the context of '{topic}'. "
            "Your task is to generate detailed, structured summaries based on the transcript provided. "
            "Be sure to highlight any key insights, practical knowledge, or important information."
        )
        
        # Combine the transcript and system role prompt for LLM interaction
        prompt = (
            f"{system_role_prompt}\n\n"
            "Transcript:\n"
            f"{transcript}\n\n"
            "Generate a structured summary based on the transcript."
        )

        response = openai.Completion.create(
            model="text-davinci-003",
            prompt=prompt,
            max_tokens=1024,  # Allow enough tokens for detailed response
            temperature=0.5
        )

        logging.info(f"Transcript interpretation completed: {response['choices'][0]['text']}")
        return response['choices'][0]['text']
    
    except Exception as e:
        logging.error(f"Failed to interpret transcript: {e}")
        return None

# Main function to download, transcribe and interpret the audio
async def process_video_transcript(video_id, openai_api_key, topic, conn):
    try:
        # Step 1: Download the audio
        audio_path = await download_audio(video_id)
        if not audio_path:
            logging.error(f"Audio download failed for video ID: {video_id}")
            return None

        # Step 2: Transcribe the audio to text
        transcript = await transcribe_audio(audio_path)
        if not transcript:
            logging.error(f"Transcription failed for video ID: {video_id}")
            return None

        # Step 3: Interpret the transcript using LLM
        interpreted_summary = await interpret_transcript(transcript, openai_api_key, topic)
        if not interpreted_summary:
            logging.error(f"Transcript interpretation failed for video ID: {video_id}")
            return None

        # Step 4: Store both transcript and summary in the database
        store_transcript_summary(conn, video_id, transcript, interpreted_summary)
        logging.info(f"Stored transcript and summary for video ID: {video_id}")

        return interpreted_summary

    except Exception as e:
        logging.error(f"Failed to process transcript for video ID {video_id}: {e}")
        return None
