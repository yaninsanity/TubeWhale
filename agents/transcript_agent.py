import os
import logging
import whisper
from youtube_dl import YoutubeDL
from utils.database import store_transcript_summary
from openai import OpenAI
import asyncio

# Initialize OpenAI client with the API key
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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

# Using YouTubeTranscriptApi to fetch video transcripts
async def fetch_transcript(video_id):
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join([entry['text'] for entry in transcript])
    except Exception as e:
        logging.warning(f"Failed to fetch transcript for video {video_id}: {e}")
        return None

# Function to interpret the transcript using OpenAI's LLM (gpt-4o-mini)
async def interpret_transcript(transcript, topic):
    try:
        # Define system role and task prompt
        system_role_prompt = (
            f"You are an expert in analyzing audio transcripts in the context of '{topic}'. "
            "Your task is to generate detailed, structured summaries based on the transcript provided. "
            "Be sure to highlight key insights, practical knowledge, and important information."
        )

        # Use OpenAI chat completion API with gpt-4o-mini model
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_role_prompt},
                {"role": "user", "content": transcript}
            ],
            max_tokens=1024,
            temperature=0.5
        )

        # Check if the response is properly structured
        if response and 'choices' in response and len(response.choices) > 0:
            # Handle the case where message is present
            if "message" in response.choices[0]:
                summary = response.generations[0].message.get("content", "").strip()
                logging.info(f"Transcript interpretation completed: {summary}")
                return summary
            else:
                logging.error("Response does not contain a valid 'message' key.")
                return None
        else:
            logging.error("Unexpected OpenAI API response structure.")
            return None
    
    except Exception as e:
        logging.error(f"Failed to interpret transcript: {e}")
        return None

# Main function to process the entire flow: fetch transcript or fallback to audio, transcribe, and interpret
async def process_video_transcript(video_id, topic, conn):
    try:
        # Step 1: Attempt to fetch the transcript first
        transcript = await fetch_transcript(video_id)
        
        if not transcript:
            logging.warning(f"Transcript not available for video ID {video_id}. Falling back to audio transcription.")
            
            # Step 2: Download the audio and transcribe it if no transcript was found
            audio_path = await download_audio(video_id)
            if not audio_path:
                logging.error(f"Audio download failed for video ID: {video_id}")
                return None

            transcript = await transcribe_audio(audio_path)
            if not transcript:
                logging.error(f"Transcription failed for video ID: {video_id}")
                return None

        # Step 3: Interpret the transcript using OpenAI's gpt-4o-mini
        interpreted_summary = await interpret_transcript(transcript, topic)
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
