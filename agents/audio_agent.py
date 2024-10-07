import os
import logging
import asyncio
from openai import OpenAI
from yt_dlp import YoutubeDL
from pydub import AudioSegment

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


# Function to split long audio files into smaller chunks based on OpenAI limits
def split_audio(audio_path, max_duration_ms=60000):
    try:
        logging.info(f"Splitting audio {audio_path} into chunks based on OpenAI API limits.")
        audio = AudioSegment.from_mp3(audio_path)
        chunks = [audio[i:i + max_duration_ms] for i in range(0, len(audio), max_duration_ms)]
        return chunks
    except Exception as e:
        logging.error(f"Failed to split audio {audio_path}: {e}")
        return []

# Function to send audio chunks to OpenAI for transcription and analysis with system rules
@retry(max_retries=3, delay=5)
async def analyze_audio_with_openai(audio_chunk, openai_api_key, topic):
    try:
        client = OpenAI(api_key=openai_api_key)  # Initialize OpenAI client inside the function

        # Convert the AudioSegment chunk to bytes
        audio_bytes = audio_chunk.export(format="mp3")

        logging.info(f"Sending audio chunk to OpenAI for transcription with topic '{topic}'.")

        # OpenAI Whisper API for transcription
        response = client.audio.transcribe("whisper-1", audio_bytes)
        transcript_text = response.text
        logging.info(f"Received transcription from OpenAI for audio chunk: {transcript_text}")

        # Defining system rules and role upfront
        system_role_prompt = (
            f"You are an expert in the domain of '{topic}' and an advanced system designed to extract key insights "
            "from multimedia content. Your role is to interpret the audio and generate a comprehensive summary. "
            "Follow these rules while summarizing: "
            "1. Ensure the summary focuses on the key aspects of the topic '{topic}'. "
            "2. Highlight any practical insights, steps, or tips shared in the audio. "
            "3. Structure the summary to include important actions, challenges, or advice given. "
            "4. Provide context where necessary, but avoid unnecessary details."
        )

        # Creating a detailed summary based on the transcript with rules
        summary_prompt = (
            f"{system_role_prompt}\n\n"
            "Transcript: \n"
            f"{transcript_text}\n\n"
            "Generate a detailed and structured summary based on this transcript."
        )

        enriched_response = client.completions.create(
            model="text-davinci-003",
            prompt=summary_prompt,
            max_tokens=1024,  # Adjust for more detailed summaries
            temperature=0.5  # Balances between creativity and adherence to the topic
        )

        return enriched_response.choices[0].text

    except Exception as e:
        logging.error(f"Failed to analyze audio with OpenAI: {e}")
        return None

# Main function to handle audio download, splitting, and OpenAI interaction
async def transcribe_audio_to_summary(video_id, openai_api_key, topic):
    try:
        # Step 1: Download the audio file
        audio_path = await download_audio(video_id)
        if not audio_path:
            logging.error(f"Audio download failed for video ID: {video_id}")
            return None

        # Step 2: Split the audio into chunks if necessary
        audio_chunks = split_audio(audio_path)
        if not audio_chunks:
            logging.error(f"Failed to split audio for video ID: {video_id}")
            return None

        # Step 3: Send each chunk to OpenAI for transcription and summarization
        full_summary = ""
        for chunk in audio_chunks:
            summary = await analyze_audio_with_openai(chunk, openai_api_key, topic)
            if summary:
                full_summary += summary + "\n"

        # Step 4: Clean up the audio file after processing
        if audio_path:
            os.remove(audio_path)
            logging.info(f"Removed audio file {audio_path} after processing.")

        return full_summary.strip()

    except Exception as e:
        logging.error(f"Failed to process video {video_id}: {e}")
        return None
