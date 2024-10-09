# audio_agent.py

import os
import logging
import asyncio
from openai import AsyncOpenAI
from yt_dlp import YoutubeDL
from pydub import AudioSegment
from io import BytesIO
import json
from dotenv import load_dotenv
import sys
import aiohttp  # Import aiohttp for HTTP requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

if not openai_api_key:
    logging.error("OpenAI API key not found. Please set it in your environment variables.")
    sys.exit(1)
aclient = AsyncOpenAI(api_key=openai_api_key)

# Retry decorator
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
        audio_path = f'downloads/{video_id}.mp3'
        if os.path.exists(audio_path):
            logging.info(f"Audio file {audio_path} already exists. Skipping download.")
            return audio_path

        logging.info(f"Downloading audio for video ID: {video_id}")

        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'downloads/{video_id}.%(ext)s',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'quiet': True,
            'no_warnings': True,
        }

        def download():
            with YoutubeDL(ydl_opts) as ydl:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                ydl.download([video_url])

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, download)

        if os.path.exists(audio_path):
            return audio_path
        else:
            logging.error(f"Audio file {audio_path} not found after download.")
            return None

    except Exception as e:
        logging.error(f"Failed to download audio for video ID {video_id}: {e}")
        return None

# Function to split audio into manageable chunks
def split_audio(audio_path, max_duration_ms=60000):
    try:
        logging.info(f"Splitting audio {audio_path} into chunks based on max duration {max_duration_ms} ms.")
        audio = AudioSegment.from_file(audio_path)
        chunks = [audio[i:i + max_duration_ms] for i in range(0, len(audio), max_duration_ms)]
        logging.info(f"Audio split into {len(chunks)} chunks.")
        return chunks
    except Exception as e:
        logging.error(f"Failed to split audio {audio_path}: {e}")
        return []

# Function to transcribe an audio chunk using OpenAI
@retry(max_retries=3, delay=5)
async def transcribe_audio_chunk(audio_chunk):
    try:
        # Convert AudioSegment to bytes
        audio_file = BytesIO()
        audio_chunk.export(audio_file, format="mp3")
        audio_file.seek(0)  # Reset file pointer

        # Transcribe audio using OpenAI Whisper API via direct HTTP request
        logging.info("Transcribing audio chunk using OpenAI Whisper API.")

        url = "https://api.openai.com/v1/audio/transcriptions"

        headers = {
            "Authorization": f"Bearer {openai_api_key}",
        }

        form_data = aiohttp.FormData()
        form_data.add_field('file',
                            audio_file,
                            filename='audio_chunk.mp3',
                            content_type='audio/mpeg')
        form_data.add_field('model', 'whisper-1')
        form_data.add_field('response_format', 'text')

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=form_data) as resp:
                if resp.status == 200:
                    transcript_text = await resp.text()
                    logging.info("Transcription completed for audio chunk.")
                    return transcript_text
                else:
                    error_text = await resp.text()
                    logging.error(f"Failed to transcribe audio chunk with OpenAI: {error_text}")
                    return None
    except Exception as e:
        logging.error(f"Failed to transcribe audio chunk with OpenAI: {e}")
        return None

# Function to summarize text using OpenAI
@retry(max_retries=3, delay=5)
async def summarize_text(transcript_text, previous_summary, topic):
    try:
        # Define system prompt and user message
        messages = [
            {"role": "system", "content": (
                f"You are an expert content creator whose goal is to produce actionable summaries for guide production.\n"
                f"Each chunk of text must be summarized with the following in mind:\n"
                f"- What are the key takeaways and steps that users should know?\n"
                f"- What insights, tools, or best practices are mentioned?\n"
                f"- What are the notable challenges and how are they addressed?\n"
                f"Focus on the topic: {topic}\n"
                f"Use the previous summary to maintain context and ensure no important details are missed."
            )},
            {"role": "user", "content": f"Previous Summary:\n{previous_summary}\n\nNew Transcript:\n{transcript_text}"}
        ]

        logging.info("Generating summary using OpenAI ChatCompletion.")
        response = await aclient.chat.completions.create(model="gpt-4",  # Use "gpt-4" if you have access
        messages=messages,
        max_tokens=1024,
        temperature=0.5)

        summary = response.choices[0].message.content.strip()
        logging.info("Summary generated for transcript chunk.")
        return summary

    except Exception as e:
        logging.error(f"Failed to summarize text with OpenAI: {e}")
        return None

# Function to recursively summarize chunk summaries
async def recursive_summarize(summaries, topic):
    try:
        while len(summaries) > 1:
            new_summaries = []
            for i in range(0, len(summaries), 2):
                summaries_to_summarize = summaries[i:i+2]
                combined_summary = "\n\n".join(summaries_to_summarize)
                summary = await summarize_text(combined_summary, "", topic)
                if summary:
                    new_summaries.append(summary)
                else:
                    logging.error("Failed to generate recursive summary.")
            summaries = new_summaries
        if summaries:
            return summaries[0]
        else:
            logging.error("No summaries to combine.")
            return None
    except Exception as e:
        logging.error(f"Failed during recursive summarization: {e}")
        return None

# Function to standardize the final summary
@retry(max_retries=3, delay=2)
async def standardize_summary(summary):
    if not summary:
        logging.error("Summary is missing. Skipping standardization.")
        return None

    logging.info("Starting standardizer agent.")

    # Standardization prompt
    standardization_prompt = f"""
    You are an expert at organizing and structuring content.
    Your job is to take the following summary and standardize it into an actionable guide format.
    Focus on:
    - Main topic of the video
    - Key insights or steps users should follow
    - Recommended tools or techniques (if applicable)
    - Best practices and tips shared
    - Notable challenges or advice

    Provide the standardized summary in the following JSON format:
    {{
        "main_topic": "...",
        "key_insights": "...",
        "recommended_tools": "...",
        "best_practices": "...",
        "challenges_and_advice": "..."
    }}

    Summary to standardize: {summary}
    """

    try:
        logging.info("Standardizing summary using OpenAI ChatCompletion.")
        response = await aclient.chat.completions.create(model="gpt-4",  # Use "gpt-4" if you have access
        messages=[{"role": "user", "content": standardization_prompt.strip()}],
        max_tokens=1024,
        temperature=0.3)

        standardized_summary_raw = response.choices[0].message.content.strip()

        # Try to parse the output as JSON
        try:
            standardized_summary = json.loads(standardized_summary_raw)
            logging.info("Standardization completed successfully.")

            # Ensure all expected keys are present
            required_fields = ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]
            for field in required_fields:
                if field not in standardized_summary:
                    standardized_summary[field] = "N/A"

            return standardized_summary
        except json.JSONDecodeError:
            logging.error("Failed to parse response as JSON. Returning raw text.")
            return standardized_summary_raw  # Return raw text if parsing fails

    except Exception as e:
        logging.error(f"Error during standardization: {e}")
        return None

# Main function to process the audio and generate standardized summary
async def transcribe_audio_to_summary(video_id, topic):
    try:
        # Step 1: Download audio file
        audio_path = await download_audio(video_id)
        if not audio_path or not os.path.exists(audio_path):
            logging.error(f"Audio download failed for video ID: {video_id}")
            return None

        # Step 2: Split audio into chunks
        audio_chunks = split_audio(audio_path, max_duration_ms=60000)  # Adjust max_duration_ms as needed
        if not audio_chunks:
            logging.error(f"Failed to split audio for video ID: {video_id}")
            return None

        # Step 3: Transcribe each audio chunk and summarize
        chunk_summaries = []
        previous_summary = ""
        for idx, chunk in enumerate(audio_chunks):
            logging.info(f"Processing audio chunk {idx + 1}/{len(audio_chunks)}")

            # Transcribe chunk
            transcript = await transcribe_audio_chunk(chunk)
            if not transcript:
                logging.error(f"Failed to transcribe audio chunk {idx + 1}")
                continue

            # Summarize chunk with context from previous summary
            summary = await summarize_text(transcript, previous_summary, topic)
            if summary:
                chunk_summaries.append(summary)
                previous_summary = summary  # Update previous summary for context
            else:
                logging.error(f"Failed to summarize audio chunk {idx + 1}")

        if not chunk_summaries:
            logging.error(f"No summaries generated for video ID: {video_id}")
            return None

        # Step 4: Recursively summarize chunk summaries to get a final summary
        logging.info("Combining chunk summaries into final summary.")
        final_summary = await recursive_summarize(chunk_summaries, topic)
        if not final_summary:
            logging.error(f"Failed to generate final summary for video ID: {video_id}")
            return None

        # Step 5: Standardize the final summary
        standardized_summary = await standardize_summary(final_summary)
        if not standardized_summary:
            logging.error(f"Failed to standardize summary for video ID: {video_id}")
            return None

        # Step 6: Clean up downloaded audio file
        # Optionally, you can keep the audio file for caching purposes
        # if audio_path and os.path.exists(audio_path):
        #     os.remove(audio_path)
        #     logging.info(f"Removed audio file {audio_path} after processing.")

        return standardized_summary

    except Exception as e:
        logging.error(f"Failed to process video {video_id}: {e}")
        return None

# Main function for unit testing
if __name__ == "__main__":
    # Get video ID and topic from command line arguments
    if len(sys.argv) < 3:
        print("Usage: python audio_agent.py <video_id> <topic>")
        sys.exit(1)

    video_id = sys.argv[1]
    topic = sys.argv[2]

    # Run the main function
    async def main():
        result = await transcribe_audio_to_summary(video_id, topic)
        if result:
            print("Standardized Summary:")
            print(json.dumps(result, indent=4, ensure_ascii=False))
        else:
            print("Failed to process the video.")

    asyncio.run(main())
