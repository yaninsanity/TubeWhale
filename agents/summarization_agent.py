import logging
from openai import AsyncOpenAI
from utils.helper import retry
from dotenv import load_dotenv
import os
import tiktoken
import asyncio
import json

# Load environment variables from .env file
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

# Initialize AsyncOpenAI client
aclient = AsyncOpenAI(api_key=api_key)

# -------------------------------
# Asynchronous retry decorator
# -------------------------------
def async_retry(max_retries=3, delay=2):
    """
    An asynchronous retry decorator to retry a function if it fails.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
            raise Exception(f"All {max_retries} attempts failed.")
        return wrapper
    return decorator

# -------------------------------------------
# Function to chunk text by token count
# -------------------------------------------
def chunk_text_by_tokens(text, max_tokens=3000, overlap=200):
    """
    Chunk the given text into smaller pieces based on token count,
    ensuring that each chunk fits within the model's context window.
    
    Parameters:
        text (str): The text to be chunked.
        max_tokens (int): The maximum tokens per chunk.
        overlap (int): The number of overlapping tokens between chunks.
    
    Returns:
        list: A list of text chunks.
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")
    tokens = tokenizer.encode(text)
    chunks = []
    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)
    return chunks

# -------------------------------------------
# Summarizer agent with context maintenance
# -------------------------------------------
@async_retry(max_retries=3, delay=2)
async def gpt_summarizer_agent(long_text, *, model="gpt-4o-mini"):
    """
    Summarizes a long piece of text by splitting it into manageable chunks,
    summarizing each chunk with context, and then combining the summaries.
    
    Parameters:
        long_text (str): The text to be summarized.
        model (str): The OpenAI model to use.
    
    Returns:
        str: The final concatenated summary.
    """
    logging.info("Starting summarization agent.")
    # Chunk the long text into smaller parts
    chunks = chunk_text_by_tokens(long_text)
    summaries = []
    previous_summary = ""

    # Process each chunk while maintaining context with the previous summary
    for i, chunk in enumerate(chunks):
        logging.info(f"Summarizing chunk {i + 1}/{len(chunks)}.")

        # Enhanced prompt with clear instructions and context
        prompt = f"""
        You are an expert content creator and summarizer. Your goal is to produce a concise, actionable summary for guide production.
        For this text chunk, please focus on:
        - Key takeaways and actionable steps
        - Important insights, tools, or best practices mentioned
        - Notable challenges and how they are addressed

        Use the previous summary as context to ensure continuity and avoid missing important details.

        Previous Summary: {previous_summary}

        Text: {chunk}
        """
        try:
            # Asynchronously call the OpenAI GPT model
            response = await aclient.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt.strip()}],
                max_tokens=1024,
                temperature=0.3
            )

            # Extract the response content
            if response and response.choices and response.choices[0].message.content:
                chunk_summary = response.choices[0].message.content.strip()
                logging.info(f"Chunk {i + 1} summary: {chunk_summary}")
                summaries.append(chunk_summary)
                previous_summary = chunk_summary  # Update context for next chunk
            else:
                logging.warning(f"Failed to summarize chunk {i + 1}.")
                continue
        except Exception as e:
            logging.error(f"Error summarizing chunk {i + 1}: {e}")
            continue

    # Combine all chunk summaries into one final summary
    final_summary = " ".join(summaries)
    logging.info("Summarization completed.")
    return final_summary

# -------------------------------------------
# Standardizer agent for structured guide output
# -------------------------------------------
@async_retry(max_retries=3, delay=2)
async def standardizer_agent(summary, *, model="gpt-4o-mini"):
    """
    Standardizes a summary into a structured, actionable guide in JSON format.
    
    Parameters:
        summary (str): The summary text to standardize.
        model (str): The OpenAI model to use.
    
    Returns:
        dict or str: The standardized summary in JSON format if possible, otherwise raw text.
    """
    if not summary:
        logging.error("Summary is missing. Skipping standardization.")
        return None

    logging.info("Starting standardizer agent.")

    # Improved prompt for structured output
    standardization_prompt = f"""
    You are an expert at organizing and structuring content.
    Your task is to take the following summary and transform it into an actionable guide.
    Please focus on:
    - The main topic of the video
    - Key insights or steps users should follow
    - Recommended tools or techniques (if applicable)
    - Best practices and useful tips
    - Notable challenges and advice

    Return the result in valid JSON format, as follows:
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
        response = await aclient.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": standardization_prompt.strip()}],
            max_tokens=1024,
            temperature=0.3
        )

        if response and response.choices and response.choices[0].message.content:
            standardized_summary_raw = response.choices[0].message.content.strip()

            # Attempt to parse the response as JSON
            try:
                standardized_summary = json.loads(standardized_summary_raw)
                logging.info("Standardization completed successfully.")

                # Ensure all required keys are present
                required_fields = ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]
                for field in required_fields:
                    if field not in standardized_summary:
                        standardized_summary[field] = "N/A"

                return standardized_summary
            except json.JSONDecodeError:
                logging.error("Failed to parse response as JSON. Returning raw text.")
                return standardized_summary_raw  # Return raw text if JSON parsing fails
        else:
            logging.error("No valid response for standardization.")
            return None

    except Exception as e:
        logging.error(f"Error during standardization: {e}")
        return None

# -------------------------------------------
# Concatenate transcript segments (helper function)
# -------------------------------------------
def concatenate_transcript(transcript_data):
    """
    Concatenates a list of transcript segments into one text block and computes total duration.
    
    Parameters:
        transcript_data (list): List of transcript segments, each with a "text" and "duration" field.
    
    Returns:
        tuple: Concatenated text and total duration.
    """
    concatenated_text = " ".join([segment["text"] for segment in transcript_data])
    total_duration = sum([segment["duration"] for segment in transcript_data])
    return concatenated_text, total_duration
