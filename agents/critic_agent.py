import logging
import openai  # Ensure that openai is correctly imported
from datetime import datetime
from utils.database import store_ai_interaction
import json

# Main function: Evaluate a list of videos and return the videos sorted by quality/relevance.
async def critic_agent(videos, api_key, conn=None):
    logging.info("Starting critic agent to rank videos.")
    openai.api_key = api_key  # Set OpenAI API key

    if not videos:
        logging.error("No videos provided to critic agent.")
        return videos  # Return original video list if none provided

    # Build a list of video descriptions for evaluation
    video_descriptions = []
    for idx, video in enumerate(videos):
        description = (
            f"Video {idx+1}:\n"
            f"Video ID: {video.get('video_id', 'N/A')}\n"
            f"Title: {video.get('title', 'N/A')}\n"
            f"Description: {video.get('description', 'N/A')}\n"
            f"Views: {video.get('view_count', 0)}\n"
            f"Likes: {video.get('like_count', 0)}\n"
            f"Comments: {video.get('comment_count', 0)}\n"
        )
        video_descriptions.append(description)

    # Enriched prompt: instruct the model to rank videos based on quality and relevance
    prompt = (
        "You are a professional video content analyst and evaluator. Your task is to rank the following videos "
        "from most to least relevant and high-quality for the topic 'Virginia fishing'. Please provide your ranking as a numbered list, "
        "and include only the unique Video ID for each entry.\n\n"
        "For example:\n"
        "1. Video ID: ABC123\n"
        "2. Video ID: DEF456\n"
        "...\n\n"
        "Below is the detailed information for each video:\n\n" + "\n".join(video_descriptions)
    )

    try:
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Sending prompt to OpenAI API for critic agent ranking.")

        # Use the proper model name (using GPT-4 here)
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,  # Increased max_tokens to ensure a complete response
            temperature=0.5
        )

        content = response.choices[0].message.content.strip()
        logging.info(f"Received response from OpenAI API: {content}")

        # Parse the ranking results from the response
        ranked_video_ids = []
        for line in content.splitlines():
            line = line.strip()
            if line and "Video ID:" in line:
                parts = line.split("Video ID:")
                if len(parts) == 2:
                    video_id = parts[1].strip()
                    ranked_video_ids.append(video_id)

        logging.info(f"Parsed ranked_video_ids: {ranked_video_ids}")

        # Create a mapping from video IDs to video objects
        video_dict = {video['video_id']: video for video in videos}

        # Sort videos based on the ranking results
        ranked_videos = []
        for vid in ranked_video_ids:
            if vid in video_dict:
                ranked_videos.append(video_dict[vid])

        # Log the AI interaction in the database if a connection is provided
        if conn:
            store_ai_interaction(
                conn,
                prompt,          # Input prompt
                content,         # Output from AI
                "critic_agent_ranking",  # Interaction type
                start_time       # Timestamp
            )

        # Fallback: if no valid ranking is parsed, sort by view count
        if not ranked_videos:
            logging.warning("Failed to parse ranking from OpenAI response. Falling back to sorting by view count.")
            ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)

        return ranked_videos

    except Exception as e:
        logging.error(f"Error in critic agent: {e}")
        logging.exception(e)  # Log full stack trace
        # In case of error, fallback to sorting by view count
        ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
        return ranked_videos
