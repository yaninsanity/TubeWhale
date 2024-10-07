# import logging
# from utils.youtube_api import get_youtube_service, fetch_video_metadata  # Unified import from utils
# from langchain_openai import OpenAI
# from googleapiclient.errors import HttpError

# # Aggregates metadata for multiple videos and calculates weighted scores
# def aggregate_video_metadata(videos, youtube_api_key):
#     logging.info("Aggregating video metadata.")

#     video_metadata_list = []
#     total_views = 0
#     total_likes = 0
#     total_comments = 0

#     for video in videos:
#         video_id = video['video_id']
#         metadata = fetch_video_metadata(video_id, youtube_api_key)  # Call fetch_video_metadata from utils

#         if metadata:
#             video_metadata_list.append(metadata)
#             total_views += metadata['view_count']
#             total_likes += metadata['like_count']
#             total_comments += metadata['comment_count']

#     # Calculate weighted scores and aggregate metadata
#     aggregated_metadata = {
#         'total_views': total_views,
#         'total_likes': total_likes,
#         'total_comments': total_comments,
#         'average_views': total_views // len(videos) if len(videos) > 0 else 0,
#         'average_likes': total_likes // len(videos) if len(videos) > 0 else 0,
#         'average_comments': total_comments // len(videos) if len(videos) > 0 else 0,
#         'videos': video_metadata_list  # Store individual video metadata for further analysis
#     }

#     logging.info(f"Aggregated metadata: {aggregated_metadata}")
#     return aggregated_metadata

# # Multi-agent search with keyword generation and video metadata aggregation
# def multiagent_search(base_keyword, agent_count, max_n, top_k, youtube_api_key, openai_api_key, dry_run=False):
#     logging.info(f"Starting multi-agent search with {agent_count} agents for keyword: {base_keyword}")

#     if dry_run:
#         logging.info("Dry run mode - skipping API calls.")
#         return {}

#     # Step 1: Use OpenAI to generate keyword variations
#     generated_keywords = keyword_generator_agent(base_keyword, max_n, openai_api_key)

#     # Adjust agent count if fewer keywords are generated
#     agent_count = min(agent_count, len(generated_keywords))

#     search_results = {}

#     # Step 2: Each agent searches for videos based on the generated keyword and aggregates metadata
#     for idx in range(agent_count):
#         keyword = generated_keywords[idx]
#         logging.info(f"Agent searching with keyword: {keyword}")

#         # Call YouTube API to search videos
#         videos = search_youtube_videos(keyword, youtube_api_key, top_k)
#         if videos:
#             # Aggregate metadata for the videos related to this keyword
#             aggregated_metadata = aggregate_video_metadata(videos, youtube_api_key)
#             search_results[keyword] = aggregated_metadata

#     logging.info(f"Search completed for {len(search_results)} keywords.")
#     return search_results

# # Keyword generation agent using OpenAI
# def keyword_generator_agent(base_keyword, max_n, openai_api_key):
#     llm = OpenAI(api_key=openai_api_key)
#     prompt = f"Generate {max_n} relevant variations of the keyword '{base_keyword}' for a YouTube video search."

#     try:
#         # Send prompt to OpenAI and generate keyword variations
#         response = llm.agenerate([prompt])
#         generated_keywords = response.generations[0][0].text.split("\n")
#         generated_keywords = list(set(filter(None, [kw.strip() for kw in generated_keywords])))  # Clean up and deduplicate
#         logging.info(f"Generated {len(generated_keywords)} keyword variations.")
#         return generated_keywords
#     except Exception as e:
#         logging.error(f"Error generating keywords with OpenAI: {e}")
#         return [base_keyword]  # If failed, return the base keyword to ensure the process continues

# # YouTube video search function
# def search_youtube_videos(keyword, youtube_api_key, top_k):
#     youtube = get_youtube_service(youtube_api_key)  # Initialize YouTube API from utils
#     logging.info(f"Fetching videos for keyword: {keyword}")

#     try:
#         request = youtube.search().list(part="snippet", q=keyword, maxResults=top_k)
#         response = request.execute()

#         videos = []
#         for item in response['items']:
#             video_data = {
#                 'video_id': item['id'].get('videoId', ''),
#                 'title': item['snippet'].get('title', 'N/A'),
#                 'description': item['snippet'].get('description', 'N/A'),
#                 'publish_time': item['snippet'].get('publishedAt', 'N/A'),
#                 'channel_title': item['snippet'].get('channelTitle', 'N/A')
#             }
#             videos.append(video_data)

#         logging.info(f"Retrieved {len(videos)} videos for keyword: {keyword}")
#         return videos
#     except HttpError as e:
#         logging.error(f"Error fetching videos for keyword '{keyword}': {e}")
#         return []
