import logging
from utils.youtube_api import get_youtube_service, fetch_video_metadata  # 重用 utils 模块中的 YouTube API 和元数据获取功能
from langchain_community.llms import OpenAI

# 重试机制，确保元数据和 AI 分析操作的稳健性
def retry_async(max_retries=3, delay=2, backoff_factor=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}. Retrying {retries + 1}/{max_retries} in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    retries += 1
                    current_delay *= backoff_factor
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# 利用 LLM 对元数据进行分析和加权
@retry_async(max_retries=3, delay=2)
# 在生成详细的评估结果时处理NoneType问题
async def analyze_metadata_with_ai(metadata, openai_api_key, topic):
    try:
        llm = get_openai_service(openai_api_key)

        prompt = f"""
        You are an expert in video content analysis. Based on the following video metadata and the topic '{topic}', 
        analyze the quality and relevance of this video. 
        Consider factors such as views, likes, comments, and the video's overall relevance to the topic. 
        Assign a weighted score to the video based on these criteria.
        
        Metadata:
        - Title: {metadata['title']}
        - Description: {metadata['description']}
        - Publish Time: {metadata['publish_time']}
        - Channel: {metadata['channel_title']}
        - View Count: {metadata['view_count']}
        - Like Count: {metadata['like_count']}
        - Comment Count: {metadata['comment_count']}
        """

        response = await llm.agenerate(prompt)

        if response is None or 'generations' not in response:
            logging.error("AI response is None or malformed.")
            return None
        
        result = response.generations[0][0].text.strip()
        if not result:
            logging.warning("Received empty result from AI analysis.")
            return None

        logging.info(f"AI analysis result: {result}")
        return result
    except Exception as e:
        logging.error(f"Failed to analyze metadata with AI: {e}")
        return None


# 主流程：提取元数据并交由 AI 进行加权分析
@retry_async(max_retries=3, delay=2)
async def fetch_and_analyze_metadata(video_id, youtube_api_key, openai_api_key, topic):
    logging.info(f"Fetching and analyzing metadata for video ID: {video_id}")

    # Step 1: 提取元数据
    metadata = fetch_video_metadata(video_id, youtube_api_key)
    if not metadata:
        logging.error(f"Failed to retrieve metadata for video ID: {video_id}")
        return None

    # Step 2: 交由 AI 进行加权分析
    result = await analyze_metadata_with_ai(metadata, openai_api_key, topic)
    if result:
        logging.info(f"AI generated analysis for video ID {video_id}: {result}")
    else:
        logging.warning(f"No AI analysis result for video ID {video_id}")
    
    return result
