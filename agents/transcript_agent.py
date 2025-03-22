#!/usr/bin/env python3
import os
import logging
import asyncio
from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService
from utils.database import init_db
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Retry decorator with exponential backoff
def retry(max_retries=3, delay=2):
    """
    Retry decorator with exponential backoff.
    If the decorated async function fails repeatedly, retry with increasing delays.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logger.error(
                        f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries} in {_delay} seconds..."
                    )
                    await asyncio.sleep(_delay)
                    _delay *= 2  # Exponential backoff
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

class VideoProcessor:
    """
    统一封装视频转录与摘要生成流程的类，通过构造函数注入所有依赖，
    保证 db、openai_service、youtube_service 的生命周期由调用方管理。
    """
    def __init__(self, db, openai_service, youtube_service):
        self.db = db
        self.openai_service = openai_service
        self.youtube_service = youtube_service

    @retry(max_retries=3, delay=5)
    async def process_video_transcript(self, video_id: str, topic: str) -> str:
        """
        视频处理流程：
          1. 尝试获取 YouTube 字幕（若为同步函数，则用 asyncio.to_thread 包装）。
          2. 若无字幕，则下载音频并调用 Whisper 进行转录（同步部分同样包装）。
          3. 利用 OpenAIService 生成结构化摘要。
          4. 将转录和摘要存入数据库（调用传入的 db 对象）。
          5. 返回生成的摘要。
        """
        try:
            # Step 1: 尝试获取字幕
            transcript = await asyncio.to_thread(self.youtube_service.fetch_transcript, video_id)
            if not transcript:
                logger.warning(f"Video {video_id} has no transcript, falling back to audio transcription.")
                # Step 2: 下载音频并进行转录
                audio_path = await asyncio.to_thread(self.youtube_service.download_audio, video_id)
                if not audio_path:
                    logger.error(f"Audio download failed for video {video_id}.")
                    return None
                transcript = await asyncio.to_thread(self.youtube_service.transcribe_audio, audio_path)
                if not transcript:
                    logger.error(f"Audio transcription failed for video {video_id}.")
                    return None

            # Step 3: 利用 OpenAIService 异步生成摘要
            interpreted_summary = await self.interpret_transcript(transcript, topic)
            if not interpreted_summary:
                logger.error(f"Transcript interpretation failed for video {video_id}.")
                return None

            # Step 4: 存储转录和摘要到数据库
            self.db.store_transcript_summary(video_id, transcript, interpreted_summary)
            logger.info(f"Video {video_id}'s transcript and summary successfully stored.")
            return interpreted_summary

        except Exception as e:
            logger.error(f"Error processing video {video_id}: {e}")
            return None

    async def interpret_transcript(self, transcript: str, topic: str) -> str:
        """
        利用 OpenAIService 根据转录文本和主题生成结构化摘要。
        """
        try:
            summary = await self.openai_service.async_completion(
                prompt=transcript,
                prompt_template=None,
                temperature=0.5,
                max_tokens=1024
            )
            summary = summary.strip() if summary else None
            if summary:
                logger.info(f"Transcript interpreted (first 100 chars): {summary[:100]}...")
            else:
                logger.error("Empty summary returned.")
            return summary
        except Exception as e:
            logger.error(f"Error during transcript interpretation: {e}")
            return None

# -------------------------------------------------------------------------------
# 主入口
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    # 从环境变量中获取 API 密钥及数据库路径
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "dummy_key")
    YOUTUBE_API_KEYS = ["your_youtube_api_key"]
    DB_PATH = "videos.db"

    # 初始化各依赖（注意：db 的生命周期由 main 管理，最后需要调用 db.close()）
    db = init_db(DB_PATH)
    openai_service = OpenAIService(api_key=OPENAI_API_KEY)
    youtube_service = YouTubeService(api_keys=YOUTUBE_API_KEYS)

    # 创建 VideoProcessor 实例，所有依赖统一注入
    processor = VideoProcessor(db, openai_service, youtube_service)

    # 示例视频 ID 和主题
    video_id = "dQw4w9WgXcQ"
    topic = "Pop Music Trends"

    # 运行视频转录和摘要生成流程
    interpreted_summary = asyncio.run(processor.process_video_transcript(video_id, topic))
    if interpreted_summary:
        logger.info(f"Final interpreted summary for video {video_id}:\n{interpreted_summary}")
    else:
        logger.error("Transcript processing failed.")

    # 关闭数据库连接
    db.close()
