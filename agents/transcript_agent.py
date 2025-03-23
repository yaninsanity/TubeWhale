#!/usr/bin/env python3
import logging
import asyncio
from typing import Dict, Any, Optional, List

from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService
from utils.helper import retry

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def maybe_async(func, *args, **kwargs):
    """
    如果 func 是 async 函数，则直接 await 调用；否则用 asyncio.to_thread 包装调用。
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return await asyncio.to_thread(func, *args, **kwargs)


@retry(max_retries=3, delay=5)
async def fetch_transcript(youtube_service: YouTubeService, video_id: str) -> Optional[str]:
    """
    获取视频字幕的函数，必须传入 YouTubeService 实例。
    如果字幕不存在，则抛出异常。
    """
    transcript = await maybe_async(youtube_service.fetch_transcript, video_id)
    if not transcript:
        raise Warning(f"Transcript not found for video {video_id}")
    return transcript


class VideoProcessor:
    """
    VideoProcessor 统一封装视频转录与摘要生成流程，
    所有依赖（数据库 db、OpenAIService、YouTubeService）均通过构造函数传入，
    确保各依赖的生命周期由调用方管理。
    """
    def __init__(self, db, openai_service: OpenAIService, youtube_service: YouTubeService):
        self.db = db
        self.openai_service = openai_service
        self.youtube_service = youtube_service

    @retry(max_retries=3, delay=5)
    async def process_video_transcript(self, video_id: str, topic: str) -> Optional[str]:
        """
        视频处理流程：
          1. 尝试获取 YouTube 字幕（调用 maybe_async()）。
          2. 若无字幕，则下载音频并调用 Whisper 进行转录。
          3. 利用 OpenAIService 异步生成结构化摘要。
          4. 将转录和摘要存入数据库（异步包装）。
          5. 返回生成的摘要。
        """
        try:
            # Step 1: 尝试获取字幕
            transcript = await maybe_async(self.youtube_service.fetch_transcript, video_id)
            if not transcript:
                logger.warning(f"Video {video_id} has no transcript, falling back to audio transcription. 😊")
                # Step 2: 下载音频并转录
                audio_path = await maybe_async(self.youtube_service.download_audio, video_id)
                if not audio_path:
                    logger.error(f"Audio download failed for video {video_id}. 😢")
                    return None
                transcript = await maybe_async(self.youtube_service.transcribe_audio, audio_path)
                if not transcript:
                    logger.error(f"Audio transcription failed for video {video_id}. 😢")
                    return None

            # Step 3: 利用 OpenAIService 异步生成摘要
            interpreted_summary = await self.openai_service.async_completion(
                prompt=transcript,
                prompt_template=None,
                temperature=0.5,
                max_tokens=1024
            )
            interpreted_summary = interpreted_summary.strip() if interpreted_summary else None
            if not interpreted_summary:
                logger.error(f"Transcript interpretation failed for video {video_id}. 😢")
                return None

            # Step 4: 异步存储转录和摘要到数据库
            await asyncio.to_thread(self.db.store_transcript_summary, video_id, transcript, interpreted_summary)
            logger.info(f"Video {video_id}'s transcript and summary successfully stored. 😊")
            return interpreted_summary

        except Exception as e:
            logger.error(f"Error processing video {video_id}: {e}")
            return None

    async def interpret_transcript(self, transcript: str, topic: str) -> Optional[str]:
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
                logger.info(f"Transcript interpreted (first 100 chars): {summary[:100]}... 😊")
            else:
                logger.error("Empty summary returned. 😢")
            return summary
        except Exception as e:
            logger.error(f"Error during transcript interpretation: {e}")
            return None


__all__ = ["fetch_transcript", "VideoProcessor"]

# ---------------------- 如果直接运行本模块，则进行简单测试 ----------------------
if __name__ == "__main__":
    from utils.database import Database
    import os

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "dummy_key")
    YOUTUBE_API_KEYS = ["your_youtube_api_key"]
    DB_PATH = "videos.db"

    db = Database(DB_PATH)
    openai_service = OpenAIService(api_key=OPENAI_API_KEY)
    youtube_service = YouTubeService(api_keys=YOUTUBE_API_KEYS)

    processor = VideoProcessor(db, openai_service, youtube_service)

    video_id = "dQw4w9WgXcQ"
    topic = "Pop Music Trends"
    result = asyncio.run(processor.process_video_transcript(video_id, topic))
    if result:
        logger.info(f"Final interpreted summary for video {video_id}:\n{result}")
    else:
        logger.error("Transcript processing failed.")
    db.close()
