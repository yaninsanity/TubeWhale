#!/usr/bin/env python3
import logging
import asyncio
import traceback
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
    获取视频字幕的函数。调用时使用重试机制。
    如果字幕不存在，则抛出异常，触发重试。
    """
    transcript = await maybe_async(youtube_service.fetch_transcript, video_id)
    if transcript is None or not transcript.strip():
        raise Warning(f"Transcript not found for video {video_id}")
    logger.info(f"Fetched transcript for video {video_id}: {transcript[:50]}...")
    return transcript


class VideoProcessor:
    """
    VideoProcessor 统一封装视频转录与摘要生成流程。
    依赖的数据库、OpenAIService、YouTubeService均通过构造函数传入，
    确保调用方统一管理各依赖的生命周期。
    """
    def __init__(self, db, openai_service: OpenAIService, youtube_service: YouTubeService):
        self.db = db
        self.openai_service = openai_service
        self.youtube_service = youtube_service

    @retry(max_retries=3, delay=5)
    async def process_video_transcript(self, video_id: str, topic: str) -> Optional[str]:
        """
        视频处理流程：
          1. 尝试获取 YouTube 字幕（通过 maybe_async() 和重试）。
          2. 若无字幕，则下载音频并调用 Whisper 进行转录。
          3. 利用 OpenAIService 异步生成结构化摘要（结合主题信息）。
          4. 将转录和摘要异步存入数据库。
          5. 返回生成的摘要文本。
        """
        try:
            logger.info(f"[VideoProcessor] Processing video {video_id} - Step 1: Fetch transcript")
            # 尝试获取字幕
            try:
                transcript = await fetch_transcript(self.youtube_service, video_id)
            except Exception as e:
                logger.warning(f"[VideoProcessor] Transcript not available for video {video_id}: {e}")
                transcript = None

            # 若无字幕，则采用音频转录方案
            if not transcript or not transcript.strip():
                logger.info(f"[VideoProcessor] Video {video_id} has no valid transcript, falling back to audio transcription.")
                audio_path = await maybe_async(self.youtube_service.download_audio, video_id)
                if not audio_path or not audio_path.strip():
                    logger.error(f"[VideoProcessor] Audio download failed for video {video_id}.")
                    return None
                logger.info(f"[VideoProcessor] Audio downloaded for video {video_id}: {audio_path}")
                transcript = await maybe_async(self.youtube_service.transcribe_audio, audio_path)
                if not transcript or not transcript.strip():
                    logger.error(f"[VideoProcessor] Audio transcription failed for video {video_id}.")
                    return None
                logger.info(f"[VideoProcessor] Audio transcription succeeded for video {video_id}: {transcript[:50]}...")

            # Step 3: 利用 OpenAIService 异步生成摘要（结合主题信息）
            logger.info(f"[VideoProcessor] Video {video_id} - Step 2: Interpret transcript with topic '{topic}'")
            interpreted_summary = await self.interpret_transcript(transcript, topic)
            if not interpreted_summary or not interpreted_summary.strip():
                logger.error(f"[VideoProcessor] Transcript interpretation failed for video {video_id}.")
                return None

            # Step 4: 异步存储转录与摘要到数据库
            logger.info(f"[VideoProcessor] Video {video_id} - Step 3: Store transcript and summary into DB")
            await asyncio.to_thread(self.db.store_transcript_summary, video_id, transcript, interpreted_summary)
            logger.info(f"[VideoProcessor] Video {video_id} transcript and summary successfully stored.")
            return interpreted_summary

        except Exception as e:
            logger.error(f"[VideoProcessor] Error processing video {video_id}: {traceback.format_exc()}")
            return None

    async def interpret_transcript(self, transcript: str, topic: str) -> Optional[str]:
        """
        利用 OpenAIService 根据转录文本和主题生成结构化摘要。
        将主题信息嵌入提示中，以便生成更聚焦的摘要。
        """
        try:
            # 构造包含主题信息的提示文本
            prompt_text = f"Generate a structured summary focusing on the topic '{topic}'.\nTranscript:\n{transcript}"
            logger.info(f"[VideoProcessor] Interpreting transcript for topic '{topic}'")
            summary = await self.openai_service.async_completion(
                prompt=prompt_text,
                prompt_template=None,
                temperature=0.5,
                max_tokens=1024
            )
            summary = summary.strip() if summary else None
            if summary:
                logger.info(f"[VideoProcessor] Transcript interpreted (first 100 chars): {summary[:100]}...")
            else:
                logger.error("[VideoProcessor] Empty summary returned during transcript interpretation.")
            return summary
        except Exception as e:
            logger.error(f"[VideoProcessor] Error during transcript interpretation: {traceback.format_exc()}")
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
