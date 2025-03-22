#!/usr/bin/env python3
import os
import logging
import asyncio
from utils.database import store_transcript_summary
from utils.openAIServices import OpenAIService  # Using the encapsulated OpenAIService
from utils.youtube import YouTubeService  # Import the encapsulated YouTubeService

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


@retry(max_retries=3, delay=5)
async def process_video_transcript(self, video_id: str, topic: str) -> str:
        """
        视频处理流程：
          1. 尝试获取 YouTube 字幕（若为同步函数，则用 asyncio.to_thread 包装）。
          2. 若无字幕，则下载音频并调用 Whisper 进行转录（同步部分同样包装）。
          3. 利用 OpenAIService 生成结构化摘要。
          4. 将转录和摘要存入数据库。
          5. 返回生成的摘要。
        """
        try:
            # Step 1: 尝试获取字幕（假设 fetch_transcript 为同步函数）
            transcript = await asyncio.to_thread(self.youtube_service.fetch_transcript, video_id)
            if not transcript:
                logger.warning(f"Video {video_id} 未获取到字幕，尝试音频转录。")
                # Step 2: 下载音频并进行转录（download_audio 和 transcribe_audio 均为同步函数）
                audio_path = await asyncio.to_thread(self.youtube_service.download_audio, video_id)
                if not audio_path:
                    logger.error(f"Video {video_id} 音频下载失败。")
                    return None
                transcript = await asyncio.to_thread(self.youtube_service.transcribe_audio, audio_path)
                if not transcript:
                    logger.error(f"Video {video_id} 音频转录失败。")
                    return None

            # Step 3: 调用 OpenAIService 异步生成摘要
            interpreted_summary = await self.interpret_transcript(transcript, topic)
            if not interpreted_summary:
                logger.error(f"Video {video_id} 转录解析失败。")
                return None

            # Step 4: 存储转录和摘要到数据库（同步调用）
            self.db.store_transcript_summary(video_id, transcript, interpreted_summary)
            logger.info(f"Video {video_id} 的转录和摘要已成功存储。")
            return interpreted_summary

        except Exception as e:
            logger.error(f"处理 Video {video_id} 的转录时出现异常：{e}")
            return None

async def interpret_transcript(self, transcript: str, topic: str) -> str:
        """
        利用 OpenAIService 根据转录文本和主题生成结构化摘要。
        """
        try:
            # 构造 prompt（如有需要可加入 system_prompt 信息）
            summary = await self.openai_service.async_completion(
                prompt=transcript,
                prompt_template=None,
                temperature=0.5,
                max_tokens=1024
            )
            summary = summary.strip() if summary else None
            if summary:
                logger.info(f"转录解析完成（前100字符）：{summary[:100]}...")
            else:
                logger.error("转录解析返回了空摘要。")
            return summary
        except Exception as e:
            logger.error(f"转录解析过程中出现异常：{e}")
            return None

# Example usage (calling the main process)
if __name__ == "__main__":
    import sys
    from utils.database import init_db

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "dummy_key")
    YOUTUBE_API_KEYS = ["your_youtube_api_key"]

    DB_PATH = "videos.db"

    # Initialize the database connection and OpenAIService
    conn = init_db(DB_PATH)
    openai_service = OpenAIService(api_key=OPENAI_API_KEY)

    # Initialize YouTubeService
    youtube_service = YouTubeService(api_keys=YOUTUBE_API_KEYS)

    # Example video ID and topic
    video_id = "dQw4w9WgXcQ"
    topic = "Pop Music Trends"

    # Run the main process: transcription and summary generation
    interpreted_summary = asyncio.run(process_video_transcript(video_id, topic, conn, openai_service, youtube_service))
    if interpreted_summary:
        logger.info(f"Final interpreted summary for video {video_id}:\n{interpreted_summary}")
    else:
        logger.error("Transcript processing failed.")

    conn.close()
