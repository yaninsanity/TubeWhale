#!/usr/bin/env python3
import os
import asyncio
import logging
import traceback
from io import BytesIO
from typing import List, Optional

from pydub import AudioSegment

# 简单的异步重试装饰器
def async_retry(max_retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger = getattr(args[0], 'logger', logging.getLogger(func.__name__))
                    logger.error(f"Attempt {attempt} for {func.__name__} failed: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(delay)
            raise last_exception
        return wrapper
    return decorator

class TranscriptAgent:
    """
    TranscriptAgent 负责对指定视频执行转录操作：
      1. 调用 YouTubeService.download_audio 下载视频音频文件。
      2. 使用 pydub 将音频切分成多个片段（例如每片 60 秒）。
      3. 并发调用 OpenAIService.transcribe_audio 对各片段进行转录（内部自愈重试）。
      4. 合并所有片段转录结果，返回最终转录文本。
    """
    MAX_CHUNK_DURATION_MS = 60000  # 每个片段 60 秒
    CONCURRENCY_LIMIT = 5          # 并发转录任务上限

    def __init__(self, openai_service, youtube_service, logger: Optional[logging.Logger] = None):
        self.openai_service = openai_service
        self.youtube_service = youtube_service
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        # 延迟创建 semaphore，避免在导入/同步上下文中无 loop 报错
        self._semaphore = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.CONCURRENCY_LIMIT)
        return self._semaphore

    def _slice_audio(self, audio_path: str) -> List[AudioSegment]:
        """
        使用 pydub 将音频文件切分为多个片段。
        """
        try:
            audio = AudioSegment.from_file(audio_path)
            duration_ms = len(audio)
            self.logger.info(f"Audio duration: {duration_ms / 1000:.1f} seconds.")
            chunks = [
                audio[i : i + self.MAX_CHUNK_DURATION_MS]
                for i in range(0, duration_ms, self.MAX_CHUNK_DURATION_MS)
            ]
            self.logger.info(f"Sliced audio into {len(chunks)} chunks.")
            return chunks
        except Exception as e:
            self.logger.error(f"Failed to slice audio {audio_path}: {e}")
            return []

    @async_retry(max_retries=3, delay=2)
    async def _transcribe_chunk(self, chunk: AudioSegment) -> str:
        """
        将单个音频片段导出为 BytesIO，并调用 OpenAIService.transcribe_audio 转录。
        """
        audio_io = BytesIO()
        try:
            chunk.export(audio_io, format="mp3")
            audio_io.seek(0)
            if not hasattr(audio_io, "name"):
                audio_io.name = "audio.mp3"
            elif not os.path.splitext(audio_io.name)[1]:
                audio_io.name += ".mp3"
        except Exception as e:
            self.logger.error(f"Error converting audio chunk to mp3: {e}")
            raise

        async with self.semaphore:
            return await self.openai_service.transcribe_audio(audio_io)

    async def fetch_transcript(self, video_id: str) -> Optional[str]:
        """
        获取指定视频的完整转录文本。
        """
        self.logger.info(f"[{video_id}] Downloading audio.")
        loop = asyncio.get_running_loop()
        try:
            audio_path = await loop.run_in_executor(
                None, self.youtube_service.download_audio, video_id
            )
        except Exception as e:
            self.logger.error(f"[{video_id}] Exception during audio download: {e}")
            return None

        if not audio_path or not os.path.exists(audio_path):
            self.logger.error(f"[{video_id}] Audio download failed.")
            return None

        self.logger.info(f"[{video_id}] Audio downloaded: {audio_path}. Starting transcription.")
        chunks = self._slice_audio(audio_path)
        if not chunks:
            self.logger.error(f"[{video_id}] No audio chunks available for transcription.")
            return None

        tasks = [asyncio.create_task(self._transcribe_chunk(chunk)) for chunk in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_lines = []
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"[{video_id}] Error transcribing chunk {idx+1}: {result}")
                self.logger.debug(traceback.format_exc())
            else:
                text = result.strip()
                if text:
                    final_lines.append(text)

        if not final_lines:
            self.logger.error(f"[{video_id}] All transcription attempts failed.")
            return None

        merged = "\n".join(final_lines)
        self.logger.info(f"[{video_id}] Transcription completed successfully.")
        return merged

async def run_transcript(
    video_id: str,
    openai_service,
    youtube_service,
    logger: Optional[logging.Logger] = None
) -> Optional[str]:
    logger = logger or logging.getLogger("TranscriptRunner")
    agent = TranscriptAgent(openai_service, youtube_service, logger=logger)
    logger.info(f"[run_transcript] Fetching transcript for video {video_id}.")
    return await agent.fetch_transcript(video_id)
