#!/usr/bin/env python3
"""
Transcript Agent

该模块负责获取 YouTube 视频的完整转录文本。
流程：
  1. 调用 YouTubeService.download_audio 下载视频音频文件（采用 pytube + ffmpeg 提取 mp3）。
  2. 将下载的音频读取为 BytesIO 对象，并调用 OpenAIService.transcribe_audio 对音频进行转录。
  3. 返回转录文本。

该方案完全依赖于音频下载与转录，不再使用字幕 API。
"""

import os
import asyncio
import logging
from io import BytesIO
from typing import Optional

from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService

def get_default_logger(name: str = __name__) -> logging.Logger:
    """
    返回一个默认的 logger，如外部未传入 logger 时使用。
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

class TranscriptAgent:
    def __init__(self, openai_service: OpenAIService, youtube_service: YouTubeService,
                 logger: Optional[logging.Logger] = None):
        """
        :param openai_service: 已初始化的 OpenAIService 实例
        :param youtube_service: 已初始化的 YouTubeService 实例
        :param logger: 外部传入的 logger 对象；若未传入则使用默认 logger
        """
        self.openai_service = openai_service
        self.youtube_service = youtube_service
        self.logger = logger or get_default_logger(self.__class__.__name__)

    async def fetch_transcript(self, video_id: str) -> Optional[str]:
        """
        获取单个视频的转录文本。
        流程：
          1. 调用 youtube_service.download_audio 下载视频音频文件。
          2. 将音频读取为 BytesIO 对象，并调用 openai_service.transcribe_audio 对音频进行转录。
          3. 返回转录文本；若失败则返回 None。
          
        :param video_id: YouTube 视频 ID
        :return: 转录文本或 None
        """
        self.logger.info(f"[{video_id}] Downloading audio.")
        try:
            loop = asyncio.get_running_loop()
            # 将同步的音频下载操作包装为异步调用
            audio_path = await loop.run_in_executor(None, self.youtube_service.download_audio, video_id)
            if not audio_path:
                self.logger.error(f"[{video_id}] Failed to download audio.")
                return None

            self.logger.info(f"[{video_id}] Audio downloaded: {audio_path}. Starting transcription.")
            try:
                with open(audio_path, "rb") as f:
                    audio_bytes = f.read()
                audio_file = BytesIO(audio_bytes)
            except Exception as e:
                self.logger.error(f"[{video_id}] Failed to read audio file {audio_path}: {e}", exc_info=True)
                return None

            transcript = await self.openai_service.transcribe_audio(audio_file)
            if transcript and transcript.strip():
                self.logger.info(f"[{video_id}] Transcription succeeded (first 50 chars): {transcript[:50]}...")
                return transcript
            else:
                self.logger.error(f"[{video_id}] Transcription failed or empty.")
                return None
        except Exception as e:
            self.logger.error(f"[{video_id}] Error in fetch_video_transcript: {e}", exc_info=True)
            return None

async def run_transcript(video_id: str, openai_service: OpenAIService,
                         youtube_service: YouTubeService,
                         logger: Optional[logging.Logger] = None) -> Optional[str]:
    """
    顶层异步函数，获取视频转录文本。
    :param video_id: YouTube 视频 ID
    :param openai_service: 已初始化的 OpenAIService 实例
    :param youtube_service: 已初始化的 YouTubeService 实例
    :param logger: 外部传入的 logger；若未传入则使用默认 logger
    :return: 转录文本或 None
    """
    logger = logger or get_default_logger()
    agent = TranscriptAgent(openai_service, youtube_service, logger=logger)
    logger.info(f"[run_transcript] Fetching transcript for video {video_id}.")
    return await agent.fetch_transcript(video_id)

# ---------------------- 示例入口 ----------------------
if __name__ == "__main__":
    async def main():
        # 请确保环境变量 OPENAI_API_KEY 已设置，或直接在此处赋值
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "sk-your_key")
        YOUTUBE_API_KEYS = ["your_youtube_api_key1", "your_youtube_api_key2"]

        # 初始化服务实例时，由外部传入 logger（或使用默认 logger）
        default_logger = get_default_logger()
        openai_service = OpenAIService(api_key=OPENAI_API_KEY)
        youtube_service = YouTubeService(api_keys=YOUTUBE_API_KEYS)

        test_video_id = "dQw4w9WgXcQ"  # 请替换为实际视频ID
        transcript_text = await run_transcript(test_video_id, openai_service, youtube_service, logger=default_logger)
        if transcript_text:
            default_logger.info(f"Transcript for video {test_video_id}:\n{transcript_text}")
        else:
            default_logger.error("Transcript retrieval failed.")

    asyncio.run(main())
