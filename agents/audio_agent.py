import os
import logging
import asyncio
import random
from io import BytesIO
import json
from typing import Any, Dict, List, Optional
from yt_dlp import YoutubeDL
from pydub import AudioSegment
import re

from utils.helper import retry

# -------------------------------
# 配置日志
# -------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -------------------------------
# AudioProcessingAgent 类
# -------------------------------
class AudioProcessingAgent:
    """
    AudioProcessingAgent 负责处理 YouTube 视频的音频，并生成标准化摘要。
    """
    DEFAULT_MAX_TOKENS = 3000
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self, openai_service, download_dir: str = "downloads", max_duration_ms: int = 60000, debug_mode: bool = False):
        self.openai_service = openai_service
        self.download_dir = download_dir
        self.max_duration_ms = max_duration_ms
        self.debug_mode = debug_mode
        os.makedirs(self.download_dir, exist_ok=True)

    # -------------------------------
    # 下载音频
    # -------------------------------
    async def download_audio(self, video_id: str) -> Optional[str]:
        """
        下载 YouTube 视频音频文件。
        """
        audio_path = os.path.join(self.download_dir, f"{video_id}.mp3")
        if os.path.exists(audio_path):
            logging.info(f"Audio file {audio_path} already exists. Skipping download.")
            return audio_path

        logging.info(f"Downloading audio for video ID: {video_id}")
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(self.download_dir, f"{video_id}.%(ext)s"),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'quiet': True,
            'no_warnings': True,
        }

        def download():
            with YoutubeDL(ydl_opts) as ydl:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                ydl.download([video_url])

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, download)

        if os.path.exists(audio_path):
            logging.info(f"Audio downloaded successfully for video ID {video_id}.")
            return audio_path
        else:
            logging.error(f"Audio file {audio_path} not found after download.")
            return None

    # -------------------------------
    # 分割音频
    # -------------------------------
    def split_audio(self, audio_path: str) -> List[AudioSegment]:
        """
        使用 pydub 将音频文件分割为固定时长的片段。
        """
        try:
            logging.info(f"Splitting audio {audio_path} into chunks of {self.max_duration_ms} ms.")
            audio = AudioSegment.from_file(audio_path)
            chunks = [audio[i:i + self.max_duration_ms] for i in range(0, len(audio), self.max_duration_ms)]
            logging.info(f"Audio split into {len(chunks)} chunks.")
            return chunks
        except Exception as e:
            logging.error(f"Failed to split audio {audio_path}: {e}")
            return []

    # -------------------------------
    # 转录音频块
    # -------------------------------
    @retry(max_retries=3, delay=5)
    async def transcribe_audio_chunk(self, audio_chunk: AudioSegment) -> Optional[str]:
        """
        使用 OpenAIService 的 Whisper 接口对单个音频块进行转录，返回转录文本。
        """
        try:
            audio_file = BytesIO()
            audio_chunk.export(audio_file, format="mp3")
            audio_file.seek(0)  # 重置文件指针

            logging.info("Transcribing audio chunk via OpenAIService's Whisper interface.")
            # 调用 OpenAIService 提供的异步语音转录方法
            transcript_text = await self.openai_service.transcribe_audio(audio_file)
            if transcript_text:
                logging.info("Transcription completed for audio chunk.")
                return transcript_text
            else:
                logging.error("Transcription returned empty result.")
                return None
        except Exception as e:
            logging.error(f"Failed to transcribe audio chunk via OpenAIService: {e}")
            return None

    # -------------------------------
    # 使用 OpenAI Service 进行文本总结
    # -------------------------------
    async def summarize_text(self, transcript_text: str, previous_summary: str, topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        使用 OpenAIService 对转录文本生成摘要。
        """
        try:
            prompt = self.openai_service.get_prompt("summarization", variables={
                "text": transcript_text,
                "previous_summary": previous_summary
            })
            # 修改为 await 调用异步方法
            response_text = await self.openai_service.async_completion(prompt=prompt)
            summary = response_text.strip()
            logging.info("Summary generated for transcript chunk.")
            return summary
        except Exception as e:
            logging.error(f"Failed to summarize text: {e}")
            return None

    # -------------------------------
    # 递归合并多个摘要
    # -------------------------------
    async def recursive_summarize(self, summaries: List[str], topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        递归合并多个摘要片段，直到仅剩下一个摘要。
        """
        try:
            while len(summaries) > 1:
                new_summaries = []
                for i in range(0, len(summaries), 2):
                    pair = summaries[i:i + 2]
                    combined = "\n\n".join(pair)
                    merged = await self.summarize_text(combined, previous_summary="", topic=topic, metadata=metadata)
                    if merged:
                        new_summaries.append(merged)
                summaries = new_summaries
            return summaries[0] if summaries else None
        except Exception as e:
            logging.error(f"Error during recursive summarization: {e}")
            return None

    # -------------------------------
    # 处理视频音频
    # -------------------------------
    async def process_video_audio(self, video_id: str, topic: str, metadata: Dict[str, Any], model: str = "gpt-4") -> Optional[Dict[str, Any]]:
        """
        综合处理单个视频音频，生成标准化摘要，流程：
          1. 下载音频
          2. 分割音频
          3. 对每个音频片段转录并生成摘要（支持上下文传递）
          4. 递归合并所有片段摘要
          5. 返回标准化摘要（字典格式或格式化 JSON 字符串）
        """
        audio_path = await self.download_audio(video_id)
        if not audio_path:
            logging.error(f"Audio download failed for video {video_id}.")
            return None
        audio_chunks = self.split_audio(audio_path)
        if not audio_chunks:
            logging.error(f"Audio splitting failed for video {video_id}.")
            return None

        chunk_summaries = []
        previous_summary = ""
        for idx, chunk in enumerate(audio_chunks):
            logging.info(f"Processing audio chunk {idx + 1}/{len(audio_chunks)} for video {video_id}.")
            transcript = await self.transcribe_audio_chunk(chunk)
            if not transcript:
                logging.error(f"Transcription failed for chunk {idx + 1}.")
                continue
            summary = await self.summarize_text(transcript, previous_summary, topic, metadata)
            if summary:
                chunk_summaries.append(summary)
                previous_summary = summary
            else:
                logging.error(f"Summarization failed for chunk {idx + 1}.")
            await asyncio.sleep(random.uniform(0.5, 2))

        if not chunk_summaries:
            logging.error(f"No summaries generated for video {video_id}.")
            return None

        final_summary = await self.recursive_summarize(chunk_summaries, topic, metadata)
        if not final_summary:
            logging.error(f"Recursive summary generation failed for video {video_id}.")
            return None

        return final_summary
