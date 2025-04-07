#!/usr/bin/env python3
"""
Audio Processing Agent

本模块通过以下步骤处理 YouTube 视频音频：
1. 如果启用了字幕 API 且传入了 TranscriptAgent，则首先调用 TranscriptAgent.fetch_transcript 获取视频字幕；
2. 如果字幕获取失败，则调用 YouTubeService.download_audio 下载视频音频（采用 pytube + ffmpeg 提取 mp3）；
3. 使用 pydub 将下载的音频文件分割为固定时长的片段；
4. 并发调用 AudioChunkProcessor 对每个音频块进行转录和摘要生成（调用 OpenAIService.transcribe_audio 与 async_completion）；
5. 递归合并所有片段摘要生成最终摘要；
6. 若配置了数据库，则记录处理日志到数据库中。

整个流程中增加了详细日志记录和错误重试（self-healing）机制，确保在出现异常时自动回退到备用方案，从而降低系统风险。
"""

import os
import logging
import asyncio
import traceback
from datetime import datetime
from functools import wraps
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

from pydub import AudioSegment
from utils.helper import retry
from utils.youtube import YouTubeService  # 请确保此模块中包含下载音频等实现
from utils.openAIServices import OpenAIService  # 使用统一封装好的 OpenAIService

# 如果外部没有传入 logger，则使用模块级默认 logger
DEFAULT_LOGGER = logging.getLogger("AudioProcessingAgent")
DEFAULT_LOGGER.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
if not DEFAULT_LOGGER.handlers:
    DEFAULT_LOGGER.addHandler(_handler)


def validate_video_id(func):
    """视频ID校验装饰器，确保传入的 YouTube 视频 ID 合法"""
    @wraps(func)
    async def wrapper(self, video_id: str, *args, **kwargs):
        if not (len(video_id) == 11 and video_id.isalnum()):
            raise ValueError(f"Invalid YouTube video ID: {video_id}")
        return await func(self, video_id, *args, **kwargs)
    return wrapper


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
    尝试获取视频字幕。
    使用 youtube_transcript_api.get_transcript 通过 asyncio.to_thread 调用，
    如果获取到的字幕为空或出错，则抛出异常以触发重试。
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        transcript_entries = await asyncio.to_thread(YouTubeTranscriptApi.get_transcript, video_id, ["en"])
        transcript = " ".join([str(entry.get('text', '')) for entry in transcript_entries])
        if not transcript or not transcript.strip():
            raise Exception("Transcript is empty.")
        DEFAULT_LOGGER.info(f"[fetch_transcript] Fetched transcript for video {video_id}: {transcript[:50]}...")
        return transcript
    except Exception as e:
        DEFAULT_LOGGER.error(f"[fetch_transcript] Error fetching transcript for video {video_id}: {e}", exc_info=True)
        raise Exception(f"Transcript not found for video {video_id}") from e


class AudioChunkProcessor:
    """
    辅助处理单个音频块：
      - 使用 OpenAIService.transcribe_audio 转录音频；
      - 根据转录结果调用 OpenAIService.async_completion 生成摘要。
    """
    def __init__(self, openai_service: OpenAIService, logger: logging.Logger):
        self.openai_service = openai_service
        self.logger = logger

    @retry(max_retries=3, delay=2)
    async def process_chunk(self, chunk: AudioSegment, chunk_idx: int,
                              previous_summary: str, topic: str) -> Optional[Tuple[int, str]]:
        try:
            self.logger.info(f"[Chunk {chunk_idx}] Starting transcription.")
            transcript = await self._transcribe_chunk(chunk)
            if not transcript:
                self.logger.error(f"[Chunk {chunk_idx}] Transcription returned empty result.")
                return None
            self.logger.info(f"[Chunk {chunk_idx}] Transcription succeeded; starting summary generation.")
            summary = await self._generate_summary(transcript, previous_summary, topic)
            self.logger.info(f"[Chunk {chunk_idx}] Summary generated.")
            return (chunk_idx, summary)
        except Exception as e:
            self.logger.error(f"[Chunk {chunk_idx}] Processing failed: {e}", exc_info=True)
            return None

    async def _transcribe_chunk(self, chunk: AudioSegment) -> Optional[str]:
        with BytesIO() as buffer:
            chunk.export(buffer, format="mp3")
            buffer.seek(0)
            return await self.openai_service.transcribe_audio(buffer)

    async def _generate_summary(self, transcript: str, previous_summary: str,
                                  topic: str) -> Optional[str]:
        try:
            prompt = self.openai_service.get_prompt("summarization", variables={
                "text": transcript,
                "previous_summary": previous_summary,
                "topic": topic
            })
        except Exception as e:
            self.logger.error(f"Error obtaining summarization prompt: {e}", exc_info=True)
            prompt = (f"Summarize the following text for topic '{topic}':\n"
                      f"Previous Summary: {previous_summary}\nText: {transcript}")
        response = await self.openai_service.async_completion(prompt=prompt)
        result = response.strip() if response else None
        if not result:
            self.logger.error("Generated summary is empty.")
        return result


class AudioProcessingAgent:
    """
    AudioProcessingAgent 负责处理 YouTube 视频音频并生成结构化摘要。
    流程：
      1. 如果启用了字幕 API 且传入了 TranscriptAgent，则调用其 fetch_transcript 获取字幕；
      2. 如果字幕不可用，则调用 YouTubeService.download_audio 下载视频音频；
      3. 分割音频为固定时长片段；
      4. 并发调用 AudioChunkProcessor 对每个音频块进行转录和摘要生成；
      5. 递归合并所有片段摘要生成最终摘要；
      6. 若配置了数据库，则记录处理日志到数据库中。
    """
    DEFAULT_MAX_TOKENS = 3000
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self,
                 openai_service: OpenAIService,
                 youtube_service: Optional[Any] = None,
                 download_dir: str = "downloads",
                 max_duration_ms: int = 60000,
                 max_concurrency: int = 3,
                 debug_mode: bool = False,
                 db: Optional[Any] = None,
                 temp_dir: str = "temp_audio",
                 use_transcript_api: bool = True,
                 transcript_agent: Optional[Any] = None,
                 logger: Optional[logging.Logger] = None):
        """
        :param transcript_agent: 可选的 TranscriptAgent 实例，用于优先获取字幕。
        :param logger: 外部传入的 logger 对象，用于统一日志记录。
        """
        self.openai_service = openai_service
        self.download_dir = download_dir
        self.max_duration_ms = max_duration_ms
        self.max_concurrency = max_concurrency
        self.debug_mode = debug_mode
        self.db = db
        self.temp_dir = temp_dir
        self.use_transcript_api = use_transcript_api
        self.transcript_agent = transcript_agent
        self.logger = logger if logger else DEFAULT_LOGGER

        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

        # 注意：确保 YouTubeService 实例中包含下载音频等实现
        self.youtube_service = youtube_service or YouTubeService(api_keys=["dummy_key"])
        self.chunk_processor = AudioChunkProcessor(openai_service, self.logger)

    async def get_transcript(self, video_id: str) -> Optional[str]:
        """
        尝试获取视频字幕。
        如果传入了 TranscriptAgent，则优先调用其 fetch_transcript 方法；否则直接返回 None。
        """
        if self.use_transcript_api and self.transcript_agent:
            try:
                # 调用 TranscriptAgent.fetch_transcript（此方法内部已使用自身的 youtube_service）
                transcript = await self.transcript_agent.fetch_transcript(video_id)
                if transcript and transcript.strip():
                    self.logger.info(f"[AudioProcessingAgent] Obtained transcript via TranscriptAgent for video {video_id}.")
                    return transcript
            except Exception as e:
                self.logger.warning(f"[AudioProcessingAgent] TranscriptAgent fetch failed for video {video_id}: {e}", exc_info=True)
        return None

    async def download_audio(self, video_id: str) -> Optional[str]:
        """
        异步下载视频音频文件（调用 YouTubeService.download_audio），返回音频文件路径。
        """
        self.logger.info(f"[Video {video_id}] Step 1: Downloading audio.")
        loop = asyncio.get_running_loop()
        audio_path = await loop.run_in_executor(None, self.youtube_service.download_audio, video_id)
        if audio_path:
            self.logger.info(f"[Video {video_id}] Audio file downloaded: {audio_path}")
            if self.db:
                try:
                    record = {
                        "process": "download_audio",
                        "video_id": video_id,
                        "audio_path": audio_path,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    await self.db.store_data("audio_processing_logs", record)
                    self.logger.info(f"[Video {video_id}] Download record stored in database.")
                except Exception as e:
                    self.logger.error(f"[Video {video_id}] Failed to store download record: {e}", exc_info=True)
        else:
            self.logger.error(f"[Video {video_id}] Audio download failed.")
        return audio_path

    def split_audio(self, audio_path: str) -> List[AudioSegment]:
        """
        使用 pydub 将音频文件分割为固定时长的片段。
        """
        try:
            self.logger.info(f"[Audio {audio_path}] Step 2: Splitting audio into {self.max_duration_ms} ms chunks.")
            audio = AudioSegment.from_file(audio_path)
            chunks = [audio[i:i + self.max_duration_ms] for i in range(0, len(audio), self.max_duration_ms)]
            self.logger.info(f"[Audio {audio_path}] Audio split into {len(chunks)} chunks.")
            return chunks
        except Exception as e:
            self.logger.error(f"[Audio {audio_path}] Failed to split audio: {e}", exc_info=True)
            return []

    @retry(max_retries=3, delay=5)
    async def transcribe_audio_chunk(self, audio_chunk: AudioSegment) -> Optional[str]:
        """
        使用 OpenAIService 转录单个音频块，返回转录文本。
        """
        try:
            with BytesIO() as audio_file:
                audio_chunk.export(audio_file, format="mp3")
                audio_file.seek(0)
                self.logger.info("Step 3: Transcribing audio chunk via OpenAIService's Whisper interface.")
                transcript_text = await self.openai_service.transcribe_audio(audio_file)
                if transcript_text:
                    self.logger.info("Audio chunk transcription succeeded.")
                    return transcript_text
                else:
                    self.logger.error("Transcription returned empty result.")
                    return None
        except Exception as e:
            self.logger.error(f"Failed to transcribe audio chunk: {e}", exc_info=True)
            return None

    async def summarize_text(self, transcript_text: str, previous_summary: str,
                             topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        根据转录文本生成摘要，调用 OpenAIService 异步接口。
        """
        try:
            self.logger.info("Step 4: Generating summary for transcript chunk.")
            prompt = self.openai_service.get_prompt("summarization", variables={
                "text": transcript_text,
                "previous_summary": previous_summary,
                "topic": topic
            })
            response_text = await self.openai_service.async_completion(prompt=prompt)
            summary = response_text.strip()
            if summary:
                self.logger.info("Summary generated for transcript chunk.")
            else:
                self.logger.error("Summary generation returned empty result.")
            return summary
        except Exception as e:
            self.logger.error(f"Failed to summarize text: {e}", exc_info=True)
            return None

    async def recursive_summarize(self, summaries: List[str],
                                  topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        递归合并多个摘要片段，直到合并成一个最终摘要。
        """
        try:
            self.logger.info("Step 5: Starting recursive summarization.")
            while len(summaries) > 1:
                new_summaries = []
                for i in range(0, len(summaries), 2):
                    pair = summaries[i:i+2]
                    combined = "\n\n".join(pair)
                    merged = await self.summarize_text(combined, previous_summary="", topic=topic, metadata=metadata)
                    if merged:
                        new_summaries.append(merged)
                    else:
                        self.logger.error(f"Recursive summarization failed for summary pair starting at index {i}.")
                summaries = new_summaries
                self.logger.info(f"Recursive summarization pass complete; {len(summaries)} summaries remain.")
            if summaries:
                self.logger.info("Recursive summarization completed successfully.")
                return summaries[0]
            else:
                self.logger.error("No summaries available after recursive summarization.")
                return None
        except Exception as e:
            self.logger.error(f"Error during recursive summarization: {e}", exc_info=True)
            return None

    async def process_video_audio(self, video_id: str, topic: str,
                                  metadata: Dict[str, Any], model: str = "gpt-4") -> Optional[str]:
        """
        完整流程：
          1. 如果启用了字幕 API，则尝试使用 get_transcript 获取字幕；
          2. 如果字幕不可用，则下载视频音频；
          3. 分割音频为若干片段；
          4. 并发调用 AudioChunkProcessor 进行音频块转录与摘要生成；
          5. 递归合并所有片段摘要生成最终摘要；
          6. 若配置了数据库，则记录处理日志。
        """
        start_time = datetime.now()
        self.logger.info(f"[Video {video_id}] Starting audio processing pipeline with topic '{topic}'.")
        try:
            # Step 0: 尝试获取字幕（优先调用 TranscriptAgent，如果可用）
            transcript = await self.get_transcript(video_id)
            if transcript and transcript.strip():
                self.logger.info(f"[Video {video_id}] Using transcript obtained from TranscriptAgent.")
            else:
                # Step 1: 下载音频
                transcript = None
                audio_path = await self.download_audio(video_id)
                if not audio_path:
                    self.logger.error(f"[Video {video_id}] Step 1 failed: Audio download failed.")
                    return None
                self.logger.info(f"[Video {video_id}] Audio downloaded: {audio_path}")

                # Step 2: 分割音频
                audio_chunks = self.split_audio(audio_path)
                if not audio_chunks:
                    self.logger.error(f"[Video {video_id}] Step 2 failed: Audio splitting failed.")
                    return None

                # Step 3: 并发处理各音频块
                self.logger.info(f"[Video {video_id}] Step 3: Processing {len(audio_chunks)} audio chunks concurrently.")
                semaphore = asyncio.Semaphore(self.max_concurrency)
                summaries = []
                previous_summary = ""
                async def process_chunk(idx, chunk):
                    async with semaphore:
                        t = await self.transcribe_audio_chunk(chunk)
                        if not t:
                            self.logger.error(f"[Video {video_id}][Chunk {idx}] Transcription failed.")
                            return None
                        s = await self.summarize_text(t, previous_summary, topic, metadata)
                        if not s:
                            self.logger.error(f"[Video {video_id}][Chunk {idx}] Summary generation failed.")
                            return None
                        return (idx, s)
                tasks = [process_chunk(idx, chunk) for idx, chunk in enumerate(audio_chunks)]
                results = await asyncio.gather(*tasks)
                for res in sorted([r for r in results if r is not None], key=lambda x: x[0]):
                    summaries.append(res[1])
                    previous_summary = res[1]
                if not summaries:
                    self.logger.error(f"[Video {video_id}] Step 3 failed: No valid summaries generated.")
                    return None

                # 将各块摘要拼接作为 transcript（可根据需要调整拼接逻辑）
                transcript = "\n".join(summaries)

            # Step 4: 递归合并摘要
            final_summary = await self.recursive_summarize([transcript], topic, metadata)
            if not final_summary:
                self.logger.error(f"[Video {video_id}] Step 4 failed: Recursive summarization failed.")
                return None

            # Step 5: 存储数据库日志（如果配置了数据库）
            if self.db:
                record = {
                    "process": "final_audio_summary",
                    "video_id": video_id,
                    "summary": final_summary,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "duration": (datetime.now() - start_time).total_seconds(),
                    "chunk_count": len(audio_chunks) if transcript is None else 0,
                    "summary_length": len(final_summary)
                }
                try:
                    await self.db.store_data("audio_processing_logs", record)
                    self.logger.info(f"[Video {video_id}] Step 5: Final audio summary recorded in database.")
                except Exception as e:
                    self.logger.error(f"[Video {video_id}] Step 5 failed: Failed to record final summary: {e}", exc_info=True)
            self.logger.info(f"[Video {video_id}] Processing completed in {(datetime.now() - start_time).total_seconds()} seconds.")
            return final_summary
        except Exception as e:
            self.logger.error(f"[Video {video_id}] Processing failed: {e}", exc_info=True)
            return None
        finally:
            self._cleanup_temp_files(video_id)

    async def extract_audio(self, video_id: str) -> BytesIO:
        """
        尝试从缓存中获取音频内容；如不存在则下载，返回 BytesIO 对象。
        """
        return await self._get_audio_content(video_id)

    async def _get_audio_content(self, video_id: str) -> BytesIO:
        cache_path = os.path.join(self.download_dir, f"{video_id}.mp3")
        if os.path.exists(cache_path):
            with open(cache_path, "rb") as f:
                return BytesIO(f.read())
        audio_path = await self.download_audio(video_id)
        if audio_path and os.path.exists(audio_path):
            with open(audio_path, "rb") as f:
                audio_bytes = BytesIO(f.read())
            with open(cache_path, "wb") as f:
                f.write(audio_bytes.getbuffer())
            return audio_bytes
        raise ValueError(f"[Video {video_id}] Failed to obtain audio content.")

    @validate_video_id
    async def transcribe_video(self, video_id: str) -> str:
        """
        提取音频后，使用 OpenAIService 对整个视频进行转录。
        """
        try:
            audio_bytes = await self.extract_audio(video_id)
            transcript = await self.openai_service.transcribe_audio(audio_bytes)
            return transcript or ""
        except Exception as e:
            self.logger.error(f"[Video {video_id}] Transcription failed: {e}", exc_info=True)
            return ""

    def _cleanup_temp_files(self, video_id: str):
        """
        清理临时及缓存文件，防止占用过多磁盘空间。
        """
        temp_files = [
            os.path.join(self.temp_dir, f"{video_id}.mp3"),
            os.path.join(self.download_dir, f"{video_id}.mp3")
        ]
        for f in temp_files:
            try:
                if os.path.exists(f):
                    os.remove(f)
                    self.logger.debug(f"[Video {video_id}] Cleaned up temp file: {f}")
            except Exception as e:
                self.logger.warning(f"[Video {video_id}] Failed to clean up {f}: {e}", exc_info=True)


def get_audio_processing_agent(openai_service: OpenAIService, **kwargs) -> AudioProcessingAgent:
    """
    工厂函数，返回 AudioProcessingAgent 实例，确保所有依赖统一注入。
    """
    return AudioProcessingAgent(openai_service=openai_service, **kwargs)


# ---------------------- 以下为简单测试代码 ----------------------
if __name__ == "__main__":
    import asyncio
    from utils.database import Database
    from async_database import AsyncDatabase  # 假设 AsyncDatabase 定义在 async_database.py
    from agents.transcript_agent import TranscriptAgent  # 请确保 TranscriptAgent 实现正确

    # 示例配置（请确保环境变量和 API Key 正确）
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "sk-your_key")
    YOUTUBE_API_KEYS = ["your_youtube_api_key1", "your_youtube_api_key2"]
    DB_PATH = "videos.db"

    # 使用同步 Database 包装成 AsyncDatabase
    sync_db = Database(DB_PATH, DEFAULT_LOGGER)
    async_db = AsyncDatabase(sync_db)

    openai_service = OpenAIService(api_key=OPENAI_API_KEY)
    youtube_service = YouTubeService(api_keys=YOUTUBE_API_KEYS)

    # 初始化 TranscriptAgent，并传入 AudioProcessingAgent 中
    transcript_agent = TranscriptAgent(openai_service, youtube_service, logger=None)

    agent = get_audio_processing_agent(
        openai_service,
        youtube_service=youtube_service,
        db=async_db,
        debug_mode=True,
        transcript_agent=transcript_agent,
        logger=DEFAULT_LOGGER  # 传入外部统一 logger
    )
    test_video_id = "dQw4w9WgXcQ"  # 请替换为合法视频ID
    test_topic = "Fraud and Risk Solutions"
    metadata = {}  # 根据需要传入视频相关的元数据

    final_summary = asyncio.run(agent.process_video_audio(test_video_id, test_topic, metadata, model="gpt-4o-mini"))
    if final_summary:
        DEFAULT_LOGGER.info(f"Final summary for video {test_video_id}:\n{final_summary}")
    else:
        DEFAULT_LOGGER.error("Processing failed.")
    asyncio.run(async_db.close())
