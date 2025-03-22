import os
import logging
import asyncio
import random
from io import BytesIO
from typing import Any, Dict, List, Optional

from pydub import AudioSegment
from utils.helper import retry  # 假设 retry 为同步重试装饰器
from utils.youtube import YouTubeService, get_youtube_service
from utils.openAIServices import OpenAIService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AudioProcessingAgent:
    """
    AudioProcessingAgent 负责处理 YouTube 视频的音频，并生成标准化摘要。
    
    流程：
      1. 使用 YouTubeService 下载视频音频文件（异步包装调用同步方法）。
      2. 使用 pydub 分割音频文件成固定时长的片段。
      3. 对每个音频片段调用 OpenAIService 的 Whisper 接口进行转录，并生成摘要。
         - 每个片段生成摘要后，可记录到数据库中。
      4. 对所有片段摘要进行递归合并，得到最终摘要。
      5. 返回最终摘要（通常为文本或 JSON 格式）。
      
    本类支持通过构造函数传入 db 对象，将处理过程中的中间结果及最终结果记录到数据库中，
    以便后续数据审计和流程监控。
    """
    DEFAULT_MAX_TOKENS = 3000
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self, openai_service: OpenAIService, youtube_service: Optional[YouTubeService] = None,
                 download_dir: str = "downloads", max_duration_ms: int = 60000, debug_mode: bool = False,
                 db: Optional[Any] = None):
        """
        :param openai_service: 提供转录与摘要能力的 OpenAIService 实例。
        :param youtube_service: 用于下载视频音频的 YouTubeService 实例；若未传入，则通过 get_youtube_service 创建默认实例。
        :param download_dir: 下载音频存放目录。
        :param max_duration_ms: 音频分割时每段的最大时长（毫秒）。
        :param debug_mode: 是否开启详细调试日志。
        :param db: 可选数据库对象，用于记录音频处理日志和摘要结果。
        """
        self.openai_service = openai_service
        self.download_dir = download_dir
        self.max_duration_ms = max_duration_ms
        self.debug_mode = debug_mode
        self.db = db
        os.makedirs(self.download_dir, exist_ok=True)
        if youtube_service is None:
            # 此处使用默认 API key "default_key"，实际请替换为正确的配置或从环境变量中读取
            self.youtube_service = get_youtube_service("default_key")
        else:
            self.youtube_service = youtube_service

    async def download_audio(self, video_id: str) -> Optional[str]:
        """
        使用 YouTubeService 下载视频音频文件，返回文件路径。
        异步包装调用同步方法。
        """
        loop = asyncio.get_running_loop()
        audio_path = await loop.run_in_executor(None, self.youtube_service.download_audio, video_id)
        if audio_path:
            logger.info(f"Audio file downloaded: {audio_path}")
            # 如果有数据库对象，记录下载日志
            if self.db:
                try:
                    from datetime import datetime
                    record = {
                        "process": "download_audio",
                        "video_id": video_id,
                        "audio_path": audio_path,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    self.db.store_data("audio_processing_logs", record)
                    logger.info(f"Download record stored for video {video_id}.")
                except Exception as e:
                    logger.error(f"Failed to store download record for video {video_id}: {e}")
        else:
            logger.error(f"Failed to download audio for video {video_id}")
        return audio_path

    def split_audio(self, audio_path: str) -> List[AudioSegment]:
        """
        使用 pydub 将音频文件分割为固定时长的片段。
        """
        try:
            logger.info(f"Splitting audio {audio_path} into chunks of {self.max_duration_ms} ms.")
            audio = AudioSegment.from_file(audio_path)
            chunks = [audio[i:i + self.max_duration_ms] for i in range(0, len(audio), self.max_duration_ms)]
            logger.info(f"Audio split into {len(chunks)} chunks.")
            return chunks
        except Exception as e:
            logger.error(f"Failed to split audio {audio_path}: {e}")
            return []

    @retry(max_retries=3, delay=5)
    async def transcribe_audio_chunk(self, audio_chunk: AudioSegment) -> Optional[str]:
        """
        使用 OpenAIService 的 Whisper 接口对单个音频块进行转录，返回转录文本。
        """
        try:
            audio_file = BytesIO()
            audio_chunk.export(audio_file, format="mp3")
            audio_file.seek(0)  # 重置文件指针

            logger.info("Transcribing audio chunk via OpenAIService's Whisper interface.")
            transcript_text = await self.openai_service.transcribe_audio(audio_file)
            if transcript_text:
                logger.info("Transcription completed for audio chunk.")
                return transcript_text
            else:
                logger.error("Transcription returned empty result.")
                return None
        except Exception as e:
            logger.error(f"Failed to transcribe audio chunk: {e}")
            return None

    async def summarize_text(self, transcript_text: str, previous_summary: str, topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        使用 OpenAIService 生成文本摘要。
        """
        try:
            prompt = self.openai_service.get_prompt("summarization", variables={
                "text": transcript_text,
                "previous_summary": previous_summary,
                "topic": topic
            })
            response_text = await self.openai_service.async_completion(prompt=prompt)
            summary = response_text.strip()
            logger.info("Summary generated for transcript chunk.")
            return summary
        except Exception as e:
            logger.error(f"Failed to summarize text: {e}")
            return None

    async def recursive_summarize(self, summaries: List[str], topic: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        递归合并多个摘要片段，直到只剩下一个摘要。
        """
        try:
            while len(summaries) > 1:
                new_summaries = []
                for i in range(0, len(summaries), 2):
                    pair = summaries[i:i+2]
                    combined = "\n\n".join(pair)
                    merged = await self.summarize_text(combined, previous_summary="", topic=topic, metadata=metadata)
                    if merged:
                        new_summaries.append(merged)
                summaries = new_summaries
            return summaries[0] if summaries else None
        except Exception as e:
            logger.error(f"Error during recursive summarization: {e}")
            return None

    async def process_video_audio(self, video_id: str, topic: str, metadata: Dict[str, Any], model: str = "gpt-4") -> Optional[Any]:
        """
        处理单个视频的音频，流程：
          1. 下载音频。
          2. 分割音频。
          3. 对每个音频块进行转录，并生成摘要（支持上下文传递，同时记录每个片段结果到数据库）。
          4. 递归合并所有片段摘要，得到最终摘要。
          5. 返回最终摘要（可能为 JSON 格式或纯文本）。
        """
        audio_path = await self.download_audio(video_id)
        if not audio_path:
            logger.error(f"Audio download failed for video {video_id}.")
            return None
        audio_chunks = self.split_audio(audio_path)
        if not audio_chunks:
            logger.error(f"Audio splitting failed for video {video_id}.")
            return None

        chunk_summaries = []
        previous_summary = ""
        for idx, chunk in enumerate(audio_chunks):
            logger.info(f"Processing audio chunk {idx + 1}/{len(audio_chunks)} for video {video_id}.")
            transcript = await self.transcribe_audio_chunk(chunk)
            if not transcript:
                logger.error(f"Transcription failed for chunk {idx + 1}.")
                continue
            summary = await self.summarize_text(transcript, previous_summary, topic, metadata)
            if summary:
                chunk_summaries.append(summary)
                previous_summary = summary
                if self.db:
                    try:
                        from datetime import datetime
                        record = {
                            "process": "audio_chunk_summary",
                            "video_id": video_id,
                            "chunk_index": idx + 1,
                            "summary": summary,
                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        self.db.store_data("audio_processing_logs", record)
                        logger.info(f"Audio chunk {idx + 1} summary recorded in database.")
                    except Exception as db_e:
                        logger.error(f"Failed to record chunk {idx + 1} summary: {db_e}")
            else:
                logger.error(f"Summarization failed for chunk {idx + 1}.")
            await asyncio.sleep(random.uniform(0.5, 2))
        
        if not chunk_summaries:
            logger.error(f"No summaries generated for video {video_id}.")
            return None
        
        final_summary = await self.recursive_summarize(chunk_summaries, topic, metadata)
        if not final_summary:
            logger.error(f"Recursive summary generation failed for video {video_id}.")
            return None

        if self.db:
            try:
                from datetime import datetime
                final_record = {
                    "process": "final_audio_summary",
                    "video_id": video_id,
                    "summary": final_summary,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                self.db.store_data("audio_processing_logs", final_record)
                logger.info(f"Final audio summary for video {video_id} recorded in database.")
            except Exception as db_e:
                logger.error(f"Failed to record final audio summary for video {video_id}: {db_e}")
        return final_summary


def get_audio_processing_agent(openai_service: OpenAIService, youtube_service: Optional[YouTubeService] = None,
                               download_dir: str = "downloads", max_duration_ms: int = 60000, debug_mode: bool = False,
                               db: Optional[Any] = None) -> AudioProcessingAgent:
    """
    工厂函数，返回 AudioProcessingAgent 实例，确保所有依赖统一注入。
    
    :param openai_service: 已初始化的 OpenAIService 实例。
    :param youtube_service: 可选的 YouTubeService 实例。
    :param download_dir: 音频下载目录。
    :param max_duration_ms: 每段音频的最大时长（毫秒）。
    :param debug_mode: 是否开启详细调试日志。
    :param db: 数据库对象，用于记录流程日志和摘要结果。
    :return: AudioProcessingAgent 实例。
    """
    return AudioProcessingAgent(
        openai_service=openai_service,
        youtube_service=youtube_service,
        download_dir=download_dir,
        max_duration_ms=max_duration_ms,
        debug_mode=debug_mode,
        db=db
    )
