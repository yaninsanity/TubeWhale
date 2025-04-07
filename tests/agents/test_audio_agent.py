#!/usr/bin/env python3
import os
import tempfile
import asyncio
import shutil
from io import BytesIO

import pytest
import pytest_asyncio
from pydub import AudioSegment

# 假设被测试代码保存在 agents/audio_agent.py 中
from agents.audio_agent import (
    validate_video_id,
    AudioChunkProcessor,
    AudioProcessingAgent,
    get_audio_processing_agent,
)

# Dummy 实现，用于模拟 OpenAIService 的行为
class DummyOpenAIService:
    async def transcribe_audio(self, audio_buffer):
        return "dummy transcript"

    async def async_completion(self, prompt):
        return "dummy summary"

    def get_prompt(self, prompt_type, variables):
        return f"Prompt for {prompt_type} with {variables}"

# Dummy 实现，用于模拟 YouTubeService 的行为
class DummyYouTubeService:
    def download_audio(self, video_id):
        dummy_audio_path = os.path.join(tempfile.gettempdir(), f"{video_id}.mp3")
        # 使用 pydub 生成一段 3 秒的静音音频并导出为 mp3 文件
        silent_audio = AudioSegment.silent(duration=3000)
        silent_audio.export(dummy_audio_path, format="mp3")
        return dummy_audio_path

# Dummy 数据库对象
class DummyDB:
    def __init__(self):
        self.records = []
    def store_data(self, table, record):
        self.records.append((table, record))
    async def store_data_async(self, table, record):
        self.records.append((table, record))

# 测试 validate_video_id 装饰器，注意需要模拟类方法（包含 self 参数）
@pytest.mark.asyncio
async def test_validate_video_id():
    class DummyClass:
        @validate_video_id
        async def dummy_func(self, video_id: str):
            return video_id

    dummy_instance = DummyClass()
    valid_video_id = "a1b2c3d4e5f"  # 11 个字母数字字符
    result = await dummy_instance.dummy_func(valid_video_id)
    assert result == valid_video_id

    # 测试视频ID不合法：长度不足
    with pytest.raises(ValueError):
        await dummy_instance.dummy_func("shortid")
    # 测试视频ID不合法：包含非字母数字字符
    with pytest.raises(ValueError):
        await dummy_instance.dummy_func("a1b2c3d4e5*")

# 测试 AudioChunkProcessor.process_chunk 方法
@pytest.mark.asyncio
async def test_audio_chunk_processor_process_chunk():
    dummy_openai = DummyOpenAIService()
    processor = AudioChunkProcessor(dummy_openai)
    # 创建 1 秒的静音音频块
    chunk = AudioSegment.silent(duration=1000)
    result = await processor.process_chunk(chunk, 0, "prev summary", "test topic")
    # 预期返回 (块索引, "dummy summary")
    assert result is not None
    idx, summary = result
    assert idx == 0
    assert summary == "dummy summary"

# 使用 pytest fixture 创建一个 AudioProcessingAgent 实例，注入 dummy 依赖
@pytest.fixture
def agent(tmp_path):
    download_dir = tmp_path / "downloads"
    temp_dir = tmp_path / "temp_audio"
    download_dir.mkdir()
    temp_dir.mkdir()
    dummy_openai = DummyOpenAIService()
    dummy_youtube = DummyYouTubeService()
    dummy_db = DummyDB()
    agent_instance = AudioProcessingAgent(
        openai_service=dummy_openai,
        youtube_service=dummy_youtube,
        download_dir=str(download_dir),
        max_duration_ms=1000,  # 设置每段 1 秒
        debug_mode=True,
        db=dummy_db,
        temp_dir=str(temp_dir)
    )
    # 由于 process_video_audio 中用到了 self.max_concurrency，
    # 这里手动设置一个合适的值用于测试
    agent_instance.max_concurrency = 2
    return agent_instance

# 测试 download_audio 方法
@pytest.mark.asyncio
async def test_download_audio(agent):
    video_id = "a1b2c3d4e5f"
    audio_path = await agent.download_audio(video_id)
    assert audio_path is not None
    assert os.path.exists(audio_path)

# 测试 split_audio 方法
def test_split_audio(agent):
    video_id = "a1b2c3d4e5f"
    audio_path = os.path.join(agent.download_dir, f"{video_id}.mp3")
    # 生成一段 3 秒静音音频写入文件
    silent_audio = AudioSegment.silent(duration=3000)
    silent_audio.export(audio_path, format="mp3")
    chunks = agent.split_audio(audio_path)
    # 每段 1 秒，预期分成 3 段
    assert len(chunks) == 3

# 测试 transcribe_audio_chunk 方法
@pytest.mark.asyncio
async def test_transcribe_audio_chunk(agent):
    chunk = AudioSegment.silent(duration=1000)
    transcript = await agent.transcribe_audio_chunk(chunk)
    assert transcript == "dummy transcript"

# 测试 recursive_summarize 方法
@pytest.mark.asyncio
async def test_recursive_summarize(agent):
    summaries = ["summary1", "summary2", "summary3", "summary4"]
    topic = "test topic"
    metadata = {}
    final_summary = await agent.recursive_summarize(summaries, topic, metadata)
    # DummyOpenAIService 总是返回 "dummy summary"
    assert final_summary == "dummy summary"

# 测试 process_video_audio 方法（整体流程测试）
@pytest.mark.asyncio
async def test_process_video_audio(agent):
    video_id = "a1b2c3d4e5f"
    topic = "test topic"
    metadata = {}
    final_summary = await agent.process_video_audio(video_id, topic, metadata)
    assert final_summary == "dummy summary"
    # 检查是否有记录写入 dummy 数据库
    assert len(agent.db.records) > 0

# 测试 transcribe_video 方法
@pytest.mark.asyncio
async def test_transcribe_video(agent):
    video_id = "a1b2c3d4e5f"
    transcript = await agent.transcribe_video(video_id)
    assert transcript == "dummy transcript"

# 测试 _cleanup_temp_files 方法，确保临时文件被清除
def test_cleanup_temp_files(agent):
    video_id = "a1b2c3d4e5f"
    temp_file = os.path.join(agent.temp_dir, f"{video_id}.mp3")
    download_file = os.path.join(agent.download_dir, f"{video_id}.mp3")
    # 创建模拟临时文件
    with open(temp_file, "w") as f:
        f.write("dummy")
    with open(download_file, "w") as f:
        f.write("dummy")
    assert os.path.exists(temp_file)
    assert os.path.exists(download_file)
    # 调用清理方法
    agent._cleanup_temp_files(video_id)
    assert not os.path.exists(temp_file)
    assert not os.path.exists(download_file)
