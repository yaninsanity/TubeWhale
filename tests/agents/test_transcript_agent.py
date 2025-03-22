# tests/test_agent.py

import asyncio
import pytest

# 模拟数据库实现：记录调用 store_transcript_summary 后保存的数据
class DummyDatabase:
    def __init__(self):
        self.storage = {}
    def store_transcript_summary(self, video_id, transcript, summary):
        self.storage[video_id] = {"transcript": transcript, "summary": summary}

# 模拟 OpenAIService 实现：async_completion 返回固定摘要
class DummyOpenAIService:
    async def async_completion(self, prompt, prompt_template, temperature, max_tokens):
        # 返回一个固定格式的摘要，方便测试验证
        return f"Dummy summary for: {prompt[:10]}"

# 模拟 YouTubeService 实现
class DummyYouTubeService:
    # 当视频 ID 为 "video_with_transcript" 时，直接返回字幕
    def fetch_transcript(self, video_id):
        if video_id == "video_with_transcript":
            return "Dummy transcript"
        return None
    # 模拟下载音频，直接返回一个伪造的文件路径
    def download_audio(self, video_id):
        return f"/dummy/path/{video_id}.mp3"
    # 模拟音频转录，返回一个固定的转录文本
    def transcribe_audio(self, audio_path):
        return "Dummy transcript from audio"

# 如果你的 Agent 类已经在项目中定义（比如在 agent.py 中），你可以直接导入：
# from agent import Agent
# 这里为了示例，我们将 process_video_transcript 与 interpret_transcript 封装在一个简单的 Agent 类中：
class Agent:
    def __init__(self, db, openai_service, youtube_service):
        self.db = db
        self.openai_service = openai_service
        self.youtube_service = youtube_service

    async def interpret_transcript(self, transcript: str, topic: str) -> str:
        try:
            summary = await self.openai_service.async_completion(
                prompt=transcript,
                prompt_template=None,
                temperature=0.5,
                max_tokens=1024
            )
            summary = summary.strip() if summary else None
            return summary
        except Exception as e:
            return None

    async def process_video_transcript(self, video_id: str, topic: str) -> str:
        try:
            # Step 1: 尝试获取字幕（同步方法用 asyncio.to_thread 包装）
            transcript = await asyncio.to_thread(self.youtube_service.fetch_transcript, video_id)
            if not transcript:
                # 未获取到字幕，使用音频下载和转录流程
                audio_path = await asyncio.to_thread(self.youtube_service.download_audio, video_id)
                if not audio_path:
                    return None
                transcript = await asyncio.to_thread(self.youtube_service.transcribe_audio, audio_path)
                if not transcript:
                    return None

            # Step 2: 生成摘要
            interpreted_summary = await self.interpret_transcript(transcript, topic)
            if not interpreted_summary:
                return None

            # Step 3: 存储转录与摘要（同步调用）
            self.db.store_transcript_summary(video_id, transcript, interpreted_summary)
            return interpreted_summary

        except Exception as e:
            return None

# ---------------------------
# 以下为测试用例
# ---------------------------

@pytest.mark.asyncio
async def test_process_video_transcript_with_existing_transcript():
    db = DummyDatabase()
    openai_service = DummyOpenAIService()
    youtube_service = DummyYouTubeService()
    agent = Agent(db, openai_service, youtube_service)

    video_id = "video_with_transcript"
    topic = "Test Topic"
    summary = await agent.process_video_transcript(video_id, topic)
    
    # 验证返回摘要不为空，并且存储数据正确（使用 fetch_transcript 得到的字幕）
    assert summary is not None
    assert db.storage[video_id]["transcript"] == "Dummy transcript"
    assert summary.startswith("Dummy summary for:")

@pytest.mark.asyncio
async def test_process_video_transcript_with_audio_fallback():
    db = DummyDatabase()
    openai_service = DummyOpenAIService()
    youtube_service = DummyYouTubeService()
    agent = Agent(db, openai_service, youtube_service)

    video_id = "video_without_transcript"
    topic = "Fallback Topic"
    summary = await agent.process_video_transcript(video_id, topic)
    
    # 此情况下 fetch_transcript 返回 None，应该走下载和音频转录流程
    assert summary is not None
    # 检查存储的数据中 transcript 应为音频转录返回的内容
    assert db.storage[video_id]["transcript"] == "Dummy transcript from audio"
    assert summary.startswith("Dummy summary for:")

@pytest.mark.asyncio
async def test_interpret_transcript_error_handling(monkeypatch):
    db = DummyDatabase()
    openai_service = DummyOpenAIService()
    youtube_service = DummyYouTubeService()
    agent = Agent(db, openai_service, youtube_service)

    # 模拟 OpenAIService 异步接口抛出异常
    async def failing_completion(*args, **kwargs):
        raise Exception("Test error")
    monkeypatch.setattr(openai_service, "async_completion", failing_completion)

    summary = await agent.interpret_transcript("Some transcript", "Test Topic")
    # 当发生异常时，应返回 None
    assert summary is None
