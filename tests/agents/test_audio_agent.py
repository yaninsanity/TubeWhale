import pytest
import os
import sys
import types
import asyncio
from io import BytesIO

import agents.audio_agent as aa
from agents.audio_agent import (
    validate_video_id,
    maybe_async,
    fetch_transcript,
    AudioChunkProcessor,
    get_audio_processing_agent,
    DEFAULT_LOGGER,
)

# 屏蔽 retry 装饰器的延迟
import utils.helper as helper_mod
@pytest.fixture(autouse=True)
def disable_retry(monkeypatch):
    monkeypatch.setattr(helper_mod, "retry", lambda *args, **kwargs: (lambda f: f))

# --- Dummy 服务实现 --------------------------------------------------

class DummyYouTubeService:
    def download_audio(self, video_id):
        # 返回一个 fake 路径
        return f"/fake/path/{video_id}.mp3"

class DummyDB:
    def __init__(self):
        self.records = []
    async def store_data(self, table, record):
        self.records.append((table, record))

class DummyTranscriptAgent:
    def __init__(self, transcript):
        self.transcript = transcript
    async def fetch_transcript(self, video_id):
        return self.transcript

class DummyOpenAIService:
    def __init__(self):
        self.prompts = {"summarization": {"user": "Summarize: {text}"}}
        self.transcribed = []
        self.summaries = []
    async def transcribe_audio(self, buffer: BytesIO):
        self.transcribed.append(buffer.getvalue())
        return "dummy transcript"
    async def async_completion(self, prompt, **kwargs):
        self.summaries.append(prompt)
        return "dummy summary"
    def get_prompt(self, name, variables=None):
        return self.prompts[name]["user"].format(**(variables or {}))

# --- Tests ----------------------------------------------------------

@pytest.mark.asyncio
async def test_validate_video_id():
    class C:
        @validate_video_id
        async def foo(self, vid):
            return vid
    inst = C()
    # 合法 ID
    assert await inst.foo("ABCDEFGHIJK") == "ABCDEFGHIJK"
    # 非法情况
    with pytest.raises(ValueError):
        await inst.foo("SHORT")
    with pytest.raises(ValueError):
        await inst.foo("BAD_ID!!!")

def test_maybe_async():
    def sync(x): return x+1
    async def af(x): return x*2
    assert asyncio.run(maybe_async(sync, 3)) == 4
    assert asyncio.run(maybe_async(af, 5)) == 10

@pytest.mark.asyncio
async def test_fetch_transcript_success(monkeypatch):
    # 模拟 youtube_transcript_api
    mod = types.ModuleType("youtube_transcript_api")
    class T:
        @staticmethod
        def get_transcript(video_id, langs):
            return [{"text":"Hi"},{"text":"Test"}]
    mod.YouTubeTranscriptApi = T
    sys.modules["youtube_transcript_api"] = mod

    txt = await fetch_transcript(None, "ABCDEFGHIJK")
    assert txt == "Hi Test"

@pytest.mark.asyncio
async def test_fetch_transcript_failure(monkeypatch):
    mod = types.ModuleType("youtube_transcript_api")
    class T:
        @staticmethod
        def get_transcript(video_id, langs):
            return []  # 触发异常
    mod.YouTubeTranscriptApi = T
    sys.modules["youtube_transcript_api"] = mod

    with pytest.raises(Exception):
        await fetch_transcript(None, "ABCDEFGHIJK")

@pytest.mark.asyncio
async def test_process_chunk_success_and_failure():
    srv = DummyOpenAIService()
    proc = AudioChunkProcessor(srv, DEFAULT_LOGGER)
    class FakeChunk:
        def export(self, buf, format): buf.write(b"123")
    # 成功
    idx, summ = await proc.process_chunk(FakeChunk(), chunk_idx=0, previous_summary="", topic="T")
    assert idx == 0 and summ == "dummy summary"
    # 转录返回空
    async def empty_trans(buf): return ""
    srv.transcribe_audio = empty_trans
    res = await proc.process_chunk(FakeChunk(), chunk_idx=1, previous_summary="", topic="T")
    assert res is None

@pytest.mark.asyncio
async def test_get_transcript_priority():
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    db = DummyDB()
    ta = DummyTranscriptAgent("subtitles")
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        db=db,
        transcript_agent=ta,
        use_transcript_api=True
    )
    # 优先走 transcript_agent
    out = await agent.get_transcript("ABCDEFGHIJK")
    assert out == "subtitles"
    # transcript_agent 返回 None 时，fallback
    ta.transcript = None
    out2 = await agent.get_transcript("ABCDEFGHIJK")
    assert out2 is None

@pytest.mark.asyncio
async def test_download_audio_and_db(monkeypatch):
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    db = DummyDB()
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        db=db,
        download_dir=".",
        temp_dir="."
    )
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    path = await agent.download_audio("VIDEO123456")
    assert path.endswith("VIDEO123456.mp3")
    # 应该在 DB 里记录了一条
    assert db.records and db.records[0][0] == "audio_processing_logs"

def test_split_audio_monkeypatched(monkeypatch):
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        download_dir=".",
        temp_dir=".",
        db=None
    )
    # Mock AudioSegment.from_file 返回自定义对象
    class FakeAudio:
        def __init__(self, length): self._len = length
        def __len__(self): return self._len
        def __getitem__(self, sl): return f"chunk{sl}"
    monkeypatch.setattr(aa.AudioSegment, "from_file", lambda path: FakeAudio(250))
    agent.max_duration_ms = 100
    chunks = agent.split_audio("ignored.mp3")
    assert len(chunks) == 3
    assert all(isinstance(c, str) for c in chunks)

@pytest.mark.asyncio
async def test_transcribe_audio_chunk(monkeypatch):
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        download_dir=".",
        temp_dir=".",
        db=None
    )
    class FakeChunk:
        def export(self, buf, format): buf.write(b"abc")
    # 成功
    text = await agent.transcribe_audio_chunk(FakeChunk())
    assert text == "dummy transcript"
    # 强制抛错
    async def bad(buf): raise RuntimeError
    openai.transcribe_audio = bad
    res = await agent.transcribe_audio_chunk(FakeChunk())
    assert res is None

@pytest.mark.asyncio
async def test_summarize_and_recursive(monkeypatch):
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        download_dir=".",
        temp_dir=".",
        db=None
    )
    # 测试 summarize_text
    out = await agent.summarize_text("txt", previous_summary="p", topic="t", metadata={})
    assert out == "dummy summary"

    # 测试 recursive_summarize
    async def echo(text, previous_summary, topic, metadata):
        return f"[{text}]"
    agent.summarize_text = echo
    final = await agent.recursive_summarize(["a","b","c","d"], topic="T", metadata={})
    assert isinstance(final, str) and final.startswith("[")

@pytest.mark.asyncio
async def test_process_video_audio_full(monkeypatch):
    openai = DummyOpenAIService()
    youtube = DummyYouTubeService()
    db = DummyDB()
    ta = DummyTranscriptAgent(None)
    agent = get_audio_processing_agent(
        openai_service=openai,
        youtube_service=youtube,
        db=db,
        download_dir=".",
        temp_dir=".",
        transcript_agent=ta,
        use_transcript_api=False
    )

    # stub 为 async def，确保 await-able
    async def fake_download(vid): return "/fake.mp3"
    agent.download_audio = fake_download
    monkeypatch.setattr(os.path, "exists", lambda p: True)

    async def fake_transcribe(chunk):
        return f"T-{chunk}"
    agent.transcribe_audio_chunk = fake_transcribe

    async def fake_summarize(text, previous_summary, topic, metadata):
        # 假设直接返回分块标记
        return text.replace("T-", "S-")
    agent.summarize_text = fake_summarize

    # 使用真实 recursive_summarize：先会把 ["S-c1", "S-c2"] 合并为 "S-c1\nS-c2"
    agent.split_audio = lambda p: ["c1","c2"]

    result = await agent.process_video_audio("ABCDEFGHIJK", "topic", {"x":1})
    # 最终应该包含 S-c1 和 S-c2，用换行分隔
    assert "S-c1" in result and "S-c2" in result

    # 最后 db 应该有记录 final_audio_summary
    assert any(r[0] == "audio_processing_logs" for r in db.records)
