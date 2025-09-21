# tests/agents/test_transcript_agent.py
import os
import types
import pytest
import asyncio
from io import BytesIO
from pydub import AudioSegment  # 用于生成测试 MP3 文件

import agents.transcript_agent as ta
from agents.transcript_agent import TranscriptAgent, async_retry, run_transcript

# --- FakeAudio & 全局打桩 -------------------------------------------------------

class FakeAudio:
    """
    一个“音频片段”桩，用于替代 pydub.AudioSegment。
    支持 len()、切片（__getitem__）和 export()。
    """
    def __init__(self, length_ms):
        self._len = length_ms

    def __len__(self):
        return self._len

    def __getitem__(self, sl):
        # 切片后返回长度为 min(stop, self._len) - start
        start = sl.start or 0
        stop = sl.stop if sl.stop is not None else self._len
        stop = min(stop, self._len)
        length = max(0, stop - start)
        return FakeAudio(length)

    def export(self, out, format="mp3"):
        # 写点任意数据
        data = b"\x00" * 10
        if hasattr(out, "write"):
            out.write(data)
        else:
            # 当 out 是文件路径
            with open(out, "wb") as f:
                f.write(data)

@pytest.fixture(autouse=True)
def patch_audiosegment(monkeypatch):
    """
    在所有测试中，把 TranscriptAgent._slice_audio 使用的 AudioSegment.from_file
    重写为返回 FakeAudio(1500)，并且不再需要 ffmpeg。
    """
    # TranscriptAgent 模块里已经做了：from pydub import AudioSegment
    # 这里替换它
    monkeypatch.setattr(ta, "AudioSegment", types.SimpleNamespace(
        from_file=lambda path: FakeAudio(1500)
    ))
    yield

# --- async_retry 装饰器 ------------------------------------------------------

@pytest.mark.asyncio
async def test_async_retry_success():
    class C:
        def __init__(self):
            self.count = 0

        @async_retry(max_retries=3, delay=0)
        async def flaky(self):
            self.count += 1
            if self.count < 2:
                raise ValueError("fail")
            return "ok"

    c = C()
    res = await c.flaky()
    assert res == "ok"
    assert c.count == 2

@pytest.mark.asyncio
async def test_async_retry_exceeded():
    class D:
        @async_retry(max_retries=2, delay=0)
        async def always_fail(self):
            raise RuntimeError("bad")

    d = D()
    with pytest.raises(RuntimeError):
        await d.always_fail()

# --- _slice_audio 方法 -------------------------------------------------------

def test_slice_audio_basic(tmp_path):
    """
    AudioSegment.from_file → FakeAudio(1500)，
    默认 MAX_CHUNK_DURATION_MS=60000 时→单 chunk 长度 1500；
    改成 1000ms 再测试分两块。
    """
    # 先写一个空文件，内容不重要
    path = tmp_path / "dummy.mp3"
    AudioSegment.silent(duration=10).export(str(path), format="mp3")

    agent = TranscriptAgent(openai_service=None, youtube_service=None, logger=None)
    chunks = agent._slice_audio(str(path))
    assert isinstance(chunks, list)
    assert len(chunks) == 1
    assert len(chunks[0]) == 1500

    agent.MAX_CHUNK_DURATION_MS = 1000
    chunks2 = agent._slice_audio(str(path))
    assert len(chunks2) == 2
    assert [len(c) for c in chunks2] == [1000, 500]

def test_slice_audio_failure(monkeypatch):
    """
    如果 from_file 抛异常，返回空列表。
    """
    monkeypatch.setattr(ta.AudioSegment, "from_file", lambda p: (_ for _ in ()).throw(RuntimeError("bad")))
    agent = TranscriptAgent(openai_service=None, youtube_service=None, logger=None)
    chunks = agent._slice_audio("ignore.mp3")
    assert chunks == []

# --- _transcribe_chunk 方法 ---------------------------------------------------

@pytest.mark.asyncio
async def test_transcribe_chunk_writes_and_calls_openai():
    """
    用一个 FakeChunk（支持 export）测试 _transcribe_chunk。
    """
    class FakeChunk:
        def export(self, buf, format="mp3"):
            buf.write(b"chunkdata")

    calls = []
    class DummyOA:
        async def transcribe_audio(self, buf):
            assert hasattr(buf, "name") and buf.name.endswith(".mp3")
            calls.append(buf.getvalue())
            return "TRANSCRIBED"

    oa = DummyOA()
    agent = TranscriptAgent(openai_service=oa, youtube_service=None, logger=None)
    out = await agent._transcribe_chunk(FakeChunk())
    assert out == "TRANSCRIBED"
    assert calls and calls[0].startswith(b"chunkdata")

# --- fetch_transcript 方法 ----------------------------------------------------

class DummyYT:
    def __init__(self, path=None, raise_on_download=False):
        self.path = path
        self.raise_on_download = raise_on_download

    def download_audio(self, vid, dry_run=False):
        if self.raise_on_download:
            raise RuntimeError("dl fail")
        return self.path

@pytest.mark.asyncio
async def test_fetch_transcript_success(monkeypatch):
    """
    download_audio 返回路径且 exists=True，
    stub _slice_audio → ['A','B']，
    stub _transcribe_chunk → 返回 T-A/T-B，
    最终合并 "T-A\nT-B"。
    """
    yt = DummyYT(path="/fake.mp3", raise_on_download=False)
    agent = TranscriptAgent(openai_service=None, youtube_service=yt, logger=None)

    monkeypatch.setattr(os.path, "exists", lambda p: True)
    monkeypatch.setattr(agent, "_slice_audio", lambda p: ["A", "B"])
    async def fake_chunk(c):
        return f"T-{c}"
    monkeypatch.setattr(agent, "_transcribe_chunk", fake_chunk)

    out = await agent.fetch_transcript("vidX")
    assert out == "T-A\nT-B"

@pytest.mark.asyncio
async def test_fetch_transcript_download_error():
    yt = DummyYT(path=None, raise_on_download=True)
    agent = TranscriptAgent(openai_service=None, youtube_service=yt, logger=None)
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_no_file(monkeypatch):
    yt = DummyYT(path="/fake.mp3")
    agent = TranscriptAgent(openai_service=None, youtube_service=yt, logger=None)
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_empty_chunks(monkeypatch):
    yt = DummyYT(path="/fake.mp3")
    agent = TranscriptAgent(openai_service=None, youtube_service=yt, logger=None)
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    monkeypatch.setattr(agent, "_slice_audio", lambda p: [])
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_all_empty_transcripts(monkeypatch):
    yt = DummyYT(path="/fake.mp3")
    agent = TranscriptAgent(openai_service=None, youtube_service=yt, logger=None)
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    monkeypatch.setattr(agent, "_slice_audio", lambda p: ["X", "Y"])
    async def fake_empty(c):
        return ""
    monkeypatch.setattr(agent, "_transcribe_chunk", fake_empty)
    res = await agent.fetch_transcript("vid")
    assert res is None

# --- run_transcript 包装 ------------------------------------------------------

@pytest.mark.asyncio
async def test_run_transcript_wraps_fetch(monkeypatch):
    async def fake_fetch(self, vid):
        return "wrapped!"
    monkeypatch.setattr(TranscriptAgent, "fetch_transcript", fake_fetch)
    out = await run_transcript("any", openai_service=None, youtube_service=None, logger=None)
    assert out == "wrapped!"
