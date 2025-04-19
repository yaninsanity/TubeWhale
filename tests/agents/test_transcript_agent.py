import os
import asyncio
import pytest
from io import BytesIO
from pydub import AudioSegment

import agents.transcript_agent as ta
from agents.transcript_agent import TranscriptAgent, async_retry, run_transcript

# --- Helpers ------------------------------------------------------------

class DummyYouTubeService:
    def __init__(self, audio_path=None, raise_on_download=False):
        self.audio_path = audio_path
        self.raise_on_download = raise_on_download

    def download_audio(self, video_id):
        if self.raise_on_download:
            raise RuntimeError("download error")
        return self.audio_path

class DummyOpenAIService:
    def __init__(self, responses=None, raise_on_transcribe=False):
        # responses: list of strings to return for each chunk
        self.responses = responses or []
        self.raise_on_transcribe = raise_on_transcribe
        self.calls = []

    async def transcribe_audio(self, audio_io):
        if self.raise_on_transcribe:
            raise RuntimeError("whisper error")
        # record that we got a BytesIO
        self.calls.append(audio_io.getbuffer()[:4])
        # pop next response or return empty
        return self.responses.pop(0) if self.responses else ""


# --- async_retry decorator ---------------------------------------------

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


# --- _slice_audio ------------------------------------------------------

def test_slice_audio_real(tmp_path):
    # create a 1500ms silent mp3
    path = tmp_path / "test.mp3"
    AudioSegment.silent(duration=1500).export(str(path), format="mp3")

    agent = TranscriptAgent(openai_service=None, youtube_service=None, logger=None)
    # default MAX_CHUNK_DURATION_MS = 60000 → single chunk
    chunks = agent._slice_audio(str(path))
    assert isinstance(chunks, list)
    total_ms = sum(len(c) for c in chunks)
    assert total_ms == 1500
    assert len(chunks) == 1

    # monkey‐patch to test smaller chunk size
    agent.MAX_CHUNK_DURATION_MS = 1000
    chunks2 = agent._slice_audio(str(path))
    assert len(chunks2) == 2
    assert sum(len(c) for c in chunks2) == 1500

def test_slice_audio_failure(monkeypatch):
    # force AudioSegment.from_file to throw
    monkeypatch.setattr(ta.AudioSegment, "from_file", lambda p: (_ for _ in ()).throw(RuntimeError("bad")))
    agent = TranscriptAgent(openai_service=None, youtube_service=None, logger=None)
    chunks = agent._slice_audio("no.mp3")
    assert chunks == []


# --- _transcribe_chunk -------------------------------------------------

@pytest.mark.asyncio
async def test_transcribe_chunk(monkeypatch):
    seg = AudioSegment.silent(duration=100)

    class DummyOA(DummyOpenAIService):
        async def transcribe_audio(self, f):
            # ensure name ends with .mp3
            assert hasattr(f, "name") and f.name.endswith(".mp3")
            return "trans"

    oa = DummyOA()
    agent = TranscriptAgent(openai_service=oa, youtube_service=None, logger=None)
    result = await agent._transcribe_chunk(seg)
    assert result == "trans"


# --- fetch_transcript --------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_transcript_success(monkeypatch, tmp_path):
    # prepare dummy audio file
    audio_file = tmp_path / "audio.mp3"
    audio_file.write_bytes(b"dummy")
    yt = DummyYouTubeService(audio_path=str(audio_file))
    oa = DummyOpenAIService(responses=["one", "two"])
    agent = TranscriptAgent(openai_service=oa, youtube_service=yt, logger=None)

    # patch slicing and transcription
    monkeypatch.setattr(agent, "_slice_audio", lambda path: ["c1", "c2"])
    async def fake_trans(chunk):
        return f"T-{chunk}"
    monkeypatch.setattr(agent, "_transcribe_chunk", fake_trans)
    # stub existence
    monkeypatch.setattr(os.path, "exists", lambda p: True)

    merged = await agent.fetch_transcript("vid123")
    assert merged == "T-c1\nT-c2"

@pytest.mark.asyncio
async def test_fetch_transcript_download_error():
    yt = DummyYouTubeService(raise_on_download=True)
    oa = DummyOpenAIService()
    agent = TranscriptAgent(openai_service=oa, youtube_service=yt, logger=None)
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_no_file(monkeypatch, tmp_path):
    audio_file = tmp_path / "audio.mp3"
    audio_file.write_bytes(b"dummy")
    yt = DummyYouTubeService(audio_path=str(audio_file))
    oa = DummyOpenAIService()
    agent = TranscriptAgent(openai_service=oa, youtube_service=yt, logger=None)
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_empty_chunks(monkeypatch, tmp_path):
    audio_file = tmp_path / "audio.mp3"
    audio_file.write_bytes(b"dummy")
    yt = DummyYouTubeService(audio_path=str(audio_file))
    oa = DummyOpenAIService()
    agent = TranscriptAgent(openai_service=oa, youtube_service=yt, logger=None)
    monkeypatch.setattr(agent, "_slice_audio", lambda path: [])
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    res = await agent.fetch_transcript("vid")
    assert res is None

@pytest.mark.asyncio
async def test_fetch_transcript_all_chunks_fail(monkeypatch, tmp_path):
    audio_file = tmp_path / "audio.mp3"
    audio_file.write_bytes(b"dummy")
    yt = DummyYouTubeService(audio_path=str(audio_file))
    oa = DummyOpenAIService(responses=[])
    agent = TranscriptAgent(openai_service=oa, youtube_service=yt, logger=None)
    monkeypatch.setattr(agent, "_slice_audio", lambda path: ["x1", "x2"])
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    res = await agent.fetch_transcript("vid")
    assert res is None


# --- run_transcript wrapper --------------------------------------------

@pytest.mark.asyncio
async def test_run_transcript_calls_fetch(monkeypatch):
    async def fake_fetch(self, vid):
        return "wrapped!"
    monkeypatch.setattr(TranscriptAgent, "fetch_transcript", fake_fetch)

    result = await run_transcript("any", openai_service=None, youtube_service=None, logger=None)
    assert result == "wrapped!"
