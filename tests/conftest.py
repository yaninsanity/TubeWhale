import sys
import os
import types
import pytest
# 将项目根目录加入 sys.path，假设 tests/ 在项目根目录下
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture(autouse=True)
def stub_openai_and_ffmpeg(monkeypatch):
    # 1) stub openai.OpenAI 客户端
    dummy_openai = types.ModuleType("openai")
    class DummyOpenAI:
        def __init__(self, api_key=None):
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=lambda *args, **kwargs: {"choices":[{"message":{"content":""}}], "usage":{}}
                )
            )
            self.completions = types.SimpleNamespace(
                create=lambda *args, **kwargs: {"choices":[{"text":""}], "usage":{}}
            )
            self.audio = types.SimpleNamespace(
                transcriptions=types.SimpleNamespace(
                    create=lambda *args, **kwargs: {"text":""}
                )
            )
    dummy_openai.OpenAI = DummyOpenAI
    monkeypatch.setitem(sys.modules, "openai", dummy_openai)

    # 2) stub pydub.AudioSegment.from_file/export，避开 FFmpeg
    import pydub
    class FakeAudio:
        def __init__(self, length_ms):
            self._len = length_ms
        def __len__(self):
            return self._len
        def __getitem__(self, sl):
            # 切片后仍返回 FakeAudio，以支持继续切
            start, stop, step = sl.start or 0, sl.stop or self._len, sl.step
            return FakeAudio(stop - start)
        def export(self, buf, format="mp3"):
            # 写一点任意 byte，测试就能继续
            buf.write(b"\x00"*10)
    monkeypatch.setattr(pydub, "AudioSegment", types.SimpleNamespace(
        from_file=lambda path: FakeAudio(1500),
        silent=lambda duration: FakeAudio(duration)
    ))
    yield