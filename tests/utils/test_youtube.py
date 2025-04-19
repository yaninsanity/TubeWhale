import os
import sys
import types
import pytest
from googleapiclient.errors import HttpError
from utils.youtube import YouTubeService

# ------------------ DummyResponse 与 DummyRequest ------------------
class DummyResponse:
    def __init__(self, reason="quotaExceeded", status=403):
        self.reason = reason
        self.status = status

class DummyRequest:
    def __init__(self, response=None, error=False, error_text="", uri="dummy_uri?q=test"):
        self.response = response
        self.error = error
        self.error_text = error_text
        self.uri = uri

    def execute(self):
        if self.error:
            raise HttpError(resp=DummyResponse(reason=self.error_text),
                            content=self.error_text.encode('utf-8'))
        return self.response

# ------------------ DummyResource 与 DummyService ------------------
class DummyResource:
    def __init__(self, call_type, behavior):
        self.call_type = call_type
        self.behavior = behavior

    def list(self, **kwargs):
        func = self.behavior.get(self.call_type)
        if not func:
            raise Exception(f"Unsupported call type: {self.call_type}")
        return func(kwargs)

class DummyService:
    def __init__(self, behavior):
        self.behavior = behavior

    def search(self):
        return DummyResource("search", self.behavior)
    def videos(self):
        return DummyResource("videos_list", self.behavior)
    def commentThreads(self):
        return DummyResource("commentThreads", self.behavior)
    def playlists(self):
        return DummyResource("playlists", self.behavior)
    def playlistItems(self):
        return DummyResource("playlistItems", self.behavior)

# ------------------ 测试 yt_dlp 音频下载逻辑 ------------------
class DummyYTDL:
    def __init__(self, opts):
        self.opts = opts
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    def download(self, url_list):
        # 使用 outtmpl 生成 mp3 文件
        outtmpl = self.opts['outtmpl']
        path = outtmpl.replace('%(ext)s', 'mp3')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(b'dummy audio content')

@pytest.fixture(autouse=True)
def stub_ytdl(monkeypatch):
    # 模拟 yt_dlp.YoutubeDL
    module = types.ModuleType('yt_dlp')
    module.YoutubeDL = DummyYTDL
    monkeypatch.setitem(sys.modules, 'yt_dlp', module)
    # 模拟 googleapiclient.discovery.build 返回 DummyService
    monkeypatch.setattr('googleapiclient.discovery.build', lambda *args, **kwargs: DummyService({}))
    yield


def test_download_audio_integration(tmp_path, monkeypatch):
    # 切换到临时目录
    monkeypatch.chdir(tmp_path)
    video_id = 'vid123'
    service = YouTubeService(api_keys=['dummy_key'], skip_key_check=True)
    result = service.download_audio(video_id)
    expected = os.path.abspath(os.path.join('downloads', f'{video_id}.mp3'))
    assert result == expected
    assert os.path.exists(expected)
    with open(expected, 'rb') as f:
        assert f.read() == b'dummy audio content'

# ------------------ 测试重试上限 ------------------
def test_max_retries_exceeded(monkeypatch):
    # 所有请求均返回 quotaExceeded
    def always_quota(kwargs):
        return DummyRequest(error=True, error_text='quotaExceeded', uri='dummy_uri')

    behavior = {'search': always_quota}
    dummy_service = DummyService(behavior)
    monkeypatch.setattr('googleapiclient.discovery.build', lambda *args, **kwargs: dummy_service)
    service = YouTubeService(api_keys=['k1', 'k2'], max_retries=1, backoff_factor=0, skip_key_check=True)

    with pytest.raises(Exception, match='Max retries exceeded'):
        service.search('query')
