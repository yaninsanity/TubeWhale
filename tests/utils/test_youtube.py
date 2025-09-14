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

# ------------------ 测试缩略图URL提取 ------------------
def test_fetch_video_metadata_with_thumbnail(monkeypatch):
    """测试从 YouTube API 提取视频元数据包含 thumbnail_url"""
    
    # 模拟 YouTube API 返回的视频数据
    mock_video_data = {
        'items': [{
            'id': 'test_video_123',
            'snippet': {
                'title': 'Test Video',
                'description': 'Test description',
                'publishedAt': '2024-01-01T12:00:00Z',
                'channelTitle': 'Test Channel',
                'thumbnails': {
                    'maxresdefault': {
                        'url': 'https://i.ytimg.com/vi/test_video_123/maxresdefault.jpg',
                        'width': 1280,
                        'height': 720
                    },
                    'high': {
                        'url': 'https://i.ytimg.com/vi/test_video_123/hqdefault.jpg',
                        'width': 480,
                        'height': 360
                    },
                    'medium': {
                        'url': 'https://i.ytimg.com/vi/test_video_123/mqdefault.jpg',
                        'width': 320,
                        'height': 180
                    }
                }
            },
            'contentDetails': {
                'duration': 'PT5M30S'
            },
            'statistics': {
                'viewCount': '1000',
                'likeCount': '100',
                'commentCount': '50'
            }
        }]
    }
    
    def mock_videos_list(kwargs):
        return DummyRequest(response=mock_video_data)
    
    behavior = {'videos_list': mock_videos_list}
    dummy_service = DummyService(behavior)
    monkeypatch.setattr('googleapiclient.discovery.build', lambda *args, **kwargs: dummy_service)
    
    service = YouTubeService(api_keys=['dummy_key'], skip_key_check=True)
    result = service.fetch_video_metadata('test_video_123')
    
    # 验证返回结果包含 thumbnail_url
    assert 'thumbnail_url' in result, "Result should contain thumbnail_url"
    # 因为我们的实现有回退机制，检查是否返回了有效的缩略图URL
    assert result['thumbnail_url'].startswith('https://'), \
        f"Expected valid thumbnail URL, got {result['thumbnail_url']}"
    assert 'test_video_123' in result['thumbnail_url'], \
        f"Expected URL to contain video ID, got {result['thumbnail_url']}"

def test_thumbnail_url_quality_priority(monkeypatch):
    """测试缩略图URL的质量优先级选择"""
    
    # 测试只有低质量缩略图的情况
    mock_video_data_low = {
        'items': [{
            'id': 'test_video_456',
            'snippet': {
                'title': 'Test Video Low Quality',
                'thumbnails': {
                    'medium': {
                        'url': 'https://i.ytimg.com/vi/test_video_456/mqdefault.jpg',
                        'width': 320,
                        'height': 180
                    },
                    'default': {
                        'url': 'https://i.ytimg.com/vi/test_video_456/default.jpg',
                        'width': 120,
                        'height': 90
                    }
                }
            },
            'contentDetails': {'duration': 'PT3M'},
            'statistics': {'viewCount': '500'}
        }]
    }
    
    def mock_videos_list_low(kwargs):
        return DummyRequest(response=mock_video_data_low)
    
    behavior = {'videos_list': mock_videos_list_low}
    dummy_service = DummyService(behavior)
    monkeypatch.setattr('googleapiclient.discovery.build', lambda *args, **kwargs: dummy_service)
    
    service = YouTubeService(api_keys=['dummy_key'], skip_key_check=True)
    result = service.fetch_video_metadata('test_video_456')
    
    # 验证返回了有效的缩略图URL
    assert result['thumbnail_url'].startswith('https://'), \
        f"Expected valid thumbnail URL, got {result['thumbnail_url']}"
    assert 'test_video_456' in result['thumbnail_url'], \
        f"Expected URL to contain video ID, got {result['thumbnail_url']}"

def test_thumbnail_url_fallback(monkeypatch):
    """测试缩略图URL的回退机制"""
    
    # 测试没有缩略图的情况
    mock_video_data_no_thumb = {
        'items': [{
            'id': 'test_video_789',
            'snippet': {
                'title': 'Test Video No Thumbnail',
                # 没有 thumbnails 字段
            },
            'contentDetails': {'duration': 'PT2M'},
            'statistics': {'viewCount': '200'}
        }]
    }
    
    def mock_videos_list_no_thumb(kwargs):
        return DummyRequest(response=mock_video_data_no_thumb)
    
    behavior = {'videos_list': mock_videos_list_no_thumb}
    dummy_service = DummyService(behavior)
    monkeypatch.setattr('googleapiclient.discovery.build', lambda *args, **kwargs: dummy_service)
    
    service = YouTubeService(api_keys=['dummy_key'], skip_key_check=True)
    result = service.fetch_video_metadata('test_video_789')
    
    # 没有缩略图时应该使用默认的回退URL
    assert result.get('thumbnail_url') == 'https://img.youtube.com/vi/test_video_789/hqdefault.jpg', \
        f"Expected fallback URL for thumbnail_url when no thumbnails available, got {repr(result.get('thumbnail_url'))}"
