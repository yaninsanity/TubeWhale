import os
import ssl
import time
import threading
import logging
import random
import asyncio
import pytest
import sys
import types
from googleapiclient.errors import HttpError
from pathlib import Path
from utils.youtube import YouTubeService, get_youtube_service, DummyRequest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ------------------ DummyResponse 与 DummyRequest ------------------
class DummyResponse:
    def __init__(self, reason="quotaExceeded", status=403):
        self.reason = reason
        self.status = status

class DummyRequest:
    def __init__(self, response=None, error=False, error_text="", 
                 uri="dummy_uri?q=test&maxResults=25&type=video&videoEmbeddable=true&videoSyndicated=true"):
        self.response = response
        self.error = error
        self.error_text = error_text
        self.uri = uri

    def execute(self):
        if self.error:
            raise HttpError(resp=DummyResponse(reason=self.error_text),
                            content=self.error_text.encode("utf-8"))
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

# ------------------ Dummy 模拟函数 ------------------
class DummyFFmpegChain:
    def __init__(self, stream_url):
        self.stream_url = stream_url
        self.output_file = None

    def output(self, output_file, *args, **kwargs):
        self.output_file = output_file
        return self

    def overwrite_output(self):
        return self

    def run(self):
        # 模拟执行 ffmpeg 命令，调用 dummy_ffmpeg_run 生成文件
        dummy_ffmpeg_run(output_file=self.output_file)
        return

def dummy_ffmpeg_input(stream_url, *args, **kwargs):
    return DummyFFmpegChain(stream_url)

def dummy_search_success(kwargs):
    return DummyRequest(response={"items": [{"id": {"videoId": "test_video"}}]})

def dummy_videos_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "snippet": {"title": "Test Video"},
            "statistics": {"viewCount": "1000", "likeCount": "100", "commentCount": "10"},
            "contentDetails": {}
        }]
    })

def dummy_comments_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "snippet": {
                "topLevelComment": {
                    "id": "c1",
                    "snippet": {
                        "authorDisplayName": "Author1",
                        "textDisplay": "Comment1",
                        "likeCount": 5,
                        "viewerRating": "none",
                        "moderationStatus": "published",
                        "publishedAt": "2021-01-01T00:00:00Z"
                    }
                }
            }
        }]
    })

def dummy_comments_multi_page(kwargs):
    if kwargs.get("pageToken") is None:
        return DummyRequest(response={
            "items": [{
                "snippet": {
                    "topLevelComment": {
                        "id": "c1",
                        "snippet": {
                            "authorDisplayName": "Author1",
                            "textDisplay": "Comment1",
                            "likeCount": 5,
                            "viewerRating": "none",
                            "moderationStatus": "published",
                            "publishedAt": "2021-01-01T00:00:00Z"
                        }
                    }
                },
            }],
            "nextPageToken": "page2"
        })
    else:
        return DummyRequest(response={
            "items": [{
                "snippet": {
                    "topLevelComment": {
                        "id": "c2",
                        "snippet": {
                            "authorDisplayName": "Author2",
                            "textDisplay": "Comment2",
                            "likeCount": 3,
                            "viewerRating": "none",
                            "moderationStatus": "published",
                            "publishedAt": "2021-01-02T00:00:00Z"
                        }
                    }
                }
            }]
        })

def dummy_playlists_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "id": "playlist1",
            "snippet": {"title": "Test Playlist"},
            "contentDetails": {"itemCount": 5},
            "status": {"privacyStatus": "public"}
        }]
    })

def dummy_playlistItems_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "snippet": {"title": "Playlist Item 1"},
            "contentDetails": {"videoId": "video1"}
        }]
    })

def dummy_playlistItems_multi_page(kwargs):
    if kwargs.get("pageToken") is None:
        return DummyRequest(response={
            "items": [{
                "snippet": {"title": "Playlist Item 1"},
                "contentDetails": {"videoId": "video1"}
            }],
            "nextPageToken": "page2"
        })
    else:
        return DummyRequest(response={
            "items": [{
                "snippet": {"title": "Playlist Item 2"},
                "contentDetails": {"videoId": "video2"}
            }]
        })

def dummy_videos_empty(kwargs):
    return DummyRequest(response={"items": []})

def dummy_comments_error(kwargs):
    return DummyRequest(error=True, error_text="some other error", uri="dummy_uri")

def make_quota_then_success():
    call_count = {'count': 0}
    def inner(kwargs):
        if call_count['count'] == 0:
            call_count['count'] += 1
            return DummyRequest(error=True, error_text="quotaExceeded", uri="dummy_uri")
        else:
            return DummyRequest(response={"items": [{"id": {"videoId": "test_video"}}]}, uri="dummy_uri")
    return inner

def dummy_rebuild_search(kwargs):
    req = DummyRequest(response={"rebuilt": True})
    req.rebuilt_params = kwargs
    return req

def dummy_rebuild_videos_list(kwargs):
    req = DummyRequest(response={"rebuilt": True})
    req.rebuilt_params = kwargs
    return req

def dummy_rebuild_commentThreads(kwargs):
    req = DummyRequest(response={"rebuilt": True})
    req.rebuilt_params = kwargs
    return req

def dummy_rebuild_playlists(kwargs):
    req = DummyRequest(response={"rebuilt": True})
    req.rebuilt_params = kwargs
    return req

def dummy_rebuild_playlistItems(kwargs):
    req = DummyRequest(response={"rebuilt": True})
    req.rebuilt_params = kwargs
    return req

# ------------------ Dummy YouTube 对象 ------------------
class DummyStream:
    def __init__(self):
        self.url = "dummy_stream_url"
    def download(self, output_path, filename):
        # 模拟将音频流下载到临时文件，生成一个 dummy 文件以供后续转换
        file_path = os.path.join(output_path, filename)
        with open(file_path, "wb") as f:
            f.write(b"dummy downloaded content")
        return file_path

class DummyStreamQuery:
    def __init__(self):
        self._dummy_stream = DummyStream()

    def filter(self, only_audio):
        return self

    def order_by(self, key):
        return self

    def desc(self):
        return self

    # 修改为方法形式，支持 first() 调用
    def first(self):
        return self._dummy_stream

class DummyYouTube:
    """模拟 pytube.YouTube 对象，用于测试 audio 下载流程"""
    def __init__(self, url, **kwargs):
        self.url = url

    @property
    def streams(self):
        return DummyStreamQuery()

# ------------------ dummy_ffmpeg_run ------------------
def dummy_ffmpeg_run(*args, output_file, **kwargs):
    # 模拟生成输出文件，写入 dummy 内容
    with open(output_file, "wb") as f:
        f.write(b"dummy audio content")
    return

# ------------------ 全局 fixture：替换 googleapiclient.discovery.build ------------------
@pytest.fixture(autouse=True)
def dummy_build_service(monkeypatch):
    monkeypatch.setattr("googleapiclient.discovery.build", lambda *args, **kwargs: DummyService({}))

# ------------------ Fixture ------------------
@pytest.fixture
def dummy_service_success():
    behavior = {
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    return DummyService(behavior)

@pytest.fixture
def youtube_service(monkeypatch, dummy_service_success):
    def dummy_build_service(self, api_key, unverified):
        return dummy_service_success
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True,
                             max_retries=2, backoff_factor=0, skip_key_check=True)
    return service

# ------------------------------ 测试下载音频（下载及提取逻辑） ------------------------------
def test_download_audio_integration(tmp_path, monkeypatch):
    # 切换工作目录到 tmp_path
    monkeypatch.chdir(tmp_path)
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()
    video_id = "video_integration"
    output_file = os.path.abspath(str(downloads_dir / f"{video_id}.mp3"))

    # 替换 pytube.YouTube 为 Dummy 实现
    monkeypatch.setattr("pytube.YouTube", DummyYouTube)
    # 构造一个假的 ffmpeg 模块，并将其注入 sys.modules
    dummy_ffmpeg_module = types.ModuleType("ffmpeg")
    dummy_ffmpeg_module.input = dummy_ffmpeg_input
    monkeypatch.setitem(sys.modules, "ffmpeg", dummy_ffmpeg_module)

    # 使用 skip_key_check=True 并传入 api_keys（列表）
    service = YouTubeService(api_keys=["dummy_key"], skip_key_check=True)
    ret = service.download_audio(video_id)

    # 断言输出文件存在，且内容符合预期
    assert os.path.exists(output_file)
    with open(output_file, "rb") as f:
        content = f.read()
    assert content == b"dummy audio content"
    
# ------------------------------ 以下为其他 API 接口测试 ------------------------------
def test_search_videos_success(youtube_service):
    response = youtube_service.search_videos("test")
    assert "items" in response
    assert youtube_service.cost_tracking["search"] >= 100
    
def test_search_channels_success(youtube_service, monkeypatch):
    def dummy_channels_success(kwargs):
        return DummyRequest(response={"items": [{"id": {"channelId": "channel1"}, "snippet": {"title": "Test Channel"}}]})
    behavior = {
        "search": dummy_channels_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    youtube_service.service = dummy_service
    response = youtube_service.search_channels("test")
    assert "items" in response
    assert response["items"][0]["id"].get("channelId") == "channel1"

def test_search_playlists_success(youtube_service, monkeypatch):
    def dummy_playlists_search(kwargs):
        return DummyRequest(response={"items": [{"id": {"playlistId": "playlist1"}, "snippet": {"title": "Test Playlist"}}]})
    behavior = {
        "search": dummy_playlists_search,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    youtube_service.service = dummy_service
    response = youtube_service.search_playlists("test")
    assert "items" in response
    assert response["items"][0]["snippet"]["title"] == "Test Playlist"

def test_generic_search_with_filters(youtube_service):
    filters = {"order": "viewCount", "regionCode": "US"}
    response = youtube_service.search("test query", max_results=10, page_token="token123", resource_type="video", filters=filters)
    assert "items" in response

def test_fetch_video_metadata_success(youtube_service):
    metadata = youtube_service.fetch_video_metadata("test_video")
    assert metadata["id"] == "test_video"
    assert metadata["snippet"]["title"] == "Test Video"
    assert metadata["view_count"] == 1000
    assert youtube_service.cost_tracking["videos_list"] >= 100

def test_fetch_video_metadata_empty(monkeypatch):
    behavior = {
        "videos_list": dummy_videos_empty,
        "search": dummy_search_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    metadata = service.fetch_video_metadata("test_video")
    assert metadata is None

def test_fetch_all_comments_success(youtube_service):
    comments = youtube_service.fetch_all_comments("test_video")
    assert len(comments) == 1
    assert comments[0]["comment_id"] == "c1"

def test_fetch_all_comments_multi_page(monkeypatch):
    behavior = {
        "commentThreads": dummy_comments_multi_page,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=2, backoff_factor=0, skip_key_check=True)
    comments = service.fetch_all_comments("test_video")
    assert len(comments) == 2
    assert comments[0]["comment_id"] == "c1"
    assert comments[1]["comment_id"] == "c2"

def test_fetch_playlist_metadata_success(youtube_service, monkeypatch):
    behavior = {
        "playlists": dummy_playlists_success,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    response = service.fetch_playlist_metadata("playlist1")
    assert response["id"] == "playlist1"
    assert response["snippet"]["title"] == "Test Playlist"
    assert response["contentDetails"]["itemCount"] == 5

def test_fetch_playlist_items_success(monkeypatch):
    behavior = {
        "playlistItems": dummy_playlistItems_success,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    items = service.fetch_playlist_items("playlist1", max_results=50)
    assert len(items) == 1
    assert items[0]["snippet"]["title"] == "Playlist Item 1"

def test_fetch_playlist_items_multi_page(monkeypatch):
    behavior = {
        "playlistItems": dummy_playlistItems_multi_page,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=2, backoff_factor=0, skip_key_check=True)
    items = service.fetch_playlist_items("playlist1", max_results=50)
    assert len(items) == 2
    assert items[0]["snippet"]["title"] == "Playlist Item 1"
    assert items[1]["snippet"]["title"] == "Playlist Item 2"

def test_quota_exceeded_key_rotation(monkeypatch):
    fresh_quota = make_quota_then_success()
    behavior = {
        "search": lambda kwargs: fresh_quota(kwargs),
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0, skip_key_check=True)
    response = service.search_videos("test")
    assert "items" in response
    assert service.get_current_key() == "key2"

def test_max_retries_exceeded(monkeypatch):
    def always_quota(kwargs):
        return DummyRequest(error=True, error_text="quotaExceeded", uri="dummy_uri")
    behavior = {
        "search": always_quota,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0, skip_key_check=True)
    with pytest.raises(Exception, match="Max retries exceeded"):
        service.search_videos("test")

def test_empty_api_keys():
    with pytest.raises(ValueError, match="No YouTube API keys provided."):
        YouTubeService(api_keys=[], unverified=True)

def test_quota_usage(youtube_service):
    youtube_service.search_videos("test")
    youtube_service.fetch_video_metadata("test_video")
    quota = youtube_service.quota_usage
    expected_total = youtube_service.cost_tracking["search"] + youtube_service.cost_tracking["videos_list"]
    assert quota["total_cost"] == expected_total

def test_fetch_all_comments_unexpected_error(monkeypatch):
    behavior = {
        "commentThreads": dummy_comments_error,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    comments = service.fetch_all_comments("test_video")
    # 当获取评论时发生异常，返回应为空列表
    assert comments == []

def test_rebuild_request_without_uri(youtube_service):
    class NoUri:
        pass
    
    # Assert the exception message in English
    with pytest.raises(Exception, match="Missing uri"):
        youtube_service._rebuild_request(NoUri(), "search")


def test_rebuild_request_for_search(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?q=test_query&maxResults=10&type=video&videoEmbeddable=true&videoSyndicated=true&pageToken=ptoken"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "search", lambda: DummyResource("search", {"search": dummy_rebuild_search}))
    new_request = youtube_service._rebuild_request(old_request, "search")
    params = new_request.rebuilt_params
    assert params.get("q") == "test_query"
    assert int(params.get("maxResults")) == 10
    assert params.get("type") == "video"
    assert params.get("videoEmbeddable") == "true"
    assert params.get("videoSyndicated") == "true"
    assert params.get("pageToken") == "ptoken"

def test_rebuild_request_for_videos_list(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?id=test_video"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "videos", lambda: DummyResource("videos_list", {"videos_list": dummy_rebuild_videos_list}))
    new_request = youtube_service._rebuild_request(old_request, "videos_list")
    params = new_request.rebuilt_params
    assert params.get("id") == "test_video"
    assert params.get("part") == "snippet,statistics,contentDetails"

def test_rebuild_request_for_commentThreads(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?videoId=test_video&maxResults=50&pageToken=token123"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "commentThreads", lambda: DummyResource("commentThreads", {"commentThreads": dummy_rebuild_commentThreads}))
    new_request = youtube_service._rebuild_request(old_request, "commentThreads")
    params = new_request.rebuilt_params
    assert params.get("videoId") == "test_video"
    assert int(params.get("maxResults")) == 50
    assert params.get("pageToken") == "token123"
    assert params.get("part") == "snippet,replies"
    assert params.get("textFormat") == "plainText"

def test_rebuild_request_for_playlists(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?id=playlist1&channelId=UC123"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "playlists", lambda: DummyResource("playlists", {"playlists": dummy_rebuild_playlists}))
    new_request = youtube_service._rebuild_request(old_request, "playlists")
    params = new_request.rebuilt_params
    assert params.get("id") == "playlist1"
    assert params.get("channelId") == "UC123"
    assert params.get("part") == "snippet,contentDetails,status"

def test_rebuild_request_for_playlistItems(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?playlistId=playlist1&maxResults=50&pageToken=token456"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "playlistItems", lambda: DummyResource("playlistItems", {"playlistItems": dummy_rebuild_playlistItems}))
    new_request = youtube_service._rebuild_request(old_request, "playlistItems")
    params = new_request.rebuilt_params
    assert params.get("playlistId") == "playlist1"
    assert int(params.get("maxResults")) == 50
    assert params.get("pageToken") == "token456"
    assert params.get("part") == "snippet,contentDetails"

def test_fetch_all_comments_with_replies(monkeypatch):
    def dummy_comments_with_replies(kwargs):
        return DummyRequest(response={
            "items": [{
                "snippet": {
                    "topLevelComment": {
                        "id": "c1",
                        "snippet": {
                            "authorDisplayName": "TopAuthor",
                            "textDisplay": "TopComment",
                            "likeCount": 10,
                            "viewerRating": "none",
                            "moderationStatus": "published",
                            "publishedAt": "2021-01-01T00:00:00Z"
                        }
                    }
                },
                "replies": {
                    "comments": [
                        {
                            "id": "c1.1",
                            "snippet": {
                                "authorDisplayName": "ReplyAuthor1",
                                "textDisplay": "ReplyComment1",
                                "likeCount": 2,
                                "viewerRating": "none",
                                "moderationStatus": "published",
                                "publishedAt": "2021-01-01T01:00:00Z"
                            }
                        },
                        {
                            "id": "r2",
                            "snippet": {
                                "authorDisplayName": "ReplyAuthor2",
                                "textDisplay": "ReplyComment2",
                                "likeCount": 3,
                                "viewerRating": "none",
                                "moderationStatus": "published",
                                "publishedAt": "2021-01-01T02:00:00Z"
                            }
                        }
                    ]
                }
            }]
        })
    behavior = {
        "commentThreads": dummy_comments_with_replies,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "playlists": dummy_playlists_success,
        "playlistItems": dummy_playlistItems_success
    }
    dummy_service = DummyService(behavior)
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: dummy_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    comments = service.fetch_all_comments("test_video")
    # 应该获取到 1 个顶级评论和 2 个回复，共 3 条记录
    assert len(comments) == 3
    top_comment = comments[0]
    assert top_comment["comment_id"] == "c1"
    assert top_comment["parent_id"] is None
    reply1 = comments[1]
    assert reply1["parent_id"] == "c1"
    reply2 = comments[2]
    assert reply2["parent_id"] == "c1"

def test_rotate_key_single_key():
    service = YouTubeService(api_keys=["only_key"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    with pytest.raises(Exception, match="All API keys have been exhausted"):
        service.rotate_key()

def test_concurrent_rotate_key(monkeypatch):
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    def rotate():
        try:
            service.rotate_key()
        except Exception:
            pass
    threads = [threading.Thread(target=rotate) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert service.get_current_key() in ["key1", "key2"]

def test_build_service_unverified(monkeypatch, caplog):
    from googleapiclient.http import build_http
    dummy_http = type("DummyHTTP", (), {})()
    def dummy_build_http():
        return dummy_http
    monkeypatch.setattr("googleapiclient.http.build_http", dummy_build_http)
    def dummy_build(*args, **kwargs):
        return "dummy_service"
    monkeypatch.setattr("googleapiclient.discovery.build", dummy_build)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    assert service.service == "dummy_service"
    assert any("Building YouTube service with unverified SSL context" in record.message for record in caplog.records)

def test_execute_request_unexpected_exception(monkeypatch, youtube_service):
    class DummyRequestUnexpected:
        uri = "dummy_uri"
        def execute(self):
            raise ValueError("Unexpected error")
    with pytest.raises(ValueError, match="Unexpected error"):
        youtube_service._execute_request(DummyRequestUnexpected(), "search")

def test_api_key_single_string(monkeypatch):
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: DummyService({"search": dummy_search_success}))
    service = YouTubeService(api_keys="single_key", unverified=True, max_retries=1, backoff_factor=0, skip_key_check=True)
    assert isinstance(service.api_keys, list)
    assert service.api_keys[0] == "single_key"

def test_get_youtube_service():
    service = get_youtube_service("dummy_key", skip_key_check=True)
    assert isinstance(service, YouTubeService)
    assert service.get_current_key() == "dummy_key"
