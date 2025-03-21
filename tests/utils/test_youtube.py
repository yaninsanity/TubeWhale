import pytest
import time
import threading
import logging
from googleapiclient.errors import HttpError
from utils.youtube import YouTubeService

# DummyResponse：用于模拟 HttpError 中的响应对象
class DummyResponse:
    def __init__(self, reason="quotaExceeded", status=403):
        self.reason = reason
        self.status = status

# DummyRequest：用于模拟 request.execute() 行为
class DummyRequest:
    def __init__(self, response=None, error=False, error_text="", uri="dummy_uri?q=test&maxResults=25&type=video&videoEmbeddable=true&videoSyndicated=true"):
        self.response = response
        self.error = error
        self.error_text = error_text
        self.uri = uri

    def execute(self):
        if self.error:
            dummy_resp = DummyResponse(reason=self.error_text, status=403)
            raise HttpError(resp=dummy_resp, content=self.error_text.encode("utf-8"))
        return self.response

# DummyResource：根据传入的 call_type 调用对应的行为函数
class DummyResource:
    def __init__(self, call_type, behavior):
        self.call_type = call_type
        self.behavior = behavior

    def list(self, **kwargs):
        func = self.behavior.get(self.call_type)
        if not func:
            raise Exception(f"Unsupported call type: {self.call_type}")
        return func(kwargs)

# DummyService：为不同资源返回独立的 DummyResource
class DummyService:
    def __init__(self, behavior):
        """
        :param behavior: dict, key 为调用类型
         ("search", "videos_list", "commentThreads", "playlists", "playlistItems")
         value 为一个函数，输入参数 kwargs 返回 DummyRequest 对象
        """
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

# Dummy 模拟函数：成功返回搜索结果（视频）
def dummy_search_success(kwargs):
    return DummyRequest(response={"items": [{"id": {"videoId": "test_video"}}]})

# Dummy 模拟函数：成功返回视频详情
def dummy_videos_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "snippet": {"title": "Test Video"},
            "statistics": {"viewCount": "1000", "likeCount": "100", "commentCount": "10"},
            "contentDetails": {}
        }]
    })

# Dummy 模拟函数：成功返回单页评论（无 nextPageToken）
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

# Dummy 模拟函数：返回两页评论
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

# Dummy 模拟函数：成功返回播放列表元数据
def dummy_playlists_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "id": "playlist1",
            "snippet": {"title": "Test Playlist"},
            "contentDetails": {"itemCount": 5},
            "status": {"privacyStatus": "public"}
        }]
    })

# Dummy 模拟函数：成功返回播放列表项（单页）
def dummy_playlistItems_success(kwargs):
    return DummyRequest(response={
        "items": [{
            "snippet": {"title": "Playlist Item 1"},
            "contentDetails": {"videoId": "video1"}
        }]
    })

# Dummy 模拟函数：返回两页播放列表项
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

# Dummy 模拟函数：返回空视频详情（items 为空）
def dummy_videos_empty(kwargs):
    return DummyRequest(response={"items": []})

# Dummy 模拟函数：评论接口抛出非 quotaExceeded 错误
def dummy_comments_error(kwargs):
    return DummyRequest(error=True, error_text="some other error", uri="dummy_uri")

# 使用闭包生成 fresh_quota_then_success 函数，避免跨测试状态共享
def make_quota_then_success():
    call_count = {'count': 0}
    def inner(kwargs):
        if call_count['count'] == 0:
            call_count['count'] += 1
            return DummyRequest(error=True, error_text="quotaExceeded", uri="dummy_uri")
        else:
            return DummyRequest(response={"items": [{"id": {"videoId": "test_video"}}]}, uri="dummy_uri")
    return inner

# 以下为额外补充的重建请求模拟函数，记录传入参数
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

# Fixture：返回一个正常的 DummyService（包含所有功能）
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

# Fixture：构造 YouTubeService，并将 _build_service 方法替换为返回 dummy_service_success
@pytest.fixture
def youtube_service(monkeypatch, dummy_service_success):
    def dummy_build_service(self, api_key, unverified):
        return dummy_service_success
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
    return service

# ------------------------------
# 原有测试用例
# 测试搜索视频成功
def test_search_videos_success(youtube_service):
    response = youtube_service.search_videos("test")
    assert "items" in response
    assert youtube_service.cost_tracking["search"] >= 100

# 测试搜索频道成功
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

# 测试搜索播放列表成功
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

# 测试通用搜索接口附加过滤参数
def test_generic_search_with_filters(youtube_service):
    filters = {"order": "viewCount", "regionCode": "US"}
    response = youtube_service.search("test query", max_results=10, page_token="token123", resource_type="video", filters=filters)
    assert "items" in response

# 测试获取视频详情成功
def test_fetch_video_metadata_success(youtube_service):
    metadata = youtube_service.fetch_video_metadata("test_video")
    assert metadata["id"] == "test_video"
    assert metadata["snippet"]["title"] == "Test Video"
    assert metadata["view_count"] == 1000
    assert youtube_service.cost_tracking["videos_list"] >= 100

# 测试获取空视频详情返回 None
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    metadata = service.fetch_video_metadata("test_video")
    assert metadata is None

# 测试获取单页评论成功
def test_fetch_all_comments_success(youtube_service):
    comments = youtube_service.fetch_all_comments("test_video")
    assert len(comments) == 1
    assert comments[0]["comment_id"] == "c1"

# 测试获取多页评论成功
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=2, backoff_factor=0)
    comments = service.fetch_all_comments("test_video")
    assert len(comments) == 2
    assert comments[0]["comment_id"] == "c1"
    assert comments[1]["comment_id"] == "c2"

# 测试获取播放列表元数据成功
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    response = service.fetch_playlist_metadata("playlist1")
    assert response["id"] == "playlist1"
    assert response["snippet"]["title"] == "Test Playlist"
    assert response["contentDetails"]["itemCount"] == 5

# 测试获取单页播放列表项成功
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    items = service.fetch_playlist_items("playlist1", max_results=50)
    assert len(items) == 1
    assert items[0]["snippet"]["title"] == "Playlist Item 1"

# 测试获取多页播放列表项成功
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=2, backoff_factor=0)
    items = service.fetch_playlist_items("playlist1", max_results=50)
    assert len(items) == 2
    assert items[0]["snippet"]["title"] == "Playlist Item 1"
    assert items[1]["snippet"]["title"] == "Playlist Item 2"

# 测试 quotaExceeded 错误下的 API key 轮换
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
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
    response = service.search_videos("test")
    assert "items" in response
    # 验证轮换后的 key为 key2
    assert service.get_current_key() == "key2"

# 测试达到最大重试次数后抛出异常
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
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
    with pytest.raises(Exception, match="Max retries exceeded"):
        service.search_videos("test")

# 测试未提供 API key 时抛出异常
def test_empty_api_keys():
    with pytest.raises(ValueError, match="No YouTube API keys provided."):
        YouTubeService(api_keys=[], unverified=True)

# 测试 quota_usage 属性返回正确累计成本
def test_quota_usage(youtube_service):
    youtube_service.search_videos("test")
    youtube_service.fetch_video_metadata("test_video")
    quota = youtube_service.quota_usage
    expected_total = youtube_service.cost_tracking["search"] + youtube_service.cost_tracking["videos_list"]
    assert quota["total_cost"] == expected_total

# 测试评论接口抛出非 quotaExceeded 错误时异常能正确冒泡
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    with pytest.raises(HttpError, match="some other error"):
        service.fetch_all_comments("test_video")

# ------------------------------
# 以下为补充的改进测试，最小风险保证

# 测试 _rebuild_request 缺少 uri 属性时抛出异常
def test_rebuild_request_without_uri(youtube_service):
    class NoUri:
        pass
    with pytest.raises(Exception, match="无法重建请求对象，因为缺少 uri 属性。请在实际实现中保存请求参数。"):
        youtube_service._rebuild_request(NoUri(), "search")

# 测试 _rebuild_request 对 search 请求的重建
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

# 测试 _rebuild_request 对 videos_list 请求的重建
def test_rebuild_request_for_videos_list(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?id=test_video"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "videos", lambda: DummyResource("videos_list", {"videos_list": dummy_rebuild_videos_list}))
    new_request = youtube_service._rebuild_request(old_request, "videos_list")
    params = new_request.rebuilt_params
    assert params.get("id") == "test_video"
    assert params.get("part") == "snippet,statistics,contentDetails"

# 测试 _rebuild_request 对 commentThreads 请求的重建
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

# 测试 _rebuild_request 对 playlists 请求的重建
def test_rebuild_request_for_playlists(monkeypatch, youtube_service):
    dummy_uri = "dummy_uri?id=playlist1&channelId=UC123"
    old_request = DummyRequest(response={"items": []}, uri=dummy_uri)
    monkeypatch.setattr(youtube_service.service, "playlists", lambda: DummyResource("playlists", {"playlists": dummy_rebuild_playlists}))
    new_request = youtube_service._rebuild_request(old_request, "playlists")
    params = new_request.rebuilt_params
    assert params.get("id") == "playlist1"
    assert params.get("channelId") == "UC123"
    assert params.get("part") == "snippet,contentDetails,status"

# 测试 _rebuild_request 对 playlistItems 请求的重建
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

# 测试 fetch_all_comments 能正确处理回复（replies）情况
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
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    comments = service.fetch_all_comments("test_video")
    # 期望有 1 条顶层评论和 2 条回复，共 3 条评论
    assert len(comments) == 3
    top_comment = comments[0]
    assert top_comment["comment_id"] == "c1"
    assert top_comment["parent_id"] is None
    reply1 = comments[1]
    # 对于 id 为 "c1.1"，应分割为 parent_id "c1" 和 child_id "1"
    assert reply1["comment_id"] == "1"
    assert reply1["parent_id"] == "c1"
    reply2 = comments[2]
    # 对于 id "r2"，没有点，则 parent_id 为顶层评论的 id
    assert reply2["comment_id"] == "r2"
    assert reply2["parent_id"] == "c1"

# 测试当只有一个 API key 时调用 rotate_key 抛出异常
def test_rotate_key_single_key():
    service = YouTubeService(api_keys=["only_key"], unverified=True, max_retries=1, backoff_factor=0)
    with pytest.raises(Exception, match="All API keys have been exhausted"):
        service.rotate_key()

# 测试多线程下调用 rotate_key 的线程安全性
def test_concurrent_rotate_key(monkeypatch):
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=1, backoff_factor=0)
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
    # 最终使用的 key 应在提供的 key 列表中
    assert service.get_current_key() in ["key1", "key2"]

# 测试 _build_service 的 unverified 分支（检查日志警告和返回值）
def test_build_service_unverified(monkeypatch, caplog):
    from googleapiclient.http import build_http
    dummy_http = type("DummyHTTP", (), {})()
    def dummy_build_http():
        return dummy_http
    monkeypatch.setattr("googleapiclient.http.build_http", dummy_build_http)
    def dummy_build(*args, **kwargs):
        return "dummy_service"
    monkeypatch.setattr("googleapiclient.discovery.build", dummy_build)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    assert service.service == "dummy_service"
    assert any("Building YouTube service with unverified SSL context" in record.message for record in caplog.records)

# 测试 _execute_request 遇到非 quotaExceeded 的意外异常时能正确冒泡（例如 ValueError）
def test_execute_request_unexpected_exception(monkeypatch, youtube_service):
    class DummyRequestUnexpected:
        uri = "dummy_uri"
        def execute(self):
            raise ValueError("Unexpected error")
    with pytest.raises(ValueError, match="Unexpected error"):
        youtube_service._execute_request(DummyRequestUnexpected(), "search")

# 测试传入单个字符串 API key 时能正确转换为列表
def test_api_key_single_string(monkeypatch):
    monkeypatch.setattr(YouTubeService, "_build_service", lambda self, api_key, unverified: DummyService({"search": dummy_search_success}))
    service = YouTubeService(api_keys="single_key", unverified=True, max_retries=1, backoff_factor=0)
    assert isinstance(service.api_keys, list)
    assert service.api_keys[0] == "single_key"
