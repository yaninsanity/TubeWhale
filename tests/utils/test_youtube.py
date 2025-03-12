import pytest
import time
import logging
from googleapiclient.errors import HttpError
from utils.youtube import YouTubeService

# 修改 DummyResponse，添加 status 属性
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
            # 构造 DummyResponse，确保 resp.reason 与 resp.status 可用
            dummy_resp = DummyResponse(reason=self.error_text, status=403)
            raise HttpError(resp=dummy_resp, content=self.error_text.encode("utf-8"))
        return self.response

# DummyService 用于模拟 YouTube API 客户端
class DummyService:
    def __init__(self, behavior):
        """
        :param behavior: dict, key 为调用类型 ("search", "videos_list", "commentThreads")
                         value 为一个函数，输入参数 kwargs 返回 DummyRequest 对象
        """
        self.behavior = behavior

    def search(self):
        return self

    def videos(self):
        return self

    def commentThreads(self):
        return self

    def list(self, **kwargs):
        call_type = ""
        if "q" in kwargs:
            call_type = "search"
        elif "id" in kwargs:
            call_type = "videos_list"
        elif "videoId" in kwargs:
            call_type = "commentThreads"
        func = self.behavior.get(call_type)
        return func(kwargs)

# Dummy 模拟函数：成功返回搜索结果
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
        # 第一页，包含 nextPageToken
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
        # 第二页，无 nextPageToken
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

# Dummy 模拟函数：返回空视频详情（items 为空）
def dummy_videos_empty(kwargs):
    return DummyRequest(response={"items": []})

# Dummy 模拟函数：评论接口抛出非 quotaExceeded 错误
def dummy_comments_error(kwargs):
    return DummyRequest(error=True, error_text="some other error", uri="dummy_uri")

# fixture：返回一个正常的 DummyService
@pytest.fixture
def dummy_service_success():
    behavior = {
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success
    }
    return DummyService(behavior)

# fixture：构造 YouTubeService，并重写 _build_service 方法返回 dummy_service_success
@pytest.fixture
def youtube_service(monkeypatch, dummy_service_success):
    def dummy_build_service(self, api_key, unverified):
        return dummy_service_success
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
    return service

# 测试 search_videos 成功返回
def test_search_videos_success(youtube_service):
    response = youtube_service.search_videos("test")
    assert "items" in response
    assert youtube_service.cost_tracking["search"] >= 100

# 测试 fetch_video_metadata 成功返回
def test_fetch_video_metadata_success(youtube_service):
    metadata = youtube_service.fetch_video_metadata("test_video")
    assert metadata["id"] == "test_video"
    assert metadata["snippet"]["title"] == "Test Video"
    assert metadata["view_count"] == 1000
    assert youtube_service.cost_tracking["videos_list"] >= 100

# 测试 fetch_video_metadata 返回空数据时返回 None
def test_fetch_video_metadata_empty(monkeypatch):
    behavior = {
        "videos_list": dummy_videos_empty,
        "search": dummy_search_success,
        "commentThreads": dummy_comments_success
    }
    dummy_service = DummyService(behavior)
    def dummy_build_service(self, api_key, unverified):
        return dummy_service
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    metadata = service.fetch_video_metadata("test_video")
    assert metadata is None

# 测试 fetch_all_comments 成功返回单页评论
def test_fetch_all_comments_success(youtube_service):
    comments = youtube_service.fetch_all_comments("test_video")
    assert len(comments) == 1
    assert comments[0]["comment_id"] == "c1"

# 测试 fetch_all_comments 返回多页评论
def test_fetch_all_comments_multi_page(monkeypatch):
    behavior = {
        "commentThreads": dummy_comments_multi_page,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success,
    }
    dummy_service = DummyService(behavior)
    def dummy_build_service(self, api_key, unverified):
        return dummy_service
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=2, backoff_factor=0)
    comments = service.fetch_all_comments("test_video")
    assert len(comments) == 2
    assert comments[0]["comment_id"] == "c1"
    assert comments[1]["comment_id"] == "c2"

# # 测试 quotaExceeded 错误下的 API key 轮换
# def test_quota_exceeded_key_rotation(monkeypatch):
#     fresh_quota = make_quota_then_success()
#     behavior = {
#         "search": lambda kwargs: fresh_quota(kwargs),
#         "videos_list": dummy_videos_success,
#         "commentThreads": dummy_comments_success
#     }
#     dummy_service = DummyService(behavior)
#     def dummy_build_service(self, api_key, unverified):
#         return dummy_service
#     monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
#     monkeypatch.setattr(time, "sleep", lambda s: None)
#     service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
#     response = service.search_videos("test")
#     assert "items" in response
#     # 断言轮换后的 key 为 key2
#     assert service.get_current_key() == "key2"

# 测试达到最大重试次数后抛出异常
def test_max_retries_exceeded(monkeypatch):
    def always_quota(kwargs):
        return DummyRequest(error=True, error_text="quotaExceeded", uri="dummy_uri")
    behavior = {
        "search": always_quota,
        "videos_list": dummy_videos_success,
        "commentThreads": dummy_comments_success
    }
    dummy_service = DummyService(behavior)
    def dummy_build_service(self, api_key, unverified):
        return dummy_service
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    service = YouTubeService(api_keys=["key1", "key2"], unverified=True, max_retries=2, backoff_factor=0)
    with pytest.raises(Exception, match="Max retries exceeded"):
        service.search_videos("test")

# 测试当未提供 API key 时抛出异常
def test_empty_api_keys():
    with pytest.raises(ValueError, match="No YouTube API keys provided."):
        YouTubeService(api_keys=[], unverified=True)

# 测试 quota_usage 属性正确返回累计成本
def test_quota_usage(youtube_service):
    youtube_service.search_videos("test")
    youtube_service.fetch_video_metadata("test_video")
    quota = youtube_service.quota_usage
    expected_total = youtube_service.cost_tracking["search"] + youtube_service.cost_tracking["videos_list"]
    assert quota["total_cost"] == expected_total

# 测试评论接口抛出非 quotaExceeded 错误时，异常能正确冒泡
def test_fetch_all_comments_unexpected_error(monkeypatch):
    behavior = {
        "commentThreads": dummy_comments_error,
        "search": dummy_search_success,
        "videos_list": dummy_videos_success
    }
    dummy_service = DummyService(behavior)
    def dummy_build_service(self, api_key, unverified):
        return dummy_service
    monkeypatch.setattr(YouTubeService, "_build_service", dummy_build_service)
    service = YouTubeService(api_keys=["key1"], unverified=True, max_retries=1, backoff_factor=0)
    with pytest.raises(HttpError, match="some other error"):
        service.fetch_all_comments("test_video")
