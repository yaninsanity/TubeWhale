import logging
import ssl
import random
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# YouTube API 的 Discovery URL
DISCOVERY_URL = "https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest"


class YouTubeService:
    """
    YouTubeService 封装了 YouTube Data API 的调用，提供搜索、视频详情、评论获取等接口。

    改进内容：
      - 增加重试机制与指数退避：每个 API 请求失败时会自动重试（默认最大重试次数可配置）。
      - 自动轮换 API key：在遇到 quotaExceeded 错误时自动轮换，如果所有 key 均失效则抛出异常。
      - 请求重构：针对 googleapiclient 请求对象不可重复使用的问题，增加了重建请求的逻辑。
      - 详细日志记录：便于追踪问题。
    """

    def __init__(self, api_keys, unverified=False, max_retries=3, backoff_factor=1):
        """
        :param api_keys: API key 列表或单个 key（字符串）
        :param unverified: 是否使用不验证 SSL 的上下文（仅作 fallback，不推荐）
        :param max_retries: 每个 API 请求的最大重试次数
        :param backoff_factor: 指数退避因子（单位秒）
        """
        if isinstance(api_keys, str):
            api_keys = [api_keys]
        if not api_keys:
            raise ValueError("No YouTube API keys provided.")
        self.api_keys = api_keys[:]  # 复制列表，避免修改原始列表
        self.shuffle_keys()
        self.current_key_index = 0
        self.unverified = unverified
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        # 记录不同 API 调用的成本（示例：固定成本 100）
        self.cost_tracking = {"search": 0, "videos_list": 0}
        current_key = self.get_current_key()
        logging.info(f"Initializing YouTubeService with API key: {current_key}")
        self.service = self._build_service(current_key, unverified)

    def shuffle_keys(self):
        """随机打乱 API key 列表"""
        random.shuffle(self.api_keys)
        logging.info("Shuffled YouTube API keys.")

    def get_current_key(self):
        """返回当前使用的 API key"""
        return self.api_keys[self.current_key_index]

    def rotate_key(self):
        """
        轮换到下一个 API key，并重新构造 service。
        如果所有 key 均已尝试，则抛出异常。
        """
        previous_key = self.get_current_key()
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        new_key = self.get_current_key()
        if new_key == previous_key:
            logging.error("All API keys have been exhausted.")
            raise Exception("All API keys have been exhausted due to quota limits.")
        logging.info(f"Rotated YouTube API key. Now using key: {new_key}")
        self.service = self._build_service(new_key, self.unverified)
        return new_key

    def _build_service(self, api_key, unverified=False):
        """
        构造 YouTube API 客户端。
        """
        try:
            if unverified:
                logging.warning("Building YouTube service with unverified SSL context. This is insecure!")
                from googleapiclient.http import build_http
                http = build_http()
                http.ssl_context = ssl._create_unverified_context()
                service = build("youtube", "v3", developerKey=api_key, http=http,
                                credentials=None, cache_discovery=False,
                                discoveryServiceUrl=DISCOVERY_URL)
            else:
                service = build("youtube", "v3", developerKey=api_key,
                                credentials=None, cache_discovery=False,
                                discoveryServiceUrl=DISCOVERY_URL)
            logging.info("YouTube service initialized successfully.")
            return service
        except HttpError as e:
            logging.error(f"HttpError while building YouTube service: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while building YouTube service: {e}")
            raise

    def _execute_request(self, request, call_type):
        """
        执行 API 请求，自动处理 quotaExceeded 错误及重试。

        :param request: API 请求对象
        :param call_type: 调用类型（"search", "videos_list", "commentThreads" 等），用于成本统计和请求重建
        """
        attempts = 0
        while attempts < self.max_retries:
            try:
                # 累计调用成本（示例：每次调用增加固定成本 100）
                if call_type in self.cost_tracking:
                    self.cost_tracking[call_type] += 100
                response = request.execute()
                return response
            except HttpError as e:
                error_text = e.content.decode("utf-8") if e.content else str(e)
                if "quotaExceeded" in error_text:
                    logging.error(f"Quota exceeded with key {self.get_current_key()} during {call_type}.")
                    try:
                        self.rotate_key()
                    except Exception as rotate_exception:
                        logging.error("All API keys exhausted. Aborting request.")
                        raise rotate_exception
                    attempts += 1
                    sleep_time = self.backoff_factor * (2 ** attempts)
                    logging.info(f"Retrying {call_type} request in {sleep_time} seconds (attempt {attempts}).")
                    time.sleep(sleep_time)
                    # 重建请求对象，因为部分请求对象可能不可重复使用
                    request = self._rebuild_request(request, call_type)
                    continue
                else:
                    logging.error(f"HTTP error in {call_type}: {error_text}")
                    raise
            except Exception as e:
                logging.error(f"Unexpected error in {call_type}: {e}")
                raise
        raise Exception(f"Max retries exceeded for {call_type} request.")

    def _rebuild_request(self, old_request, call_type):
        """
        尝试重建 API 请求对象。
        注意：googleapiclient 的请求对象可能没有直接的参数存储，这里简单从 URI 中解析参数（实际场景可能需要更复杂的逻辑）。
        目前支持：search、videos.list、commentThreads.list
        """
        if not hasattr(old_request, "uri"):
            raise Exception("无法重建请求对象，因为缺少 uri 属性。请在实际实现中保存请求参数。")
        params = old_request.uri.split("?")[1] if "?" in old_request.uri else ""
        query_params = dict(item.split("=") for item in params.split("&") if "=" in item)
        if call_type == "search":
            new_request = self.service.search().list(
                part="snippet",
                q=query_params.get("q", ""),
                maxResults=int(query_params.get("maxResults", 25)),
                type="video",
                videoEmbeddable=query_params.get("videoEmbeddable", "true"),
                videoSyndicated=query_params.get("videoSyndicated", "true"),
                pageToken=query_params.get("pageToken")
            )
            return new_request
        elif call_type == "videos_list":
            new_request = self.service.videos().list(
                part="snippet,statistics,contentDetails",
                id=query_params.get("id", "")
            )
            return new_request
        elif call_type == "commentThreads":
            new_request = self.service.commentThreads().list(
                part="snippet,replies",
                videoId=query_params.get("videoId", ""),
                maxResults=int(query_params.get("maxResults", 100)),
                textFormat="plainText",
                pageToken=query_params.get("pageToken")
            )
            return new_request
        else:
            raise Exception(f"Unsupported call_type for rebuilding request: {call_type}")

    def search_videos(self, keyword, max_results=25, page_token=None):
        """
        使用 search.list 接口搜索视频。
        :param keyword: 搜索关键词
        :param max_results: 返回视频数量（单页），默认为25
        :param page_token: 分页令牌
        :return: API 响应字典
        """
        request = self.service.search().list(
            part="snippet",
            q=keyword,
            maxResults=max_results,
            type="video",
            videoEmbeddable="true",
            videoSyndicated="true",
            pageToken=page_token
        )
        return self._execute_request(request, call_type="search")

    def fetch_video_metadata(self, video_id):
        """
        获取视频详细元数据。
        :param video_id: 视频 ID
        :return: 包含 snippet、statistics、contentDetails 的字典，并转换部分字段为整数
        """
        request = self.service.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        )
        response = self._execute_request(request, call_type="videos_list")
        if not response.get("items"):
            logging.error(f"No metadata found for video ID {video_id}")
            return None
        video_info = response["items"][0]
        snippet = video_info.get("snippet", {})
        stats = video_info.get("statistics", {})
        content_details = video_info.get("contentDetails", {})
        metadata = {
            "id": video_id,
            "snippet": snippet,
            "statistics": stats,
            "contentDetails": content_details,
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0))
        }
        return metadata

    def fetch_all_comments(self, video_id):
        """
        获取视频所有评论（包括回复）。
        :param video_id: 视频 ID
        :return: 评论列表，每条评论为一个字典
        """
        all_comments = []
        request = self.service.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText"
        )
        while request:
            response = self._execute_request(request, call_type="commentThreads")
            for item in response.get("items", []):
                top_comment = item["snippet"]["topLevelComment"]
                top_snippet = top_comment["snippet"]
                top_id = top_comment["id"]
                all_comments.append({
                    "comment_id": top_id,
                    "author": top_snippet["authorDisplayName"],
                    "text": top_snippet["textDisplay"],
                    "like_count": top_snippet.get("likeCount", 0),
                    "viewer_rating": top_snippet.get("viewerRating", "none"),
                    "moderation_status": top_snippet.get("moderationStatus", "published"),
                    "publish_time": top_snippet["publishedAt"],
                    "parent_id": None
                })
                if "replies" in item and "comments" in item["replies"]:
                    for reply in item["replies"]["comments"]:
                        reply_snippet = reply["snippet"]
                        reply_id = reply["id"]
                        if "." in reply_id:
                            parent_id, child_id = reply_id.split(".", 1)
                        else:
                            parent_id = top_id
                            child_id = reply_id
                        all_comments.append({
                            "comment_id": child_id,
                            "author": reply_snippet["authorDisplayName"],
                            "text": reply_snippet["textDisplay"],
                            "like_count": reply_snippet.get("likeCount", 0),
                            "viewer_rating": reply_snippet.get("viewerRating", "none"),
                            "moderation_status": reply_snippet.get("moderationStatus", "published"),
                            "publish_time": reply_snippet["publishedAt"],
                            "parent_id": parent_id
                        })
            if "nextPageToken" in response:
                request = self.service.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    textFormat="plainText",
                    pageToken=response["nextPageToken"]
                )
            else:
                request = None
        logging.info(f"Fetched {len(all_comments)} comments for video ID: {video_id}")
        return all_comments

    @property
    def quota_usage(self):
        """返回当前调用成本统计信息。"""
        total_cost = self.cost_tracking["search"] + self.cost_tracking["videos_list"]
        return {
            "search_cost": self.cost_tracking["search"],
            "videos_list_cost": self.cost_tracking["videos_list"],
            "total_cost": total_cost
        }
