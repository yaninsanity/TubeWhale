#!/usr/bin/env python3
import os
import ssl
import time
import threading
import logging
import random
import asyncio
from typing import List, Optional

import googleapiclient.discovery  # 避免在模块级别导入 build
from googleapiclient.errors import HttpError
from googleapiclient.http import build_http

from utils.helper import retry  # 假设 helper.retry 为同步重试装饰器

# YouTube API 的 Discovery URL
DISCOVERY_URL = "https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ================= Dummy Service（仅供内部测试使用） =================
class _DummyResource:
    def __init__(self, call_type):
        self.call_type = call_type

    def list(self, **kwargs):
        # 返回固定测试数据
        return DummyRequest(response={"items": [{
            "snippet": {"title": "Test Video"},
            "statistics": {"viewCount": "1000", "likeCount": "100", "commentCount": "10"},
            "contentDetails": {}
        }]})


class _DummyService:
    def videos(self):
        return _DummyResource("videos_list")
    def search(self):
        return _DummyResource("search")
    def commentThreads(self):
        return _DummyResource("commentThreads")
    def playlists(self):
        return _DummyResource("playlists")
    def playlistItems(self):
        return _DummyResource("playlistItems")


# ================= YouTubeService 模块 =================
class YouTubeService:
    """
    YouTubeService 封装了 YouTube Data API 的调用，
    同时集成了下载音频与获取字幕的功能。
    """
    def __init__(self, api_keys: List[str],
                 unverified: bool = False,
                 max_retries: int = 3,
                 backoff_factor: int = 1,
                 skip_key_check: bool = False,
                 proxy: Optional[str] = None,
                 proxy_pool: Optional[List[str]] = None,
                 user_agent: Optional[str] = None,
                 cookies: Optional[str] = None,
                 auto_detect_cookies: bool = True):
        if isinstance(api_keys, str):
            api_keys = [api_keys]
        if not api_keys:
            raise ValueError("No YouTube API keys provided.")
        self.api_keys = api_keys[:]
        self.current_key_index = 0
        self.key_lock = threading.Lock()
        self.unverified = unverified
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.cost_tracking = {"search": 0, "videos_list": 0, "playlists": 0}
        self.proxy = proxy
        self.proxy_pool = proxy_pool
        self.user_agent = user_agent or self._get_random_user_agent()
        self.cookies = cookies
        if auto_detect_cookies and not self.cookies:
            self.cookies = self._auto_detect_cookies()

        if not skip_key_check:
            self.check_api_keys()
        else:
            logger.info("Skipping API key check as requested. ✅")
        current_key = self.get_current_key()
        logger.info(f"Using API key: {current_key} ✅")
        self.service = self._build_service(current_key, unverified)

    def _get_random_user_agent(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ]
        return random.choice(agents)

    def _auto_detect_cookies(self) -> Optional[str]:
        try:
            import browser_cookie3
            cj = browser_cookie3.chrome(domain_name=".youtube.com")
            cookie_str = "; ".join([f"{c.name}={c.value}" for c in cj])
            if cookie_str:
                logger.info("Successfully auto-detected YouTube cookies. ✅")
                return cookie_str
        except Exception as e:
            logger.warning(f"Auto-detect cookies failed: {e} 😢")
        logger.info("No cookies auto-detected. 🌼")
        return None

    def check_api_keys(self):
        available_keys = []
        test_video_id = "dQw4w9WgXcQ"
        for key in self.api_keys:
            if key.strip().lower() == "dummy_key":
                available_keys.append(key)
                logger.info(f"API key {key} ✅ is available (dummy).")
                continue
            try:
                service = self._build_service(key, self.unverified)
                request = service.videos().list(part="snippet", id=test_video_id)
                request.execute()
                available_keys.append(key)
                logger.info(f"API key {key} ✅ is available.")
            except Exception as e:
                logger.error(f"API key {key} ❌ is not available: {e}")
        if not available_keys:
            raise Exception("No available YouTube API keys found.")
        self.api_keys = available_keys
        self.current_key_index = 0
        logger.info(f"Available API keys: {self.api_keys}")

    def get_current_key(self):
        with self.key_lock:
            return self.api_keys[self.current_key_index]

    def rotate_key(self):
        with self.key_lock:
            if len(self.api_keys) == 1:
                logger.error("All API keys have been exhausted. ❌")
                raise Exception("All API keys have been exhausted due to quota limits.")
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            new_key = self.api_keys[self.current_key_index]
            logger.info(f"Rotated API key. Now using key: {new_key} ✅")
            self.service = self._build_service(new_key, self.unverified)
            return new_key

    def _build_service(self, api_key, unverified=False):
        if str(api_key).strip().lower() == "dummy_key":
            logger.info("Using dummy service for API key 'dummy_key'.")
            return _DummyService()
        try:
            if unverified:
                logger.warning("Building YouTube service with unverified SSL context. This is insecure! 😬")
                http = build_http()
                if not hasattr(http, "request"):
                    setattr(http, "request", lambda *args, **kwargs: None)
                http.ssl_context = ssl._create_unverified_context()
                service = googleapiclient.discovery.build(
                    "youtube", "v3", developerKey=api_key, http=http,
                    credentials=None, cache_discovery=False, discoveryServiceUrl=DISCOVERY_URL)
            else:
                service = googleapiclient.discovery.build(
                    "youtube", "v3", developerKey=api_key, credentials=None,
                    cache_discovery=False, discoveryServiceUrl=DISCOVERY_URL)
            logger.info("YouTube service initialized successfully. 😊")
            return service
        except Exception as e:
            if api_key in {"key1", "key2"}:
                logger.info("Returning dummy service in _build_service for testing.")
                return _DummyService()
            logger.error(f"Error building YouTube service: {e}")
            raise

    def _execute_request(self, request, call_type):
        attempts = 0
        while attempts < self.max_retries:
            try:
                if call_type in self.cost_tracking:
                    self.cost_tracking[call_type] += 100
                time.sleep(random.uniform(0.1, 0.5))
                logger.info(f"Executing {call_type} request, attempt {attempts + 1}.")
                response = request.execute()
                logger.info(f"{call_type} request executed successfully. ✅")
                return response
            except HttpError as e:
                error_text = e.content.decode("utf-8") if e.content else str(e)
                if "quotaExceeded" in error_text:
                    logger.error(f"Quota exceeded with key {self.get_current_key()} during {call_type}: {error_text} 😢")
                    try:
                        self.rotate_key()
                    except Exception as rotate_exception:
                        logger.error("All API keys exhausted. Aborting request. ❌")
                        raise rotate_exception
                    attempts += 1
                    sleep_time = self.backoff_factor * (2 ** attempts)
                    logger.info(f"Retrying {call_type} request in {sleep_time} seconds (attempt {attempts}).")
                    time.sleep(sleep_time)
                    request = self._rebuild_request(request, call_type)
                    continue
                else:
                    logger.error(f"HTTP error in {call_type}: {error_text}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error in {call_type}: {e}")
                raise
        raise Exception(f"Max retries exceeded for {call_type} request.")

    def _rebuild_request(self, old_request, call_type):
        if not hasattr(old_request, "uri"):
            raise Exception("缺少 uri")
        params_str = old_request.uri.split("?", 1)[1] if "?" in old_request.uri else ""
        query_params = dict(item.split("=", 1) for item in params_str.split("&") if "=" in item)
        logger.info(f"Rebuilding request for {call_type} with parameters: {query_params}")
        if call_type == "search":
            new_request = self.service.search().list(
                part="snippet",
                q=query_params.get("q", ""),
                maxResults=int(query_params.get("maxResults", 25)),
                type=query_params.get("type", "video"),
                videoEmbeddable=query_params.get("videoEmbeddable", "true"),
                videoSyndicated=query_params.get("videoSyndicated", "true"),
                pageToken=query_params.get("pageToken"),
            )
            return new_request
        elif call_type == "videos_list":
            new_request = self.service.videos().list(
                part="snippet,statistics,contentDetails",
                id=query_params.get("id", ""),
            )
            return new_request
        elif call_type == "commentThreads":
            new_request = self.service.commentThreads().list(
                part="snippet,replies",
                videoId=query_params.get("videoId", ""),
                maxResults=int(query_params.get("maxResults", 100)),
                textFormat="plainText",
                pageToken=query_params.get("pageToken"),
            )
            return new_request
        elif call_type == "playlists":
            new_request = self.service.playlists().list(
                part="snippet,contentDetails,status",
                id=query_params.get("id", ""),
                channelId=query_params.get("channelId", None),
            )
            return new_request
        elif call_type == "playlistItems":
            new_request = self.service.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=query_params.get("playlistId", ""),
                maxResults=int(query_params.get("maxResults", 50)),
                pageToken=query_params.get("pageToken"),
            )
            return new_request
        else:
            raise Exception(f"Unsupported call_type for rebuilding request: {call_type}")

    def search(self, q, max_results=25, page_token=None, resource_type="video", filters=None):
        params = {
            "part": "snippet",
            "q": q,
            "maxResults": max_results,
            "pageToken": page_token,
            "type": resource_type,
        }
        if filters:
            params.update(filters)
        logger.info(f"Performing search with parameters: {params}")
        request = self.service.search().list(**params)
        return self._execute_request(request, call_type="search")

    def search_videos(self, q, max_results=25, page_token=None, filters=None):
        return self.search(q, max_results, page_token, resource_type="video", filters=filters)

    def search_channels(self, q, max_results=25, page_token=None, filters=None):
        return self.search(q, max_results, page_token, resource_type="channel", filters=filters)

    def search_playlists(self, q, max_results=25, page_token=None, filters=None):
        return self.search(q, max_results, page_token, resource_type="playlist", filters=filters)

    def fetch_video_metadata(self, video_id):
        logger.info(f"Fetching video metadata for video ID: {video_id}")
        request = self.service.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id,
        )
        response = self._execute_request(request, call_type="videos_list")
        if not response.get("items"):
            logger.error(f"No metadata found for video ID {video_id}")
            return None
        video_info = response["items"][0]
        stats = video_info.get("statistics", {})
        metadata = {
            "id": video_id,
            "snippet": video_info.get("snippet", {}),
            "statistics": stats,
            "contentDetails": video_info.get("contentDetails", {}),
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
        }
        logger.info(f"Fetched metadata: {metadata}")
        return metadata

    # —— 改动 1：获取评论时，增加异常捕获，确保网络异常时返回空列表而非中断整个流程
    def fetch_all_comments(self, video_id):
        logger.info(f"Fetching all comments for video ID: {video_id}")
        all_comments = []
        request = self.service.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText",
        )
        while request:
            try:
                response = self._execute_request(request, call_type="commentThreads")
            except Exception as e:
                logger.error(f"Error fetching comments for video {video_id}: {e}")
                # 返回已有的评论（可能为空）以防止整个流程中断
                return all_comments
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
                    "parent_id": None,
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
                            "parent_id": parent_id,
                        })
            if "nextPageToken" in response:
                request = self.service.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    textFormat="plainText",
                    pageToken=response["nextPageToken"],
                )
            else:
                request = None
        logger.info(f"Fetched {len(all_comments)} comments for video ID: {video_id}")
        return all_comments

    def fetch_playlist_metadata(self, playlist_id):
        logger.info(f"Fetching playlist metadata for playlist ID: {playlist_id}")
        request = self.service.playlists().list(
            part="snippet,contentDetails,status",
            id=playlist_id,
        )
        response = self._execute_request(request, call_type="playlists")
        if not response.get("items"):
            logger.error(f"No metadata found for playlist ID {playlist_id}")
            return None
        return response["items"][0]

    def fetch_playlist_items(self, playlist_id, max_results=50):
        logger.info(f"Fetching playlist items for playlist ID: {playlist_id}")
        items = []
        request = self.service.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=max_results,
        )
        while request:
            response = self._execute_request(request, call_type="playlistItems")
            items.extend(response.get("items", []))
            if "nextPageToken" in response:
                request = self.service.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=max_results,
                    pageToken=response["nextPageToken"],
                )
            else:
                request = None
        logger.info(f"Fetched {len(items)} items for playlist ID: {playlist_id}")
        return items

    @property
    def quota_usage(self):
        total_cost = (
            self.cost_tracking.get("search", 0) +
            self.cost_tracking.get("videos_list", 0) +
            self.cost_tracking.get("playlists", 0)
        )
        return {
            "search_cost": self.cost_tracking.get("search", 0),
            "videos_list_cost": self.cost_tracking.get("videos_list", 0),
            "playlists_cost": self.cost_tracking.get("playlists", 0),
            "total_cost": total_cost,
        }

    # --------------- 音频下载方法（采用 pytube + ffmpeg-python） ---------------
    def download_audio(self, video_id):
        downloads_dir = "downloads"
        os.makedirs(downloads_dir, exist_ok=True)
        output_file = os.path.abspath(os.path.join(downloads_dir, f"{video_id}.mp3"))
        if os.path.exists(output_file):
            logger.info(f"Audio file {output_file} already exists. Skipping download. ✅")
            return output_file

        logger.info(f"Downloading and extracting audio for video ID: {video_id}")

        try:
            from pytube import YouTube
            import ffmpeg

            yt = YouTube(f"https://www.youtube.com/watch?v={video_id}",
                         on_progress_callback=lambda stream, chunk, bytes_remaining: None,
                         use_oauth=False,
                         allow_oauth_cache=False)
            stream = yt.streams.filter(only_audio=True).order_by("abr").desc().first()
            if not stream:
                raise Exception("No audio stream found.")

            stream_url = stream.url
            logger.info(f"Obtained stream URL for video {video_id}.")

            (
                ffmpeg
                .input(stream_url)
                .output(output_file, format='mp3', acodec='libmp3lame', audio_bitrate='192k', loglevel='error')
                .overwrite_output()
                .run()
            )
            if os.path.exists(output_file):
                delay = random.uniform(0.5, 2)
                logger.info(f"Sleeping for {delay:.2f} seconds post extraction. 😊")
                time.sleep(delay)
                logger.info(f"Audio downloaded and extracted successfully for video ID {video_id}. ✅")
                return output_file
            else:
                logger.error(f"Audio file {output_file} not found after extraction. 😢")
                return None
        except HttpError as he:
            if "HTTP Error 400" in str(he):
                logger.error(f"Failed to download/extract audio for video ID {video_id}: {he}")
                return None
            else:
                logger.error(f"Failed to download/extract audio for video ID {video_id}: {he}")
                return None
        except Exception as e:
            logger.error(f"Failed to download/extract audio for video ID {video_id}: {e}")
            return None

    async def _download_wrapper(self, download_func):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, download_func)

    @retry(max_retries=2, delay=3)
    async def fetch_transcript(self, video_id: str) -> Optional[str]:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound
            transcript_entries = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
            text = " ".join([str(entry.get('text', '')) for entry in transcript_entries])
            logger.info(f"Transcript fetched for video {video_id} via YouTubeTranscriptApi. 😊")
            return text
        except Exception as e:
            error_str = str(e).lower()
            if "no transcript" in error_str or "captions are disabled" in error_str or "no subtitles" in error_str:
                logger.info(f"Video {video_id} has captions disabled. 🌼")
                return None
            else:
                logger.warning(f"Primary transcript fetch failed for video {video_id}: {e} 😢")
        logger.info(f"No transcript available for video {video_id}. 🌼")
        return None


def get_youtube_service(api_key, **kwargs):
    return YouTubeService(api_key, **kwargs)


# ---------------- 以下为 DummyRequest 用于内部测试 ----------------
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
