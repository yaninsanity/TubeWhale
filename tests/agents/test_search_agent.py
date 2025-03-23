import pytest
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from agents.search_agent import SearchAgent

# Dummy OpenAIService 模拟
class DummyOpenAIService:
    def get_prompt(self, prompt_template: str, variables: Dict[str, Any] = None) -> Dict[str, str]:
        if prompt_template == "keyword_generation":
            return {"user": "keyword1\nkeyword2\nkeyword3"}
        if prompt_template == "structured_output":
            # 返回模拟的摘要提示模板
            return {"user": f"Summary: Dummy summary for text: {variables.get('text', '')}"}
        return {"user": variables.get("input", "") if variables else ""}

    def completion(self, prompt: str = "", prompt_template: str = "", template_vars: Dict[str, Any] = None, **kwargs) -> str:
        # 针对关键词扩展模板，返回固定关键词变体
        if prompt_template == "keyword_generation":
            return "keyword1\nkeyword2\nkeyword3"
        # 如果 prompt 中包含 "summary:" 则返回固定摘要文本
        if "summary:" in prompt.lower():
            return "Summary: Found videos for keyword1 (3), keyword2 (3), keyword3 (3)."
        return prompt

# Dummy YouTubeService 模拟
class DummyYouTubeService:
    def search_videos(self, q: str, max_results: int = 10, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        items = []
        for i in range(max_results):
            video_id = f"{q}_vid_{i}"
            item = {
                "id": {"videoId": video_id},
                "snippet": {
                    "title": f"Title for {q} video {i}",
                    "description": f"Description for {q} video {i}",
                    "publishedAt": (now - timedelta(seconds=i * 60)).isoformat().replace("+00:00", "Z")
                }
            }
            items.append(item)
        return {"items": items}

@pytest.fixture
def dummy_openai_service():
    return DummyOpenAIService()

@pytest.fixture
def dummy_youtube_service():
    return DummyYouTubeService()

@pytest.fixture
def default_settings() -> Dict[str, Any]:
    return {
        "default_filter": {"videoEmbeddable": "true", "videoSyndicated": "true"},
        "max_results": 3,
        "enable_brainstorm": True,
        "brainstorm_prompt_template": "keyword_generation",
        "enable_refine": True,
        "order_by": "weight",
        "order_direction": "desc",
        "enable_optimization": True,
        "enable_summary": True,
    }

@pytest.fixture
def search_agent(dummy_youtube_service, dummy_openai_service, default_settings):
    return SearchAgent(dummy_youtube_service, dummy_openai_service, settings=default_settings)

@pytest.mark.asyncio
async def test_generate_keywords_with_brainstorm(search_agent):
    keywords = search_agent.generate_keywords("input_keyword")
    assert keywords == ["keyword1", "keyword2", "keyword3"]

def test_generate_keywords_without_brainstorm(search_agent):
    search_agent.settings["enable_brainstorm"] = False
    keywords = search_agent.generate_keywords("input_keyword")
    assert keywords == ["input_keyword"]

def test_search_by_keyword(search_agent):
    results = search_agent.search_by_keyword("example")
    assert len(results) == 3
    for i, item in enumerate(results):
        assert item["video_id"] == f"example_vid_{i}"
        assert f"Title for example video {i}" in item["title"]

def test_aggregate_search(search_agent):
    aggregated = search_agent.aggregate_search("base")
    expected_keys = {"keyword1", "keyword2", "keyword3"}
    assert set(aggregated.keys()) == expected_keys
    for kw, lst in aggregated.items():
        assert len(lst) == 3

def test_deduplicate_results(search_agent):
    aggregated = {
        "kw1": [
            {"video_id": "vid_1", "search_keyword": "kw1", "title": "Title 1", "publishedAt": "2020-01-01T00:00:00Z"},
            {"video_id": "vid_2", "search_keyword": "kw1", "title": "Title 2", "publishedAt": "2020-01-02T00:00:00Z"},
        ],
        "kw2": [
            {"video_id": "vid_2", "search_keyword": "kw2", "title": "Title 2", "publishedAt": "2020-01-02T00:00:00Z"},
            {"video_id": "vid_3", "search_keyword": "kw2", "title": "Title 3", "publishedAt": "2020-01-03T00:00:00Z"},
        ]
    }
    deduped = search_agent.deduplicate_results(aggregated)
    vids = {item["video_id"] for item in deduped}
    assert vids == {"vid_1", "vid_2", "vid_3"}
    for item in deduped:
        if item["video_id"] == "vid_2":
            assert item["weight"] == 4

def test_optimize_variations(search_agent):
    results = [
        {"video_id": "vid_1", "weight": 2},
        {"video_id": "vid_2", "weight": 5},
        {"video_id": "vid_3", "weight": 3},
    ]
    optimized = search_agent.optimize_variations(results)
    assert optimized[0]["video_id"] == "vid_2"
    assert optimized[1]["video_id"] == "vid_3"
    assert optimized[2]["video_id"] == "vid_1"

def test_refine_results(search_agent):
    results = [
        {"video_id": "vid_1", "publish_time": "2020-01-01T00:00:00Z", "title": "A", "weight": 2},
        {"video_id": "vid_2", "publish_time": "2020-01-03T00:00:00Z", "title": "B", "weight": 3},
        {"video_id": "vid_3", "publish_time": "2020-01-02T00:00:00Z", "title": "C", "weight": 1},
    ]
    refined = search_agent.refine_results(results, top_n=2)
    refined_ids = [item["video_id"] for item in refined]
    # 预期 refined 中应包含表现较好的（排序后前 2 个）视频 id
    assert set(refined_ids) <= {"vid_2", "vid_3"}

def test_summarize_results(search_agent):
    # 假设 SearchAgent.execute_search 返回的结果中包含 summary 字段
    # 为了测试，这里模拟直接调用 summarize_results 方法（你需要确保 SearchAgent 中有该方法）
    if not hasattr(search_agent, "summarize_results"):
        # 如果没有 summarize_results 方法，则使用 execute_search 返回的 summary 字段
        result = search_agent.execute_search("sample")
        summary = result.get("summary", "")
    else:
        summary = search_agent.summarize_results([
            {"video_id": "vid_1", "search_keyword": "keyword1", "title": "Title 1"},
            {"video_id": "vid_2", "search_keyword": "keyword1", "title": "Title 2"},
            {"video_id": "vid_3", "search_keyword": "keyword2", "title": "Title 3"},
        ])
    # 检查返回的摘要中包含 "Summary:" 字样（根据 DummyOpenAIService.completion 模拟返回）
    assert "Summary:" in summary

@pytest.mark.asyncio
async def test_execute_search(search_agent):
    result = await search_agent.execute_search("sample")
    expected_keywords = {"keyword1", "keyword2", "keyword3"}
    assert set(result["keywords_searched"]) == expected_keywords
    for kw, lst in result["aggregated_by_keyword"].items():
        assert len(lst) == 3
    assert result["total_unique_videos"] <= 9
    assert len(result["refined_results"]) > 0
    assert search_agent.last_search_result == result
