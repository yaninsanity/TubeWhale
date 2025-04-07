import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

# 假设 SearchAgent 定义在 search_agent.py 文件中
from agents.search_agent import SearchAgent

# Dummy 数据库实现，用于记录调用（仅作为示例，不做任何实际存储）
class DummyDB:
    async def store_ai_interaction(self, input_data, output_data, interaction_type, tokens_used, cost, duration_ms):
        return

    async def store_keyword_analysis(self, record):
        return

# Dummy OpenAIService 模拟实现
class DummyOpenAIService:
    def __init__(self):
        # 模拟存储 prompt 模板
        self.prompts = {
            "keyword_generation": "dummy keyword prompt",
            "structured_output": "dummy structured prompt"
        }

    def get_prompt(self, prompt_template, variables=None):
        # 根据不同模板返回简单拼接后的字符串
        if prompt_template == "keyword_generation":
            return f"Generate keywords for: {variables['base_keyword']}"
        elif prompt_template == "structured_output":
            return f"Summarize: {variables['text']}"
        else:
            return f"Prompt for: {variables}"

    async def async_completion(self, prompt, prompt_template):
        # 模拟关键词生成和摘要生成的返回
        if "Generate keywords" in prompt:
            # 返回换行分隔的关键词列表
            return "test1\ntest2\n"
        elif "Summarize" in prompt:
            return "Summary of videos."
        else:
            return "Default response."

# Dummy YouTubeService 模拟实现
class DummyYouTubeService:
    def search_videos(self, q, max_results, filters):
        # 根据查询关键词返回模拟的搜索结果
        return {
            "items": [
                {
                    "id": {"videoId": f"{q}_vid1"},
                    "snippet": {
                        "title": f"{q} Video 1",
                        "description": "Description 1",
                        "publishedAt": "2023-01-01T00:00:00Z"
                    }
                },
                {
                    "id": {"videoId": f"{q}_vid2"},
                    "snippet": {
                        "title": f"{q} Video 2",
                        "description": "Description 2",
                        "publishedAt": "2023-01-02T00:00:00Z"
                    }
                },
            ]
        }

# pytest fixture 用于构造 SearchAgent 实例
@pytest.fixture
def search_agent():
    youtube_service = DummyYouTubeService()
    openai_service = DummyOpenAIService()
    db = DummyDB()
    agent = SearchAgent(youtube_service, openai_service, db=db)
    return agent

# 测试关键词生成
@pytest.mark.asyncio
async def test_generate_keywords(search_agent):
    keywords = await search_agent.generate_keywords("example")
    # 根据 DummyOpenAIService，返回的关键词列表为 ["test1", "test2"]
    assert keywords == ["test1", "test2"]

# 测试聚合搜索
@pytest.mark.asyncio
async def test_aggregate_search(search_agent):
    aggregated = await search_agent.aggregate_search("sample")
    # DummyOpenAIService 返回固定的 "test1" 与 "test2" 两个关键词
    assert "test1" in aggregated
    assert "test2" in aggregated
    # 每个关键词的搜索结果均由 DummyYouTubeService 返回 2 个视频
    assert len(aggregated["test1"]) == 2
    assert len(aggregated["test2"]) == 2

# 测试去重逻辑：对于相同 video_id，累计 weight
def test_deduplicate_results(search_agent):
    # 构造包含重复 video_id 的聚合结果
    aggregated = {
        "kw1": [
            {"search_keyword": "kw1", "video_id": "vid1", "title": "Title 1", "description": "Desc", "publish_time": "2023-01-01T00:00:00Z"},
            {"search_keyword": "kw1", "video_id": "vid2", "title": "Title 2", "description": "Desc", "publish_time": "2023-01-02T00:00:00Z"},
        ],
        "kw2": [
            {"search_keyword": "kw2", "video_id": "vid1", "title": "Title 1", "description": "Desc", "publish_time": "2023-01-01T00:00:00Z"},
        ]
    }
    deduped = search_agent.deduplicate_results(aggregated)
    # 应该得到两个唯一的视频 vid1 和 vid2
    video_ids = {item["video_id"] for item in deduped}
    assert video_ids == {"vid1", "vid2"}
    # 检查 vid1 的 weight 应为 kw1 中的 2 个结果加上 kw2 中的 1 个结果，总共 3
    for item in deduped:
        if item["video_id"] == "vid1":
            assert item["weight"] == 3

# 测试结果排序（优化）逻辑
def test_optimize_variations(search_agent):
    results = [
        {"video_id": "vid1", "weight": 5, "publish_time": "2023-01-01T00:00:00Z"},
        {"video_id": "vid2", "weight": 10, "publish_time": "2023-01-02T00:00:00Z"},
        {"video_id": "vid3", "weight": 7, "publish_time": "2023-01-03T00:00:00Z"},
    ]
    optimized = search_agent.optimize_variations(results)
    weights = [item["weight"] for item in optimized]
    # 应该按 weight 降序排序
    assert weights == sorted(weights, reverse=True)

# 测试精炼结果，默认按照发布时间降序返回 top_n 个视频
def test_refine_results(search_agent):
    results = [
        {"video_id": "vid1", "weight": 5, "publish_time": "2023-01-01T00:00:00Z"},
        {"video_id": "vid2", "weight": 10, "publish_time": "2023-01-03T00:00:00Z"},
        {"video_id": "vid3", "weight": 7, "publish_time": "2023-01-02T00:00:00Z"},
    ]
    refined = search_agent.refine_results(results, top_n=2)
    # 按发布时间降序排序，最新的应当是 vid2，其次是 vid3
    assert len(refined) == 2
    assert refined[0]["video_id"] == "vid2"
    assert refined[1]["video_id"] == "vid3"

# 测试摘要生成（利用 DummyOpenAIService 返回固定摘要）
@pytest.mark.asyncio
async def test_summarize_results(search_agent):
    results = [
        {"search_keyword": "kw1", "video_id": "vid1", "title": "Title 1", "description": "Desc", "publish_time": "2023-01-01T00:00:00Z"},
        {"search_keyword": "kw2", "video_id": "vid2", "title": "Title 2", "description": "Desc", "publish_time": "2023-01-02T00:00:00Z"},
    ]
    summary = await search_agent.summarize_results(results)
    assert "Summary of videos." in summary

# 测试完整的搜索工作流
@pytest.mark.asyncio
async def test_execute_search(search_agent):
    result = await search_agent.execute_search("sample")
    # 检查返回结果是否包含所有预期的键
    expected_keys = {
        "keywords_searched",
        "aggregated_by_keyword",
        "deduplicated_results",
        "refined_results",
        "total_unique_videos",
        "summary",
        "execution_time_seconds",
    }
    assert expected_keys.issubset(result.keys())
    # 检查 refined_results 是否为列表且数量不超过预期
    assert isinstance(result["refined_results"], list)
