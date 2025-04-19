import pytest
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import pytest_asyncio

from agents.search_agent import SearchAgent

# --- Dummy implementations for dependencies ----------------------

@pytest.fixture
def dummy_openai():
    class DummyOpenAI:
        def __init__(self):
            # need both prompts
            self.prompts = {
                "keyword_generation": {"system": "", "user": ""},
                "summarization": {"system": "", "user": ""},
                "structured_output": {"system": "", "user": ""},
            }
            self.interactions = []
        async def async_completion(self, *, prompt: str, prompt_template: str):
            # when generating keywords, return two lines
            if prompt_template == "keyword_generation":
                return "kwA\nkwB\n"
            # otherwise return a fixed summary
            return "DUMMY_SUMMARY"
        def get_prompt(self, prompt_name: str, variables: Optional[Dict[str, Any]] = None) -> str:
            # just return a recognizable string
            return f"<PROMPT {prompt_name} {variables}>"
    return DummyOpenAI()

@pytest.fixture
def dummy_youtube():
    class DummyYT:
        def search_videos(self, q: str, max_results: int, filters: Dict[str, Any]):
            # always return two items
            return {
                "items": [
                    {
                        "id": {"videoId": "X1"},
                        "snippet": {
                            "title": f"Title {q}-1",
                            "description": "D1",
                            "publishedAt": "2022-01-02T00:00:00Z",
                        }
                    },
                    {
                        "id": {"videoId": "X2"},
                        "snippet": {
                            "title": f"Title {q}-2",
                            "description": "D2",
                            "publishedAt": "2022-01-01T00:00:00Z",
                        }
                    },
                ]
            }
    return DummyYT()

@pytest.fixture
def dummy_db():
    class DummyDB:
        def __init__(self):
            self.ai_interactions = []
            self.keyword_analysis = []
        async def store_ai_interaction(self, **kwargs):
            self.ai_interactions.append(kwargs)
        async def store_keyword_analysis(self, records: List[Dict[str, Any]]):
            self.keyword_analysis.extend(records)
    return DummyDB()

@pytest.fixture
def agent(dummy_youtube, dummy_openai, dummy_db):
    # minimal settings to exercise all branches
    settings = {
        "enable_brainstorm": True,
        "brainstorm_prompt_template": "keyword_generation",
        "max_keywords": 2,
        "max_results": 2,
        "enable_refine": True,
        "enable_optimization": True,
        "order_by": "weight",
        "order_direction": "desc",
        "enable_summary": True,
    }
    # quiet logging
    logger = logging.getLogger("test_search_agent")
    logger.setLevel(logging.CRITICAL)
    return SearchAgent(
        youtube_service=dummy_youtube,
        openai_service=dummy_openai,
        db=dummy_db,
        settings=settings,
        logger=logger,
    )

# --- Tests ---------------------------------------------------------

@pytest.mark.asyncio
async def test_generate_keywords_and_db_logging(agent):
    kws = await agent.generate_keywords("baseKW")
    assert kws == ["kwA", "kwB"]
    # check AI interaction logged
    assert len(agent.db.ai_interactions) == 1
    entry = agent.db.ai_interactions[0]
    assert entry["interaction_type"] == "keyword_generation"
    assert entry["input_data"]["base_keyword"] == "baseKW"

@pytest.mark.asyncio
async def test_generate_keywords_disabled(agent):
    agent.settings["enable_brainstorm"] = False
    kws = await agent.generate_keywords("solo")
    assert kws == ["solo"]
    # no new interactions
    assert agent.db.ai_interactions == []

@pytest.mark.asyncio
async def test_aggregate_search_calls_search_by_keyword(agent):
    agg = await agent.aggregate_search("foo")
    # should have two keys
    assert set(agg.keys()) == {"kwA", "kwB"}
    # each keyword list items should map properly
    for kw, lst in agg.items():
        assert all(item["search_keyword"] == kw for item in lst)
        assert {item["video_id"] for item in lst} == {"X1", "X2"}

def test_search_by_keyword_transforms_items(agent):
    # direct call
    results = agent.search_by_keyword("Z", filters={"any": "x"})
    assert len(results) == 2
    first = results[0]
    assert first["search_keyword"] == "Z"
    assert first["video_id"] in {"X1", "X2"}
    assert "Title Z" in first["title"]

def test_deduplicate_results_accumulates_weight(agent):
    aggregated = {
        "a": [{"video_id": "V1"}, {"video_id": "V2"}],
        "b": [{"video_id": "V1"}],
    }
    dedup = agent.deduplicate_results(aggregated)
    # two unique
    ids = {item["video_id"] for item in dedup}
    assert ids == {"V1", "V2"}
    # V1 saw 2 in 'a' + 1 in 'b' = 3, V2 saw 2 in 'a'
    m = {item["video_id"]: item for item in dedup}
    assert m["V1"]["weight"] == 3
    assert m["V2"]["weight"] == 2

def test_optimize_variations(agent):
    data = [
        {"video_id": "u", "weight": 1},
        {"video_id": "v", "weight": 5},
        {"video_id": "w", "weight": 3},
    ]
    sorted_ = agent.optimize_variations(data)
    assert [d["video_id"] for d in sorted_] == ["v", "w", "u"]

def test_refine_results_respects_publish_time(agent):
    # weight doesn't matter here because refine resorts by publish_time
    items = [
        {"video_id": "u", "weight": 10, "publish_time": "2022-01-01T00:00:00Z"},
        {"video_id": "v", "weight": 1,  "publish_time": "2022-01-03T00:00:00Z"},
        {"video_id": "w", "weight": 5,  "publish_time": "2022-01-02T00:00:00Z"},
    ]
    top2 = agent.refine_results(items, top_n=2)
    # expect two most recent by date: v (Jan 3), w (Jan 2)
    assert [i["video_id"] for i in top2] == ["v", "w"]

@pytest.mark.asyncio
async def test_summarize_results_and_no_db(agent):
    # two keywords
    results = [
        {"search_keyword": "kwA", "title": "T1"},
        {"search_keyword": "kwB", "title": "T2"},
    ]
    summary = await agent.summarize_results(results)
    assert summary == "DUMMY_SUMMARY"
    # shouldn't record keyword_analysis here
    assert agent.db.keyword_analysis == []

@pytest.mark.asyncio
async def test_summarize_results_empty(agent):
    # empty list → gets the no‑videos message
    # disable summary flag
    agent.settings["enable_summary"] = False
    summary = await agent.summarize_results([])
    assert summary == ""

    agent.settings["enable_summary"] = True
    summary2 = await agent.summarize_results([])
    assert summary2 == "No videos found to summarize."

@pytest.mark.asyncio
async def test_execute_search_full_flow(agent):
    out = await agent.execute_search("baseX")
    # top‑level keys
    for k in ("keywords_searched","aggregated_by_keyword","deduplicated_results",
              "refined_results","total_unique_videos","summary","execution_time_seconds"):
        assert k in out
    assert set(out["keywords_searched"]) == {"kwA","kwB"}
    assert out["summary"] == "DUMMY_SUMMARY"
    # should have stored keyword_analysis
    assert len(agent.db.keyword_analysis) == 1
    rec = agent.db.keyword_analysis[0]
    assert rec["keyword"] == "baseX"
    # timestamp is a string
    assert isinstance(rec["timestamp"], str)
    # last_search_result reflects the same dict
    assert agent.last_search_result == out
