import pytest
import asyncio
import json
from agents.summarizer_agent import (
    SummarizerAgent,
    gpt_summarizer_agent,
    chunk_text_by_tokens
)

# Dummy OpenAIService 模拟
class DummyOpenAIService:
    def __init__(self):
        self.fail_get_prompt = False
        self.invalid_json = False

    def get_prompt(self, prompt_name: str, variables: dict = None) -> dict:
        if self.fail_get_prompt:
            raise Exception("Simulated get_prompt failure")
        if prompt_name == "summarization":
            text = variables.get("text", "")
            prev = variables.get("previous_summary", "")
            return {"prompt": f"Summarization prompt. Previous Summary: {prev}. Text: {text}"}
        return {"prompt": "Default prompt"}

    def completion(self, prompt: str, model: str = "gpt-4o-mini", **kwargs) -> str:
        if self.invalid_json or "fail_json" in prompt:
            return "This is not a JSON string"
        if "chunk_test" in prompt:
            result = {
                "main_topic": "Test Topic",
                "key_insights": "Insight A; Insight B",
                "recommended_tools": "Tool X, Tool Y",
                "best_practices": "Practice X",
                "challenges_and_advice": "Challenge Z"
            }
            return json.dumps(result)
        # 默认返回 JSON 格式摘要
        result = {
            "main_topic": "Default Topic",
            "key_insights": "Default Insight",
            "recommended_tools": "Default Tool",
            "best_practices": "Default Practice",
            "challenges_and_advice": "Default Advice"
        }
        return json.dumps(result)

class DummyOpenAIServiceWrapper:
    """
    包装 DummyOpenAIService 以模拟 OpenAIService 实例。
    """
    def __init__(self, dummy: DummyOpenAIService):
        self.dummy = dummy
        self.models = {"dummy": {}}
        self.prompts = {
            "summarization": {"prompt": "Summarization prompt. Previous Summary: {{ previous_summary }}. Text: {{ text }}"},
            "default": {"prompt": "Default prompt"}
        }
        self.default_model = "dummy"
        self.default_prompt = "default"

    def get_prompt(self, prompt_name, variables=None):
        return self.dummy.get_prompt(prompt_name, variables)

    def completion(self, **kwargs):
        return self.dummy.completion(kwargs.get("prompt", ""), model=kwargs.get("model", "gpt-4o-mini"))

@pytest.fixture
def dummy_openai_service():
    return DummyOpenAIService()

@pytest.fixture
def openai_service_instance(dummy_openai_service):
    return DummyOpenAIServiceWrapper(dummy_openai_service)

def test_chunk_text_by_tokens():
    # 为了验证切片效果，设置较小的 max_tokens 参数
    text = "A" * 3500
    # 这里设置 max_tokens=100, overlap=20，预期生成至少 3 个片段
    chunks = chunk_text_by_tokens(text, max_tokens=100, overlap=20)
    assert len(chunks) >= 3

@pytest.mark.asyncio
async def test_summarize_chunk_success(openai_service_instance):
    agent = SummarizerAgent(openai_service_instance, debug_mode=True)
    chunk = "This is a chunk_test segment."
    summary = await agent.summarize_chunk(chunk, previous_summary="Context info", model="gpt-4o-mini", prompt_name="summarization")
    # 应返回 JSON 格式的摘要字符串
    try:
        parsed = json.loads(summary)
    except Exception:
        parsed = None
    assert isinstance(parsed, dict)
    for key in ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]:
        assert key in parsed

@pytest.mark.asyncio
async def test_summarize_chunk_fallback(openai_service_instance, dummy_openai_service):
    dummy_openai_service.fail_get_prompt = True
    agent = SummarizerAgent(openai_service_instance, debug_mode=True)
    chunk = "Fallback test chunk."
    summary = await agent.summarize_chunk(chunk, previous_summary="Context", model="gpt-4o-mini", prompt_name="summarization")
    assert summary != ""

def test_merge_summaries():
    agent = SummarizerAgent(openai_service=None)
    summaries = ["Summary part 1.", "Summary part 2.", "Summary part 3."]
    merged = agent.merge_summaries(summaries)
    assert "Summary part 1." in merged
    assert "Summary part 3." in merged

def test_validate_json_output_success():
    agent = SummarizerAgent(openai_service=None)
    valid_json = json.dumps({
        "main_topic": "Topic",
        "key_insights": "Insight",
        "recommended_tools": "Tool",
        "best_practices": "Practice",
        "challenges_and_advice": "Advice"
    })
    parsed = agent.validate_json_output(valid_json)
    assert isinstance(parsed, dict)
    assert parsed["main_topic"] == "Topic"

def test_validate_json_output_fail():
    agent = SummarizerAgent(openai_service=None)
    invalid_json = "Not a JSON string"
    parsed = agent.validate_json_output(invalid_json)
    assert parsed is None

@pytest.mark.asyncio
async def test_summarize_with_chunking(openai_service_instance):
    agent = SummarizerAgent(openai_service_instance, enable_chunking=True, debug_mode=True)
    # 构造长文本，使其切分成多个片段。这里用 max_tokens 参数控制切片效果。
    long_text = "Chunk test text. " * 500  # 生成较长文本
    final_summary = await agent.summarize(long_text, model="gpt-4o-mini", prompt_name="summarization")
    assert final_summary != ""
    try:
        data = json.loads(final_summary)
        assert isinstance(data, dict)
    except json.JSONDecodeError:
        assert isinstance(final_summary, str)

@pytest.mark.asyncio
async def test_summarize_without_chunking(openai_service_instance):
    agent = SummarizerAgent(openai_service_instance, enable_chunking=False, debug_mode=True)
    text = "This is a test summary without chunking. " * 50
    final_summary = await agent.summarize(text, model="gpt-4o-mini", prompt_name="summarization")
    assert final_summary != ""

@pytest.mark.asyncio
async def test_factory_function(openai_service_instance):
    long_text = "Factory function test text. " * 200
    future = gpt_summarizer_agent(long_text, model="gpt-4o-mini", openai_service=openai_service_instance, enable_chunking=True, debug_mode=True, prompt_name="summarization")
    result = await future
    try:
        data = json.loads(result)
        assert isinstance(data, dict)
    except json.JSONDecodeError:
        assert isinstance(result, str)
    assert result != ""
