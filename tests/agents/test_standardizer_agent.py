import pytest
import asyncio
import json
from agents.standardizer_agent import StandardizerAgent, standardizer_agent

# Dummy OpenAIService 模拟，用于测试 get_prompt 与 completion 方法
class DummyOpenAIService:
    def __init__(self):
        self.last_prompt_vars = None
        self.fail_get_prompt = False  # 模拟 get_prompt 失败
        self.always_fail_json = False  # 模拟 completion 返回非 JSON

    def get_prompt(self, prompt_name: str, variables: dict = None) -> dict:
        if self.fail_get_prompt:
            raise Exception("Simulated get_prompt failure")
        if prompt_name == "structured_output":
            self.last_prompt_vars = variables
            # 返回模拟的 YAML 模板，其中模板字符串中用 {{ text }} 占位
            return {"prompt": f"Structured output prompt. Text: {variables.get('text', '')}"}
        return {"prompt": "Default prompt"}

    def completion(self, prompt: str, model: str = "gpt-4", **kwargs) -> str:
        # 如果 always_fail_json 为 True 或 prompt 中包含 "fail_json"，返回无法解析的文本
        if self.always_fail_json or "fail_json" in prompt:
            return "This is not a JSON string"
        # 如果 prompt 中包含 "slice_test"，返回固定的 JSON 格式响应
        if "slice_test" in prompt:
            result = {
                "main_topic": "Test Topic",
                "key_insights": "Insight A; Insight B",
                "recommended_tools": "Tool X, Tool Y",
                "best_practices": "Practice X",
                "challenges_and_advice": "Challenge Z"
            }
            return json.dumps(result)
        # 默认返回固定 JSON 响应
        result = {
            "main_topic": "Default Topic",
            "key_insights": "Default Insight",
            "recommended_tools": "Default Tool",
            "best_practices": "Default Practice",
            "challenges_and_advice": "Default Advice"
        }
        return json.dumps(result)

@pytest.fixture
def dummy_openai_service():
    return DummyOpenAIService()

class DummyOpenAIServiceWrapper:
    """
    包装 DummyOpenAIService 模拟一个 OpenAIService 实例，
    提供 models、prompts、default_model、default_prompt 属性。
    """
    def __init__(self, dummy):
        self.dummy = dummy
        self.models = {"dummy": {}}
        self.prompts = {
            "structured_output": {"prompt": "Structured output prompt. Text: {{ text }}"},
            "default": {"prompt": "Default prompt"}
        }
        self.default_model = "dummy"
        self.default_prompt = "default"
    def get_prompt(self, prompt_name, variables=None):
        return self.dummy.get_prompt(prompt_name, variables)
    def completion(self, **kwargs):
        return self.dummy.completion(kwargs.get("prompt", ""), model=kwargs.get("model", "gpt-4"))

@pytest.fixture
def openai_service_instance(dummy_openai_service):
    return DummyOpenAIServiceWrapper(dummy_openai_service)

# 测试 _build_prompt 正常流程
@pytest.mark.asyncio
async def test_build_prompt_success(openai_service_instance):
    agent = StandardizerAgent(openai_service_instance, debug_mode=True)
    summary = "This is a valid test summary."
    prompt = agent._build_prompt(summary)
    assert "Structured output prompt" in prompt
    assert summary in prompt
    # 检查反向注入的 JSON 模板是否存在（预期包含 "main_topic" 关键字）
    assert '"main_topic"' in prompt

# 测试 _build_prompt 异常时的 fallback 逻辑
def test_build_prompt_fallback(dummy_openai_service):
    dummy_openai_service.fail_get_prompt = True
    wrapper = DummyOpenAIServiceWrapper(dummy_openai_service)
    agent = StandardizerAgent(wrapper, debug_mode=True)
    summary = "Fallback test summary."
    prompt = agent._build_prompt(summary)
    assert "You are an expert in summarizing" in prompt
    assert summary in prompt

# 测试 _slice_text 方法
def test_slice_text():
    agent = StandardizerAgent(openai_service=None)
    short_text = "Short text."
    long_text = "A" * 3500  # 3500 个字符
    slices = agent._slice_text(short_text)
    assert len(slices) == 1
    slices_long = agent._slice_text(long_text, max_length=1000)
    assert len(slices_long) == 4

# 测试 _merge_results 方法
def test_merge_results():
    agent = StandardizerAgent(openai_service=None)
    # 模拟多个片段结果，其中第二个为 None
    results = [None, {"main_topic": "Topic", "key_insights": "Insight", "recommended_tools": "Tool",
                      "best_practices": "Practice", "challenges_and_advice": "Advice"}]
    merged = agent._merge_results(results)
    assert merged is not None
    assert isinstance(merged, dict)
    assert merged["main_topic"] == "Topic"

# 测试当 enable_standardization 为 False 时直接返回原始摘要
@pytest.mark.asyncio
async def test_standardize_disabled(openai_service_instance):
    agent = StandardizerAgent(openai_service_instance, enable_standardization=False)
    summary = "Test summary."
    result = await agent.standardize(summary)
    assert result == summary

# 测试空摘要返回 None
@pytest.mark.asyncio
async def test_standardize_empty_summary(openai_service_instance):
    agent = StandardizerAgent(openai_service_instance, debug_mode=True)
    result = await agent.standardize("")
    assert result is None

# 测试标准化成功返回 JSON 输出（触发 slice_test 标识）
@pytest.mark.asyncio
async def test_standardize_success(openai_service_instance):
    agent = StandardizerAgent(openai_service_instance, debug_mode=True)
    summary = "This is a slice_test summary for standardization."
    result = await agent.standardize(summary)
    assert isinstance(result, dict)
    for key in ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]:
        assert key in result

# 测试 API 返回非 JSON 内容时返回原始文本
@pytest.mark.asyncio
async def test_standardize_invalid_json(openai_service_instance, dummy_openai_service):
    dummy_openai_service.always_fail_json = True
    agent = StandardizerAgent(openai_service_instance, debug_mode=True)
    summary = "This summary will trigger fail_json."
    result = await agent.standardize(summary)
    assert isinstance(result, str)
    assert "This is not a JSON string" in result

# 测试长文本切片流程（模拟长文本），返回第一个有效片段结果
@pytest.mark.asyncio
async def test_standardize_long_text(openai_service_instance):
    agent = StandardizerAgent(openai_service_instance, debug_mode=True)
    long_summary = "LongText " * 1000  # 足够长以触发切片
    result = await agent.standardize(long_summary)
    assert result is not None

# 测试工厂函数 standardizer_agent 返回 Future 对象并能正确调用
@pytest.mark.asyncio
async def test_factory_function(openai_service_instance):
    summary = "Factory function test summary. slice_test"
    future = standardizer_agent(summary, model="gpt-4", openai_service=openai_service_instance, debug_mode=True)
    result = await future
    assert isinstance(result, dict)
    assert "main_topic" in result
