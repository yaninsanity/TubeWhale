import asyncio
import json
import pytest

from agents.summarizer_agent import dynamic_chunk_text, SummarizerAgent, gpt_summarizer_agent, JSON_TEMPLATE


class DummyService:
    """
    A dummy OpenAIService stub for testing summarization behavior.
    mode="valid": completion returns valid JSON.
    mode="invalid": returns invalid JSON first, then valid JSON on structured_output.
    """
    def __init__(self, mode="valid"):
        self.default_model = "test-model"
        self.mode = mode

    def completion(self, *, model, prompt_template, template_vars):
        if prompt_template == "summarization":
            return (json.dumps(JSON_TEMPLATE)
                    if self.mode == "valid"
                    else "not_json")
        # structured_output always returns valid JSON_TEMPLATE
        if prompt_template == "structured_output":
            return json.dumps(JSON_TEMPLATE)
        return json.dumps(JSON_TEMPLATE)


def test_dynamic_chunk_text_paragraph_merge():
    # With a large target_chunk_size, all paragraphs get merged into one chunk
    text = "Para1.\n\nPara2.\n\nPara3."
    chunks = dynamic_chunk_text(text, target_chunk_size=50, overlap_ratio=0.2)
    assert isinstance(chunks, list)
    assert len(chunks) == 1
    assert chunks[0] == "Para1.\nPara2.\nPara3."


def test_dynamic_chunk_text_paragraph_split_when_small_target():
    # If we force a small target_chunk_size, paragraphs must split into multiple chunks
    text = "P1\n\nP2\n\nP3\n\nP4\n\nP5"
    chunks = dynamic_chunk_text(text, target_chunk_size=5, overlap_ratio=0.1)
    assert len(chunks) >= 2
    for c in chunks:
        assert isinstance(c, str)
        # each chunk should not be empty and should respect the small size
        assert len(c) <= 6  # allow a little overlap


def test_dynamic_chunk_text_empty_and_whitespace():
    assert dynamic_chunk_text("", target_chunk_size=10) == []
    assert dynamic_chunk_text("   \n\n  ", target_chunk_size=10) == []


@pytest.mark.asyncio
async def test_summarize_valid_json():
    service = DummyService(mode="valid")
    agent = SummarizerAgent(service, concurrency=2, target_chunk_size=50, overlap_ratio=0.1, max_rounds=1)
    text = "Test valid JSON summary."
    result = await agent.summarize(
        text,
        model="any",
        summarization_prompt="summarization",
        output_prompt="structured_output"
    )
    data = json.loads(result)
    for key in JSON_TEMPLATE:
        assert key in data


@pytest.mark.asyncio
async def test_summarize_invalid_then_fallback():
    service = DummyService(mode="invalid")
    agent = SummarizerAgent(service, concurrency=2, target_chunk_size=50, overlap_ratio=0.1, max_rounds=1)
    text = "Test fallback JSON summary."
    result = await agent.summarize(
        text,
        model="any",
        summarization_prompt="summarization",
        output_prompt="structured_output"
    )
    data = json.loads(result)
    for key in JSON_TEMPLATE:
        assert key in data


@pytest.mark.asyncio
async def test_summarize_empty_text_raises():
    service = DummyService()
    agent = SummarizerAgent(service)
    with pytest.raises(ValueError):
        await agent.summarize("   ")


@pytest.mark.asyncio
async def test_gpt_summarizer_agent_future():
    service = DummyService(mode="valid")
    future = gpt_summarizer_agent(
        "Some long text to summarize.",
        service=service,
        model="any",
        concurrency=1,
        target_chunk_size=100,
        overlap_ratio=0.1,
        max_rounds=1
    )
    assert isinstance(future, asyncio.Future)
    result = await future
    data = json.loads(result)
    for key in JSON_TEMPLATE:
        assert key in data
