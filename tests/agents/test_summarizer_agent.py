import sys
import os
import logging
import json
import asyncio
import re
import pytest
from datetime import datetime
from typing import Any, Dict, List, Optional

# Ensure the project's root directory is in sys.path so that the agents module can be imported.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from agents.summarizer_agent import (
    SummarizerAgent,
    gpt_summarizer_agent,
    dynamic_chunk_text,
    chunk_text_by_tokens,
    safe_format_prompt,
    JSON_TEMPLATE,
)

# ---------------- Dummy Implementations ----------------

class DummyDB:
    """A dummy database that records stored entries."""
    def __init__(self):
        self.records = []

    async def store_data_async(self, table: str, record: Dict[str, Any]):
        self.records.append((table, record))

    def store_data(self, table: str, record: Dict[str, Any]):
        self.records.append((table, record))

class DummyOpenAIService:
    """
    A dummy OpenAIService implementation that supports two modes:
      - mode="valid": returns a valid JSON string when summarizing.
      - mode="invalid": returns an invalid string to trigger reformatting.
    If the prompt contains "Reformat", it returns a valid JSON string.
    """
    def __init__(self, mode: str = "valid"):
        self.models = {"gpt-4o-mini"}
        self.default_model = "gpt-4o-mini"
        self.prompts = {
            "summarization": "Summarize: {text}\nPrevious summary: {previous_summary}",
        }
        self.mode = mode

    def get_prompt(self, prompt_name: str, variables: Dict[str, Any]) -> str:
        return self.prompts.get(prompt_name, "Default prompt: {text}")

    def completion(self, prompt: str, model: str) -> str:
        if "Reformat" in prompt:
            return json.dumps(JSON_TEMPLATE)
        if self.mode == "valid":
            return json.dumps(JSON_TEMPLATE)
        else:
            return "invalid"

# ---------------- Tests for dynamic_chunk_text ----------------

def test_dynamic_chunk_text_normal():
    """
    Test the dynamic_chunk_text function under normal conditions:
      1. Construct a text with multiple short paragraphs.
      2. Ensure the function produces multiple non-empty chunks.
      3. Verify that adjacent chunks contain the expected overlapping portion.
    """
    paragraph = "This is a test sentence to verify dynamic text chunking."
    text = "\n\n".join([paragraph] * 5)  # Create a text with 5 paragraphs.
    target_chunk_size = 100  # A small target size to force splitting.
    overlap_ratio = 0.2

    chunks = dynamic_chunk_text(text, target_chunk_size=target_chunk_size, overlap_ratio=overlap_ratio)
    # Expect more than one chunk.
    assert len(chunks) > 1, f"Expected more than 1 chunk, got {len(chunks)}"
    # Verify each chunk is non-empty.
    for idx, chunk in enumerate(chunks):
        assert len(chunk.strip()) > 0, f"Chunk {idx} is empty"
    # Verify alias function output matches.
    chunks_alias = chunk_text_by_tokens(text, target_chunk_size=target_chunk_size, overlap_ratio=overlap_ratio)
    assert chunks == chunks_alias, "Alias function output differs from dynamic_chunk_text"
    
    # Check overlapping: for each adjacent pair, the latter should start with the expected overlap plus a newline.
    for i in range(len(chunks) - 1):
        prev_chunk = chunks[i]
        curr_chunk = chunks[i + 1]
        expected_overlap_length = int(len(prev_chunk) * overlap_ratio)
        if expected_overlap_length > 0:
            expected_overlap = prev_chunk[-expected_overlap_length:]
            expected_prefix = expected_overlap + "\n"
            actual_prefix = curr_chunk[:len(expected_prefix)]
            assert actual_prefix == expected_prefix, (
                f"Chunk {i+1} does not start with the expected overlap. "
                f"Expected: {repr(expected_prefix)}, got: {repr(actual_prefix)}"
            )

def test_dynamic_chunk_text_overflow():
    """
    Test the dynamic_chunk_text function when a single paragraph exceeds the target_chunk_size:
      1. Construct a very long paragraph (one paragraph).
      2. Verify that the function produces multiple non-empty chunks.
    """
    # Construct a very long paragraph by repeating a sentence.
    paragraph = "This is a test sentence to verify dynamic text chunking. " * 1000
    text = paragraph  # Only one paragraph.
    target_chunk_size = 150  # A small target size.
    overlap_ratio = 0.2

    chunks = dynamic_chunk_text(text, target_chunk_size=target_chunk_size, overlap_ratio=overlap_ratio)
    # Ensure at least one chunk is produced.
    assert len(chunks) > 0, "No chunks produced"
    # Ensure all chunks are non-empty.
    for idx, chunk in enumerate(chunks):
        assert len(chunk.strip()) > 0, f"Chunk {idx} is empty"

# ---------------- Tests for safe_format_prompt ----------------

def test_safe_format_prompt():
    prompt_template = "Hello, {text}. Previous: {previous_summary}"
    variables = {"text": "Test"}
    formatted = safe_format_prompt(prompt_template, variables)
    assert "Test" in formatted
    assert "Previous:" in formatted

# ---------------- Tests for SummarizerAgent Methods ----------------

@pytest.fixture
def dummy_db():
    return DummyDB()

@pytest.fixture
def openai_valid():
    return DummyOpenAIService(mode="valid")

@pytest.fixture
def openai_invalid():
    return DummyOpenAIService(mode="invalid")

@pytest.fixture
def summarizer_agent_valid(openai_valid, dummy_db):
    return SummarizerAgent(
        openai_service=openai_valid,
        enable_chunking=True,
        debug_mode=True,
        db=dummy_db,
        concurrency=2,
        target_chunk_size=50,  # Small size for testing
        overlap_ratio=0.2,
        logger=logging.getLogger("test_logger")
    )

@pytest.fixture
def summarizer_agent_invalid(openai_invalid, dummy_db):
    return SummarizerAgent(
        openai_service=openai_invalid,
        enable_chunking=True,
        debug_mode=True,
        db=dummy_db,
        concurrency=2,
        target_chunk_size=50,
        overlap_ratio=0.2,
        logger=logging.getLogger("test_logger")
    )

def test_chunk_text(summarizer_agent_valid):
    long_text = ("This is a long test text to check text chunking. " * 5) + "\n" + ("Another long text segment. " * 5)
    chunks = summarizer_agent_valid.chunk_text(long_text)
    assert len(chunks) >= 1
    for chunk in chunks:
        assert isinstance(chunk, str)

@pytest.mark.asyncio
async def test_summarize_chunk_valid(summarizer_agent_valid):
    chunk = "This is a test chunk for summarization."
    summary = await summarizer_agent_valid.summarize_chunk(
        chunk, previous_summary="Initial summary", model="gpt-4o-mini", prompt_name="summarization"
    )
    try:
        data = json.loads(summary)
        for key in JSON_TEMPLATE.keys():
            assert key in data
    except json.JSONDecodeError:
        pytest.fail("summarize_chunk did not return a valid JSON string.")

def test_merge_summaries(summarizer_agent_valid):
    summaries = ["Summary part 1", "Summary part 2", "Summary part 3"]
    merged = summarizer_agent_valid.merge_summaries(summaries)
    assert "Summary part 1" in merged
    assert "Summary part 3" in merged

def test_validate_json_output():
    valid_text = json.dumps(JSON_TEMPLATE)
    agent = SummarizerAgent(openai_service=DummyOpenAIService(), debug_mode=True)
    data = agent.validate_json_output(valid_text)
    assert data is not None
    invalid_text = "this is not json"
    data2 = agent.validate_json_output(invalid_text)
    assert data2 is None

@pytest.mark.asyncio
async def test_reformat_summary_as_json(summarizer_agent_invalid):
    merged_summary = "invalid"
    reformatted = await summarizer_agent_invalid.reformat_summary_as_json(merged_summary, model="gpt-4o-mini")
    try:
        data = json.loads(reformatted)
        for key in JSON_TEMPLATE.keys():
            assert key in data
    except json.JSONDecodeError:
        pytest.fail("reformat_summary_as_json did not return a valid JSON string.")

# ---------------- Tests for Overall Summarization Flow ----------------

@pytest.mark.asyncio
async def test_summarize_valid(summarizer_agent_valid):
    long_text = "\n".join([f"Paragraph {i}: This is a long test text to trigger chunking. " * 5 for i in range(10)])
    final_summary = await summarizer_agent_valid.summarize(long_text, model="gpt-4o-mini", prompt_name="summarization")
    try:
        data = json.loads(final_summary)
        for key in JSON_TEMPLATE.keys():
            assert key in data
    except json.JSONDecodeError:
        pytest.fail("Final summary is not valid JSON.")

@pytest.mark.asyncio
async def test_summarize_invalid(summarizer_agent_invalid):
    long_text = "\n".join([f"Paragraph {i}: This is an invalid test text to trigger chunking. " * 5 for i in range(5)])
    final_summary = await summarizer_agent_invalid.summarize(long_text, model="gpt-4o-mini", prompt_name="summarization")
    try:
        data = json.loads(final_summary)
        for key in JSON_TEMPLATE.keys():
            assert key in data
    except json.JSONDecodeError:
        pytest.fail("Final summary after reformatting is not valid JSON.")

# ---------------- Tests for the Factory Function ----------------

def test_gpt_summarizer_agent_error():
    with pytest.raises(ValueError):
        _ = gpt_summarizer_agent("Test text")

@pytest.mark.asyncio
async def test_gpt_summarizer_agent_success(openai_valid, dummy_db):
    future = gpt_summarizer_agent(
        "This is a long text used to generate a summary.",
        model="gpt-4o-mini",
        openai_service=openai_valid,
        db=dummy_db,
        enable_chunking=False  # Summarize the text as a whole
    )
    result = await future
    try:
        data = json.loads(result)
        for key in JSON_TEMPLATE.keys():
            assert key in data
    except json.JSONDecodeError:
        pytest.fail("Factory function did not return a valid JSON summary.")
