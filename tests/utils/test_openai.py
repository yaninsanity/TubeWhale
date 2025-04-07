import asyncio
import os
import tempfile
import yaml
import pytest
from datetime import datetime
from io import BytesIO

# Ensure the module path is correct.
from utils.openAIServices import OpenAIService

# ---------------- Dummy Client Implementation ----------------

class DummyChatCompletionsStream:
    def create(self, messages, **kwargs):
        # Simulate a streaming response by yielding two dictionary chunks.
        yield {"choices": [{"delta": {"content": "dummy stream part 1"}}]}
        yield {"choices": [{"delta": {"content": "dummy stream part 2"}}]}

class DummyCompletionsStream:
    def create(self, prompt, **kwargs):
        # Simulate a streaming response for non-chat completions.
        yield {"choices": [{"text": "dummy stream text part 1"}]}
        yield {"choices": [{"text": "dummy stream text part 2"}]}

class DummyChatCompletions:
    def create(self, messages, **kwargs):
        # Return a non-stream chat response.
        return {
            "choices": [{"message": {"content": "dummy chat response"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20}
        }

class DummyCompletions:
    def create(self, prompt, **kwargs):
        # Return a non-stream text completion.
        return {
            "choices": [{"text": "dummy text response"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 10}
        }

class DummyEmbeddings:
    def create(self, **kwargs):
        return {
            "data": [{"embedding": [0.1, 0.2, 0.3]}],
            "usage": {"prompt_tokens": 3}
        }

class DummyAudioTranscriptions:
    def create(self, file, model, response_format, **kwargs):
        return {"text": "dummy transcription"}

class DummyClient:
    def __init__(self):
        # For non-streaming completions.
        self.chat = type("DummyChat", (), {
            "completions": DummyChatCompletions()
        })
        self.completions = DummyCompletions()
        self.embeddings = DummyEmbeddings()
        self.audio = type("DummyAudio", (), {
            "transcriptions": DummyAudioTranscriptions()
        })
        # For streaming, add separate attributes.
        self.chat_stream = type("DummyChatStream", (), {
            "completions": DummyChatCompletionsStream()
        })
        self.completions_stream = DummyCompletionsStream()

# ---------------- Fixtures ----------------

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def service_with_client(dummy_client):
    service = OpenAIService(api_key="sk-dummy")
    # Override the client for non-streaming methods.
    service.client = dummy_client
    # For streaming, we'll override _retry_api_call to use our streaming attributes.
    # Here we simulate that if 'stream' is True and model type is 'chat', then use dummy_client.chat_stream.completions.create.
    original_retry = service._retry_api_call
    def retry_wrapper(func, *args, **kwargs):
        if kwargs.get("stream"):
            # Decide which streaming method to use based on the function passed.
            if func.__name__ == "create" and "messages" in kwargs:
                return dummy_client.chat_stream.completions.create(*args, **kwargs)
            elif func.__name__ == "create":
                return dummy_client.completions_stream.create(*args, **kwargs)
        return original_retry(func, *args, **kwargs)
    service._retry_api_call = retry_wrapper
    return service

@pytest.fixture
def temp_config_file():
    config = {
        "models": {
            "default": {
                "model_name": "dummy-gpt",
                "type": "chat",
                "context_length": 4096,
                "max_tokens": 50,
                "temperature": 0.5,
                "price": {"prompt": 0.001, "completion": 0.002},
                "api_key": "sk-dummy"
            }
        },
        "prompts": {
            "default": {
                "prompt": "You are a helpful assistant.",
                "description": "Default prompt"
            },
            "summarization": {
                "prompt": (
                    "Based on the text provided below and considering the previous summary (if any), produce a refined and concise summary.\n"
                    "Previous Summary: \"{previous_summary}\"\n"
                    "Text: \"{text}\"\n"
                    "Your summary should be engaging, clear, and directly useful."
                ),
                "description": "Summarization prompt"
            }
        },
        "max_retries": 2,
        "default_model": "default"
    }
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as tmp:
        yaml.safe_dump(config, tmp)
        tmp_path = tmp.name
    yield tmp_path
    os.remove(tmp_path)

# ---------------- Tests ----------------

def test_load_configuration(temp_config_file):
    service = OpenAIService(config_path=temp_config_file)
    assert "default" in service.models
    assert "default" in service.prompts
    assert service.default_model == "default"

def test_get_prompt_with_variables(temp_config_file):
    service = OpenAIService(config_path=temp_config_file)
    prompt = service.get_prompt("summarization", {"text": "Hello world", "previous_summary": "None"})
    assert "Hello world" in prompt
    assert "None" in prompt

def test_completion(service_with_client):
    result = service_with_client.completion(prompt="Test prompt", prompt_template="default")
    assert "dummy chat response" in result

@pytest.mark.asyncio
async def test_async_completion(service_with_client):
    result = await service_with_client.async_completion(prompt="Async test", prompt_template="default")
    assert "dummy chat response" in result

def test_stream_completion(service_with_client):
    stream_gen = service_with_client.stream_completion(prompt="Stream test", prompt_template="default")
    collected = ""
    for piece in stream_gen:
        collected += piece
    assert "dummy stream part 1" in collected
    assert "dummy stream part 2" in collected

def test_embedding(service_with_client):
    result = service_with_client.embedding(input_data="Test input", model="default")
    assert isinstance(result, list)
    assert result == [0.1, 0.2, 0.3]

@pytest.mark.asyncio
async def test_transcribe_audio(service_with_client):
    dummy_audio = BytesIO(b"dummy audio data")
    transcription = await service_with_client.transcribe_audio(dummy_audio)
    assert "dummy transcription" in transcription

def test_retry_api_call(service_with_client):
    def always_fail(*args, **kwargs):
        raise Exception("Always fails")
    with pytest.raises(Exception) as excinfo:
        service_with_client._retry_api_call(always_fail)
    assert "failed after" in str(excinfo.value)

def test_update_usage(service_with_client):
    initial_prompt_tokens = service_with_client.total_prompt_tokens
    initial_cost = service_with_client.total_cost
    usage = {"prompt_tokens": 100, "completion_tokens": 50}
    model_cfg = service_with_client.models.get(service_with_client.default_model)
    service_with_client._update_usage(usage, model_cfg)
    assert service_with_client.total_prompt_tokens == initial_prompt_tokens + 100
    expected_cost = round((100 * model_cfg["price"]["prompt"] + 50 * model_cfg["price"]["completion"]) / 1000.0, 6)
    assert service_with_client.total_cost == initial_cost + expected_cost
