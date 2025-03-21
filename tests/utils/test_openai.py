import os
import tempfile
import pytest
import yaml
import logging
import openai
from utils.openAIServices import OpenAIService

# ------------------ 测试用 YAML 配置 ------------------

# Chat 类型模型配置
DUMMY_YAML = """
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    top_p: 1.0
    frequency_penalty: 0.0
    presence_penalty: 0.0
    price:
      prompt: 0.002
      completion: 0.002
    api_key: "dummy_api_key"
prompts:
  default:
    prompt: "Default system message."
    description: "A default system message for chat."
  test_prompt:
    prompt: "Hello, {user_input}!"
    description: "A test prompt with user input placeholder."
default_model: "test-model"
max_retries: 2
"""

# Completion 类型模型配置
DUMMY_YAML_COMPLETION = """
models:
  test-completion:
    model_name: "text-davinci-003"
    type: "completion"
    context_length: 2048
    max_tokens: 150
    temperature: 0.7
    top_p: 1.0
    frequency_penalty: 0.0
    presence_penalty: 0.0
    price:
      prompt: 0.001
      completion: 0.002
    api_key: "dummy_api_key"
prompts:
  default:
    prompt: "Default text prompt: "
    description: "A default prompt for text completion."
default_model: "test-completion"
max_retries: 2
"""

# Audio 类型模型配置（标准）
AUDIO_YAML = """
models:
  test-audio:
    model_name: "whisper-1"
    type: "audio"
    context_length: null
    max_tokens: null
    temperature: null
    price:
      prompt: 0.003
      completion: 0.000
    api_key: "dummy_api_key_audio"
prompts:
  default:
    prompt: "Audio system message."
    description: "A default system message for audio."
default_model: "test-audio"
max_retries: 2
"""

# Audio 类型模型配置——价格为数字
AUDIO_YAML_PRICE_NUMBER = """
models:
  test-audio:
    model_name: "whisper-1"
    type: "audio"
    price: 0.005
    api_key: "dummy_api_key_audio"
prompts:
  default:
    prompt: "Audio system message."
    description: "A default system message for audio."
default_model: "test-audio"
max_retries: 2
"""

# Audio 类型模型配置——价格使用 input/output 键
AUDIO_YAML_PRICE_IO = """
models:
  test-audio:
    model_name: "whisper-1"
    type: "audio"
    price:
      input: 0.004
      output: 0.001
    api_key: "dummy_api_key_audio"
prompts:
  default:
    prompt: "Audio system message."
    description: "A default system message for audio."
default_model: "test-audio"
max_retries: 2
"""

# Image 类型模型配置（用于测试其它类型）
IMAGE_YAML = """
models:
  test-image:
    model_name: "image-model-001"
    type: "image"
    context_length: 512
    max_tokens: 0
    temperature: 0.0
    price: 0
    api_key: "dummy_api_key_image"
prompts:
  default:
    prompt: "Image system message."
    description: "A default system message for image."
default_model: "test-image"
max_retries: 2
"""

# 包装型配置：单层包装的情况
WRAPPED_YAML = """
config:
  models:
    wrapped-model:
      model_name: "gpt-3.5-turbo"
      type: "chat"
      context_length: 4096
      max_tokens: 100
      temperature: 0.5
      price:
        prompt: 0.002
        completion: 0.002
      api_key: "dummy_api_key_wrapped"
  prompts:
    default:
      prompt: "Wrapped default system message."
      description: "Wrapped prompt."
  default_model: "wrapped-model"
  max_retries: 3
"""

# 用于测试 invalid YAML 格式（非法内容）
INVALID_YAML = "::: not a valid yaml :::"

# Models 部分为无效类型
INVALID_MODELS_YAML = """
models: 12345
prompts:
  default:
    prompt: "Default message."
default_model: "any"
max_retries: 2
"""

# Prompts 部分为无效类型
INVALID_PROMPTS_YAML = """
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    api_key: "dummy_api_key"
prompts: 98765
default_model: "test-model"
max_retries: 2
"""

# ------------------ Fixture ------------------

@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(DUMMY_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def config_file_completion(tmp_path):
    path = tmp_path / "config_completion.yaml"
    path.write_text(DUMMY_YAML_COMPLETION, encoding="utf-8")
    return str(path)

@pytest.fixture
def audio_config_file(tmp_path):
    path = tmp_path / "audio_config.yaml"
    path.write_text(AUDIO_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def audio_config_file_price_number(tmp_path):
    path = tmp_path / "audio_config_price_number.yaml"
    path.write_text(AUDIO_YAML_PRICE_NUMBER, encoding="utf-8")
    return str(path)

@pytest.fixture
def audio_config_file_price_io(tmp_path):
    path = tmp_path / "audio_config_price_io.yaml"
    path.write_text(AUDIO_YAML_PRICE_IO, encoding="utf-8")
    return str(path)

@pytest.fixture
def image_config_file(tmp_path):
    path = tmp_path / "image_config.yaml"
    path.write_text(IMAGE_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def wrapped_config_file(tmp_path):
    path = tmp_path / "wrapped_config.yaml"
    path.write_text(WRAPPED_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def invalid_yaml_file(tmp_path):
    path = tmp_path / "invalid.yaml"
    path.write_text(INVALID_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def invalid_models_yaml_file(tmp_path):
    path = tmp_path / "invalid_models.yaml"
    path.write_text(INVALID_MODELS_YAML, encoding="utf-8")
    return str(path)

@pytest.fixture
def invalid_prompts_yaml_file(tmp_path):
    path = tmp_path / "invalid_prompts.yaml"
    path.write_text(INVALID_PROMPTS_YAML, encoding="utf-8")
    return str(path)

# ------------------ Dummy Response 定义 ------------------

class DummyChatResponse:
    def __init__(self):
        self.choices = [{"message": {"content": "Dummy chat response"}}]
        self.usage = {"prompt_tokens": 10, "completion_tokens": 5}

class DummyCompletionResponse:
    def __init__(self):
        self.choices = [{"text": "Dummy completion response"}]
        self.usage = {"prompt_tokens": 8, "completion_tokens": 4}

class DummyEmbeddingResponse:
    def __init__(self):
        self.data = [{"embedding": [0.1, 0.2, 0.3]}]
        self.usage = {"prompt_tokens": 5}

class DummyAudioResponse:
    def __init__(self):
        self.choices = [{"text": "Dummy audio response"}]
        self.usage = {"prompt_tokens": 0, "completion_tokens": 0}

class DummyImageResponse:
    def __init__(self):
        self.choices = [{"text": "Dummy image response"}]
        self.usage = {"prompt_tokens": 0, "completion_tokens": 0}

# ------------------ Dummy API 方法 ------------------

def dummy_chat_completion_create(*args, **kwargs):
    return DummyChatResponse()

def dummy_completion_create(*args, **kwargs):
    return DummyCompletionResponse()

def dummy_embedding_create(*args, **kwargs):
    return DummyEmbeddingResponse()

def dummy_audio_create(*args, **kwargs):
    return DummyAudioResponse()

def dummy_image_create(*args, **kwargs):
    return DummyImageResponse()

def dummy_streaming_response():
    # 模拟 chat 类型流式响应
    class DummyChunk:
        def __init__(self, content):
            self.choices = [{"delta": {"content": content}}]
    yield DummyChunk("Chunk 1, ")
    yield DummyChunk("Chunk 2.")

def dummy_chat_stream_create(*args, **kwargs):
    return dummy_streaming_response()

def dummy_completion_stream_create(*args, **kwargs):
    # 模拟非 chat 模型流式响应
    class DummyChunk:
        def __init__(self, text):
            self.choices = [{"text": text}]
    return iter([DummyChunk("Text Chunk 1 "), DummyChunk("Text Chunk 2")])

# 为 audio 流式响应使用与非 chat 流式响应相同的实现
dummy_audio_stream_create = dummy_completion_stream_create

def dummy_stream_empty(*args, **kwargs):
    class DummyChunk:
        def __init__(self):
            self.choices = []
    return iter([DummyChunk()])

# ------------------ 基础功能测试 ------------------

def test_load_configuration(config_file):
    service = OpenAIService(config_file)
    assert service.default_model == "test-model"
    assert "test-model" in service.models
    assert "default" in service.prompts
    assert "test_prompt" in service.prompts
    assert service.max_retries == 2

def test_load_configuration_wrapped(wrapped_config_file):
    service = OpenAIService(wrapped_config_file)
    assert service.default_model == "wrapped-model"
    assert "wrapped-model" in service.models
    assert service.prompts["default"]["prompt"] == "Wrapped default system message."
    assert service.max_retries == 3

# ------------------ get_prompt 测试 ------------------

def test_get_prompt_formatting(config_file):
    service = OpenAIService(config_file)
    result = service.get_prompt("test_prompt", {"user_input": "world"})
    assert result == "Hello, world!"

def test_get_prompt_fallback(config_file):
    service = OpenAIService(config_file)
    result = service.get_prompt("non_exist")
    assert result == "Default system message."

def test_get_prompt_with_none(config_file):
    service = OpenAIService(config_file)
    result = service.get_prompt(None, {"user_input": "none"})
    assert result == "Default system message."

def test_get_prompt_extra_variables(config_file):
    service = OpenAIService(config_file)
    result = service.get_prompt("test_prompt", {"user_input": "extra", "unused": "value"})
    assert result == "Hello, extra!"

def test_get_prompt_formatting_error(tmp_path):
    bad_yaml = """
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    api_key: "dummy_api_key"
prompts:
  default:
    prompt: "Hello, {user}!"
default_model: "test-model"
max_retries: 2
"""
    path = tmp_path / "bad.yaml"
    path.write_text(bad_yaml, encoding="utf-8")
    service = OpenAIService(str(path))
    result = service.get_prompt("default", {"user_input": "world"})
    # 格式化失败时返回原始模板
    assert result == "Hello, {user}!"

# ------------------ _load_yaml_file 测试 ------------------

def test_load_yaml_file_success(tmp_path):
    content = "key: value\nnumber: 123"
    path = tmp_path / "valid.yaml"
    path.write_text(content, encoding="utf-8")
    service = OpenAIService()
    data = service._load_yaml_file(str(path))
    assert data["key"] == "value"
    assert data["number"] == 123

# ------------------ _process_model_config 测试 ------------------

def test_process_model_config_guess_type():
    cfg = {"model_name": "unknown-model"}
    service = OpenAIService()
    processed = service._process_model_config(cfg)
    assert processed["type"] == "completion"

def test_process_model_config_audio():
    cfg = {"model_name": "whisper-large", "context_length": None, "max_tokens": None, "temperature": None}
    service = OpenAIService()
    processed = service._process_model_config(cfg)
    assert processed["type"] == "audio"
    assert processed["context_length"] == 0
    assert processed["max_tokens"] == 0
    assert processed["temperature"] == 0

def test_process_model_config_embedding():
    cfg = {"model_name": "text-embedding-ada-002"}
    service = OpenAIService()
    processed = service._process_model_config(cfg)
    assert processed["type"] == "embedding"

def test_process_model_config_image():
    cfg = {"model_name": "image-model-001", "context_length": 512, "max_tokens": None, "temperature": None, "price": 0}
    service = OpenAIService()
    processed = service._process_model_config(cfg)
    # 未指定 type，默认按规则判断为 completion 或 image；本测试允许二者之一
    assert processed["type"] in ("completion", "image")

# ------------------ Completion 测试 ------------------

def test_completion_chat(monkeypatch, config_file):
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", dummy_chat_completion_create)
    response = service.completion(prompt="Test message", prompt_template="default")
    assert response == "Dummy chat response"
    assert service.total_prompt_tokens == 10
    assert service.total_completion_tokens == 5
    assert abs(service.total_cost - 0.00003) < 1e-6

def test_completion_text(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    monkeypatch.setattr(openai, "Completion", type("DummyCompletion", (), {"create": dummy_completion_create}))
    response = service.completion(prompt="What is the weather?", prompt_template="default")
    assert response == "Dummy completion response"
    assert service.total_prompt_tokens == 8
    assert service.total_completion_tokens == 4
    assert abs(service.total_cost - 0.000016) < 1e-6

def test_completion_no_usage(monkeypatch, config_file):
    class DummyNoUsage:
        def __init__(self):
            self.choices = [{"message": {"content": "No usage response"}}]
    def no_usage_create(*args, **kwargs):
        return DummyNoUsage()
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", no_usage_create)
    response = service.completion(prompt="No usage", prompt_template="default")
    assert response == "No usage response"
    assert service.total_prompt_tokens == 0
    assert service.total_cost == 0.0

def test_completion_empty_choices(monkeypatch, config_file_completion):
    class DummyEmptyChoices:
        def __init__(self):
            self.choices = []
            self.usage = {"prompt_tokens": 0, "completion_tokens": 0}
    def empty_choices_create(*args, **kwargs):
        return DummyEmptyChoices()
    service = OpenAIService(config_file_completion)
    monkeypatch.setattr(openai, "Completion", type("EmptyChoices", (), {"create": empty_choices_create}))
    response = service.completion(prompt="Empty choices", prompt_template="default")
    assert response == ""

def test_completion_model_not_configured(config_file):
    service = OpenAIService(config_file)
    with pytest.raises(ValueError):
        service.completion(model="non_exist", prompt="Test", prompt_template="default")

def test_completion_no_prompt(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    def dummy_completion_empty(*args, **kwargs):
        class DummyResp:
            def __init__(self):
                self.choices = [{"text": ""}]
                self.usage = {"prompt_tokens": 0, "completion_tokens": 0}
        return DummyResp()
    monkeypatch.setattr(openai, "Completion", type("DummyEmpty", (), {"create": dummy_completion_empty}))
    response = service.completion(prompt="", prompt_template="")
    assert response == ""

def test_completion_api_failure(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    def always_fail(*args, **kwargs):
        raise Exception("Simulated failure")
    monkeypatch.setattr(openai, "Completion", type("AlwaysFail", (), {"create": always_fail}))
    with pytest.raises(Exception):
        service.completion(prompt="Failure test", prompt_template="default")

# ------------------ Streaming Completion 测试 ------------------

def test_stream_completion_chat(monkeypatch, config_file):
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", dummy_chat_stream_create)
    stream_gen = service.stream_completion(prompt="Streaming test", prompt_template="default")
    output = "".join(list(stream_gen))
    assert output == "Chunk 1, Chunk 2."

def test_stream_completion_nonchat(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    monkeypatch.setattr(openai, "Completion", type("DummyCompletionStream", (), {"create": dummy_completion_stream_create}))
    stream_gen = service.stream_completion(prompt="Non-chat stream test", prompt_template="default")
    output = "".join(list(stream_gen))
    assert output == "Text Chunk 1 Text Chunk 2"

def test_stream_completion_empty_choices(monkeypatch, config_file):
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", dummy_stream_empty)
    with pytest.raises(IndexError):
        list(service.stream_completion(prompt="Empty stream", prompt_template="default"))

def test_stream_completion_api_failure(monkeypatch, config_file):
    service = OpenAIService(config_file)
    def always_fail_stream(*args, **kwargs):
        raise Exception("Streaming failure")
    monkeypatch.setattr(openai.ChatCompletion, "create", always_fail_stream)
    with pytest.raises(Exception):
        list(service.stream_completion(prompt="Fail stream", prompt_template="default"))

# ------------------ Retry 机制测试 ------------------

def test_retry_completion(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    call_count = {"count": 0}
    def retry_dummy_completion_create(*args, **kwargs):
        if call_count["count"] < 1:
            call_count["count"] += 1
            raise Exception("Simulated API failure")
        return DummyCompletionResponse()
    monkeypatch.setattr(openai, "Completion", type("RetryDummyCompletion", (), {"create": retry_dummy_completion_create}))
    response = service.completion(prompt="Retry test", prompt_template="default")
    assert response == "Dummy completion response"
    assert service.total_prompt_tokens == 8
    assert service.total_completion_tokens == 4

def test_retry_embedding(monkeypatch, config_file):
    service = OpenAIService(config_file)
    call_count = {"count": 0}
    def retry_dummy_embedding_create(*args, **kwargs):
        if call_count["count"] < 1:
            call_count["count"] += 1
            raise Exception("Simulated embedding API failure")
        return DummyEmbeddingResponse()
    monkeypatch.setattr(openai, "Embedding", type("RetryDummyEmbedding", (), {"create": retry_dummy_embedding_create}))
    embedding_result = service.embedding(input_data="Test embedding")
    assert embedding_result == [0.1, 0.2, 0.3]
    assert service.total_prompt_tokens == 5

# ------------------ Embedding 测试 ------------------

def test_embedding(monkeypatch, config_file):
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai, "Embedding", type("DummyEmbedding", (), {"create": dummy_embedding_create}))
    embedding_result = service.embedding(input_data="Test embedding")
    assert embedding_result == [0.1, 0.2, 0.3]
    assert service.total_prompt_tokens == 5

def test_embedding_no_input(config_file):
    service = OpenAIService(config_file)
    with pytest.raises(ValueError):
        service.embedding(input_data=None)

def test_embedding_api_failure(monkeypatch, config_file):
    service = OpenAIService(config_file)
    def always_fail_embedding(*args, **kwargs):
        raise Exception("Embedding failure")
    monkeypatch.setattr(openai, "Embedding", type("AlwaysFailEmbedding", (), {"create": always_fail_embedding}))
    with pytest.raises(Exception):
        service.embedding(input_data="Fail embedding")

def test_embedding_invalid_response(monkeypatch, config_file):
    service = OpenAIService(config_file)
    def invalid_embedding_response(*args, **kwargs):
        return {"usage": {"prompt_tokens": 5}}
    monkeypatch.setattr(openai, "Embedding", type("InvalidEmbedding", (), {"create": invalid_embedding_response}))
    with pytest.raises(RuntimeError):
        service.embedding(input_data="Test invalid")

def test_embedding_no_usage(monkeypatch, config_file):
    class DummyNoUsageEmbedding:
        def __init__(self):
            self.data = [{"embedding": [0.1, 0.2, 0.3]}]
    def no_usage_embedding(*args, **kwargs):
        return DummyNoUsageEmbedding()
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai, "Embedding", type("NoUsageEmbedding", (), {"create": no_usage_embedding}))
    result = service.embedding(input_data="Test no usage")
    assert result == [0.1, 0.2, 0.3]
    assert service.total_prompt_tokens == 0
    assert service.total_cost == 0.0

# ------------------ Audio 类型测试 ------------------

def test_completion_audio(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudio", (), {"create": dummy_audio_create}))
    response = service.completion(prompt="Audio test", prompt_template="default")
    assert response == "Dummy audio response"
    audio_cfg = service.models["test-audio"]
    assert audio_cfg["context_length"] == 0
    assert audio_cfg["max_tokens"] == 0
    assert audio_cfg["temperature"] == 0

def test_stream_audio(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudioStream", (), {"create": dummy_audio_stream_create}))
    stream_gen = service.stream_completion(prompt="Audio stream test", prompt_template="default")
    output = "".join(list(stream_gen))
    assert output == "Text Chunk 1 Text Chunk 2"

# 针对 audio 类型的额外测试：价格字段解析（数字和 input/output）
def test_audio_price_number(tmp_path, audio_config_file_price_number):
    service = OpenAIService(audio_config_file_price_number)
    cfg = service.models["test-audio"]
    assert isinstance(cfg["price"], dict)
    assert cfg["price"]["prompt"] == 0.005
    assert cfg["price"]["completion"] == 0.0

def test_audio_price_io(tmp_path, audio_config_file_price_io):
    service = OpenAIService(audio_config_file_price_io)
    cfg = service.models["test-audio"]
    assert cfg["price"]["prompt"] == 0.004
    assert cfg["price"]["completion"] == 0.001

# 针对 audio 类型额外测试：完成调用返回 usage 信息时累加 tokens 与 cost
def test_audio_completion_with_usage(monkeypatch, tmp_path):
    audio_yaml_with_usage = """
models:
  test-audio:
    model_name: "whisper-1"
    type: "audio"
    context_length: null
    max_tokens: null
    temperature: null
    price:
      prompt: 0.003
      completion: 0.000
    api_key: "dummy_api_key_audio"
prompts:
  default:
    prompt: "Audio system message."
    description: "A default system message for audio."
default_model: "test-audio"
max_retries: 2
"""
    config_path = tmp_path / "audio_with_usage.yaml"
    config_path.write_text(audio_yaml_with_usage, encoding="utf-8")
    service = OpenAIService(str(config_path))
    class DummyAudioResponseWithUsage:
        def __init__(self):
            self.choices = [{"text": "Audio response with usage"}]
            self.usage = {"prompt_tokens": 50, "completion_tokens": 0}
    def dummy_audio_with_usage(*args, **kwargs):
        return DummyAudioResponseWithUsage()
    monkeypatch.setattr(openai, "Completion", type("DummyAudioWithUsage", (), {"create": dummy_audio_with_usage}))
    response = service.completion(prompt="Audio test with usage", prompt_template="default")
    assert response == "Audio response with usage"
    expected_cost = 50 * 0.003 / 1000.0
    assert abs(service.total_cost - expected_cost) < 1e-6

# 测试 audio 类型的 streaming 调用返回无 usage 信息
def test_audio_stream_no_usage(monkeypatch, audio_config_file):
    class DummyAudioStreamNoUsage:
        def __init__(self, content):
            self.choices = [{"text": content}]  # 使用 "text" 键，而不是 "delta"
    def dummy_audio_stream_no_usage(*args, **kwargs):
        return iter([DummyAudioStreamNoUsage("Audio stream no usage")])
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudioStreamNoUsage", (), {"create": dummy_audio_stream_no_usage}))
    output = "".join(list(service.stream_completion(prompt="Audio stream", prompt_template="default")))
    assert "Audio stream no usage" in output
    assert service.total_prompt_tokens == 0
    assert service.total_cost == 0.0

# 测试 audio 类型的 embedding 调用（允许调用）
def test_embedding_audio(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Embedding", type("DummyEmbedding", (), {"create": dummy_embedding_create}))
    result = service.embedding(model="test-audio", input_data="Audio input")
    assert result == [0.1, 0.2, 0.3]

# ------------------ Image 类型测试 ------------------

def test_completion_image(monkeypatch, image_config_file):
    service = OpenAIService(image_config_file)
    # 对于 image 模型，通常调用 completion 方法返回文本描述
    monkeypatch.setattr(openai, "Completion", type("DummyImage", (), {"create": dummy_image_create}))
    response = service.completion(prompt="Image test", prompt_template="default")
    # 由于 image 模型处理方式与 completion 类似，此处返回 DummyImageResponse
    assert response == "Dummy image response"
    image_cfg = service.models["test-image"]
    assert image_cfg["context_length"] == 512

# ------------------ YAML 文件加载异常测试 ------------------

def test_load_yaml_file_error(tmp_path):
    fake_path = tmp_path / "non_existent.yaml"
    service = OpenAIService()
    with pytest.raises(Exception):
        service._load_yaml_file(str(fake_path))

def test_load_yaml_invalid(invalid_yaml_file):
    with pytest.raises(Exception):
        OpenAIService(invalid_yaml_file)

# ------------------ Models / Prompts 配置测试 ------------------

def test_load_models_invalid(invalid_models_yaml_file):
    service = OpenAIService(invalid_models_yaml_file)
    assert service.models == {}

def test_load_prompts_invalid(invalid_prompts_yaml_file):
    service = OpenAIService(invalid_prompts_yaml_file)
    assert service.prompts == {}

def test_load_prompts_list(tmp_path):
    dummy_yaml_list_prompts = """
models:
  list-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    price:
      prompt: 0.002
      completion: 0.002
    api_key: "dummy_api_key"
prompts:
  - name: list_prompt
    prompt: "List prompt message."
    description: "Prompt from list."
default_model: "list-model"
max_retries: 2
"""
    path = tmp_path / "config_list.yaml"
    path.write_text(dummy_yaml_list_prompts, encoding="utf-8")
    service = OpenAIService(str(path))
    result = service.get_prompt("list_prompt")
    assert result == "List prompt message."

def test_load_prompts_config_from_list(tmp_path):
    dummy_yaml = """
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    api_key: "dummy_api_key"
prompts:
  - name: list_prompt
    prompt: "Prompt from list"
    description: "Desc"
default_model: "test-model"
max_retries: 2
"""
    path = tmp_path / "config_prompts_list.yaml"
    path.write_text(dummy_yaml, encoding="utf-8")
    service = OpenAIService(str(path))
    assert "list_prompt" in service.prompts
    assert service.prompts["list_prompt"]["prompt"] == "Prompt from list"

# ------------------ Additional Tests ------------------

def test_completion_explicit_model(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    with pytest.raises(ValueError):
        service.completion(model="non_existing_model", prompt="Test", prompt_template="default")
    
    custom_yaml = """
models:
  model_a:
    model_name: "text-davinci-003"
    type: "completion"
    context_length: 2048
    max_tokens: 100
    temperature: 0.7
    price:
      prompt: 0.001
      completion: 0.002
    api_key: "dummy_api_key_a"
  model_b:
    model_name: "text-davinci-003"
    type: "completion"
    context_length: 2048
    max_tokens: 150
    temperature: 0.8
    price:
      prompt: 0.002
      completion: 0.003
    api_key: "dummy_api_key_b"
prompts:
  default:
    prompt: "Default text prompt: "
    description: "A default prompt."
default_model: "model_a"
max_retries: 2
"""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml", encoding="utf-8") as f:
        f.write(custom_yaml)
        custom_config_path = f.name
    service2 = OpenAIService(custom_config_path)
    def dummy_text_create_b(*args, **kwargs):
        return DummyCompletionResponse()
    monkeypatch.setattr(openai, "Completion", type("DummyTextB", (), {"create": dummy_text_create_b}))
    response = service2.completion(model="model_b", prompt="Weather?", prompt_template="default")
    assert response == "Dummy completion response"
    os.remove(custom_config_path)

def test_completion_no_prompt_template(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    def dummy_text_create_no_template(*args, **kwargs):
        assert kwargs.get("prompt") == "Direct prompt without template"
        return DummyCompletionResponse()
    monkeypatch.setattr(openai, "Completion", type("DummyTextNoTemplate", (), {"create": dummy_text_create_no_template}))
    response = service.completion(prompt="Direct prompt without template", prompt_template=None)
    assert response == "Dummy completion response"

def test_stream_completion_nonchat_formatting_exception(monkeypatch, config_file_completion):
    bad_text_yaml = """
models:
  test-model:
    model_name: "text-davinci-003"
    type: "completion"
    context_length: 2048
    max_tokens: 150
    temperature: 0.7
    price:
      prompt: 0.001
      completion: 0.002
    api_key: "dummy_api_key"
prompts:
  bad_template:
    prompt: "Answer: {wrong_placeholder}"
    description: "Bad template."
  default:
    prompt: "Default text prompt: "
    description: "Default prompt."
default_model: "test-model"
max_retries: 2
"""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml", encoding="utf-8") as f:
        f.write(bad_text_yaml)
        bad_config_path = f.name
    service_bad = OpenAIService(bad_config_path)
    class DummyChunk:
        def __init__(self, text):
            self.choices = [{"text": text}]
    def dummy_nonchat_stream_create(*args, **kwargs):
        return iter([DummyChunk("Non-chat stream chunk")])
    monkeypatch.setattr(openai, "Completion", type("DummyNonChatStream", (), {"create": dummy_nonchat_stream_create}))
    stream_gen = service_bad.stream_completion(prompt="Test", prompt_template="bad_template")
    output = "".join(list(stream_gen))
    assert "Non-chat stream chunk" in output
    os.remove(bad_config_path)

def test_embedding_multiple_inputs(monkeypatch, tmp_path):
    class DummyMultipleEmbeddingResponse:
        def __init__(self):
            self.data = [
                {"embedding": [0.1, 0.2, 0.3]},
                {"embedding": [0.4, 0.5, 0.6]}
            ]
            self.usage = {"prompt_tokens": 12}
    def dummy_embedding_multiple(*args, **kwargs):
        return DummyMultipleEmbeddingResponse()
    config_path = tmp_path / "dummy.yaml"
    config_path.write_text(DUMMY_YAML, encoding="utf-8")
    service = OpenAIService(str(config_path))
    monkeypatch.setattr(openai, "Embedding", type("DummyEmbeddingMultiple", (), {"create": dummy_embedding_multiple}))
    result = service.embedding(input_data=["Text 1", "Text 2"])
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == [0.1, 0.2, 0.3]
    assert result[1] == [0.4, 0.5, 0.6]

def test_api_credentials_set(monkeypatch, tmp_path):
    global_api_yaml = """
api_key: "global_dummy_api_key"
api_base: "https://api.global.example.com"
api_type: "global_type"
api_version: "v1"
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    price:
      prompt: 0.002
      completion: 0.002
    api_key: null
prompts:
  default:
    prompt: "Default system message."
    description: "A default system message for chat."
default_model: "test-model"
max_retries: 2
"""
    config_path = tmp_path / "global_api.yaml"
    config_path.write_text(global_api_yaml, encoding="utf-8")
    service = OpenAIService(str(config_path))
    def capture_api_credentials(*args, **kwargs):
        assert openai.api_key == "global_dummy_api_key"
        assert openai.api_base == "https://api.global.example.com"
        assert openai.api_type == "global_type"
        assert openai.api_version == "v1"
        return DummyChatResponse()
    monkeypatch.setattr(openai.ChatCompletion, "create", capture_api_credentials)
    service.completion(prompt="Test", prompt_template="default")

def test_multiple_calls_accumulation(monkeypatch, config_file):
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", dummy_chat_completion_create)
    _ = service.completion(prompt="First call", prompt_template="default")
    _ = service.completion(prompt="Second call", prompt_template="default")
    assert service.total_prompt_tokens == 20
    assert service.total_completion_tokens == 10
    expected_cost = (20 * 0.002 + 10 * 0.002) / 1000.0
    assert abs(service.total_cost - expected_cost) < 1e-6

def test_completion_retry_exhaust(monkeypatch, config_file_completion):
    service = OpenAIService(config_file_completion)
    call_count = {"count": 0}
    def always_fail_retry(*args, **kwargs):
        call_count["count"] += 1
        raise Exception("Simulated failure")
    monkeypatch.setattr(openai, "Completion", type("AlwaysFailRetry", (), {"create": always_fail_retry}))
    with pytest.raises(Exception):
        service.completion(prompt="Should fail", prompt_template="default")
    assert call_count["count"] == service.max_retries

def test_streaming_no_usage(monkeypatch, config_file):
    class DummyStreamNoUsage:
        def __init__(self, content):
            self.choices = [{"delta": {"content": content}}]
    def dummy_stream_no_usage(*args, **kwargs):
        return iter([DummyStreamNoUsage("No usage stream")])
    service = OpenAIService(config_file)
    monkeypatch.setattr(openai.ChatCompletion, "create", dummy_stream_no_usage)
    list(service.stream_completion(prompt="Stream test", prompt_template="default"))
    assert service.total_prompt_tokens == 0
    assert service.total_cost == 0.0

# ------------------ Audio 类型额外测试 ------------------

def test_audio_completion(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudio", (), {"create": dummy_audio_create}))
    response = service.completion(prompt="Audio test", prompt_template="default")
    assert response == "Dummy audio response"
    audio_cfg = service.models["test-audio"]
    assert audio_cfg["context_length"] == 0
    assert audio_cfg["max_tokens"] == 0
    assert audio_cfg["temperature"] == 0

def test_audio_stream(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudioStream", (), {"create": dummy_audio_stream_create}))
    stream_gen = service.stream_completion(prompt="Audio stream test", prompt_template="default")
    output = "".join(list(stream_gen))
    assert output == "Text Chunk 1 Text Chunk 2"

def test_audio_price_number(tmp_path, audio_config_file_price_number):
    service = OpenAIService(audio_config_file_price_number)
    cfg = service.models["test-audio"]
    assert isinstance(cfg["price"], dict)
    assert cfg["price"]["prompt"] == 0.005
    assert cfg["price"]["completion"] == 0.0

def test_audio_price_io(tmp_path, audio_config_file_price_io):
    service = OpenAIService(audio_config_file_price_io)
    cfg = service.models["test-audio"]
    assert cfg["price"]["prompt"] == 0.004
    assert cfg["price"]["completion"] == 0.001

def test_audio_completion_with_usage(monkeypatch, tmp_path):
    audio_yaml_with_usage = """
models:
  test-audio:
    model_name: "whisper-1"
    type: "audio"
    context_length: null
    max_tokens: null
    temperature: null
    price:
      prompt: 0.003
      completion: 0.000
    api_key: "dummy_api_key_audio"
prompts:
  default:
    prompt: "Audio system message."
    description: "A default system message for audio."
default_model: "test-audio"
max_retries: 2
"""
    config_path = tmp_path / "audio_with_usage.yaml"
    config_path.write_text(audio_yaml_with_usage, encoding="utf-8")
    service = OpenAIService(str(config_path))
    class DummyAudioResponseWithUsage:
        def __init__(self):
            self.choices = [{"text": "Audio response with usage"}]
            self.usage = {"prompt_tokens": 50, "completion_tokens": 0}
    def dummy_audio_with_usage(*args, **kwargs):
        return DummyAudioResponseWithUsage()
    monkeypatch.setattr(openai, "Completion", type("DummyAudioWithUsage", (), {"create": dummy_audio_with_usage}))
    response = service.completion(prompt="Audio test with usage", prompt_template="default")
    assert response == "Audio response with usage"
    expected_cost = 50 * 0.003 / 1000.0
    assert abs(service.total_cost - expected_cost) < 1e-6

def test_audio_stream_no_usage(monkeypatch, audio_config_file):
    class DummyAudioStreamNoUsage:
        def __init__(self, content):
            self.choices = [{"text": content}]  # 确保使用 "text" 键
    def dummy_audio_stream_no_usage(*args, **kwargs):
        return iter([DummyAudioStreamNoUsage("Audio stream no usage")])
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyAudioStreamNoUsage", (), {"create": dummy_audio_stream_no_usage}))
    output = "".join(list(service.stream_completion(prompt="Audio stream", prompt_template="default")))
    assert "Audio stream no usage" in output
    assert service.total_prompt_tokens == 0
    assert service.total_cost == 0.0

# ------------------ Audio 的 Embedding 测试 ------------------

def test_embedding_audio(monkeypatch, audio_config_file):
    service = OpenAIService(audio_config_file)
    monkeypatch.setattr(openai, "Embedding", type("DummyEmbedding", (), {"create": dummy_embedding_create}))
    result = service.embedding(model="test-audio", input_data="Audio input")
    assert result == [0.1, 0.2, 0.3]

# ------------------ Image 类型测试 ------------------

def test_completion_image(monkeypatch, image_config_file):
    service = OpenAIService(image_config_file)
    monkeypatch.setattr(openai, "Completion", type("DummyImage", (), {"create": dummy_image_create}))
    response = service.completion(prompt="Image test", prompt_template="default")
    assert response == "Dummy image response"
    image_cfg = service.models["test-image"]
    assert image_cfg["context_length"] == 512

# ------------------ YAML 文件加载异常测试 ------------------

def test_load_yaml_file_error(tmp_path):
    fake_path = tmp_path / "non_existent.yaml"
    service = OpenAIService()
    with pytest.raises(Exception):
        service._load_yaml_file(str(fake_path))

def test_load_yaml_invalid(invalid_yaml_file):
    with pytest.raises(Exception):
        OpenAIService(invalid_yaml_file)

# ------------------ Models / Prompts 配置测试 ------------------

def test_load_models_invalid(invalid_models_yaml_file):
    service = OpenAIService(invalid_models_yaml_file)
    assert service.models == {}

def test_load_prompts_invalid(invalid_prompts_yaml_file):
    service = OpenAIService(invalid_prompts_yaml_file)
    assert service.prompts == {}

def test_load_prompts_list(tmp_path):
    dummy_yaml_list_prompts = """
models:
  list-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    price:
      prompt: 0.002
      completion: 0.002
    api_key: "dummy_api_key"
prompts:
  - name: list_prompt
    prompt: "List prompt message."
    description: "Prompt from list."
default_model: "list-model"
max_retries: 2
"""
    path = tmp_path / "config_list.yaml"
    path.write_text(dummy_yaml_list_prompts, encoding="utf-8")
    service = OpenAIService(str(path))
    result = service.get_prompt("list_prompt")
    assert result == "List prompt message."

def test_load_prompts_config_from_list(tmp_path):
    dummy_yaml = """
models:
  test-model:
    model_name: "gpt-3.5-turbo"
    type: "chat"
    context_length: 4096
    max_tokens: 100
    temperature: 0.5
    api_key: "dummy_api_key"
prompts:
  - name: list_prompt
    prompt: "Prompt from list"
    description: "Desc"
default_model: "test-model"
max_retries: 2
"""
    path = tmp_path / "config_prompts_list.yaml"
    path.write_text(dummy_yaml, encoding="utf-8")
    service = OpenAIService(str(path))
    assert "list_prompt" in service.prompts
    assert service.prompts["list_prompt"]["prompt"] == "Prompt from list"
