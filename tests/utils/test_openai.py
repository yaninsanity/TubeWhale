import os
import time
import yaml
import pytest
import asyncio
from io import BytesIO

import utils.openAIServices as svc_mod
from utils.openAIServices import OpenAIService

# --- Fixtures ----------------------------------------------------------------

@pytest.fixture
def temp_config(tmp_path):
    cfg = {
        "models": {
            "m1": {
                "model_name": "chat-model",
                "type": "chat",
                "max_tokens": 10,
                "temperature": 0.5,
                "price": {"prompt": 0.001, "completion": 0.002},
            },
            "p1": {
                "model_name": "plain-model",
                "type": "completion",
                "max_tokens": 8,
                "temperature": 0.3,
                "price": 0.001
            }
        },
        "prompts": {
            "foo": {
                "system": "System says: {{xyz}}",
                "user": "User says: {{text}}"
            }
        },
        "default_model": "m1"
    }
    path = tmp_path / "openai_config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return str(path)

# --- Tests -------------------------------------------------------------------

def test_init_with_api_key_only(monkeypatch):
    # 确保不会自动加载同目录的 openai_config.yaml
    monkeypatch.setattr(OpenAIService, "_get_default_config_path", lambda self: "/nonexistent.yaml")
    monkeypatch.setattr(os.path, "exists", lambda p: False)

    svc = OpenAIService(api_key="abc123")
    # 使用内置 default 模型
    assert svc.default_model == "default"
    assert "default" in svc.models
    # default prompt
    assert svc.default_prompt == "default"
    assert svc.prompts["default"]["user"].startswith("You are a helpful assistant.")
    # completion 方法可调用
    assert callable(svc.completion)

def test_load_and_validate_config(temp_config):
    svc = OpenAIService(config_path=temp_config)
    # models 加载
    assert set(svc.models.keys()) == {"m1", "p1"}
    # default_model 对应 YAML
    assert svc.default_model == "m1"
    # prompts 加载
    assert "foo" in svc.prompts
    # get_system_prompt
    sys_p = svc.get_system_prompt("foo")
    assert "System says:" in sys_p
    # get_prompt 变量替换
    user_p = svc.get_prompt("foo", {"text": "hello"})
    assert "hello" in user_p

def test_reload_configuration(tmp_path):
    # 准备两个配置文件
    cfg1 = {"models": {"a": {}}, "prompts": {"x": ""}, "default_model": "a"}
    cfg2 = {"models": {"b": {}}, "prompts": {"y": ""}, "default_model": "b"}
    p1 = tmp_path / "c1.yaml"; p1.write_text(yaml.safe_dump(cfg1))
    p2 = tmp_path / "c2.yaml"; p2.write_text(yaml.safe_dump(cfg2))

    svc = OpenAIService(config_path=str(p1))
    assert svc.default_model == "a"

    svc.reload_configuration(str(p2))
    assert svc.default_model == "b"

    with pytest.raises(FileNotFoundError):
        svc.reload_configuration(str(tmp_path / "nope.yaml"))

def test_build_chat_and_plain_prompts(temp_config):
    svc = OpenAIService(config_path=temp_config)
    # chat 消息构造
    msgs = svc._build_chat_messages(prompt="!", prompt_template="foo", template_vars={"text": "T", "xyz": "Z"})
    assert any(m["role"] == "system" for m in msgs)
    assert any("User says: T" in m["content"] for m in msgs)

    # plain prompt
    plain = svc._build_plain_prompt(prompt="??", prompt_template="foo", template_vars={"text": "T"})
    assert "User says: T" in plain and "??" in plain

def test_retry_api_call_success_and_fail():
    svc = OpenAIService(api_key="k")
    # 模拟首次失败、二次成功
    calls = {"n": 0}
    def flaky(x):
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("fail")
        return "ok"
    res = svc._retry_api_call(flaky, None)
    assert res == "ok"

    # 始终失败
    def always_bad():
        raise RuntimeError("bad")
    with pytest.raises(RuntimeError):
        svc._retry_api_call(always_bad)

@pytest.mark.parametrize("is_chat", [True, False])
def test_completion_updates_usage(monkeypatch, temp_config, is_chat):
    svc = OpenAIService(config_path=temp_config)
    # 准备两种客户端
    if is_chat:
        class C:
            def __init__(self, data): self._d = data
            def model_dump(self): return self._d
        svc.client.chat = type("Chat",(object,),{
            "completions": type("Y",(object,),{"create": staticmethod(lambda **kw: C({
                "choices":[{"message":{"content":"resp"}}],
                "usage":{"prompt_tokens":1,"completion_tokens":2}
            }) ) })
        })()
        svc.models["m1"]["type"]="chat"
        svc.default_model="m1"
    else:
        svc.client.completions = type("Plain",(object,),{
            "create": staticmethod(lambda **kw: {"choices":[{"text":"resp"}],"usage":{"prompt_tokens":2,"completion_tokens":3}})
        })()
        svc.models["p1"]["type"]="completion"
        svc.default_model="p1"

    before = (svc.total_prompt_tokens, svc.total_completion_tokens, svc.total_cost)
    out = svc.completion(model=svc.default_model, prompt="p", prompt_template="foo", template_vars={"text":"v"})
    assert out in ("resp", "")
    assert svc.total_prompt_tokens > before[0]
    assert svc.total_completion_tokens > before[1]
    assert svc.total_cost > before[2]

@pytest.mark.asyncio
async def test_transcribe_audio_success_and_retry(monkeypatch):
    # 准备拆分一次失败、一次成功的序列
    class R:
        def __init__(self, text): self._d={"text":text}
        def model_dump(self): return self._d

    seq = [Exception("bad"), R("gotit")]
    def fake_create(file, model, response_format):
        v = seq.pop(0)
        if isinstance(v, Exception): raise v
        return v

    dummy_client = type("C",(object,),{
        "audio": type("A",(object,),{"transcriptions": type("T",(object,),{"create": staticmethod(fake_create)})})
    })
    svc = OpenAIService(api_key="k", client=dummy_client)
    b = BytesIO(b"1234"); b.name="x"
    res = await svc.transcribe_audio(b)
    assert res == "gotit"

    # 全部失败
    seq2 = [Exception(), Exception(), Exception()]
    def bad_create(*args, **kw): raise RuntimeError("still bad")
    dummy2 = type("C2",(object,),{
        "audio": type("A2",(object,),{"transcriptions": type("T2",(object,),{"create": staticmethod(bad_create)})})
    })
    svc2 = OpenAIService(api_key="k", client=dummy2)
    b2 = BytesIO(b"xx")
    with pytest.raises(RuntimeError):
        await svc2.transcribe_audio(b2)
