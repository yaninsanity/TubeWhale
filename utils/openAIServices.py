#!/usr/bin/env python3
import asyncio
import logging
import os
import re
import threading
import time
import yaml
from io import BytesIO
from typing import Any, Callable, Dict, Generator, List, Optional, Union

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_handler)


class OpenAIService:
    """
    OpenAIService：封装 OpenAI API 调用，
      - 自动加载同目录下 openai_config.yaml（models + prompts）；
      - 动态注入全局 api_key；
      - 同步/异步 completion、stream、embedding、Whisper 转录；
      - 重试 & usage 统计；
      - reload_configuration 支持热加载。
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        api_key: Optional[str] = None,
        client: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.models: Dict[str, Dict[str, Any]] = {}
        # prompts[name] = {"system": "...", "user": "..."}
        self.prompts: Dict[str, Dict[str, str]] = {}
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        self.max_retries = 3
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_cost = 0.0
        self._lock = threading.Lock()

        self.client = client
        self.logger = logger or logging.getLogger(__name__)

        # —— 一律尝试加载本模块同目录下的 openai_config.yaml —— 
        if config_path is None:
            default_cfg = self._get_default_config_path()
            if os.path.exists(default_cfg):
                config_path = default_cfg
                self.logger.info(f"Using config file: {default_cfg}")
            else:
                self.logger.warning("No config_path provided and no default openai_config.yaml found.")

        # —— 优先从 YAML 加载 —— 
        if config_path:
            self.load_configuration(config_path)
            # 若同时给了 api_key，就注入到每个 model（若它们 YAML 中未指定）
            if api_key:
                for m in self.models.values():
                    m.setdefault("api_key", api_key)
            self._validate_models()

        # —— 仅传 api_key，则注入内置 default —— 
        elif api_key:
            self.models = {
                "default": {
                    "model_name": "gpt-4",
                    "type": "chat",
                    "context_length": 8192,
                    "max_tokens": 100,
                    "temperature": 0.7,
                    "price": {"prompt": 0.003, "completion": 0.003},
                    "api_key": api_key,
                }
            }
            self.prompts = {"default": {"system": "", "user": "You are a helpful assistant."}}
            self.default_model = "default"
            self.default_prompt = "default"
            self.logger.info("Initialized with built-in default + API key.")

        else:
            self.logger.info("Initialized with empty configuration (no models, no prompts).")

        # —— 兜底 fallback —— 
        if not self.models:
            self.models["default"] = {
                "model_name": "gpt-4",
                "type": "chat",
                "context_length": 8192,
                "max_tokens": 100,
                "temperature": 0.7,
                "price": {"prompt": 0.003, "completion": 0.003},
            }
            self.default_model = "default"
        if not self.prompts:
            self.prompts["default"] = {"system": "", "user": "You are a helpful assistant."}
            self.default_prompt = "default"

        # —— 最后确保有一个 OpenAI Client —— 
        if self.client is None:
            import openai

            key = (
                self.models[self.default_model].get("api_key")
                or os.environ.get("OPENAI_API_KEY")
            )
            self.client = openai.OpenAI(api_key=key)
            self.logger.info("Instantiated internal OpenAI client.")

    def _get_default_config_path(self) -> str:
        # openAIServices.py 同目录下
        base = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
        return os.path.join(base, "openai_config.yaml")

    def reload_configuration(self, new_config_path: Optional[str] = None):
        path = new_config_path or self._get_default_config_path()
        if not os.path.exists(path):
            raise FileNotFoundError(f"Cannot reload, not found: {path}")
        self.load_configuration(path)
        self._validate_models()
        self.logger.info(f"Configuration reloaded from {path}")

    def _validate_models(self):
        if not self.models:
            raise RuntimeError("No models configured.")
        if self.default_model not in self.models:
            old = self.default_model
            self.default_model = next(iter(self.models))
            self.logger.warning(
                f"default_model '{old}' invalid → '{self.default_model}'"
            )

    def load_configuration(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        # 如果顶层包了一层 key，就 unwrap
        if (
            isinstance(data, dict)
            and len(data) == 1
            and any(k in data[next(iter(data))] for k in ("models", "prompts"))
        ):
            data = next(iter(data.values()))

        # —— 加载 models —— 
        raw_models = data.get("models", {})
        self.models = {}
        for key, cfg in raw_models.items():
            m = dict(cfg)
            m.setdefault("model_name", key)
            t = str(m.get("type", "")).lower()
            if "audio" in t or "whisper" in key:
                m["type"] = "audio"
            elif "embed" in t:
                m["type"] = "embedding"
            elif "image" in t:
                m["type"] = "image"
            else:
                m["type"] = "chat"
            m["max_tokens"] = int(m.get("max_tokens", 0))
            m["temperature"] = float(m.get("temperature", 0.0))
            price = m.get("price", {})
            if isinstance(price, dict):
                m["price"] = {
                    "prompt": float(price.get("prompt", 0.0)),
                    "completion": float(price.get("completion", 0.0)),
                }
            else:
                m["price"] = {"prompt": float(price), "completion": 0.0}
            self.models[key] = m

        # —— 加载 prompts —— 
        raw_prompts = data.get("prompts", {})
        self.prompts = {}
        for pname, block in raw_prompts.items():
            if isinstance(block, dict):
                self.prompts[pname] = {
                    "system": block.get("system", "").strip(),
                    "user": block.get("user", "").strip(),
                }
            else:
                # 简单 string 当成 user
                self.prompts[pname] = {"system": "", "user": str(block).strip()}

        # —— 选默认 —— 
        self.default_model = (
            data.get("default_model")
            if data.get("default_model") in self.models
            else next(iter(self.models))
        )
        self.default_prompt = (
            "default" if "default" in self.prompts else next(iter(self.prompts))
        )

        self.logger.info(
            f"Loaded config: models={list(self.models)}, default_model={self.default_model}"
        )
        self.logger.info(
            f"Loaded prompts: {list(self.prompts)}, default_prompt={self.default_prompt}"
        )

    def get_system_prompt(self, prompt_name: Optional[str] = None) -> str:
        name = prompt_name or self.default_prompt
        if name not in self.prompts:
            name = self.default_prompt
        return self.prompts[name].get("system", "")

    def get_prompt(
        self, prompt_name: Optional[str] = None, variables: Optional[Dict[str, Any]] = None
    ) -> str:
        name = prompt_name or self.default_prompt
        if name not in self.prompts:
            self.logger.warning(
                f"Prompt '{name}' not found; using default '{self.default_prompt}'."
            )
            name = self.default_prompt
        template = self.prompts[name].get("user", "")

        # —— 规范大括号写法，支持 {{ var }} / { var } —— 
        template = re.sub(r"\{\{\s*(\w+)\s*\}\}", r"{\1}", template)
        template = re.sub(r"\{\s*(\w+)\s*\}", r"{\1}", template)

        if variables:
            try:
                return template.format(**variables)
            except Exception as e:
                self.logger.error(f"Formatting prompt '{name}' failed: {e}")
        return template

    def _build_chat_messages(
        self,
        prompt: Optional[str],
        prompt_template: Optional[str],
        template_vars: Optional[Dict[str, Any]],
    ) -> List[Dict[str, str]]:
        msgs: List[Dict[str, str]] = []
        if prompt_template:
            sys_txt = self.get_system_prompt(prompt_template)
            if sys_txt:
                msgs.append({"role": "system", "content": sys_txt})
            user_txt = self.get_prompt(prompt_template, template_vars)
            if prompt:
                user_txt += "\n" + prompt
            msgs.append({"role": "user", "content": user_txt})
        else:
            if prompt:
                msgs.append({"role": "user", "content": prompt})
        return msgs

    def _build_plain_prompt(
        self,
        prompt: Optional[str],
        prompt_template: Optional[str],
        template_vars: Optional[Dict[str, Any]],
    ) -> str:
        if prompt_template:
            base = self.get_prompt(prompt_template, template_vars)
            return base + (prompt or "")
        return prompt or ""

    def _retry_api_call(self, fn: Callable, *args, **kwargs):
        last = None
        for i in range(1, self.max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last = e
                self.logger.error(f"API attempt {i} failed: {e}")
                time.sleep(1)
        raise last

    def completion(
        self,
        *,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        mk = model or self.default_model
        if mk not in self.models:
            raise ValueError(f"Model '{mk}' not configured.")
        cfg = self.models[mk]

        if cfg["type"] == "chat":
            msgs = self._build_chat_messages(prompt, prompt_template, template_vars)
            resp = self._retry_api_call(
                self.client.chat.completions.create,
                model=cfg["model_name"],
                messages=msgs,
                **kwargs,
            )
        else:
            txt = self._build_plain_prompt(prompt, prompt_template, template_vars)
            resp = self._retry_api_call(
                self.client.completions.create,
                model=cfg["model_name"],
                prompt=txt,
                **kwargs,
            )

        if hasattr(resp, "model_dump"):
            resp = resp.model_dump()
        choice = resp.get("choices", [{}])[0]
        out = (
            choice.get("message", {}).get("content", "")
            if cfg["type"] == "chat"
            else choice.get("text", "")
        )

        # —— usage 统计 —— 
        usage = resp.get("usage", {}) or {}
        pt = usage.get("prompt_tokens", 0) or usage.get("total_tokens", 0)
        ct = usage.get("completion_tokens", 0)
        with self._lock:
            self.total_prompt_tokens += pt
            self.total_completion_tokens += ct
            cost = (pt * cfg["price"]["prompt"] + ct * cfg["price"]["completion"]) / 1000
            self.total_cost += cost
        self.logger.info(f"Usage updated: prompt {pt}, completion {ct}, cost ${cost:.6f}")

        return out or ""

    async def transcribe_audio(self, audio_file: BytesIO) -> str:
        """
        Whisper 转录，重试 self.max_retries 次。
        """
        audio_file.seek(0)
        if not hasattr(audio_file, "name") or not os.path.splitext(audio_file.name)[1]:
            audio_file.name = getattr(audio_file, "name", "audio") + ".mp3"

        for attempt in range(1, self.max_retries + 1):
            try:
                audio_file.seek(0)
                resp = await asyncio.to_thread(
                    self.client.audio.transcriptions.create,
                    file=audio_file,
                    model="whisper-1",
                    response_format="text",
                )
                if hasattr(resp, "model_dump"):
                    resp = resp.model_dump()
                if isinstance(resp, dict) and "text" in resp:
                    return resp["text"]
                if isinstance(resp, str):
                    return resp
                raise RuntimeError(f"Unexpected transcription response: {type(resp)}")
            except Exception as e:
                self.logger.error(f"Whisper attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(1)
        raise RuntimeError("Whisper transcription failed after all retries.")
