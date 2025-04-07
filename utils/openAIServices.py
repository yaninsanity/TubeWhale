#!/usr/bin/env python3
import asyncio
import logging
import os
import threading
import time
import yaml
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(_handler)


class OpenAIService:
    """
    OpenAIService 模块封装了所有 OpenAI API 相关调用，提供以下功能：
      - 支持通过 YAML 文件或直接传入 API key 加载模型与提示配置；
      - 同步接口：文本生成（completion）、嵌入计算（embedding）、流式输出（stream_completion）；
      - 异步接口：async_completion、transcribe_audio 等；
      - 内置错误重试机制（默认 3 次重试），并记录 token 使用和成本统计；
      - 支持 chat、completion、embedding、audio.transcriptions 等多种调用方式。
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        api_key: Optional[str] = None,
        client: Optional[Any] = None,
        logger: Optional[logging.Logger] = None
    ):
        # 若 config_path 为直接传入的 API key，则调整参数
        if config_path is not None and config_path.startswith("sk-"):
            api_key = config_path
            config_path = None

        self.models: Dict[str, Dict[str, Any]] = {}
        self.prompts: Dict[str, Dict[str, str]] = {}
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        self.max_retries: int = 3
        self.total_prompt_tokens: int = 0
        self.total_completion_tokens: int = 0
        self.total_cost: float = 0.0
        self._lock = threading.Lock()

        self.client = client
        self.logger = logger if logger else logging.getLogger(__name__)

        # 若直接传入 API key，则不加载配置文件
        if api_key:
            config_path = None

        # 尝试加载默认配置文件 openai_config.yaml（如果未传入 api_key）
        if config_path is None and api_key is None:
            default_config = self._get_default_config_path()
            if default_config and os.path.exists(default_config):
                config_path = default_config
                self.logger.info(f"Using default config file: {default_config}")
            else:
                self.logger.warning("No configuration provided and default config file not found; using empty configuration.")

        if config_path:
            self.load_configuration(config_path)
        elif api_key:
            # 使用内置默认配置
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
            self.prompts = {
                "default": {
                    "prompt": "You are a helpful assistant.",
                    "description": "Default prompt",
                },
                "keyword_generation": {
                    "prompt": (
                        "Generate up to {max_n} relevant keyword variations for the base keyword '{base_keyword}' "
                        "to search for high topic-related YouTube videos.\nReturn each keyword on a separate line without numbering."
                    ),
                    "description": "Keyword generation prompt",
                },
                "summarization": {
                    "prompt": (
                        "Based on the text provided below and considering the previous summary (if any), produce a refined and concise summary.\n"
                        "Previous Summary: \"{previous_summary}\"\n"
                        "Text: \"{text}\"\n"
                        "Your summary should be engaging, clear, and directly useful."
                    ),
                    "description": "Summarization prompt",
                }
            }
            self.default_model = "default"
            self.default_prompt = "default"
            self.logger.info("Initialized OpenAIService with direct API key and default configuration.")
        else:
            self.logger.info("Initialized OpenAIService with empty configuration.")

        if not self.models:
            self.logger.warning("No models loaded from configuration, using built-in default model.")
            self.models = {
                "default": {
                    "model_name": "gpt-4",
                    "type": "chat",
                    "context_length": 8192,
                    "max_tokens": 100,
                    "temperature": 0.7,
                    "price": {"prompt": 0.003, "completion": 0.003},
                }
            }
            self.default_model = "default"
        if not self.prompts:
            self.logger.warning("No prompts loaded from configuration, using built-in default prompts.")
            self.prompts = {
                "default": {
                    "prompt": "You are a helpful assistant.",
                    "description": "Default prompt",
                },
                "summarization": {
                    "prompt": (
                        "Based on the text provided below and considering the previous summary (if any), produce a refined and concise summary.\n"
                        "Previous Summary: \"{previous_summary}\"\n"
                        "Text: \"{text}\"\n"
                        "Your summary should be engaging, clear, and directly useful."
                    ),
                    "description": "Summarization prompt",
                }
            }
            self.default_prompt = "default"

        if self.client is None:
            import openai
            default_api_key = self.models.get(self.default_model, {}).get("api_key") or os.environ.get("OPENAI_API_KEY")
            self.client = openai.OpenAI(api_key=default_api_key)
            self.logger.info("Using new OpenAI client instance.")

    def _get_default_config_path(self) -> Optional[str]:
        try:
            default_dir = os.path.dirname(__file__)
        except Exception:
            default_dir = os.getcwd()
        return os.path.join(default_dir, "openai_config.yaml")

    def load_configuration(self, config_path: str) -> None:
        config_data = self._load_yaml_file(config_path)
        self.logger.info(f"Raw configuration loaded: {config_data}")
        if isinstance(config_data, dict) and len(config_data) == 1:
            first_key = next(iter(config_data))
            if isinstance(config_data[first_key], dict) and (
                "models" in config_data[first_key] or "prompts" in config_data[first_key]
            ):
                config_data = config_data[first_key]
        self._load_models_config(config_data.get("models", {}), config_data)
        self._load_prompts_config(config_data.get("prompts", {}))
        if "max_retries" in config_data:
            self.max_retries = config_data["max_retries"]
        if "default_model" in config_data and config_data["default_model"] in self.models:
            self.default_model = config_data["default_model"]
        elif self.models:
            self.default_model = next(iter(self.models))
        if "default" in self.prompts:
            self.default_prompt = "default"
        elif self.prompts:
            self.default_prompt = next(iter(self.prompts))
        self.logger.info(f"Configuration loaded. Models: {list(self.models.keys())}, Prompts: {list(self.prompts.keys())}")

    def _load_yaml_file(self, path: str) -> Any:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return data
        except Exception as e:
            logger.error(f"Failed to load configuration file {path}: {e}")
            raise Exception(f"Failed to load configuration file: {e}") from e

    def _load_models_config(self, models_section: Any, full_config: dict) -> None:
        models: Dict[str, Dict[str, Any]] = {}
        if isinstance(models_section, list):
            for model_cfg in models_section:
                if isinstance(model_cfg, dict):
                    model_key = model_cfg.get("name") or model_cfg.get("model_name")
                    if model_key:
                        models[model_key] = self._process_model_config(model_cfg)
        elif isinstance(models_section, dict):
            for model_key, model_cfg in models_section.items():
                if isinstance(model_cfg, dict):
                    if "model_name" not in model_cfg:
                        model_cfg["model_name"] = model_key
                    models[model_key] = self._process_model_config(model_cfg)
        else:
            logger.warning("No valid models configuration found.")
        self.models = models

        global_api_key = full_config.get("api_key")
        global_api_base = full_config.get("api_base")
        global_api_type = full_config.get("api_type")
        global_api_version = full_config.get("api_version")
        for m, cfg in self.models.items():
            if not cfg.get("api_key") and global_api_key:
                cfg["api_key"] = global_api_key
            if not cfg.get("api_base") and global_api_base:
                cfg["api_base"] = global_api_base
            if not cfg.get("api_type") and global_api_type:
                cfg["api_type"] = global_api_type
            if not cfg.get("api_version") and global_api_version:
                cfg["api_version"] = global_api_version

    def _process_model_config(self, model_cfg: Dict[str, Any]) -> Dict[str, Any]:
        cfg = dict(model_cfg)
        if "model_name" not in cfg and "name" in cfg:
            cfg["model_name"] = cfg["name"]
        model_name = cfg.get("model_name", "")
        model_type = cfg.get("type") or cfg.get("category")
        if not model_type:
            if "embedding" in model_name or model_name.startswith("text-embedding-"):
                model_type = "embedding"
            elif model_name.startswith("gpt-") or "turbo" in model_name or model_name.startswith("text-davinci-") or model_name.startswith("code-davinci-"):
                model_type = "chat"
            elif model_name.startswith("whisper-") or model_name.startswith("audio-"):
                model_type = "audio"
            elif model_name.lower().startswith("image") or "dall-e" in model_name.lower():
                model_type = "image"
            else:
                model_type = "completion"
        cfg["type"] = model_type

        if model_type not in ("chat", "completion"):
            cfg["context_length"] = cfg.get("context_length", 0)
            cfg["max_tokens"] = cfg.get("max_tokens", 0)
            cfg["temperature"] = cfg.get("temperature", 0)
        else:
            if "context_length" not in cfg:
                logger.warning(f"Model {model_name}: context_length not specified in config.")
            cfg["max_tokens"] = cfg.get("max_tokens", 0)
            cfg["temperature"] = cfg.get("temperature", 0)

        if "price" in cfg and isinstance(cfg["price"], dict):
            price_cfg = cfg["price"]
            price_cfg["prompt"] = float(price_cfg.get("prompt", 0.0))
            price_cfg["completion"] = float(price_cfg.get("completion", 0.0))
            cfg["price"] = price_cfg
        elif "price" in cfg and isinstance(cfg["price"], (int, float)):
            cfg["price"] = {"prompt": float(cfg["price"]), "completion": 0.0}
        else:
            cfg["price"] = {"prompt": 0.0, "completion": 0.0}
        return cfg

    def _load_prompts_config(self, prompts_section: Any) -> None:
        prompts: Dict[str, Dict[str, str]] = {}
        if isinstance(prompts_section, dict):
            for prompt_name, prompt_value in prompts_section.items():
                if prompt_value is not None:
                    if isinstance(prompt_value, str):
                        prompts[prompt_name] = {"prompt": prompt_value, "description": ""}
                    elif isinstance(prompt_value, dict):
                        if "prompt" not in prompt_value and "user" in prompt_value:
                            prompt_value["prompt"] = prompt_value["user"]
                        prompts[prompt_name] = {
                            "prompt": prompt_value.get("prompt", ""),
                            "description": prompt_value.get("description", "")
                        }
                    else:
                        logger.warning(f"Ignoring prompt {prompt_name} with unsupported type.")
        elif isinstance(prompts_section, list):
            for entry in prompts_section:
                if isinstance(entry, dict):
                    name = entry.get("name") or entry.get("id") or entry.get("prompt")
                    if name:
                        prompts[name] = {
                            "prompt": entry.get("prompt", ""),
                            "description": entry.get("description", "")
                        }
                    else:
                        logger.warning("Prompt entry without a name or id field.")
        else:
            logger.warning("No valid prompts configuration found.")
        self.prompts = prompts
        logger.info(f"Loaded prompts: {list(self.prompts.keys())}")

    def get_prompt(self, prompt_name: Optional[str], variables: Optional[Dict[str, Any]] = None) -> str:
        if prompt_name is None:
            prompt_name = self.default_prompt
        if prompt_name not in self.prompts:
            logger.warning(f"Prompt '{prompt_name}' not found. Using default prompt.")
            prompt_name = self.default_prompt
        prompt_entry = self.prompts.get(prompt_name, {})
        prompt_text = prompt_entry.get("prompt") or ""
        if variables:
            try:
                prompt_text = prompt_text.format(**variables)
            except Exception as e:
                logger.error(f"Error formatting prompt template '{prompt_name}' with variables {variables}: {e}")
        return prompt_text

    def _build_chat_messages(
        self,
        prompt: Optional[str],
        prompt_template: Optional[str],
        template_vars: Optional[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        messages = []
        if self.default_prompt:
            system_prompt_text = self.get_prompt(self.default_prompt)
            if system_prompt_text:
                messages.append({"role": "system", "content": system_prompt_text})
        if prompt_template:
            prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
            if prompt:
                if "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    prompt_text += "\n" + prompt
                else:
                    try:
                        prompt_text = prompt_text.format(user_input=prompt)
                    except Exception:
                        prompt_text += "\n" + prompt
            messages.append({"role": "user", "content": prompt_text})
        elif prompt:
            messages.append({"role": "user", "content": prompt})
        return messages

    def _build_plain_prompt(
        self,
        prompt: Optional[str],
        prompt_template: Optional[str],
        template_vars: Optional[Dict[str, Any]]
    ) -> str:
        final_prompt = ""
        if prompt_template:
            prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
            if prompt:
                if "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    final_prompt = prompt_text + prompt
                else:
                    try:
                        final_prompt = prompt_text.format(user_input=prompt)
                    except Exception:
                        final_prompt = prompt_text + prompt
            else:
                final_prompt = prompt_text
        else:
            final_prompt = prompt or ""
        return final_prompt

    def _update_usage(self, usage: Dict[str, Any], model_cfg: Dict[str, Any]) -> None:
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        with self._lock:
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += completion_tokens
            price_info = model_cfg.get("price", {})
            prompt_cost = price_info.get("prompt", 0.0)
            completion_cost = price_info.get("completion", 0.0)
            cost = round((prompt_tokens * prompt_cost + completion_tokens * completion_cost) / 1000.0, 6)
            self.total_cost += cost
        logger.info(f"Model {model_cfg.get('model_name')} used {prompt_tokens} prompt tokens and {completion_tokens} completion tokens, cost approx ${cost:.6f}.")

    def _retry_api_call(self, func: Callable, *args, **kwargs) -> Any:
        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                last_exception = e
                logger.error(f"Attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(1)
                else:
                    raise Exception(f"API call failed after {self.max_retries} attempts.") from e
        raise Exception("Unexpected error in _retry_api_call") from last_exception

    def _normalize_model_key(self, model_key: Optional[str]) -> Optional[str]:
        if model_key is None:
            return model_key
        if model_key in self.models:
            return model_key
        alt = model_key.replace('-', '_')
        if alt in self.models:
            return alt
        alt2 = model_key.replace('_', '-')
        if alt2 in self.models:
            return alt2
        return model_key

    def completion(
        self,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        model_key = model or self.default_model
        model_key = self._normalize_model_key(model_key)
        if model_key is None or model_key not in self.models:
            raise ValueError(f"Model '{model}' is not configured.")
        model_cfg = self.models[model_key]
        model_name = model_cfg.get("model_name", model_key)
        model_type = model_cfg.get("type")

        if model_type == "chat":
            messages = self._build_chat_messages(prompt, prompt_template, template_vars)
        else:
            final_prompt = self._build_plain_prompt(prompt, prompt_template, template_vars)

        params: Dict[str, Any] = {
            "temperature": model_cfg.get("temperature"),
            "max_tokens": model_cfg.get("max_tokens"),
            "top_p": model_cfg.get("top_p"),
            "frequency_penalty": model_cfg.get("frequency_penalty"),
            "presence_penalty": model_cfg.get("presence_penalty"),
        }
        params.update(kwargs)
        params["model"] = model_name
        params["stream"] = False

        if self.client is None:
            raise Exception("No OpenAI client available. Please provide a valid API key or client.")

        if model_type == "chat":
            response = self._retry_api_call(self.client.chat.completions.create, messages=messages, **params)
        else:
            combined_prompt = final_prompt
            response = self._retry_api_call(self.client.completions.create, prompt=combined_prompt, **params)

        # 如果返回对象不是字典，则尝试使用 model_dump() 转换
        if not isinstance(response, dict) and hasattr(response, "model_dump"):
            response = response.model_dump()

        if response is None:
            raise Exception("OpenAI API call failed without response.")

        result_text = ""
        choices = response.get("choices", [])
        if choices:
            if model_type == "chat":
                result_text = choices[0].get("message", {}).get("content", "")
            else:
                result_text = choices[0].get("text", "")
        usage = response.get("usage")
        if usage:
            self._update_usage(usage, model_cfg)
        else:
            logger.info("API call completed. (Usage details not provided)")
        return result_text

    def stream_completion(
        self,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Generator[str, None, None]:
        model_key = model or self.default_model
        model_key = self._normalize_model_key(model_key)
        if model_key is None or model_key not in self.models:
            raise ValueError(f"Model '{model}' is not configured.")
        model_cfg = self.models[model_key]
        model_name = model_cfg.get("model_name", model_key)
        model_type = model_cfg.get("type")
        if model_type == "chat":
            messages = self._build_chat_messages(prompt, prompt_template, template_vars)
        else:
            final_prompt = self._build_plain_prompt(prompt, prompt_template, template_vars)
        params: Dict[str, Any] = {
            "temperature": model_cfg.get("temperature"),
            "max_tokens": model_cfg.get("max_tokens"),
            "top_p": model_cfg.get("top_p"),
            "frequency_penalty": model_cfg.get("frequency_penalty"),
            "presence_penalty": model_cfg.get("presence_penalty"),
        }
        params.update(kwargs)
        params["model"] = model_name
        params["stream"] = True

        if self.client is None:
            raise Exception("No OpenAI client available. Please provide a valid API key or client.")

        if model_type == "chat":
            stream = self._retry_api_call(self.client.chat.completions.create, messages=messages, **params)
        else:
            combined_prompt = final_prompt
            stream = self._retry_api_call(self.client.completions.create, prompt=combined_prompt, **params)

        # 若返回的 stream 对象非字典列表，则在每个 chunk 上尝试转换
        partial_text = ""
        for chunk in stream:
            if not isinstance(chunk, dict) and hasattr(chunk, "model_dump"):
                chunk = chunk.model_dump()
            choices = chunk.get("choices", [])
            if model_type == "chat":
                delta = choices[0].get("delta", {}) if choices else {}
                text_part = delta.get("content", "")
            else:
                text_part = choices[0].get("text", "") if choices else ""
            partial_text += text_part
            yield text_part
        logger.info(f"Streaming completed for model {model_name}. Total output length: {len(partial_text)} characters.")

    def embedding(
        self,
        model: Optional[str] = None,
        input_data: Any = None,
        **kwargs
    ) -> Union[List[float], List[List[float]]]:
        model_key = model or self.default_model
        model_key = self._normalize_model_key(model_key)
        if model_key is None or model_key not in self.models:
            raise ValueError(f"Model '{model}' is not configured.")
        model_cfg = self.models[model_key]
        model_name = model_cfg.get("model_name", model_key)
        model_type = model_cfg.get("type")
        if model_type != "embedding":
            logger.warning(f"Model {model_name} may not support embeddings (type={model_type}). Proceeding.")
        if input_data is None:
            raise ValueError("No input provided for embedding.")
        params: Dict[str, Any] = {}
        params.update(kwargs)
        params["model"] = model_name
        params["input"] = input_data

        if self.client is None:
            raise Exception("No OpenAI client available. Please provide a valid API key or client.")

        response = self._retry_api_call(self.client.embeddings.create, **params)
        if not isinstance(response, dict) and hasattr(response, "model_dump"):
            response = response.model_dump()
        if response is None:
            raise Exception("OpenAI Embedding API call failed without response.")
        data_list = response.get("data")
        if data_list is None:
            logger.error("Unexpected response format from embedding API.")
            raise RuntimeError("Invalid response from embedding API.")
        embeddings = [item.get("embedding") for item in data_list if item.get("embedding") is not None]
        result = embeddings[0] if len(embeddings) == 1 else embeddings

        usage = response.get("usage")
        if usage:
            prompt_tokens = usage.get("prompt_tokens", 0) or usage.get("total_tokens", 0)
            with self._lock:
                self.total_prompt_tokens += prompt_tokens
                price_info = model_cfg.get("price", {})
                prompt_cost = price_info.get("prompt", 0.0)
                cost = round((prompt_tokens * prompt_cost) / 1000.0, 6)
                self.total_cost += cost
            logger.info(f"Model {model_name} embedding used {prompt_tokens} tokens, cost approx ${cost:.6f}.")
        else:
            logger.info("Embedding call completed. (Usage details not provided)")
        return result

    async def async_completion(
        self,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        return await asyncio.to_thread(
            lambda: self.completion(
                model=model,
                prompt=prompt,
                prompt_template=prompt_template,
                template_vars=template_vars,
                **kwargs
            )
        )

    async def transcribe_audio(self, audio_file: BytesIO) -> str:
        """
        异步调用 OpenAI 的 Whisper API 进行音频转录。要求传入的 audio_file 是一个包含有效音频数据的 BytesIO 对象，
        且必须包含正确的文件扩展名信息。若没有 name 属性，则默认设置为 "audio.mp3"。
        """
        # 确保文件指针处于开头位置
        audio_file.seek(0)
        
        # 如果没有 name 属性，则设置一个默认文件名，确保 API 能识别格式（比如 mp3）
        if not hasattr(audio_file, 'name'):
            audio_file.name = "audio.mp3"
        else:
            # 如果存在但没有扩展名，则补充 mp3 扩展名
            if not os.path.splitext(audio_file.name)[1]:
                audio_file.name += ".mp3"

        def call_transcription():
            return self.client.audio.transcriptions.create(
                file=audio_file,
                model="whisper-1",
                response_format="text",
            )

        try:
            response = await asyncio.to_thread(call_transcription)
        except Exception as e:
            self.logger.error(f"Error calling transcription API: {e}")
            return ""

        # 如果返回对象不是字典但具有 model_dump 方法，则调用 model_dump()
        if not isinstance(response, dict) and hasattr(response, "model_dump"):
            response = response.model_dump()
        
        # 返回转录文本，如果存在则返回 "text" 字段，否则返回空字符串
        if isinstance(response, dict):
            return response.get("text", "")
        return ""
