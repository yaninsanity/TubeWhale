import os
import yaml
import logging
from typing import Dict, Any, List, Generator, Optional
import openai

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 默认配置文件所在目录（可根据需要修改）
DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_DIR, "openai_config.yaml")


class OpenAIService:
    def __init__(
        self,
        config_path: Optional[str] = None,
        api_key: Optional[str] = None,
        client: Optional[Any] = None
    ):
        """
        初始化 OpenAIService：
          - 如果提供 config_path，则从 YAML 配置文件加载模型和提示；
          - 如果未传入 config_path，则自动使用 DEFAULT_CONFIG_FILE；
          - 如果传入 api_key，则采用简单默认配置（默认使用 GPT‑4 模型）；
          - 如果传入 client，则直接使用该客户端；
          - 若两者均未提供，则构造空配置（仅供内部方法单元测试使用）。

        :param config_path: YAML 配置文件路径
        :param api_key: API 密钥
        :param client: 已实例化的 OpenAI 客户端
        """
        self.models: Dict[str, Dict[str, Any]] = {}
        self.prompts: Dict[str, Dict[str, str]] = {}
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        self.max_retries: int = 3  # 默认重试次数
        self.total_prompt_tokens: int = 0
        self.total_completion_tokens: int = 0
        self.total_cost: float = 0.0

        # 优先使用外部传入的 client
        self.client = client

        if config_path is None and api_key is None:
            # 如果都未传入，则自动加载默认配置文件（如果存在）
            if os.path.exists(DEFAULT_CONFIG_FILE):
                config_path = DEFAULT_CONFIG_FILE
                logger.info(f"Using default config file: {DEFAULT_CONFIG_FILE}")
            else:
                logger.warning("No configuration provided and default config file not found; using empty configuration.")

        if config_path:
            self.load_configuration(config_path)
        elif api_key:
            # 直接通过 api_key 构造默认配置，默认使用 GPT‑4
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
                    "description": "默认提示",
                },
                "keyword_generation": {
                    "prompt": (
                        "Generate up to {max_n} relevant keyword variations for the base keyword '{base_keyword}' "
                        "to search for high topic-related YouTube videos.\nReturn each keyword on a separate line without numbering."
                    ),
                    "description": "生成关键词变体的提示",
                },
            }
            self.default_model = "default"
            self.default_prompt = "default"
            logger.info("Initialized OpenAIService with direct API key and default configuration.")
        else:
            logger.info("Initialized OpenAIService with empty configuration.")

        # 如果没有传入 client，则将 openai 模块作为默认客户端
        if self.client is None:
            self.client = openai
            logger.info("Using openai module as client.")

    def load_configuration(self, config_path: str) -> None:
        """
        从 YAML 文件加载配置，包括模型和提示模板。
        
        :param config_path: 配置文件路径
        """
        config_data = self._load_yaml_file(config_path)
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
        logger.info(f"Configuration loaded. Models: {list(self.models.keys())}, Prompts: {list(self.prompts.keys())}")

    def _load_yaml_file(self, path: str) -> Any:
        """
        加载 YAML 文件内容。
        
        :param path: YAML 文件路径
        :return: 解析后的数据
        :raises Exception: 加载文件失败时抛出异常
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return data
        except Exception as e:
            logger.error(f"Failed to load configuration file {path}: {e}")
            raise Exception(f"Failed to load configuration file: {e}") from e

    def _load_models_config(self, models_section: Any, full_config: dict) -> None:
        """
        加载模型配置，支持 list 和 dict 两种格式，同时应用全局 API 参数。
        
        :param models_section: 模型配置部分数据
        :param full_config: 整个配置文件数据
        """
        models = {}
        if isinstance(models_section, list):
            for model_cfg in models_section:
                if not isinstance(model_cfg, dict):
                    continue
                model_key = model_cfg.get("name") or model_cfg.get("model_name")
                if not model_key:
                    logger.warning("Skipping model config without a name identifier.")
                    continue
                models[model_key] = self._process_model_config(model_cfg)
        elif isinstance(models_section, dict):
            for model_key, model_cfg in models_section.items():
                if not isinstance(model_cfg, dict):
                    logger.warning(f"Model config for {model_key} is not a dict, skipping.")
                    continue
                if "model_name" not in model_cfg:
                    model_cfg["model_name"] = model_key
                models[model_key] = self._process_model_config(model_cfg)
        else:
            logger.warning("No valid models configuration found.")
        self.models = models

        # 应用全局 API 设置
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
        """
        处理单个模型的配置，自动推断模型类型和设置默认值。

        :param model_cfg: 单个模型的配置字典
        :return: 处理后的模型配置字典
        """
        cfg = dict(model_cfg)
        if "model_name" not in cfg and "name" in cfg:
            cfg["model_name"] = cfg["name"]
        model_name = cfg.get("model_name", "")
        model_type = cfg.get("type") or cfg.get("category")
        if not model_type:
            if "embedding" in model_name or model_name.startswith("text-embedding-"):
                model_type = "embedding"
            elif model_name.startswith("gpt-") or "turbo" in model_name or model_name.startswith("text-davinci-") or model_name.startswith("code-davinci-"):
                # 默认选择 chat 模式（GPT‑4 更适合聊天）
                model_type = "chat"
            elif model_name.startswith("whisper-") or model_name.startswith("audio-"):
                model_type = "audio"
            elif model_name.lower().startswith("image") or "dall-e" in model_name.lower():
                model_type = "image"
            else:
                model_type = "completion"
        cfg["type"] = model_type

        # 设置 context_length、max_tokens 和 temperature 的默认值
        if model_type not in ("chat", "completion"):
            cfg["context_length"] = 0 if cfg.get("context_length") is None else cfg["context_length"]
            cfg["max_tokens"] = 0 if cfg.get("max_tokens") is None else cfg["max_tokens"]
            cfg["temperature"] = 0 if cfg.get("temperature") is None else cfg["temperature"]
        else:
            if "context_length" not in cfg:
                logger.warning(f"Model {model_name}: context_length not specified in config.")
            cfg["max_tokens"] = 0 if cfg.get("max_tokens") is None else cfg["max_tokens"]
            cfg["temperature"] = 0 if cfg.get("temperature") is None else cfg["temperature"]

        # 处理价格信息
        if "price" in cfg and isinstance(cfg["price"], dict):
            price_cfg = cfg["price"]
            if cfg.get("type") == "audio":
                if "input" in price_cfg:
                    price_cfg["prompt"] = float(price_cfg["input"])
                if "output" in price_cfg:
                    price_cfg["completion"] = float(price_cfg["output"])
            else:
                if "input" in price_cfg or "output" in price_cfg:
                    if "prompt" not in price_cfg and "input" in price_cfg:
                        price_cfg["prompt"] = float(price_cfg["input"])
                    if "completion" not in price_cfg and "output" in price_cfg:
                        price_cfg["completion"] = float(price_cfg["output"])
            if "prompt" not in price_cfg:
                price_cfg["prompt"] = 0.0
            if "completion" not in price_cfg:
                price_cfg["completion"] = 0.0
            cfg["price"] = price_cfg
        elif "price" in cfg and isinstance(cfg["price"], (int, float)):
            cfg["price"] = {"prompt": float(cfg["price"]), "completion": 0.0}
        else:
            cfg["price"] = {"prompt": 0.0, "completion": 0.0}
        return cfg

    def _load_prompts_config(self, prompts_section: Any) -> None:
        """
        加载提示模板配置，支持 dict 和 list 两种格式。
        
        :param prompts_section: 提示模板部分数据
        """
        prompts = {}
        if isinstance(prompts_section, dict):
            for prompt_name, prompt_value in prompts_section.items():
                if prompt_value is None:
                    continue
                if isinstance(prompt_value, str):
                    prompts[prompt_name] = {"prompt": prompt_value, "description": ""}
                elif isinstance(prompt_value, dict):
                    prompt_text = prompt_value.get("prompt", "")
                    desc = prompt_value.get("description", "")
                    prompts[prompt_name] = {"prompt": prompt_text, "description": desc}
                else:
                    logger.warning(f"Ignoring prompt {prompt_name} with unsupported type.")
        elif isinstance(prompts_section, list):
            for entry in prompts_section:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name") or entry.get("id") or entry.get("prompt")
                if not name:
                    logger.warning("Prompt entry without a name or id field.")
                    continue
                prompt_text = entry.get("prompt", "")
                desc = entry.get("description", "")
                prompts[name] = {"prompt": prompt_text, "description": desc}
        else:
            logger.warning("No valid prompts configuration found.")
        self.prompts = prompts

    def get_prompt(self, prompt_name: Optional[str], variables: Optional[Dict[str, Any]] = None) -> str:
        """
        根据提示名称获取并格式化提示文本。
        
        :param prompt_name: 提示模板名称
        :param variables: 格式化所需变量
        :return: 格式化后的提示文本
        """
        if prompt_name is None:
            prompt_name = self.default_prompt
        if prompt_name not in self.prompts:
            logger.warning(f"Prompt '{prompt_name}' not found. Using default prompt.")
            prompt_name = self.default_prompt
        prompt_entry = self.prompts.get(prompt_name, {})
        prompt_text = prompt_entry.get("prompt", "")
        if variables:
            try:
                prompt_text = prompt_text.format(**variables)
            except Exception as e:
                logger.error(f"Error formatting prompt template '{prompt_name}' with variables {variables}: {e}")
        return prompt_text

    def _build_chat_messages(self, prompt: Optional[str], prompt_template: Optional[str], template_vars: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        构建 chat 模型的消息列表，包含系统和用户消息。
        
        :param prompt: 用户直接提供的提示
        :param prompt_template: 模板名称（可选）
        :param template_vars: 模板变量
        :return: 消息字典列表
        """
        messages = []
        # 添加系统消息
        if self.default_prompt:
            system_prompt_text = self.get_prompt(self.default_prompt)
            if system_prompt_text:
                messages.append({"role": "system", "content": system_prompt_text})

        # 添加用户消息
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

    def _build_plain_prompt(self, prompt: Optional[str], prompt_template: Optional[str], template_vars: Optional[Dict[str, Any]]) -> str:
        """
        构建非 chat 模型的最终提示文本。
        
        :param prompt: 用户直接提供的提示
        :param prompt_template: 模板名称（可选）
        :param template_vars: 模板变量
        :return: 最终的提示文本
        """
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

    def completion(
        self,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        使用指定模型和提示生成完整回复（非流式调用）。

        :param model: 模型配置键
        :param prompt: 用户直接输入的提示文本
        :param prompt_template: 提示模板名称（可选）
        :param template_vars: 提示模板变量
        :param kwargs: 其他 OpenAI API 参数
        :return: 生成的回复文本
        :raises Exception: 当 API 调用失败时或模型未配置时抛出异常
        """
        model_key = model or self.default_model
        if model_key is None:
            raise ValueError("No model specified and no default model set.")
        if model_key not in self.models:
            raise ValueError(f"Model '{model_key}' is not configured.")
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
        params["stream"] = False  # 非流式调用

        if self.client is None:
            raise Exception("No OpenAI client available. Please provide a valid API key or client.")

        response = None
        for attempt in range(1, self.max_retries + 1):
            try:
                if model_type == "chat":
                    response = self.client.ChatCompletion.create(messages=messages, **params)
                else:
                    response = self.client.Completion.create(prompt=final_prompt, **params)
                break
            except Exception as e:
                logger.error(f"Attempt {attempt} - API call failed for model {model_name}: {e}")
                if attempt == self.max_retries:
                    raise Exception(f"API call failed after {self.max_retries} attempts.") from e

        if response is None:
            raise Exception("OpenAI API call failed without response.")

        # 解析返回结果
        result_text = ""
        choices = response.choices if hasattr(response, "choices") else response.get("choices", [])
        if choices:
            if model_type == "chat":
                if isinstance(choices[0], dict):
                    result_text = choices[0].get("message", {}).get("content", "")
                else:
                    result_text = choices[0].message.get("content", "")
            else:
                if isinstance(choices[0], dict):
                    result_text = choices[0].get("text", "")
                else:
                    result_text = choices[0].text
        # 处理 token 使用情况及费用计算
        usage = (
            response.usage if hasattr(response, "usage")
            else response.get("usage") if isinstance(response, dict)
            else None
        )
        if usage:
            prompt_tokens = usage.get("prompt_tokens", 0)
            completion_tokens = usage.get("completion_tokens", 0)
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += completion_tokens
            price_info = model_cfg.get("price", {})
            prompt_cost = price_info.get("prompt", 0.0)
            completion_cost = price_info.get("completion", 0.0)
            cost = (prompt_tokens * prompt_cost + completion_tokens * completion_cost) / 1000.0
            self.total_cost += cost
            logger.info(f"Model {model_name} used {prompt_tokens} prompt tokens and {completion_tokens} completion tokens, cost approx ${cost:.6f}.")
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
        """
        使用指定模型和提示生成回复（流式调用）。

        :param model: 模型配置键
        :param prompt: 用户直接输入的提示文本
        :param prompt_template: 提示模板名称（可选）
        :param template_vars: 提示模板变量
        :param kwargs: 其他 OpenAI API 参数
        :yield: 每次返回生成的文本片段
        :raises Exception: 当 API 调用失败时抛出异常
        """
        model_key = model or self.default_model
        if model_key is None or model_key not in self.models:
            raise ValueError(f"Model '{model_key}' is not configured.")
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

        try:
            if model_type == "chat":
                stream = self.client.ChatCompletion.create(messages=messages, **params)
            else:
                stream = self.client.Completion.create(prompt=final_prompt, **params)
        except Exception as e:
            logger.error(f"Streaming API call failed for model {model_name}: {e}")
            raise Exception(f"Streaming API call failed: {e}") from e

        partial_text = ""
        for chunk in stream:
            if model_type == "chat":
                first_choice = chunk.choices[0]
                delta = first_choice.get("delta", {}) if isinstance(first_choice, dict) else getattr(first_choice, "delta", {})
                text_part = delta.get("content", "")
            else:
                first_choice = chunk.choices[0]
                text_part = first_choice.get("text", "") if isinstance(first_choice, dict) else getattr(first_choice, "text", "")
            partial_text += text_part
            yield text_part
        logger.info(f"Streaming completed for model {model_name}. Total length of output: {len(partial_text)} characters.")

    def embedding(
        self,
        model: Optional[str] = None,
        input_data: Any = None,
        **kwargs
    ) -> List[float]:
        """
        使用指定模型生成文本或数据的嵌入向量。

        :param model: 模型配置键
        :param input_data: 输入数据（文本或其他可处理的数据）
        :param kwargs: 其他 OpenAI API 参数
        :return: 嵌入向量列表（若仅生成一个向量，则直接返回该向量）
        :raises ValueError: 当输入为空时
        :raises RuntimeError: 当返回格式异常时
        """
        model_key = model or self.default_model
        if model_key is None:
            raise ValueError("No model specified for embedding and no default model set.")
        if model_key not in self.models:
            raise ValueError(f"Model '{model_key}' is not configured.")
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

        response = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.client.Embedding.create(**params)
                break
            except Exception as e:
                logger.error(f"Attempt {attempt} - Embedding API call failed for model {model_name}: {e}")
                if attempt == self.max_retries:
                    raise Exception(f"Embedding API call failed after {self.max_retries} attempts.") from e
        if response is None:
            raise Exception("OpenAI Embedding API call failed without response.")
        # 修改处理：如果 response 是 dict，则使用 .get("data")
        if isinstance(response, dict):
            data_list = response.get("data")
        else:
            data_list = getattr(response, "data", None)
        if data_list is None:
            logger.error("Unexpected response format from embedding API.")
            raise RuntimeError("Invalid response from embedding API.")
        embeddings = [item.get("embedding") for item in data_list if item.get("embedding") is not None]
        result = embeddings[0] if len(embeddings) == 1 else embeddings

        usage = (
            response.usage if hasattr(response, "usage")
            else response.get("usage") if isinstance(response, dict)
            else None
        )
        if usage:
            prompt_tokens = usage.get("prompt_tokens", 0) or usage.get("total_tokens", 0)
            self.total_prompt_tokens += prompt_tokens
            price_info = model_cfg.get("price", {})
            prompt_cost = price_info.get("prompt", 0.0)
            cost = (prompt_tokens * prompt_cost) / 1000.0
            self.total_cost += cost
            logger.info(f"Model {model_name} embedding used {prompt_tokens} tokens, cost approx ${cost:.6f}.")
        else:
            logger.info("Embedding call completed. (Usage details not provided)")
        return result
