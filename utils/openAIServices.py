import openai
import yaml
import logging
from typing import Dict, Any, List, Generator, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class OpenAIService:
    def __init__(self, config_path: Optional[str] = None):
        """
        OpenAIService provides a wrapper for interacting with OpenAI models defined in a YAML configuration.
        It supports multiple models (text, chat, embedding), prompt templates, token usage tracking,
        and both streaming and non-streaming completions.
        """
        # Configuration data structures
        self.models: Dict[str, Dict[str, Any]] = {}
        self.prompts: Dict[str, Dict[str, str]] = {}
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        self.max_retries: int = 3  # default retry count
        # Usage tracking
        self.total_prompt_tokens: int = 0
        self.total_completion_tokens: int = 0
        self.total_cost: float = 0.0

        if config_path:
            self.load_configuration(config_path)

    def load_configuration(self, config_path: str) -> None:
        """
        Load and parse the configuration from a YAML file.
        This includes model configurations and prompt templates.
        """
        config_data = self._load_yaml_file(config_path)
        # If config_data has a single top-level key containing models or prompts, unwrap it
        if isinstance(config_data, dict) and len(config_data) == 1:
            first_key = next(iter(config_data))
            if isinstance(config_data[first_key], dict) and ("models" in config_data[first_key] or "prompts" in config_data[first_key]):
                config_data = config_data[first_key]
        # Load components
        self._load_models_config(config_data.get("models", {}), config_data)
        self._load_prompts_config(config_data.get("prompts", {}))
        # Load global settings if any
        if "max_retries" in config_data:
            self.max_retries = config_data["max_retries"]
        if "default_model" in config_data:
            default_model = config_data["default_model"]
            if default_model in self.models:
                self.default_model = default_model
        # Determine a default model if not set
        if self.default_model is None and self.models:
            self.default_model = next(iter(self.models))
        # Set default prompt key if present
        if "default" in self.prompts:
            self.default_prompt = "default"
        elif self.prompts:
            self.default_prompt = next(iter(self.prompts))
        logger.info(f"Configuration loaded. Models: {list(self.models.keys())}, Prompts: {list(self.prompts.keys())}")

    def _load_yaml_file(self, path: str) -> Any:
        """Read a YAML configuration file and return the parsed data."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            return data
        except Exception as e:
            logger.error(f"Failed to load configuration file {path}: {e}")
            raise

    def _load_models_config(self, models_section: Any, full_config: dict) -> None:
        """
        Parse and validate the models section of the configuration.
        Supports both list and dict format for multiple models.
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
                if model_cfg is None:
                    continue
                if not isinstance(model_cfg, dict):
                    logger.warning(f"Model config for {model_key} is not a dict, skipping.")
                    continue
                # 保持 YAML 中的 key，同时补充 model_name（如果不存在）
                if "model_name" not in model_cfg:
                    model_cfg["model_name"] = model_key
                models[model_key] = self._process_model_config(model_cfg)
        else:
            logger.warning("No valid models configuration found.")
        self.models = models

        # Apply global API settings to models if not overridden
        global_api_key = full_config.get("api_key")
        global_api_base = full_config.get("api_base")
        global_api_type = full_config.get("api_type")
        global_api_version = full_config.get("api_version")
        for m, cfg in self.models.items():
            if "api_key" not in cfg or cfg["api_key"] is None:
                if global_api_key:
                    cfg["api_key"] = global_api_key
            if "api_base" not in cfg or cfg["api_base"] is None:
                if global_api_base:
                    cfg["api_base"] = global_api_base
            if "api_type" not in cfg or cfg["api_type"] is None:
                if global_api_type:
                    cfg["api_type"] = global_api_type
            if "api_version" not in cfg or cfg["api_version"] is None:
                if global_api_version:
                    cfg["api_version"] = global_api_version

    def _process_model_config(self, model_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and fill defaults for a single model configuration entry.
        Ensures required fields are present and optional fields have defaults.
        For non-text models (like embedding or image), fill missing context_length, max_tokens, temperature with 0.
        """
        cfg = dict(model_cfg)
        if "model_name" not in cfg:
            if "name" in cfg:
                cfg["model_name"] = cfg["name"]
        # Determine model type if not specified
        model_name = cfg.get("model_name", "")
        model_type = cfg.get("type") or cfg.get("category")
        if not model_type:
            if "embedding" in model_name or model_name.startswith("text-embedding-"):
                model_type = "embedding"
            elif model_name.startswith("gpt-") or "turbo" in model_name or model_name.startswith("text-davinci-") or model_name.startswith("code-davinci-"):
                if "gpt-3.5-turbo" in model_name or "gpt-4" in model_name:
                    model_type = "chat"
                else:
                    model_type = "completion"
            elif model_name.startswith("whisper-") or model_name.startswith("audio-"):
                model_type = "audio"
            elif model_name.lower().startswith("image") or "dall-e" in model_name.lower():
                model_type = "image"
            else:
                model_type = "completion"
        cfg["type"] = model_type

        # Fill missing fields for non-text models with 0
        if model_type not in ("chat", "completion"):
            if "context_length" not in cfg or cfg.get("context_length") is None:
                cfg["context_length"] = 0
            if "max_tokens" not in cfg or cfg.get("max_tokens") is None:
                cfg["max_tokens"] = 0
            if "temperature" not in cfg or cfg.get("temperature") is None:
                cfg["temperature"] = 0
        else:
            if "context_length" not in cfg:
                logger.warning(f"Model {model_name}: context_length not specified in config.")
            if "max_tokens" not in cfg:
                cfg["max_tokens"] = 0
            if "temperature" not in cfg:
                cfg["temperature"] = 0
        # Price handling
        if "price" in cfg and isinstance(cfg["price"], dict):
            price_cfg = cfg["price"]
            if "input" in price_cfg or "output" in price_cfg:
                if "prompt" not in price_cfg and "input" in price_cfg:
                    price_cfg["prompt"] = price_cfg["input"]
                if "completion" not in price_cfg and "output" in price_cfg:
                    price_cfg["completion"] = price_cfg["output"]
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
        Parse the prompts section of the configuration.
        Supports prompt entries with or without description. Stores prompt text and description.
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

    def get_prompt(self, prompt_name: Optional[str], variables: Dict[str, Any] = None) -> str:
        """
        Retrieve and render a prompt template by name. If variables provided, format the prompt with them.
        Fallback to default prompt if the specified name is not found.
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

    def completion(self, model: Optional[str] = None, prompt: Optional[str] = None,
                   prompt_template: Optional[str] = None, template_vars: Dict[str, Any] = None,
                   **kwargs) -> str:
        """
        Generate a completion (chat or text) from the specified model with the given prompt.
        This is a non-streaming call that returns the full completion result.
        """
        model_key = model or self.default_model
        if model_key is None:
            raise ValueError("No model specified and no default model set.")
        if model_key not in self.models:
            raise ValueError(f"Model '{model_key}' is not configured.")
        model_cfg = self.models[model_key]
        model_name = model_cfg.get("model_name", model_key)
        model_type = model_cfg.get("type")
        messages = None
        final_prompt = None

        if model_type == "chat":
            messages = []
            if self.default_prompt:
                system_prompt_text = self.get_prompt(self.default_prompt)
                if system_prompt_text:
                    messages.append({"role": "system", "content": system_prompt_text})
            if prompt_template:
                if prompt_template != self.default_prompt:
                    prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
                    if prompt and "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                        prompt_text = prompt_text + ("\n" + prompt if prompt else "")
                    elif not prompt and template_vars:
                        pass  # prompt_text already formatted with variables
                    elif prompt and "{" in self.prompts.get(prompt_template, {}).get("prompt", ""):
                        try:
                            prompt_text = prompt_text.format(user_input=prompt)
                        except Exception:
                            prompt_text = prompt_text + ("\n" + prompt)
                    if prompt_text:
                        messages.append({"role": "user", "content": prompt_text})
            if prompt and (prompt_template is None or prompt_template == self.default_prompt):
                messages.append({"role": "user", "content": prompt})
        else:
            if prompt_template:
                prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
                if prompt and "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    final_prompt = prompt_text + prompt
                elif prompt:
                    try:
                        final_prompt = prompt_text.format(user_input=prompt)
                    except Exception:
                        final_prompt = prompt_text + prompt
                else:
                    final_prompt = prompt_text
            else:
                final_prompt = prompt or ""

        # Set API credentials for this model
        api_key = model_cfg.get("api_key")
        api_base = model_cfg.get("api_base")
        api_type = model_cfg.get("api_type")
        api_version = model_cfg.get("api_version")
        if api_key:
            openai.api_key = api_key
        if api_base:
            openai.api_base = api_base
        if api_type:
            openai.api_type = api_type
        if api_version:
            openai.api_version = api_version

        # Prepare API call parameters
        params = {}
        if "temperature" in model_cfg and "temperature" not in kwargs:
            params["temperature"] = model_cfg["temperature"]
        if "max_tokens" in model_cfg and "max_tokens" not in kwargs:
            params["max_tokens"] = model_cfg["max_tokens"]
        if "top_p" in model_cfg and "top_p" not in kwargs:
            params["top_p"] = model_cfg["top_p"]
        if "frequency_penalty" in model_cfg and "frequency_penalty" not in kwargs:
            params["frequency_penalty"] = model_cfg["frequency_penalty"]
        if "presence_penalty" in model_cfg and "presence_penalty" not in kwargs:
            params["presence_penalty"] = model_cfg["presence_penalty"]
        params.update(kwargs)
        params["model"] = model_name

        # Call the OpenAI API (with retries)
        response = None
        for attempt in range(1, self.max_retries + 1):
            try:
                if model_type == "chat":
                    response = openai.ChatCompletion.create(messages=messages, **params)
                else:
                    response = openai.Completion.create(prompt=final_prompt, **params)
                break
            except Exception as e:
                logger.error(f"Attempt {attempt} - API call failed for model {model_name}: {e}")
                if attempt == self.max_retries:
                    raise
                continue

        if response is None:
            raise RuntimeError("OpenAI API call failed without response.")
        # Extract completion text from response
        if model_type == "chat":
            choices = response.choices if hasattr(response, "choices") else response.get("choices", [])
            if choices:
                if isinstance(choices[0], dict):
                    result_text = choices[0].get("message", {}).get("content", "")
                else:
                    result_text = choices[0].message.get("content", "")
            else:
                result_text = ""
        else:
            choices = response.choices if hasattr(response, "choices") else response.get("choices", [])
            if choices:
                if isinstance(choices[0], dict):
                    result_text = choices[0].get("text", "")
                else:
                    result_text = choices[0].text
            else:
                result_text = ""

        # Token usage and cost logging
        usage = response.usage if hasattr(response, "usage") else response.get("usage") if isinstance(response, dict) else None
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

    def stream_completion(self, model: Optional[str] = None, prompt: Optional[str] = None,
                           prompt_template: Optional[str] = None, template_vars: Dict[str, Any] = None,
                           **kwargs) -> Generator[str, None, None]:
        """
        Stream a completion (for chat or text models) from the specified model with the given prompt.
        Yields the completion text in parts as they are received from the API.
        """
        model_key = model or self.default_model
        if model_key is None or model_key not in self.models:
            raise ValueError(f"Model '{model_key}' is not configured.")
        model_cfg = self.models[model_key]
        model_name = model_cfg.get("model_name", model_key)
        model_type = model_cfg.get("type")
        messages = None
        final_prompt = None

        if model_type == "chat":
            messages = []
            if self.default_prompt:
                system_prompt_text = self.get_prompt(self.default_prompt)
                if system_prompt_text:
                    messages.append({"role": "system", "content": system_prompt_text})
            if prompt_template and prompt_template != self.default_prompt:
                prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
                if prompt and "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    prompt_text = prompt_text + ("\n" + prompt if prompt else "")
                elif prompt and "{" in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    try:
                        prompt_text = prompt_text.format(user_input=prompt)
                    except Exception:
                        prompt_text = prompt_text + ("\n" + prompt)
                if prompt_text:
                    messages.append({"role": "user", "content": prompt_text})
            if prompt and (prompt_template is None or prompt_template == self.default_prompt):
                messages.append({"role": "user", "content": prompt})
        else:
            if prompt_template:
                prompt_text = self.get_prompt(prompt_template, variables=template_vars or {})
                if prompt and "{" not in self.prompts.get(prompt_template, {}).get("prompt", ""):
                    final_prompt = prompt_text + prompt
                elif prompt:
                    try:
                        final_prompt = prompt_text.format(user_input=prompt)
                    except Exception:
                        final_prompt = prompt_text + prompt
                else:
                    final_prompt = prompt_text
            else:
                final_prompt = prompt or ""

        # Set API credentials for this model
        api_key = model_cfg.get("api_key")
        api_base = model_cfg.get("api_base")
        api_type = model_cfg.get("api_type")
        api_version = model_cfg.get("api_version")
        if api_key:
            openai.api_key = api_key
        if api_base:
            openai.api_base = api_base
        if api_type:
            openai.api_type = api_type
        if api_version:
            openai.api_version = api_version

        # Prepare parameters for streaming API call
        params = {}
        if "temperature" in model_cfg and "temperature" not in kwargs:
            params["temperature"] = model_cfg["temperature"]
        if "max_tokens" in model_cfg and "max_tokens" not in kwargs:
            params["max_tokens"] = model_cfg["max_tokens"]
        if "top_p" in model_cfg and "top_p" not in kwargs:
            params["top_p"] = model_cfg["top_p"]
        if "frequency_penalty" in model_cfg and "frequency_penalty" not in kwargs:
            params["frequency_penalty"] = model_cfg["frequency_penalty"]
        if "presence_penalty" in model_cfg and "presence_penalty" not in kwargs:
            params["presence_penalty"] = model_cfg["presence_penalty"]
        params.update(kwargs)
        params["model"] = model_name
        params["stream"] = True

        # Initiate the streaming API call
        try:
            if model_type == "chat":
                stream = openai.ChatCompletion.create(messages=messages, **params)
            else:
                stream = openai.Completion.create(prompt=final_prompt, **params)
        except Exception as e:
            logger.error(f"Streaming API call failed for model {model_name}: {e}")
            raise

        partial_text = ""
        for chunk in stream:
            if model_type == "chat":
                first_choice = chunk.choices[0]
                if isinstance(first_choice, dict):
                    delta = first_choice.get("delta", {})
                else:
                    delta = first_choice.delta
                text_part = delta.get("content", "")
            else:
                first_choice = chunk.choices[0]
                if isinstance(first_choice, dict):
                    text_part = first_choice.get("text", "")
                else:
                    text_part = first_choice.text
            partial_text += text_part
            yield text_part
        logger.info(f"Streaming completed for model {model_name}. Total length of output: {len(partial_text)} characters.")

    def embedding(self, model: Optional[str] = None, input_data: Any = None, **kwargs) -> List[float]:
        """
        Get embedding for the input text using the specified embedding model.
        Returns the embedding vector (or list of vectors for multiple inputs).
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
        # Set API credentials for this model
        api_key = model_cfg.get("api_key")
        api_base = model_cfg.get("api_base")
        api_type = model_cfg.get("api_type")
        api_version = model_cfg.get("api_version")
        if api_key:
            openai.api_key = api_key
        if api_base:
            openai.api_base = api_base
        if api_type:
            openai.api_type = api_type
        if api_version:
            openai.api_version = api_version

        if input_data is None:
            raise ValueError("No input provided for embedding.")
        params = {}
        params.update(kwargs)
        params["model"] = model_name
        params["input"] = input_data

        # Call the OpenAI Embedding API (with retries)
        response = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = openai.Embedding.create(**params)
                break
            except Exception as e:
                logger.error(f"Attempt {attempt} - Embedding API call failed for model {model_name}: {e}")
                if attempt == self.max_retries:
                    raise
                continue
        if response is None:
            raise RuntimeError("OpenAI Embedding API call failed without response.")
        data_list = response.get("data") if isinstance(response, dict) else getattr(response, "data", None)
        if data_list is None:
            logger.error("Unexpected response format from embedding API.")
            raise RuntimeError("Invalid response from embedding API.")
        embeddings = [item.get("embedding") for item in data_list if item.get("embedding") is not None]
        result = embeddings[0] if len(embeddings) == 1 else embeddings

        usage = response.usage if hasattr(response, "usage") else response.get("usage") if isinstance(response, dict) else None
        if usage:
            prompt_tokens = usage.get("prompt_tokens", 0) or usage.get("total_tokens", 0)
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += 0
            price_info = model_cfg.get("price", {})
            prompt_cost = price_info.get("prompt", 0.0)
            cost = (prompt_tokens * prompt_cost) / 1000.0
            self.total_cost += cost
            logger.info(f"Model {model_name} embedding used {prompt_tokens} tokens, cost approx ${cost:.6f}.")
        else:
            logger.info("Embedding call completed. (Usage details not provided)")
        return result
