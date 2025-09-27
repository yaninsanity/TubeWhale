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

# Import the new configuration manager
from utils.configuration_manager import ConfigurationManager, ConfigurationMode, get_configuration_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_handler)


class OpenAIService:
    """
    Enhanced OpenAIService with enterprise configuration management:
      - Integration with ConfigurationManager for dynamic template loading
      - Support for environment-specific configurations  
      - Hot-reload capabilities with enterprise template injection
      - Thread-safe configuration updates
      - CLI-friendly template overrides
      - Comprehensive usage tracking and retry mechanisms
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        api_key: Optional[str] = None,
        client: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
        configuration_manager: Optional[ConfigurationManager] = None,
        template_id_override: Optional[str] = None,
        model_override: Optional[str] = None,
    ):
        # Legacy support for existing configuration format
        self.models: Dict[str, Dict[str, Any]] = {}
        self.prompts: Dict[str, Dict[str, str]] = {}
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        
        # Enhanced configuration management
        self.config_manager = configuration_manager or get_configuration_manager()
        self.template_id_override = template_id_override
        self.model_override = model_override
        
        # Usage tracking and settings
        self.max_retries = 3
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_cost = 0.0
        self._lock = threading.Lock()

        self.client = client
        self.logger = logger or logging.getLogger(__name__)

        # —— Enhanced configuration loading with ConfigurationManager —— 
        self._load_configurations_from_manager()
        
        # —— Legacy fallback for direct configuration —— 
        if config_path or (not self.models and not self.prompts):
            self._load_legacy_configuration(config_path, api_key)
        
        # —— Apply CLI overrides if provided —— 
        if template_id_override or model_override:
            self.config_manager.override_for_cli(template_id_override, model_override)
            self._load_configurations_from_manager()
        
        # —— Ensure OpenAI Client is initialized —— 
        if self.client is None:
            import openai
            
            # Get API key from current default model or environment
            api_key_to_use = api_key
            if self.default_model and self.default_model in self.models:
                api_key_to_use = self.models[self.default_model].get("api_key") or api_key_to_use
            api_key_to_use = api_key_to_use or os.environ.get("OPENAI_API_KEY")
            
            if not api_key_to_use:
                self.logger.warning("No API key found. OpenAI client may not work properly.")
            
            self.client = openai.OpenAI(api_key=api_key_to_use)
            self.logger.info("Instantiated OpenAI client with enhanced configuration.")

    def _load_configurations_from_manager(self):
        """Load configurations from the ConfigurationManager"""
        try:
            # Get configuration in OpenAI service compatible format
            config_data = self.config_manager.get_openai_service_config()
            
            # Update models and prompts
            self.models = config_data.get("models", {})
            self.prompts = config_data.get("prompts", {})
            self.default_model = config_data.get("default_model")
            self.default_prompt = config_data.get("default_prompt")
            
            self.logger.info(
                f"Loaded from ConfigurationManager: {len(self.models)} models, "
                f"{len(self.prompts)} prompts, mode: {self.config_manager.mode.value}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load configurations from manager: {e}")
            self._load_fallback_configuration()

    def _load_legacy_configuration(self, config_path: Optional[str], api_key: Optional[str]):
        """Legacy configuration loading for backward compatibility"""
        if config_path is None:
            default_cfg = self._get_default_config_path()
            if os.path.exists(default_cfg):
                config_path = default_cfg
                self.logger.info(f"Using legacy config file: {default_cfg}")
            else:
                self.logger.warning("No config_path provided and no default config found.")

        if config_path:
            self.load_configuration(config_path)
            # Inject API key if provided
            if api_key:
                for m in self.models.values():
                    m.setdefault("api_key", api_key)
            self._validate_models()
        elif api_key:
            # Fallback to built-in default with API key
            self._load_fallback_configuration(api_key)

    def _load_fallback_configuration(self, api_key: Optional[str] = None):
        """Load fallback configuration when other methods fail"""
        self.models = {
            "default": {
                "model_name": "gpt-4",
                "type": "chat",
                "context_length": 8192,
                "max_tokens": 1000,
                "temperature": 0.7,
                "price": {"prompt": 0.003, "completion": 0.003},
                "api_key": api_key,
            }
        }
        self.prompts = {"default": {"system": "", "user": "You are a helpful assistant."}}
        self.default_model = "default"
        self.default_prompt = "default"
        self.logger.info("Loaded fallback configuration.")

    def _get_default_config_path(self) -> str:
        # openAIServices.py 同目录下
        base = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
        return os.path.join(base, "openai_config.yaml")

    def reload_configuration(self, new_config_path: Optional[str] = None, force_manager_reload: bool = True):
        """
        Enhanced configuration reload with ConfigurationManager support
        
        Args:
            new_config_path: Legacy path-based reload (for backward compatibility)
            force_manager_reload: Whether to force ConfigurationManager reload
        """
        if force_manager_reload and hasattr(self, 'config_manager'):
            # Use ConfigurationManager for hot-reload
            self.config_manager.reload_configuration(force=True)
            self._load_configurations_from_manager()
            self.logger.info("Configuration reloaded via ConfigurationManager")
        elif new_config_path:
            # Legacy file-based reload
            path = new_config_path or self._get_default_config_path()
            if not os.path.exists(path):
                raise FileNotFoundError(f"Cannot reload, not found: {path}")
            self.load_configuration(path)
            self._validate_models()
            self.logger.info(f"Legacy configuration reloaded from {path}")
        else:
            # Try ConfigurationManager first, then legacy
            try:
                if hasattr(self, 'config_manager'):
                    self.config_manager.reload_configuration(force=True)
                    self._load_configurations_from_manager()
                    self.logger.info("Configuration reloaded via ConfigurationManager")
                else:
                    raise AttributeError("No ConfigurationManager available")
            except Exception as e:
                self.logger.warning(f"ConfigurationManager reload failed: {e}, trying legacy reload")
                path = self._get_default_config_path()
                if os.path.exists(path):
                    self.load_configuration(path)
                    self._validate_models()
                    self.logger.info(f"Legacy configuration reloaded from {path}")
                else:
                    raise FileNotFoundError("No configuration source available for reload")

    def update_template_override(self, template_id: Optional[str], model_override: Optional[str] = None):
        """
        Update template and model overrides at runtime
        
        Args:
            template_id: Template ID to use for prompts
            model_override: Model name to use for completions
        """
        if hasattr(self, 'config_manager'):
            self.config_manager.override_for_cli(template_id, model_override)
            self._load_configurations_from_manager()
            self.logger.info(f"Runtime override applied: template='{template_id}', model='{model_override}'")
        else:
            # Legacy override mechanism
            if template_id and template_id in self.prompts:
                self.default_prompt = template_id
            if model_override and model_override in self.models:
                self.default_model = model_override
            self.logger.info(f"Legacy override applied: template='{template_id}', model='{model_override}'")

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

    @classmethod
    def create_with_template(cls, 
                           template_id: Optional[str] = None,
                           model_override: Optional[str] = None,
                           api_key: Optional[str] = None,
                           mode: Optional[ConfigurationMode] = None) -> 'OpenAIService':
        """
        Factory method to create OpenAIService with specific template configuration
        
        Args:
            template_id: Template ID to use for prompts
            model_override: Model name to use for completions  
            api_key: OpenAI API key
            mode: Configuration mode (development, production, etc.)
            
        Returns:
            Configured OpenAIService instance
        """
        # Get configuration manager for the specified mode
        config_manager = get_configuration_manager(mode=mode)
        
        # Apply overrides if provided
        if template_id or model_override:
            config_manager.override_for_cli(template_id, model_override)
        
        return cls(
            api_key=api_key,
            configuration_manager=config_manager,
            template_id_override=template_id,
            model_override=model_override
        )

    def completion(
        self,
        *,
        model: Optional[str] = None,
        prompt: Optional[str] = None,
        prompt_template: Optional[str] = None,
        template_vars: Optional[Dict[str, Any]] = None,
        expert_role: Optional[str] = None,
        template_domain: Optional[str] = None,
        video_data: Optional[Dict[str, Any]] = None,
        force_json_output: bool = True,
        **kwargs
    ) -> str:
        mk = model or self.default_model
        if mk not in self.models:
            raise ValueError(f"Model '{mk}' not configured.")
        cfg = self.models[mk]

        # ✨ Enhanced template integration with admin-managed templates
        try:
            from apps.templates_app.integrator import template_openai_integrator
            
            # Use template integrator if prompt_template is provided
            if prompt_template:
                integrated_config = template_openai_integrator.get_integrated_prompt(
                    template_id=prompt_template,
                    role=expert_role,
                    domain=template_domain or "general",
                    video_data=video_data,
                    custom_variables=template_vars
                )
                
                # Build messages using integrated prompt
                msgs = []
                if integrated_config["system_prompt"]:
                    msgs.append({"role": "system", "content": integrated_config["system_prompt"]})
                
                user_content = integrated_config["user_prompt"]
                if prompt:  # Append additional prompt if provided
                    user_content += f"\n\n{prompt}"
                
                msgs.append({"role": "user", "content": user_content})
                
                # Apply template config parameters
                template_config = integrated_config.get("template_config", {})
                if "max_tokens" in template_config:
                    kwargs.setdefault("max_tokens", template_config["max_tokens"])
                if "temperature" in template_config:
                    kwargs.setdefault("temperature", template_config["temperature"])
                
                # Force JSON output if requested and schema available
                if force_json_output and integrated_config.get("json_schema"):
                    json_instruction = f"\n\nIMPORTANT: Return your response as valid JSON following this structure: {integrated_config['json_schema']}"
                    msgs[-1]["content"] += json_instruction
                
                self.logger.info(f"✅ Using integrated template: {prompt_template} with role: {expert_role}")
                
            else:
                # Fallback to original message building
                if cfg["type"] == "chat":
                    msgs = self._build_chat_messages(prompt, prompt_template, template_vars)
                else:
                    raise ValueError("Non-chat models require template integration")
                    
        except ImportError:
            self.logger.warning("Template integrator not available, using fallback")
            if cfg["type"] == "chat":
                msgs = self._build_chat_messages(prompt, prompt_template, template_vars)
            else:
                txt = self._build_plain_prompt(prompt, prompt_template, template_vars)

        # Execute API call
        if cfg["type"] == "chat":
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
