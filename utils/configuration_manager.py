#!/usr/bin/env python3
"""
Enterprise Configuration Manager
Industrial-grade configuration management with hot-reload, environment isolation, and template injection
"""

import os
import yaml
import json
import threading
import time
from typing import Dict, Any, List, Optional, Union, Callable
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ConfigurationMode(Enum):
    """Configuration environment modes"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"
    CLI = "cli"


@dataclass
class ModelConfiguration:
    """Structured model configuration"""
    model_name: str
    type: str
    context_length: int
    max_tokens: int
    temperature: float
    price: Dict[str, float]
    api_key: Optional[str] = None
    enabled: bool = True
    environment_specific: Dict[str, Any] = None

    def __post_init__(self):
        if self.environment_specific is None:
            self.environment_specific = {}


@dataclass
class PromptConfiguration:
    """Structured prompt configuration"""
    system: str
    user: str
    template_id: Optional[str] = None
    parameters: Dict[str, Any] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}
        if self.metadata is None:
            self.metadata = {}


class ConfigurationManager:
    """
    Enterprise Configuration Manager
    
    Features:
    - Environment-based configuration isolation
    - Hot-reload support with file watching
    - Template injection from enterprise template engine
    - Thread-safe configuration updates
    - Validation and error handling
    - CLI-friendly configuration override
    """

    def __init__(self, 
                 config_dir: Optional[str] = None,
                 mode: ConfigurationMode = ConfigurationMode.DEVELOPMENT,
                 auto_reload: bool = True,
                 validation_strict: bool = True):
        
        self.config_dir = Path(config_dir or os.path.dirname(__file__))
        self.mode = mode
        self.auto_reload = auto_reload
        self.validation_strict = validation_strict
        
        # Thread-safe storage
        self._lock = threading.RLock()
        self._models: Dict[str, ModelConfiguration] = {}
        self._prompts: Dict[str, PromptConfiguration] = {}
        self._template_cache: Dict[str, Dict[str, Any]] = {}
        self._watchers: Dict[str, float] = {}  # file_path -> last_modified
        
        # Configuration state
        self.default_model: Optional[str] = None
        self.default_prompt: Optional[str] = None
        self._last_reload: float = time.time()
        
        # Template injection hooks
        self._template_injectors: List[Callable[[], Dict[str, PromptConfiguration]]] = []
        
        # Initialize
        self._load_configurations()
        
        if self.auto_reload:
            self._start_file_watcher()

    def register_template_injector(self, injector: Callable[[], Dict[str, PromptConfiguration]]):
        """Register a template injection function"""
        with self._lock:
            self._template_injectors.append(injector)
            self._inject_templates()

    def _inject_templates(self):
        """Execute all template injectors and merge results"""
        try:
            for injector in self._template_injectors:
                try:
                    templates = injector()
                    if isinstance(templates, dict):
                        for template_id, config in templates.items():
                            if isinstance(config, PromptConfiguration):
                                self._prompts[template_id] = config
                            elif isinstance(config, dict):
                                # Convert dict to PromptConfiguration
                                self._prompts[template_id] = PromptConfiguration(
                                    system=config.get('system', ''),
                                    user=config.get('user', ''),
                                    template_id=template_id,
                                    parameters=config.get('parameters', {}),
                                    metadata=config.get('metadata', {})
                                )
                except Exception as e:
                    logger.error(f"Template injector failed: {e}")
        except Exception as e:
            logger.error(f"Template injection failed: {e}")

    def _load_configurations(self):
        """Load configurations from all sources"""
        with self._lock:
            # Load base configuration
            self._load_base_config()
            
            # Load environment-specific overrides
            self._load_environment_config()
            
            # Inject templates from external sources
            self._inject_templates()
            
            # Validate configurations
            if self.validation_strict:
                self._validate_configurations()
            
            logger.info(f"Loaded {len(self._models)} models, {len(self._prompts)} prompts for {self.mode.value}")

    def _load_base_config(self):
        """Load base OpenAI configuration"""
        base_config_path = self.config_dir / "openai_config.yaml"
        if not base_config_path.exists():
            logger.warning(f"Base config not found: {base_config_path}")
            return
            
        self._watchers[str(base_config_path)] = base_config_path.stat().st_mtime
        
        with open(base_config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        # Load models
        raw_models = data.get("models", {})
        for key, cfg in raw_models.items():
            self._models[key] = ModelConfiguration(
                model_name=cfg.get("model_name", key),
                type=cfg.get("type", "chat"),
                context_length=int(cfg.get("context_length", 4096)),
                max_tokens=int(cfg.get("max_tokens", 1000)),
                temperature=float(cfg.get("temperature", 0.7)),
                price=cfg.get("price", {"prompt": 0.0, "completion": 0.0}),
                api_key=cfg.get("api_key"),
                enabled=cfg.get("enabled", True)
            )
        
        # Load prompts
        raw_prompts = data.get("prompts", {})
        for key, cfg in raw_prompts.items():
            if isinstance(cfg, dict):
                self._prompts[key] = PromptConfiguration(
                    system=cfg.get("system", ""),
                    user=cfg.get("user", ""),
                    template_id=key,
                    parameters=cfg.get("parameters", {}),
                    metadata=cfg.get("metadata", {})
                )
            else:
                self._prompts[key] = PromptConfiguration(
                    system="",
                    user=str(cfg),
                    template_id=key
                )
        
        # Set defaults
        self.default_model = data.get("default_model") or next(iter(self._models), None)
        self.default_prompt = "default" if "default" in self._prompts else next(iter(self._prompts), None)

    def _load_environment_config(self):
        """Load environment-specific configuration overrides"""
        env_config_path = self.config_dir / f"openai_config.{self.mode.value}.yaml"
        if not env_config_path.exists():
            return
            
        self._watchers[str(env_config_path)] = env_config_path.stat().st_mtime
        
        with open(env_config_path, 'r', encoding='utf-8') as f:
            env_data = yaml.safe_load(f)
        
        # Override model configurations
        env_models = env_data.get("models", {})
        for key, overrides in env_models.items():
            if key in self._models:
                # Update existing model
                for attr, value in overrides.items():
                    if hasattr(self._models[key], attr):
                        setattr(self._models[key], attr, value)
            else:
                # Add new environment-specific model
                self._models[key] = ModelConfiguration(**overrides)
        
        # Override prompt configurations
        env_prompts = env_data.get("prompts", {})
        for key, cfg in env_prompts.items():
            if isinstance(cfg, dict):
                self._prompts[key] = PromptConfiguration(
                    system=cfg.get("system", ""),
                    user=cfg.get("user", ""),
                    template_id=key,
                    parameters=cfg.get("parameters", {}),
                    metadata=cfg.get("metadata", {})
                )
            else:
                self._prompts[key] = PromptConfiguration(
                    system="",
                    user=str(cfg),
                    template_id=key
                )

    def _validate_configurations(self):
        """Validate loaded configurations"""
        if not self._models:
            raise ValueError("No models configured")
        
        if self.default_model and self.default_model not in self._models:
            logger.warning(f"Default model '{self.default_model}' not found, using first available")
            self.default_model = next(iter(self._models))
        
        # Validate API keys for enabled models
        for name, model in self._models.items():
            if model.enabled and not model.api_key:
                api_key = os.environ.get("OPENAI_API_KEY")
                if not api_key:
                    logger.warning(f"No API key configured for model '{name}'")
                else:
                    model.api_key = api_key

    def _start_file_watcher(self):
        """Start background file watcher for hot-reload"""
        def watch_files():
            while self.auto_reload:
                try:
                    should_reload = False
                    for file_path, last_modified in self._watchers.items():
                        if os.path.exists(file_path):
                            current_modified = os.path.getmtime(file_path)
                            if current_modified > last_modified:
                                should_reload = True
                                break
                    
                    if should_reload:
                        logger.info("Configuration files changed, reloading...")
                        self.reload_configuration()
                    
                    time.sleep(1)  # Check every second
                except Exception as e:
                    logger.error(f"File watcher error: {e}")
                    time.sleep(5)  # Longer sleep on error
        
        watcher_thread = threading.Thread(target=watch_files, daemon=True)
        watcher_thread.start()

    def reload_configuration(self, force: bool = False):
        """Hot-reload configurations"""
        with self._lock:
            if not force and time.time() - self._last_reload < 1.0:
                return  # Avoid rapid reloads
            
            self._models.clear()
            self._prompts.clear()
            self._template_cache.clear()
            
            self._load_configurations()
            self._last_reload = time.time()
            logger.info("Configuration reloaded successfully")

    def get_model_config(self, model_name: Optional[str] = None) -> Optional[ModelConfiguration]:
        """Get model configuration"""
        with self._lock:
            name = model_name or self.default_model
            return self._models.get(name) if name else None

    def get_prompt_config(self, prompt_name: Optional[str] = None) -> Optional[PromptConfiguration]:
        """Get prompt configuration"""
        with self._lock:
            name = prompt_name or self.default_prompt
            return self._prompts.get(name) if name else None

    def list_models(self, enabled_only: bool = True) -> Dict[str, ModelConfiguration]:
        """List available models"""
        with self._lock:
            if enabled_only:
                return {k: v for k, v in self._models.items() if v.enabled}
            return self._models.copy()

    def list_prompts(self) -> Dict[str, PromptConfiguration]:
        """List available prompts"""
        with self._lock:
            return self._prompts.copy()

    def get_openai_service_config(self) -> Dict[str, Any]:
        """Get configuration in OpenAI service compatible format"""
        with self._lock:
            models = {}
            prompts = {}
            
            for name, config in self._models.items():
                if config.enabled:
                    models[name] = {
                        "model_name": config.model_name,
                        "type": config.type,
                        "context_length": config.context_length,
                        "max_tokens": config.max_tokens,
                        "temperature": config.temperature,
                        "price": config.price,
                        "api_key": config.api_key
                    }
            
            for name, config in self._prompts.items():
                prompts[name] = {
                    "system": config.system,
                    "user": config.user
                }
            
            return {
                "models": models,
                "prompts": prompts,
                "default_model": self.default_model,
                "default_prompt": self.default_prompt
            }

    def export_config(self, format: str = "yaml") -> str:
        """Export current configuration"""
        config_data = self.get_openai_service_config()
        
        if format.lower() == "yaml":
            return yaml.dump(config_data, default_flow_style=False)
        elif format.lower() == "json":
            return json.dumps(config_data, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def override_for_cli(self, template_id: Optional[str] = None, model_override: Optional[str] = None):
        """CLI-specific configuration override"""
        with self._lock:
            if template_id and template_id in self._prompts:
                self.default_prompt = template_id
                logger.info(f"CLI override: using template '{template_id}'")
            
            if model_override and model_override in self._models:
                self.default_model = model_override
                logger.info(f"CLI override: using model '{model_override}'")


# Global instance
_config_manager: Optional[ConfigurationManager] = None


def get_configuration_manager(
    mode: Optional[ConfigurationMode] = None,
    force_reload: bool = False
) -> ConfigurationManager:
    """Get or create global configuration manager"""
    global _config_manager
    
    if _config_manager is None or force_reload:
        # Determine mode from environment if not specified
        if mode is None:
            env_mode = os.environ.get('TUBEWHALE_MODE', 'development').lower()
            try:
                mode = ConfigurationMode(env_mode)
            except ValueError:
                mode = ConfigurationMode.DEVELOPMENT
        
        _config_manager = ConfigurationManager(mode=mode)
        
        # Register enterprise template injector
        def enterprise_template_injector() -> Dict[str, PromptConfiguration]:
            """Inject templates from enterprise template engine"""
            try:
                from service.enterprise_template_engine import TemplateEngine
                engine = TemplateEngine(validation_strict=False)
                
                templates = {}
                for metadata in engine.list_templates(include_metadata=True):
                    template_id = metadata.get("id") or metadata.get("name")
                    if not template_id:
                        continue
                    
                    template = engine.get_template(template_id)
                    if not template:
                        continue
                    
                    templates[template_id] = PromptConfiguration(
                        system="",  # Enterprise templates might not have system prompts
                        user=str(template.get("prompt", "")),
                        template_id=template_id,
                        parameters=template.get("parameters", {}),
                        metadata=metadata
                    )
                
                return templates
            except Exception as e:
                logger.warning(f"Enterprise template injection failed: {e}")
                return {}
        
        _config_manager.register_template_injector(enterprise_template_injector)
    
    return _config_manager