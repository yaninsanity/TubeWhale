#!/usr/bin/env python3
"""
CLI动态配置管理器
最小侵入设计，提供简洁的Django后端交互接口
"""

import json
import os
import logging
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class CLIDynamicConfig:
    """CLI动态配置结构 - 简化的接口设计"""
    
    # 核心执行参数
    keyword: Optional[str] = None
    top_k: Optional[int] = None
    concurrency: Optional[int] = None
    
    # 模式控制
    dry_run: bool = False
    pure_youtube: bool = False
    backend_mode: bool = False
    
    # Prompt配置 - 简化为自动选择
    auto_prompt: bool = True
    prompt_template_id: Optional[str] = None
    custom_prompt: Optional[str] = None
    
    # 输出控制
    output_format: str = "json"
    verbose: bool = False
    
    # 功能开关
    audio_analysis: bool = False
    persist_summaries: bool = True
    
    def to_cli_args(self) -> list:
        """转换为CLI参数列表 - 最小化参数传递"""
        args = []
        
        # 核心参数
        if self.keyword:
            args.extend(["--keyword", self.keyword])
        if self.top_k:
            args.extend(["--top-k", str(self.top_k)])
        if self.concurrency:
            args.extend(["--concurrency", str(self.concurrency)])
            
        # 模式开关 - 只添加启用的选项
        if self.dry_run:
            args.append("--dry-run")
        if self.pure_youtube:
            args.append("--pure-youtube")
        if self.backend_mode:
            args.extend(["--backend-mode", "--output-format", self.output_format])
        if self.verbose:
            args.append("--verbose")
        if self.audio_analysis:
            args.append("--audio")
        if not self.persist_summaries:
            args.append("--no-persist")
            
        # Prompt配置 - 智能选择
        if self.custom_prompt:
            args.extend(["--prompt-final", self.custom_prompt])
        if self.auto_prompt:
            args.append("--auto-prompt")
        if self.prompt_template_id:
            args.extend(["--prompt-template-id", self.prompt_template_id])
            
        return args
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CLIDynamicConfig':
        """从字典创建配置 - 容错设计"""
        # 字段映射，支持多种命名约定
        field_mappings = {
            'keyword': ['keyword', 'search_keyword', 'query'],
            'top_k': ['top_k', 'max_results', 'limit', 'count'],
            'concurrency': ['concurrency', 'parallel', 'workers'],
            'dry_run': ['dry_run', 'test_mode', 'preview'],
            'custom_prompt': ['custom_prompt', 'prompt_final', 'prompt']
        }
        
        # 提取并标准化字段
        config_data = {}
        for field, aliases in field_mappings.items():
            for alias in aliases:
                if alias in data:
                    config_data[field] = data[alias]
                    break
        
        # 添加其他直接匹配的字段
        for field in ['pure_youtube', 'backend_mode', 'auto_prompt', 
                     'prompt_template_id', 'output_format', 'verbose',
                     'audio_analysis', 'persist_summaries']:
            if field in data:
                config_data[field] = data[field]
        
        return cls(**config_data)

class CLIAccessibilityManager:
    """CLI可访问性管理器 - 降低框架复杂度"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self._default_config = CLIDynamicConfig()
    
    def create_simple_config(self, **kwargs) -> CLIDynamicConfig:
        """创建简单配置 - 一行代码解决大部分需求"""
        return CLIDynamicConfig(**kwargs)
    
    def create_youtube_analysis_config(self, keyword: str, max_videos: int = 5) -> CLIDynamicConfig:
        """快速创建YouTube分析配置"""
        return CLIDynamicConfig(
            keyword=keyword,
            top_k=max_videos,
            backend_mode=True,
            auto_prompt=True,
            output_format="json",
            dry_run=False
        )
    
    def create_test_config(self, keyword: str) -> CLIDynamicConfig:
        """快速创建测试配置"""
        return CLIDynamicConfig(
            keyword=keyword,
            top_k=2,
            backend_mode=True,
            auto_prompt=True,
            output_format="json",
            dry_run=True,
            verbose=True
        )
    
    def create_production_config(self, keyword: str, videos: int = 10) -> CLIDynamicConfig:
        """快速创建生产配置"""
        return CLIDynamicConfig(
            keyword=keyword,
            top_k=videos,
            concurrency=3,
            backend_mode=True,
            auto_prompt=True,
            output_format="json",
            audio_analysis=False,  # 生产环境可选
            persist_summaries=True
        )
    
    def validate_config(self, config: CLIDynamicConfig) -> Dict[str, Any]:
        """验证配置有效性 - 降低出错率"""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "suggestions": []
        }
        
        # 必要字段检查
        if not config.keyword:
            validation_result["errors"].append("keyword is required")
            validation_result["valid"] = False
        
        # 范围检查
        if config.top_k and (config.top_k < 1 or config.top_k > 50):
            validation_result["warnings"].append("top_k should be between 1 and 50")
        
        if config.concurrency and (config.concurrency < 1 or config.concurrency > 10):
            validation_result["warnings"].append("concurrency should be between 1 and 10")
        
        # 兼容性检查
        if config.pure_youtube and config.audio_analysis:
            validation_result["errors"].append("pure_youtube mode cannot be used with audio_analysis")
            validation_result["valid"] = False
        
        # 性能建议
        if config.concurrency and config.concurrency > 5 and config.audio_analysis:
            validation_result["suggestions"].append("High concurrency with audio analysis may cause rate limiting")
        
        return validation_result
    
    def optimize_config_for_backend(self, config: CLIDynamicConfig) -> CLIDynamicConfig:
        """为后端调用优化配置"""
        optimized = CLIDynamicConfig(**asdict(config))
        
        # 强制启用后端模式
        optimized.backend_mode = True
        optimized.output_format = "json"
        
        # 智能默认值
        if not optimized.top_k:
            optimized.top_k = 5  # 合理的默认值
        
        if not optimized.concurrency:
            optimized.concurrency = 3  # 平衡的并发数
        
        # 自动启用prompt系统
        if not optimized.custom_prompt and not optimized.prompt_template_id:
            optimized.auto_prompt = True
        
        return optimized
    
    def get_minimal_args_for_django(self, keyword: str, **options) -> list:
        """为Django提供最简化的CLI参数生成"""
        config = self.create_youtube_analysis_config(keyword)
        
        # 应用选项覆盖
        for key, value in options.items():
            if hasattr(config, key):
                setattr(config, key, value)
        
        # 优化配置
        config = self.optimize_config_for_backend(config)
        
        # 验证配置
        validation = self.validate_config(config)
        if not validation["valid"]:
            self.logger.error(f"Configuration validation failed: {validation['errors']}")
            raise ValueError(f"Invalid configuration: {validation['errors']}")
        
        return config.to_cli_args()

class DynamicCLIInterface:
    """动态CLI接口 - 为Django提供极简的调用方式"""
    
    def __init__(self):
        self.accessibility_mgr = CLIAccessibilityManager()
    
    def analyze_youtube_videos(self, keyword: str, max_videos: int = 5, **kwargs) -> list:
        """最简单的YouTube视频分析接口"""
        return self.accessibility_mgr.get_minimal_args_for_django(
            keyword, top_k=max_videos, **kwargs
        )
    
    def quick_test(self, keyword: str) -> list:
        """快速测试接口"""
        return self.accessibility_mgr.get_minimal_args_for_django(
            keyword, dry_run=True, top_k=2, verbose=True
        )
    
    def production_analysis(self, keyword: str, videos: int = 10, 
                          audio: bool = False, concurrency: int = 3) -> list:
        """生产环境分析接口"""
        return self.accessibility_mgr.get_minimal_args_for_django(
            keyword, top_k=videos, audio_analysis=audio, 
            concurrency=concurrency, dry_run=False
        )
    
    def custom_prompt_analysis(self, keyword: str, prompt: str, **kwargs) -> list:
        """自定义prompt分析接口"""
        return self.accessibility_mgr.get_minimal_args_for_django(
            keyword, custom_prompt=prompt, **kwargs
        )

# 全局实例 - 简化导入
dynamic_cli = DynamicCLIInterface()

def get_cli_args_for_django(keyword: str, **options) -> list:
    """Django使用的一行代码接口"""
    return dynamic_cli.analyze_youtube_videos(keyword, **options)

def get_test_cli_args(keyword: str) -> list:
    """测试用的一行代码接口"""
    return dynamic_cli.quick_test(keyword)