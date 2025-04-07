import logging
import json
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import tiktoken
from utils.openAIServices import OpenAIService
from utils.helper import async_retry

# Use the global logger instead of passing it around
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def chunk_text_by_tokens(text: str, max_tokens: int = 3000, overlap: int = 200) -> List[str]:
    """
    使用 tiktoken 按 token 数对文本进行切片，确保每个片段不会超出模型上下文窗口。
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")
    tokens = tokenizer.encode(text)
    chunks = []
    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)
    return chunks

class StandardizerAgent:
    """
    StandardizerAgent 将输入摘要转换为结构化的指南格式，输出严格遵循 JSON 格式，
    便于后续数据分析和处理。

    主要流程：
      1. 若文本过长则进行切片；
      2. 构造标准化 prompt（优先从 OpenAIService 获取 YAML 模板，否则使用默认模板）；
      3. 调用 OpenAIService 异步生成回复，并尝试解析为 JSON 格式，不足字段自动补全默认值；
      4. 合并多个片段的结果（本例中简单返回第一个有效结果）；
      5. 如果提供数据库对象，则异步记录每个片段和最终结果到数据库表 "standardized_summaries"。
    """
    DEFAULT_MAX_TOKENS = 3000
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self, openai_service: OpenAIService, enable_standardization: bool = True, 
                 debug_mode: bool = False, db: Optional[Any] = None, logger: Optional[logging.Logger] = None):
        """
        :param openai_service: 已初始化的 OpenAIService 实例。
        :param enable_standardization: 是否启用标准化处理。
        :param debug_mode: 是否开启详细调试日志。
        :param db: 可选数据库对象，用于记录标准化结果（需要提供 store_data 或 store_data_async 接口）。
        """
        self.openai_service = openai_service
        self.enable_standardization = enable_standardization
        self.debug_mode = debug_mode
        self.db = db
        self.logger = logger or logging.getLogger(__name__)

    def _build_prompt(self, summary: str) -> str:
        """
        构造标准化 prompt，优先调用 openai_service.get_prompt 获取 YAML 模板，
        并注入预设 JSON 模板作为格式要求。
        """
        try:
            prompt_template = self.openai_service.get_prompt("structured_output", variables={"text": summary})
            # 如果返回的是字典则提取 "prompt" 字段，否则直接使用字符串
            prompt = prompt_template.get("prompt", "") if isinstance(prompt_template, dict) else prompt_template
            if self.debug_mode:
                logger.debug(f"Loaded 'structured_output' prompt: {prompt[:100]}...")
            if not prompt:
                raise ValueError("Empty prompt from YAML.")
        except Exception as e:
            logger.error(f"Error retrieving 'structured_output' prompt: {e}")
            prompt = (
                "You are an expert in summarizing and extracting structured information.\n"
                "Transform the following text into a standardized guide in JSON format.\n"
                "Output must strictly follow this JSON schema:\n"
                f"{json.dumps(self.JSON_TEMPLATE, indent=2)}\n"
                f"Text: {summary}"
            )
        injected = json.dumps(self.JSON_TEMPLATE, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt}\nEnsure your output adheres exactly to the following JSON format:\n{injected}"
        return full_prompt

    def _slice_text(self, text: str, max_length: int = DEFAULT_MAX_TOKENS) -> List[str]:
        """
        将长文本切片，确保每个片段长度不超过 max_length。
        """
        if len(text) <= max_length:
            return [text]
        slices = []
        start = 0
        while start < len(text):
            slices.append(text[start:start + max_length])
            start += max_length
        if self.debug_mode:
            logger.debug(f"Sliced text into {len(slices)} segments.")
        return slices

    def _merge_results(self, results: List[Any]) -> Any:
        """
        合并多个片段的标准化结果，简单返回第一个非 None 结果。
        """
        for res in results:
            if res is not None:
                return res
        return None

    @async_retry(max_retries=3, delay=2)
    async def standardize(self, summary: str, model: str = "gpt-4", video_id: Optional[str] = None) -> Any:
        """
        异步调用 OpenAI API，将输入摘要转换为结构化输出。
        若文本过长则切片处理，并合并各片段结果；若配置了数据库则记录标准化结果。
        
        :param summary: 待标准化的摘要文本
        :param model: 使用的 OpenAI 模型名称。如果模型未配置，则自动回退为默认模型（即 openai_service.default_model）。
        :param video_id: 可选视频ID，用于记录标准化结果
        :return: 标准化结果（若能解析为 JSON则返回字典，否则返回原始文本）
        """
        if not self.enable_standardization:
            logger.info("Standardization disabled. Returning original summary.")
            return summary
        if not summary:
            logger.error("No summary provided. Skipping standardization.")
            return None

        # 如果指定模型不存在，则回退到默认模型
        if model not in self.openai_service.models:
            logger.warning(f"Model '{model}' not configured. Falling back to default '{self.openai_service.default_model}'.")
            model = self.openai_service.default_model

        logger.info("Starting standardization process.")
        segments = self._slice_text(summary) if len(summary) > self.DEFAULT_MAX_TOKENS else [summary]
        results = []
        for idx, segment in enumerate(segments):
            prompt = self._build_prompt(segment)
            if self.debug_mode:
                logger.debug(f"Segment {idx + 1} prompt (first 150 chars): {prompt[:150]}...")
            try:
                response_text = self.openai_service.completion(prompt=prompt, model=model)
                response_text = response_text.strip()
                if self.debug_mode:
                    logger.debug(f"Segment {idx + 1} API response (first 150 chars): {response_text[:150]}...")
                try:
                    standardized = json.loads(response_text)
                    # 自动补全缺失字段
                    for field, default in self.JSON_TEMPLATE.items():
                        if field not in standardized:
                            standardized[field] = default
                    results.append(standardized)
                except json.JSONDecodeError:
                    logger.error("JSON parsing failed for segment. Using raw response.")
                    results.append(response_text)
            except Exception as e:
                logger.error(f"Error standardizing segment {idx + 1}: {e}\n{traceback.format_exc()}")
                results.append(None)
        merged = self._merge_results(results)
        if merged is not None:
            logger.info("Standardization completed successfully.")
            if video_id and self.db:
                record = {
                    "video_id": video_id,
                    "standardized_summary": json.dumps(merged, ensure_ascii=False),
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                try:
                    if hasattr(self.db, "store_data_async"):
                        await self.db.store_data_async("standardized_summaries", record)
                    else:
                        await asyncio.to_thread(self.db.store_data, "standardized_summaries", record)
                    logger.info(f"Standardized summary for video {video_id} recorded in database.")
                except Exception as db_e:
                    logger.error(f"Failed to record standardized summary for video {video_id}: {db_e}")
        else:
            logger.error("All segments failed standardization.")
        return merged

def standardizer_agent(summary: str, model: str = "gpt-4", openai_service: Optional[OpenAIService] = None, 
                       db: Optional[Any] = None, debug_mode: bool = False) -> asyncio.Future:
    """
    工厂函数：异步调用 StandardizerAgent.standardize 方法，并确保记录整个标准化流程到数据库。
    """
    if openai_service is None:
        raise ValueError("An OpenAIService instance must be provided.")
    agent = StandardizerAgent(openai_service=openai_service, debug_mode=debug_mode, db=db)
    return asyncio.ensure_future(agent.standardize(summary, model=model))

__all__ = ["StandardizerAgent", "standardizer_agent"]
