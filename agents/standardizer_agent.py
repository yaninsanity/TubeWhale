import logging
import json
import asyncio
from datetime import datetime
from typing import Any, Dict, Optional, List

from utils.openAIServices import OpenAIService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def async_retry(max_retries: int = 3, delay: int = 2):
    """异步重试装饰器，确保 API 调用失败时自动重试"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
            raise Exception(f"All {max_retries} attempts failed.")
        return wrapper
    return decorator


class StandardizerAgent:
    """
    StandardizerAgent 将输入摘要转换为结构化的指南格式，输出严格遵循 JSON 格式，
    便于后续数据分析和处理。

    主要流程：
      1. 输入校验与切片：当文本过长时进行切片处理。
      2. 构造标准化 prompt：通过 OpenAIService.get_prompt("structured_output", {"text": summary}) 获取 YAML 模板，
         并注入预设 JSON 模板，确保 GPT 返回符合要求的格式。
      3. 异步调用 OpenAI API 生成回复，并尝试解析为 JSON；若解析失败，则返回原始文本。
      4. 对缺失的关键字段自动补全 "N/A"。
      5. 支持动态控制（enable_standardization、debug_mode）以及自动重试。
      6. 若提供数据库对象和 video_id，则记录标准化结果到数据库（表名 "standardized_summaries"）。
    """

    # 默认文本切片阈值（按字符计算）
    DEFAULT_MAX_LENGTH = 3000

    # 预设 JSON 模板，用于指导 GPT 输出统一结构
    JSON_TEMPLATE = {
        "main_topic": "...",
        "key_insights": "...",
        "recommended_tools": "...",
        "best_practices": "...",
        "challenges_and_advice": "..."
    }

    def __init__(self, openai_service: OpenAIService, enable_standardization: bool = True, 
                 debug_mode: bool = False, db: Optional[Any] = None):
        """
        :param openai_service: 已初始化的 OpenAIService 实例。
        :param enable_standardization: 是否启用标准化处理。
        :param debug_mode: 是否开启详细调试日志。
        :param db: 可选数据库对象，用于记录标准化结果（需在数据库中定义表 "standardized_summaries"）。
        """
        self.openai_service = openai_service
        self.enable_standardization = enable_standardization
        self.debug_mode = debug_mode
        self.db = db

    def _build_prompt(self, summary: str) -> str:
        """
        构造标准化 prompt，优先调用 openai_service.get_prompt 获取 YAML 模板，
        并注入预设 JSON 模板作为格式要求。

        :param summary: 待标准化的文本摘要
        :return: 构造后的 prompt 文本
        """
        try:
            prompt_template = self.openai_service.get_prompt("structured_output", variables={"text": summary})
            prompt = prompt_template.get("prompt", "") if isinstance(prompt_template, dict) else prompt_template
            if self.debug_mode:
                logger.debug(f"Loaded structured_output prompt: {prompt[:100]}...")
            if not prompt:
                raise ValueError("Empty prompt from YAML.")
        except Exception as e:
            logger.error(f"Error retrieving 'structured_output' prompt: {e}")
            prompt = (
                "You are an expert in summarizing and extracting structured information.\n"
                "Transform the following text into a standardized guide in JSON format. "
                "Output must follow this JSON schema:\n"
                f"{json.dumps(self.JSON_TEMPLATE, indent=2)}\n"
                f"Text: {summary}"
            )
        injected = json.dumps(self.JSON_TEMPLATE, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt}\nEnsure your output adheres exactly to the following JSON format:\n{injected}"
        return full_prompt

    def _slice_text(self, text: str, max_length: int = DEFAULT_MAX_LENGTH) -> List[str]:
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
        合并多个片段的标准化结果，目前简单返回第一个非 None 结果。
        """
        for res in results:
            if res is not None:
                return res
        return None

    @async_retry(max_retries=3, delay=2)
    async def standardize(self, summary: str, model: str = "gpt-4", video_id: Optional[str] = None) -> Any:
        """
        异步调用 OpenAI API，将输入摘要转换为结构化输出。
        若文本过长则进行切片处理，并合并各片段结果。
        如果提供了 video_id 及数据库对象，则记录标准化结果到数据库表 "standardized_summaries"。

        :param summary: 待标准化的摘要文本
        :param model: 使用的 OpenAI 模型名称
        :param video_id: 可选视频ID，用于记录标准化结果
        :return: 标准化结果（解析为 JSON 时返回字典，否则返回原始文本）
        """
        if not self.enable_standardization:
            logger.info("Standardization disabled. Returning original summary.")
            return summary
        if not summary:
            logger.error("No summary provided. Skipping standardization.")
            return None

        logger.info("Starting standardization process.")
        segments = self._slice_text(summary) if len(summary) > self.DEFAULT_MAX_LENGTH else [summary]
        results = []

        for idx, segment in enumerate(segments):
            prompt = self._build_prompt(segment)
            if self.debug_mode:
                logger.debug(f"Segment {idx + 1} prompt: {prompt[:150]}...")
            try:
                response_text = self.openai_service.completion(prompt=prompt, model=model)
                response_text = response_text.strip()
                if self.debug_mode:
                    logger.debug(f"Segment {idx + 1} API response: {response_text[:150]}...")
                try:
                    standardized = json.loads(response_text)
                    # 补全缺失字段
                    for field in self.JSON_TEMPLATE.keys():
                        if field not in standardized:
                            standardized[field] = "N/A"
                    results.append(standardized)
                except json.JSONDecodeError:
                    logger.error("JSON parsing failed for segment. Using raw response.")
                    results.append(response_text)
            except Exception as e:
                logger.error(f"Error standardizing segment {idx + 1}: {e}")
                results.append(None)

        merged = self._merge_results(results)
        if merged is not None:
            logger.info("Standardization completed successfully.")
            # 如果提供了 video_id 和 db，则记录标准化结果到数据库
            if video_id and self.db:
                try:
                    record = {
                        "video_id": video_id,
                        "standardized_summary": json.dumps(merged, ensure_ascii=False),
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    # 这里调用 store_data 方法存入表 "standardized_summaries"
                    self.db.store_data("standardized_summaries", record)
                    logger.info(f"Standardized summary for video {video_id} recorded in database.")
                except Exception as record_e:
                    logger.error(f"Failed to record standardized summary for video {video_id}: {record_e}")
        else:
            logger.error("All segments failed standardization.")
        return merged

def standardizer_agent(summary: str, model: str = "gpt-4", openai_service: Optional[OpenAIService] = None, 
                       db: Optional[Any] = None, debug_mode: bool = False) -> asyncio.Future:
    """
    工厂函数：异步调用 StandardizerAgent.standardize 方法，并确保记录标准化结果到数据库。
    
    :param summary: 待标准化的摘要文本
    :param model: 使用的 OpenAI 模型名称
    :param openai_service: 已初始化的 OpenAIService 实例（必须提供）
    :param db: 数据库对象，用于记录标准化结果
    :param debug_mode: 是否开启详细调试日志
    :return: asyncio.Future 对象，await 后返回标准化输出
    """
    if openai_service is None:
        raise ValueError("An OpenAIService instance must be provided.")
    agent = StandardizerAgent(openai_service=openai_service, debug_mode=debug_mode, db=db)
    return asyncio.ensure_future(agent.standardize(summary, model=model))
