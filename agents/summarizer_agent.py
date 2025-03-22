import logging
import json
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import tiktoken
from utils.openAIServices import OpenAIService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def async_retry(max_retries: int = 3, delay: int = 2):
    """
    异步重试装饰器，用于在 API 调用失败时自动重试。
    """
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

class SummarizerAgent:
    """
    SummarizerAgent 负责对长文本生成摘要，支持文本切片、逐片摘要、摘要合并，并记录整个过程。
    
    工作流程：
      1. 若启用切片，则使用 tiktoken 将长文本切分成多个片段；
      2. 对每个片段调用 OpenAIService.completion 生成摘要，并传入前一片段摘要作为上下文；
      3. 合并所有片段摘要生成最终摘要；
      4. 尝试将最终摘要解析为 JSON 格式，若失败则返回原始文本；
      5. 记录每个片段摘要及最终摘要到数据库（若提供 db）。
    """
    DEFAULT_MAX_TOKENS = 3000
    # 预设 JSON 模板，用于校验输出
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self, openai_service: OpenAIService, enable_chunking: bool = True, debug_mode: bool = False, db: Optional[Any] = None):
        """
        :param openai_service: 已初始化的 OpenAIService 实例。
        :param enable_chunking: 是否对长文本进行切片（默认 True）。
        :param debug_mode: 是否开启详细调试日志。
        :param db: 可选数据库对象，用于记录摘要流程（如写入 "summarization_logs" 表）。
        """
        self.openai_service = openai_service
        self.enable_chunking = enable_chunking
        self.debug_mode = debug_mode
        self.db = db

    def chunk_text(self, text: str, max_tokens: Optional[int] = None, overlap: int = 200) -> List[str]:
        max_tokens = max_tokens or self.DEFAULT_MAX_TOKENS
        chunks = chunk_text_by_tokens(text, max_tokens=max_tokens, overlap=overlap)
        logger.info(f"Text chunked into {len(chunks)} segments.")
        if self.debug_mode:
            logger.debug(f"Chunk sizes: {[len(chunk) for chunk in chunks]}")
        return chunks

    @async_retry(max_retries=3, delay=2)
    async def summarize_chunk(self, chunk: str, previous_summary: str = "", model: str = "gpt-4o-mini", prompt_name: str = "summarization") -> str:
        """
        对单个文本片段生成摘要，传入前一片段摘要作为上下文。
        """
        try:
            prompt_template = self.openai_service.get_prompt(prompt_name, variables={"text": chunk, "previous_summary": previous_summary})
        except Exception as e:
            logger.error(f"Error obtaining '{prompt_name}' prompt: {e}")
            prompt_template = {"prompt": f"Summarize the following text:\nPrevious Summary: {previous_summary}\nText: {chunk}"}
        final_prompt = prompt_template.get("prompt", "") if isinstance(prompt_template, dict) else prompt_template
        if self.debug_mode:
            logger.debug(f"Chunk prompt: {final_prompt[:150]}...")
        try:
            response_text = self.openai_service.completion(prompt=final_prompt, model=model)
            response_text = response_text.strip()
            logger.info(f"Chunk summarized (first 100 chars): {response_text[:100]}...")
            return response_text
        except Exception as e:
            logger.error(f"Error summarizing chunk: {e}")
            return ""

    def merge_summaries(self, summaries: List[str]) -> str:
        if not summaries:
            return ""
        merged = " ".join(summaries)
        logger.info("Summaries merged into final summary.")
        return merged

    def validate_json_output(self, text: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(text)
            for key, default in self.JSON_TEMPLATE.items():
                if key not in data:
                    data[key] = default
            return data
        except json.JSONDecodeError:
            logger.error("JSON parsing failed for final summary.")
            return None

    async def summarize(self, long_text: str, model: str = "gpt-4o-mini", prompt_name: str = "summarization") -> str:
        if not long_text:
            logger.error("No text provided for summarization.")
            return ""
        logger.info("Starting summarization process.")
        chunks = self.chunk_text(long_text) if self.enable_chunking else [long_text]
        summaries = []
        previous_summary = ""
        for idx, chunk in enumerate(chunks):
            logger.info(f"Summarizing chunk {idx + 1}/{len(chunks)}.")
            chunk_summary = await self.summarize_chunk(chunk, previous_summary, model=model, prompt_name=prompt_name)
            if chunk_summary:
                summaries.append(chunk_summary)
                previous_summary = chunk_summary
                # 记录每个片段摘要到数据库（如果 db 存在）
                if self.db:
                    try:
                        record = {
                            "process": "chunk_summary",
                            "chunk_index": idx + 1,
                            "summary": chunk_summary,
                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        self.db.store_data("summarization_logs", record)
                        logger.info(f"Chunk {idx + 1} summary recorded in database.")
                    except Exception as db_e:
                        logger.error(f"Failed to record chunk {idx + 1} summary: {db_e}")
            else:
                logger.warning(f"No summary generated for chunk {idx + 1}.")
        merged = self.merge_summaries(summaries)
        validated = self.validate_json_output(merged)
        final_summary = json.dumps(validated, ensure_ascii=False, indent=2) if validated is not None else merged
        logger.info("Final summarization process completed.")
        if self.db:
            try:
                final_record = {
                    "process": "final_summary",
                    "summary": final_summary,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                self.db.store_data("summarization_logs", final_record)
                logger.info("Final summary recorded in database.")
            except Exception as db_e:
                logger.error(f"Failed to record final summary: {db_e}")
        return final_summary

def gpt_summarizer_agent(long_text: str, *, model: str = "gpt-4o-mini", openai_service: Optional[OpenAIService] = None, enable_chunking: bool = True, debug_mode: bool = False, prompt_name: str = "summarization", db: Optional[Any] = None) -> asyncio.Future:
    """
    工厂函数：异步调用 SummarizerAgent.summarize 方法，并确保记录整个摘要流程到数据库。
    
    :param long_text: 待摘要的长文本
    :param model: 使用的 OpenAI 模型名称
    :param openai_service: 已加载 YAML 配置的 OpenAIService 实例（必须提供）
    :param enable_chunking: 是否对长文本进行切片处理（默认 True）
    :param debug_mode: 是否开启详细调试日志
    :param prompt_name: 提示模板名称，用于摘要（默认为 "summarization"）
    :param db: 数据库对象，用于记录摘要流程
    :return: asyncio.Future 对象，await 后返回最终摘要文本
    """
    if openai_service is None:
        raise ValueError("An OpenAIService instance must be provided.")
    agent = SummarizerAgent(openai_service=openai_service, enable_chunking=enable_chunking, debug_mode=debug_mode, db=db)
    return asyncio.ensure_future(agent.summarize(long_text, model=model, prompt_name=prompt_name))
