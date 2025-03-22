import logging
import json
import asyncio
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

    :param text: 待切分的文本
    :param max_tokens: 每个片段的最大 token 数
    :param overlap: 分片间的重叠 token 数（确保上下文连续性）
    :return: 文本片段列表
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")
    tokens = tokenizer.encode(text)
    chunks = []
    # 注意：步长为 max_tokens - overlap
    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)
    return chunks

class SummarizerAgent:
    """
    SummarizerAgent 负责对长文本（例如论文、报告或视频 transcript）生成摘要，
    同时支持通过 YAML 配置动态加载提示模板，实现不同领域（如论文信息采集）的灵活定制。

    工作流程：
      1. 如果启用切片（enable_chunking=True），则使用 tiktoken 将长文本切分成多个片段。
      2. 对每个片段调用 OpenAIService.completion 生成摘要，调用时使用从 YAML 配置中加载的提示模板（默认模板名称为 "summarization"，可传入 prompt_name 覆盖）。
      3. 每个片段摘要时传入前一片段摘要作为上下文，保证摘要连贯性。
      4. 将所有片段摘要合并生成最终摘要；默认采用简单拼接，后续可扩展二次摘要合并逻辑。
      5. 尝试将输出解析为 JSON 格式，若解析失败则返回原始文本，并对缺失关键字段进行补全。

    参数说明：
      - openai_service: 已初始化的 OpenAIService 实例（统一加载 YAML 配置）。
      - enable_chunking: 是否启用文本切片（默认 True）。
      - debug_mode: 是否输出详细调试日志（默认 False）。
    """
    DEFAULT_MAX_TOKENS = 3000

    # 预设 JSON 模板（用于校验输出）
    JSON_TEMPLATE = {
        "main_topic": "N/A",
        "key_insights": "N/A",
        "recommended_tools": "N/A",
        "best_practices": "N/A",
        "challenges_and_advice": "N/A"
    }

    def __init__(self, openai_service: OpenAIService, enable_chunking: bool = True, debug_mode: bool = False):
        self.openai_service = openai_service
        self.enable_chunking = enable_chunking
        self.debug_mode = debug_mode

    def chunk_text(self, text: str, max_tokens: Optional[int] = None, overlap: int = 200) -> List[str]:
        max_tokens = max_tokens or self.DEFAULT_MAX_TOKENS
        return chunk_text_by_tokens(text, max_tokens=max_tokens, overlap=overlap)

    @async_retry(max_retries=3, delay=2)
    async def summarize_chunk(self, chunk: str, previous_summary: str = "", model: str = "gpt-4o-mini", prompt_name: str = "summarization") -> str:
        """
        对单个文本片段生成摘要。使用传入的 prompt_name 从 YAML 中获取提示模板，
        若获取失败则使用默认硬编码提示；同时传入前一片段摘要作为上下文。

        :param chunk: 单个文本片段
        :param previous_summary: 前一片段摘要作为上下文
        :param model: 使用的 OpenAI 模型名称
        :param prompt_name: 提示模板名称（默认为 "summarization"）
        :return: 当前片段生成的摘要文本
        """
        try:
            prompt_template = self.openai_service.get_prompt(prompt_name, variables={
                "text": chunk,
                "previous_summary": previous_summary
            })
        except Exception as e:
            logger.error(f"Error obtaining '{prompt_name}' prompt: {e}")
            prompt_template = {"prompt": f"Summarize the following text:\nPrevious Summary: {previous_summary}\nText: {chunk}"}
        final_prompt = prompt_template.get("prompt", "") if isinstance(prompt_template, dict) else prompt_template
        if self.debug_mode:
            logger.debug(f"Summarize chunk prompt: {final_prompt[:150]}...")
        try:
            response_text = self.openai_service.completion(prompt=final_prompt, model=model)
            response_text = response_text.strip()
            if self.debug_mode:
                logger.debug(f"Summarize chunk response: {response_text[:150]}...")
            return response_text
        except Exception as e:
            logger.error(f"Error summarizing chunk: {e}")
            return ""

    def merge_summaries(self, summaries: List[str]) -> str:
        """
        合并多个摘要片段为最终摘要。当前采用简单拼接策略，后续可扩展二次摘要逻辑。

        :param summaries: 摘要片段列表
        :return: 合并后的最终摘要文本
        """
        if not summaries:
            return ""
        return " ".join(summaries)

    def validate_json_output(self, text: str) -> Optional[Dict[str, Any]]:
        """
        尝试将文本解析为 JSON，并对缺失字段进行补全。

        :param text: 待解析文本
        :return: 如果解析成功则返回字典，否则返回 None
        """
        try:
            data = json.loads(text)
            for key, default in self.JSON_TEMPLATE.items():
                if key not in data:
                    data[key] = default
            return data
        except json.JSONDecodeError:
            return None

    async def summarize(self, long_text: str, model: str = "gpt-4o-mini", prompt_name: str = "summarization") -> str:
        """
        对长文本生成最终摘要。支持切片处理，当启用 enable_chunking 时，
        将文本分片、逐片生成摘要，并合并各片段摘要；否则直接生成摘要。

        :param long_text: 待摘要的长文本
        :param model: 使用的 OpenAI 模型名称
        :param prompt_name: 提示模板名称（默认为 "summarization"）
        :return: 最终生成的摘要文本。如果能解析为 JSON，则返回格式化后的 JSON 字符串；否则返回纯文本摘要。
        """
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
            else:
                logger.warning(f"No summary generated for chunk {idx + 1}.")
        merged = self.merge_summaries(summaries)
        validated = self.validate_json_output(merged)
        if validated is not None:
            logger.info("Final summary parsed as JSON successfully.")
            return json.dumps(validated, ensure_ascii=False, indent=2)
        logger.info("Final summary returned as plain text.")
        return merged

def gpt_summarizer_agent(long_text: str, *, model: str = "gpt-4o-mini", openai_service: Optional[OpenAIService] = None, enable_chunking: bool = True, debug_mode: bool = False, prompt_name: str = "summarization") -> asyncio.Future:
    """
    工厂函数：异步调用 SummarizerAgent.summarize 方法，返回 asyncio.Future 对象。

    外部系统调用时，只需传入待处理文本、模型名称、OpenAIService 实例（已加载 YAML 配置）以及可选 flag，
    从而确保所有摘要调用统一使用 YAML 配置中的提示模板，便于灵活调整和成本统一管理。

    :param long_text: 待摘要的长文本（例如论文、报告、或视频 transcript）
    :param model: 使用的 OpenAI 模型名称（例如 "gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo" 等）
    :param openai_service: 已加载 YAML 配置的 OpenAIService 实例（必须提供）
    :param enable_chunking: 是否对长文本进行切片处理（默认 True）
    :param debug_mode: 是否开启详细调试日志
    :param prompt_name: 提示模板名称，用于摘要（默认为 "summarization"）
    :return: asyncio.Future 对象，await 后返回最终摘要文本
    """
    if openai_service is None:
        raise ValueError("An OpenAIService instance must be provided.")
    agent = SummarizerAgent(openai_service=openai_service, enable_chunking=enable_chunking, debug_mode=debug_mode)
    return asyncio.ensure_future(agent.summarize(long_text, model=model, prompt_name=prompt_name))
