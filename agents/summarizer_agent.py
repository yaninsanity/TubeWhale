#!/usr/bin/env python3
import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional

from tqdm import tqdm
from utils.openAIServices import OpenAIService

# 最终 JSON 架构
JSON_TEMPLATE: Dict[str, str] = {
    "main_topic": "N/A",
    "key_insights": "N/A",
    "recommended_tools": "N/A",
    "best_practices": "N/A",
    "challenges_and_advice": "N/A",
}


def dynamic_chunk_text(
    text: str,
    target_chunk_size: int = 2000,
    overlap_ratio: float = 0.1
) -> List[str]:
    """
    按段落智能切分文本，若单段过长则滑窗切分。
    """
    text = text.strip()
    if not text:
        return []
    paras = re.split(r'\n+', text)
    # 单段过长
    if len(paras) == 1 and len(paras[0]) > target_chunk_size:
        long = paras[0]
        ov = int(target_chunk_size * overlap_ratio)
        chunks = []
        i = 0
        while i < len(long):
            chunks.append(long[i : i + target_chunk_size])
            i += target_chunk_size - ov
        logging.info(f"[Chunk] single-paragraph → {len(chunks)} chunks")
        return chunks

    # 多段合并
    chunks = []
    buffer = ""
    for p in paras:
        if not buffer:
            buffer = p
        elif len(buffer) + len(p) + 1 <= target_chunk_size:
            buffer += "\n" + p
        else:
            chunks.append(buffer)
            ov = int(len(buffer) * overlap_ratio)
            buffer = (buffer[-ov:] + "\n" + p) if ov else p
    if buffer:
        chunks.append(buffer)
    logging.info(f"[Chunk] paragraph-based → {len(chunks)} chunks")
    return chunks


class SummarizerAgent:
    """
    SummarizerAgent：对长文本分块摘要 + 合并 + 严格 JSON 结构化输出
    """

    def __init__(
        self,
        service: OpenAIService,
        *,
        concurrency: int = 5,
        target_chunk_size: int = 2000,
        overlap_ratio: float = 0.1,
        max_rounds: int = 1,
        logger: Optional[logging.Logger] = None,
        dry_run: bool = False,
    ):
        self.service = service
        self.concurrency = concurrency
        self.target_chunk_size = target_chunk_size
        self.overlap_ratio = overlap_ratio
        self.max_rounds = max_rounds
        self.logger = logger or logging.getLogger(__name__)
        self.dry_run = dry_run

    async def summarize(
        self,
        text: str,
        *,
        model: Optional[str] = None,
        summarization_prompt: str = "summarization",
        output_prompt: str = "structured_output",
    ) -> str:
        if not text.strip():
            raise ValueError("No text provided for summarization.")

        if self.dry_run:
            # 🎭 Dry run mode: return mock structured summary
            self.logger.info("▶ Begin summarization workflow")
            self.logger.info("🎭 Dry run mode: using mock summarization")
            mock_result = {
                "main_topic": "Non-existent content",
                "key_insights": "In a real scenario, content from the video would be condensed into key points and insights",
                "recommended_tools": "Transcript generator for video content", 
                "best_practices": "Presenting condensed information in an easy-to-understand, engaging format",
                "challenges_and_advice": "Generating summaries from non-existent or empty content is not possible"
            }
            self.logger.info("▶ Summarization completed")
            return json.dumps(mock_result, indent=2)

        model = model or self.service.default_model  # type: ignore
        self.logger.info("▶ Begin summarization workflow")

        # 支持多轮递归摘要
        previous_summary = ""
        merged = text
        for rnd in range(self.max_rounds):
            chunks = dynamic_chunk_text(merged, self.target_chunk_size, self.overlap_ratio)
            self.logger.info(f"  • Round {rnd+1}: split into {len(chunks)} chunks")

            sem = asyncio.Semaphore(self.concurrency)
            async def worker(idx: int, chunk: str):
                async with sem:
                    try:
                        out = await asyncio.to_thread(
                            lambda: self.service.completion(
                                model=model,
                                prompt_template=summarization_prompt,
                                template_vars={"text": chunk, "previous_summary": previous_summary},
                            )
                        )
                        return idx, out.strip()
                    except Exception as e:
                        self.logger.error(f"    ✗ Chunk {idx+1} failed: {e}")
                        return idx, ""

            tasks = [asyncio.create_task(worker(i, c)) for i, c in enumerate(chunks)]
            results = [""] * len(chunks)
            for t in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Summarizing chunks"):
                i, summ = await t
                results[i] = summ

            merged = "\n".join(results)
            self.logger.info(f"  • Round {rnd+1} merged summaries")
            previous_summary = merged
            if self.max_rounds == 1:
                break

        # 最终结构化 JSON 输出
        return await self._to_structured_json(merged, model, output_prompt)

    async def _to_structured_json(
        self, merged: str, model: str, output_prompt: str
    ) -> str:
        try:
            data = json.loads(merged)
            self.logger.info("  ✓ merged is valid JSON")
        except json.JSONDecodeError:
            self.logger.warning("  ⚠ merged not valid JSON → calling structured_output prompt")
            formatted = await asyncio.to_thread(
                lambda: self.service.completion(
                    model=model,
                    prompt_template=output_prompt,
                    template_vars={"text": merged},
                )
            )
            try:
                data = json.loads(formatted)
                self.logger.info("  ✓ structured_output produced valid JSON")
            except json.JSONDecodeError as e:
                self.logger.error(f"  ✗ structured_output JSON parse failed: {e}")
                return merged

        for k, v in JSON_TEMPLATE.items():
            data.setdefault(k, v)
        final = json.dumps(data, ensure_ascii=False, indent=2)
        self.logger.info("▶ Summarization completed")
        return final


def gpt_summarizer_agent(
    long_text: str,
    *,
    service: OpenAIService,
    model: Optional[str] = None,
    summarization_prompt: str = "summarization",
    output_prompt: str = "structured_output",
    concurrency: int = 5,
    target_chunk_size: int = 2000,
    overlap_ratio: float = 0.1,
    max_rounds: int = 1,
    logger: Optional[logging.Logger] = None,
    dry_run: bool = False,
) -> asyncio.Future:
    agent = SummarizerAgent(
        service,
        concurrency=concurrency,
        target_chunk_size=target_chunk_size,
        overlap_ratio=overlap_ratio,
        max_rounds=max_rounds,
        logger=logger,
        dry_run=dry_run,
    )
    return asyncio.ensure_future(
        agent.summarize(
            long_text,
            model=model,
            summarization_prompt=summarization_prompt,
            output_prompt=output_prompt,
        )
    )
