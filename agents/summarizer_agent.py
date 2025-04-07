#!/usr/bin/env python3
import logging
import json
import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from tqdm import tqdm
from utils.openAIServices import OpenAIService
from utils.helper import async_retry  # Assume async_retry decorator is implemented

# Fixed JSON template for final summary validation
JSON_TEMPLATE = {
    "main_topic": "N/A",
    "key_insights": "N/A",
    "recommended_tools": "N/A",
    "best_practices": "N/A",
    "challenges_and_advice": "N/A"
}

def dynamic_chunk_text(text: str, target_chunk_size: int = 2000, overlap_ratio: float = 0.1) -> List[str]:
    """
    Dynamically chunk text:
      1. Split text by newline.
      2. Merge paragraphs into chunks roughly target_chunk_size characters.
      3. Retain an overlap (overlap_ratio) between chunks to ensure context continuity.
      
    If the text contains only one paragraph that exceeds target_chunk_size, split it into fixed-size
    chunks with the specified overlap.
    """
    text = text.strip()
    if not text:
        return []
    paragraphs = re.split(r'\n+', text)
    # Handle the case where there is only one very long paragraph.
    if len(paragraphs) == 1 and len(paragraphs[0]) > target_chunk_size:
        long_text = paragraphs[0]
        chunks = []
        overlap = int(target_chunk_size * overlap_ratio)
        start = 0
        while start < len(long_text):
            end = start + target_chunk_size
            chunks.append(long_text[start:end])
            start = end - overlap  # move back by overlap
        logging.info(f"Dynamic chunking (single long paragraph) produced {len(chunks)} chunks.")
        return chunks

    chunks = []
    current_chunk = ""
    for para in paragraphs:
        if not current_chunk:
            current_chunk = para
            continue
        tentative = current_chunk + "\n" + para
        if len(tentative) < target_chunk_size:
            current_chunk = tentative
        else:
            if current_chunk.strip():
                chunks.append(current_chunk)
            overlap_length = int(len(current_chunk) * overlap_ratio)
            if overlap_length > 0:
                current_chunk = current_chunk[-overlap_length:] + "\n" + para
            else:
                current_chunk = para
    if current_chunk.strip():
        chunks.append(current_chunk)
    logging.info(f"Dynamic chunking produced {len(chunks)} chunks (target ~{target_chunk_size} chars, overlap_ratio {overlap_ratio}).")
    return chunks

# Alias for compatibility with tests.
chunk_text_by_tokens = dynamic_chunk_text

def safe_format_prompt(prompt_template: str, variables: Dict[str, Any]) -> str:
    """
    Safely format a prompt template. If a key (such as previous_summary) is missing,
    a default value is provided to prevent formatting exceptions.
    """
    vars_copy = variables.copy()
    if "previous_summary" not in vars_copy:
        vars_copy["previous_summary"] = ""
    try:
        formatted = prompt_template.format(**vars_copy)
    except Exception as e:
        logging.error(f"Error formatting prompt with variables {vars_copy}: {e}")
        formatted = f"Previous Summary: {vars_copy.get('previous_summary','')}\nText: {vars_copy.get('text','')}"
    return formatted

class SummarizerAgent:
    """
    SummarizerAgent generates summaries for long texts. Its main functions are:
      1. Dynamically chunking text (with configurable target size and overlap).
      2. Concurrently calling the OpenAI API to summarize each chunk (using async_retry).
      3. Merging all chunk summaries and validating the format (must match the fixed JSON template);
         if invalid, reformatting is performed.
      4. Optionally logging each chunk and the final summary into a database.
      5. Displaying progress via tqdm.
    """
    def __init__(self, openai_service: OpenAIService, enable_chunking: bool = True,
                 debug_mode: bool = False, db: Optional[Any] = None, concurrency: int = 5,
                 target_chunk_size: int = 2000, overlap_ratio: float = 0.1,
                 logger: Optional[logging.Logger] = None):
        self.openai_service = openai_service
        self.enable_chunking = enable_chunking
        self.debug_mode = debug_mode
        self.db = db
        self.concurrency = concurrency
        self.target_chunk_size = target_chunk_size
        self.overlap_ratio = overlap_ratio
        self.logger = logger or logging.getLogger(__name__)

    def chunk_text(self, text: str) -> List[str]:
        chunks = dynamic_chunk_text(text, target_chunk_size=self.target_chunk_size,
                                    overlap_ratio=self.overlap_ratio)
        if self.debug_mode:
            self.logger.debug(f"Chunk lengths: {[len(chunk) for chunk in chunks]}")
        return chunks

    @async_retry(max_retries=3, delay=2)
    async def summarize_chunk(self, chunk: str, previous_summary: str = "", 
                                model: str = "gpt-4o-mini", prompt_name: str = "summarization") -> str:
        """
        Call the OpenAI API to summarize a single chunk.
        Uses safe_format_prompt to ensure the prompt format is correct.
        If retries fail, returns an empty string.
        """
        if model not in self.openai_service.models:
            fallback = self.openai_service.default_model
            self.logger.warning(f"Model '{model}' not configured. Falling back to default '{fallback}'.")
            model = fallback

        prompt_template = self.openai_service.get_prompt(prompt_name, variables={})
        variables = {
            "text": chunk,
            "previous_summary": previous_summary
        }
        formatted_prompt = safe_format_prompt(prompt_template, variables)
        if self.debug_mode:
            self.logger.debug(f"Chunk prompt (first 150 chars): {formatted_prompt[:150]}")
        try:
            response_text = self.openai_service.completion(prompt=formatted_prompt, model=model)
            response_text = response_text.strip()
            self.logger.info(f"Chunk summarized (first 100 chars): {response_text[:100]}")
            return response_text
        except Exception as e:
            self.logger.error(f"Error summarizing chunk: {e}")
            return ""

    def merge_summaries(self, summaries: List[str]) -> str:
        merged = "\n".join(summaries)
        self.logger.info("Merged individual chunk summaries.")
        return merged

    def validate_json_output(self, text: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(text)
            # Ensure all keys in the JSON template exist.
            for key, default in JSON_TEMPLATE.items():
                if key not in data:
                    data[key] = default
            return data
        except json.JSONDecodeError:
            self.logger.error("Final merged summary is not valid JSON.")
            return None

    async def reformat_summary_as_json(self, merged_summary: str, model: str = "gpt-4o-mini") -> str:
        """
        If the merged summary cannot be parsed as JSON, reformat it using OpenAI.
        """
        fallback_template = ("Reformat the following summary into a JSON object with keys: {keys}.\n"
                             "Summary: {summary}")
        variables = {
            "keys": list(JSON_TEMPLATE.keys()),
            "summary": merged_summary
        }
        formatted_prompt = safe_format_prompt(fallback_template, variables)
        if self.debug_mode:
            self.logger.debug(f"Reformat prompt (first 150 chars): {formatted_prompt[:150]}")
        try:
            reformatted = self.openai_service.completion(prompt=formatted_prompt, model=model)
            reformatted = reformatted.strip()
            self.logger.info(f"Reformatted summary (first 100 chars): {reformatted[:100]}")
            return reformatted
        except Exception as e:
            self.logger.error(f"Error reformatting summary: {e}")
            return merged_summary

    async def _process_chunk(self, idx: int, chunk: str, model: str,
                               prompt_name: str, semaphore: asyncio.Semaphore) -> (int, str):
        """
        Internal method: Process a single chunk under semaphore control, call summarize_chunk,
        and log the result into the database if provided.
        """
        async with semaphore:
            summary = await self.summarize_chunk(chunk, previous_summary="", model=model, prompt_name=prompt_name)
            if self.db:
                record = {
                    "process": "chunk_summary",
                    "chunk_index": idx + 1,
                    "summary": summary,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                try:
                    if hasattr(self.db, "store_data_async"):
                        await self.db.store_data_async("summarization_logs", record)
                    else:
                        await asyncio.to_thread(self.db.store_data, "summarization_logs", record)
                    self.logger.info(f"Chunk {idx+1} summary stored in DB.")
                except Exception as db_e:
                    self.logger.error(f"DB error for chunk {idx+1}: {db_e}")
            return idx, summary

    async def summarize(self, long_text: str, model: str = "gpt-4o-mini",
                        prompt_name: str = "summarization") -> str:
        """
        Overall summarization workflow:
          1. Dynamically chunk the long text (if enabled, otherwise use the whole text).
          2. Concurrently call summarize_chunk for each chunk, displaying progress via tqdm.
          3. Merge all chunk summaries and try to parse as JSON; if parsing fails, reformat.
          4. Log the final summary in the database (if provided) and return the JSON-formatted summary.
        """
        if not long_text:
            self.logger.error("No text provided for summarization.")
            return ""
        self.logger.info("Starting summarization process.")
        chunks = self.chunk_text(long_text) if self.enable_chunking else [long_text]
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = [self._process_chunk(idx, chunk, model, prompt_name, semaphore)
                 for idx, chunk in enumerate(chunks)]
        results = [None] * len(tasks)
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Summarizing chunks"):
            idx, summary = await future
            results[idx] = summary

        merged = self.merge_summaries(results)
        validated = self.validate_json_output(merged)
        if validated is None:
            self.logger.info("Merged summary not valid JSON, attempting reformat...")
            reformatted = await self.reformat_summary_as_json(merged, model=model)
            validated = self.validate_json_output(reformatted)
            final_summary = json.dumps(validated, ensure_ascii=False, indent=2) if validated else merged
        else:
            final_summary = json.dumps(validated, ensure_ascii=False, indent=2)
        self.logger.info("Final summarization process completed.")
        if self.db:
            final_record = {
                "process": "final_summary",
                "summary": final_summary,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            try:
                if hasattr(self.db, "store_data_async"):
                    await self.db.store_data_async("summarization_logs", final_record)
                else:
                    await asyncio.to_thread(self.db.store_data, "summarization_logs", final_record)
                self.logger.info("Final summary stored in DB.")
            except Exception as db_e:
                self.logger.error(f"DB error for final summary: {db_e}")
        return final_summary

def gpt_summarizer_agent(
    long_text: str, *,
    model: str = "gpt-4o-mini",
    openai_service: Optional[OpenAIService] = None,
    enable_chunking: bool = True,
    debug_mode: bool = False,
    prompt_name: str = "summarization",
    db: Optional[Any] = None,
    concurrency: int = 5,
    target_chunk_size: int = 2000,
    overlap_ratio: float = 0.1,
    logger: Optional[logging.Logger] = None
) -> asyncio.Future:
    """
    Factory function: constructs a SummarizerAgent instance and asynchronously calls summarize
    to generate the final summary.
    """
    if openai_service is None:
        raise ValueError("An OpenAIService instance must be provided.")
    agent = SummarizerAgent(
        openai_service=openai_service,
        enable_chunking=enable_chunking,
        debug_mode=debug_mode,
        db=db,
        concurrency=concurrency,
        target_chunk_size=target_chunk_size,
        overlap_ratio=overlap_ratio,
        logger=logger
    )
    return asyncio.ensure_future(agent.summarize(long_text, model=model, prompt_name=prompt_name))

__all__ = ["SummarizerAgent", "gpt_summarizer_agent", "dynamic_chunk_text", "chunk_text_by_tokens"]
