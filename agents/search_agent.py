#!/usr/bin/env python3
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

# 假设 YouTubeService 与 OpenAIService 已经实现
from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SearchAgent:
    """
    SearchAgent 负责协调 YouTube 搜索与 OpenAI 文本生成，提供如下功能：
      1. 关键词生成：根据用户输入生成多个关键词变体（Brainstorm）。
      2. 聚合搜索：对每个关键词调用 YouTubeService 搜索视频，结果以字典形式存储（keyword -> [results]）。
      3. 结果去重：将各关键词搜索结果合并后，基于 video_id 去重，并累计各关键词返回数量作为权重。
      4. 结果精炼：对去重后的结果进行排序（先按权重，再按发布时间）。
      5. 摘要生成：调用 OpenAIService 的异步接口生成聚合结果摘要。
      6. 结果记录：将搜索过程中的各个步骤记录到数据库（如 AI 交互记录、搜索日志）。
    """

    def __init__(
        self,
        youtube_service: YouTubeService,
        openai_service: OpenAIService,
        db: Optional[Any] = None,  # 支持异步数据库对象
        settings: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.youtube_service = youtube_service
        self.openai_service = openai_service
        self.db = db
        self.settings = settings or {
            "default_filter": {"videoEmbeddable": "true", "videoSyndicated": "true"},
            "max_results": 10,
            "enable_brainstorm": True,
            "brainstorm_prompt_template": "keyword_generation",
            "max_keywords": 5,
            "enable_refine": True,
            "order_by": "weight",
            "order_direction": "desc",
            "enable_optimization": True,
            "enable_summary": True,
        }
        self._last_search_result: Optional[Dict[str, Any]] = None
        self.logger = logger or self._get_default_logger()
        self.logger.info("[SearchAgent] Initialized with settings: %s", self.settings)

    def _get_prompt_content(
        self,
        prompt_template: str,
        template_vars: Optional[Dict[str, Any]] = None,
        extra_text: Optional[str] = None
    ) -> str:
        try:
            raw_prompt = self.openai_service.get_prompt(prompt_template, variables=template_vars)
            prompt_text = raw_prompt + (f"\n{extra_text}" if extra_text else "")
            return prompt_text
        except Exception as e:
            logger.error(f"[SearchAgent] Error getting prompt content: {e}")
            return ""

    async def generate_keywords(self, base_keyword: str) -> List[str]:
        """
        异步调用 OpenAIService 生成关键词列表，并记录 AI 交互日志。
        """
        if not self.settings.get("enable_brainstorm", True):
            logger.info("[SearchAgent] Brainstorm disabled; returning original keyword.")
            return [base_keyword]
        try:
            logger.info(f"[SearchAgent] Generating keywords for base keyword: {base_keyword}")
            prompt_vars = {
                "base_keyword": base_keyword,
                "max_n": self.settings.get("max_keywords", 5)
            }
            prompt_text = self._get_prompt_content(
                prompt_template=self.settings["brainstorm_prompt_template"],
                template_vars=prompt_vars
            )
            keywords_response = await self.openai_service.async_completion(
                prompt=prompt_text,
                prompt_template=self.settings["brainstorm_prompt_template"]
            )
            keywords_response = str(keywords_response)
            keywords = [kw.strip() for kw in keywords_response.strip().split("\n") if kw.strip()]
            if not keywords:
                logger.warning("[SearchAgent] No keywords generated; using original keyword.")
                keywords = [base_keyword]
            logger.info(f"[SearchAgent] Generated keyword variations: {keywords}")
            # 记录 AI 交互（假设数据库对象提供 store_ai_interaction 方法）
            if self.db and hasattr(self.db, "store_ai_interaction"):
                await self.db.store_ai_interaction(
                    input_data={"base_keyword": base_keyword, "prompt": prompt_text},
                    output_data={"generated_keywords": keywords},
                    interaction_type="keyword_generation",
                    tokens_used=0,   # 请替换为实际 token 数量
                    cost=0.0,        # 请替换为实际成本
                    duration_ms=0    # 请替换为实际耗时
                )
            # 如有其他搜索日志记录需求，也可调用 db.store_search_log(...)
            return keywords
        except Exception as e:
            logger.error(f"[SearchAgent] Exception during keyword generation: {e}")
            return [base_keyword]

    async def aggregate_search(self, base_keyword: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        异步聚合搜索：先生成关键词，再对每个关键词进行搜索，并记录每个关键词的搜索结果。
        """
        logger.info(f"[SearchAgent] Aggregating search results for base keyword: {base_keyword}")
        keywords = await self.generate_keywords(base_keyword)
        aggregated: Dict[str, List[Dict[str, Any]]] = {}
        for kw in keywords:
            results = self.search_by_keyword(kw, filters=filters)
            aggregated[kw] = results
            logger.info(f"[SearchAgent] Aggregated {len(results)} results for keyword: {kw}")
        return aggregated

    def search_by_keyword(self, keyword: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        调用 YouTubeService 根据关键词搜索视频，并返回视频列表。
        """
        try:
            logger.info(f"[SearchAgent] Searching videos for keyword: {keyword}")
            max_results = self.settings.get("max_results", 15)
            effective_filters = filters or self.settings.get("default_filter", {})
            response = self.youtube_service.search_videos(q=keyword, max_results=max_results, filters=effective_filters)
            results = [{
                "search_keyword": keyword,
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "publish_time": item["snippet"]["publishedAt"],
            } for item in response.get("items", [])]
            logger.info(f"[SearchAgent] Keyword '{keyword}' returned {len(results)} videos.")
            return results
        except Exception as e:
            logger.error(f"[SearchAgent] Error during search for keyword '{keyword}': {e}")
            return []

    def deduplicate_results(self, aggregated: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        对聚合的搜索结果根据 video_id 去重，并累计各关键词返回结果数量作为权重。
        """
        logger.info("[SearchAgent] Starting deduplication of results.")
        dedup = {}
        for kw, results in aggregated.items():
            for item in results:
                vid = item.get("video_id")
                if vid not in dedup:
                    item["weight"] = len(results)
                    dedup[vid] = item
                else:
                    dedup[vid]["weight"] += len(results)
        total_items = sum(len(v) for v in aggregated.values())
        logger.info(f"[SearchAgent] Deduplicated {total_items} aggregated items to {len(dedup)} unique videos.")
        return list(dedup.values())

    def optimize_variations(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        按照设定的排序规则对搜索结果进行优化排序。
        """
        logger.info("[SearchAgent] Optimizing result variations.")
        order_by = self.settings.get("order_by", "weight")
        reverse = (self.settings.get("order_direction", "desc") == "desc")
        try:
            optimized = sorted(results, key=lambda v: v.get(order_by, 0), reverse=reverse)
            logger.info(f"[SearchAgent] Optimized results using order_by='{order_by}' direction {'desc' if reverse else 'asc'}.")
            return optimized
        except Exception as e:
            logger.error(f"[SearchAgent] Error during optimization: {e}")
            return results

    def refine_results(self, results: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        根据发布时间等因素对搜索结果进一步精炼，返回前 N 个结果。
        """
        logger.info("[SearchAgent] Refining results.")
        if not self.settings.get("enable_refine", True):
            logger.info("[SearchAgent] Refinement disabled; returning original results.")
            return results
        if self.settings.get("enable_optimization", True):
            results = self.optimize_variations(results)
        refined = sorted(results, key=lambda v: v.get("publish_time", ""), reverse=True)
        logger.info(f"[SearchAgent] Refined results to top {top_n} items.")
        return refined[:top_n]

    async def summarize_results(self, results: List[Dict[str, Any]]) -> str:
        """
        调用 OpenAIService 生成聚合结果摘要。
        """
        logger.info("[SearchAgent] Generating summary of results.")
        if not self.settings.get("enable_summary", True):
            logger.info("[SearchAgent] Summary generation disabled.")
            return ""
        if not results:
            logger.info("[SearchAgent] No results found for summarization.")
            return "No videos found to summarize."

        # 根据关键词将视频标题分组，构造摘要输入
        keyword_groups: Dict[str, List[str]] = {}
        for item in results:
            kw = item.get("search_keyword", "unknown")
            keyword_groups.setdefault(kw, []).append(item["title"])
        summary_lines = []
        for kw, titles in keyword_groups.items():
            summary_lines.append(f"Keyword '{kw}' yielded {len(titles)} videos:")
            for title in titles:
                summary_lines.append(f"  - {title}")
        summary_input = "\n".join(summary_lines)

        template = "structured_output" if "structured_output" in self.openai_service.prompts else "summarization"
        prompt = self._get_prompt_content(template, template_vars={"text": summary_input})
        try:
            summary_text = await self.openai_service.async_completion(
                prompt=prompt,
                prompt_template=template
            )
            logger.info("[SearchAgent] Generated summary for aggregated results.")
        except Exception as e:
            logger.error(f"[SearchAgent] Error during summary generation: {e}")
            summary_text = "Summary generation failed."
        return summary_text

    async def execute_search(self, base_keyword: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        执行完整的搜索流程：生成关键词、搜索、去重、精炼、摘要生成，并将搜索结果记录到数据库。
        """
        logger.info(f"[SearchAgent] Executing full search workflow for keyword '{base_keyword}'.")
        start_time = datetime.now()
        aggregated = await self.aggregate_search(base_keyword, filters=filters)
        deduped = self.deduplicate_results(aggregated)
        refined = self.refine_results(deduped)
        summary_text = await self.summarize_results(refined)
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[SearchAgent] Search workflow completed in {total_time:.2f} seconds.")
        result = {
            "keywords_searched": list(aggregated.keys()),
            "aggregated_by_keyword": aggregated,
            "deduplicated_results": deduped,
            "refined_results": refined,
            "total_unique_videos": len(deduped),
            "summary": summary_text,
            "execution_time_seconds": total_time
        }
        self._last_search_result = result

        # 将搜索摘要记录到数据库（如果数据库对象提供相关方法）
        if self.db and hasattr(self.db, "store_keyword_analysis"):
            try:
                record = {
                    "keyword": base_keyword,
                    "critique": summary_text,
                    "total_views": len(refined),
                    "total_likes": 0,
                    "weighted_score": 0.0,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                await self.db.store_keyword_analysis([record])
                logger.info("[SearchAgent] Search summary recorded in database.")
            except Exception as e:
                logger.error(f"[SearchAgent] Failed to record search summary: {e}")
        return result

    @property
    def last_search_result(self) -> Optional[Dict[str, Any]]:
        return self._last_search_result
