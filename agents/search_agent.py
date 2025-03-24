import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SearchAgent:
    """
    SearchAgent 负责协调 YouTube 搜索与 OpenAI 文本生成，
    提供高度弹性配置和自愈机制：
      1. 关键词生成：根据用户输入生成多个关键词变体（Brainstorm）。
      2. 聚合搜索：对每个关键词调用 YouTubeService 搜索视频，
         结果以字典形式存储（keyword -> [results]）。
      3. 结果去重：将各关键词搜索结果合并后，基于 video_id 去重，
         并累计各关键词返回的数量作为权重。
      4. 结果精炼：对去重后的结果进行排序（可先按权重、再按发布时间）。
      5. 摘要生成：调用 OpenAIService 生成聚合结果摘要。
      6. 结果记录：若传入数据库对象，则将搜索摘要记录到数据库中（异步写入）。
    """

    def __init__(
        self,
        youtube_service: YouTubeService,
        openai_service: OpenAIService,
        db: Optional[Any] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        :param youtube_service: 用于视频搜索的 YouTubeService 实例。
        :param openai_service: 用于文本生成的 OpenAIService 实例。
        :param db: 数据库对象，用于记录搜索摘要（可选）。
                   建议传入支持异步操作的 AsyncDatabase 对象，或者提供异步接口。
        :param settings: 全局设置，默认包含：
            - default_filter: 默认搜索过滤条件。
            - max_results: 每个关键词搜索返回的视频数上限。
            - enable_brainstorm: 是否启用关键词扩展。
            - brainstorm_prompt_template: 关键词扩展使用的 prompt 模板名称。
            - enable_refine: 是否启用结果精炼（排序）。
            - order_by: 排序依据（默认 "weight"）。
            - order_direction: 排序方向（"desc" 或 "asc"，默认 "desc"）。
            - enable_optimization: 是否启用最终排序优化。
            - enable_summary: 是否启用摘要生成。
        """
        self.youtube_service = youtube_service
        self.openai_service = openai_service
        self.db = db
        self.settings = settings or {
            "default_filter": {"videoEmbeddable": "true", "videoSyndicated": "true"},
            "max_results": 10,
            "enable_brainstorm": True,
            "brainstorm_prompt_template": "keyword_generation",
            "enable_refine": True,
            "order_by": "weight",
            "order_direction": "desc",
            "enable_optimization": True,
            "enable_summary": True,
        }
        self._last_search_result: Optional[Dict[str, Any]] = None

    def _get_prompt_content(
        self,
        prompt_template: str,
        template_vars: Optional[Dict[str, Any]] = None,
        extra_text: Optional[str] = None
    ) -> str:
        try:
            raw_prompt = self.openai_service.get_prompt(prompt_template, variables=template_vars)
            prompt_text = raw_prompt.get("user", "") if isinstance(raw_prompt, dict) else raw_prompt
            if extra_text:
                prompt_text += "\n" + extra_text
            return prompt_text
        except Exception as e:
            logger.error(f"[SearchAgent] Error getting prompt content: {e}")
            return ""

    def generate_keywords(self, base_keyword: str) -> List[str]:
        if not self.settings.get("enable_brainstorm", True):
            logger.info("[SearchAgent] Brainstorm disabled; returning original keyword.")
            return [base_keyword]
        try:
            logger.info(f"[SearchAgent] Generating keywords for base keyword: {base_keyword}")
            prompt_vars = {"base_keyword": base_keyword, "max_n": 5}
            keywords_text = self._get_prompt_content(
                prompt_template=self.settings["brainstorm_prompt_template"],
                template_vars=prompt_vars
            )
            keywords = [kw.strip() for kw in keywords_text.strip().split("\n") if kw.strip()]
            if not keywords:
                logger.warning("[SearchAgent] No keywords generated; using original keyword.")
                return [base_keyword]
            logger.info(f"[SearchAgent] Generated keyword variations: {keywords}")
            return keywords
        except Exception as e:
            logger.error(f"[SearchAgent] Exception during keyword generation: {e}")
            return [base_keyword]

    def search_by_keyword(self, keyword: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            logger.info(f"[SearchAgent] Searching videos for keyword: {keyword}")
            max_results = self.settings.get("max_results", 10)
            effective_filters = filters or self.settings.get("default_filter", {})
            response = self.youtube_service.search_videos(q=keyword, max_results=max_results, filters=effective_filters)
            items = response.get("items", [])
            results = []
            for item in items:
                results.append({
                    "search_keyword": keyword,
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "publish_time": item["snippet"]["publishedAt"],
                })
            logger.info(f"[SearchAgent] Keyword '{keyword}' returned {len(results)} videos.")
            return results
        except Exception as e:
            logger.error(f"[SearchAgent] Error during search for keyword '{keyword}': {e}")
            return []

    def aggregate_search(self, base_keyword: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[SearchAgent] Aggregating search results for base keyword: {base_keyword}")
        keywords = self.generate_keywords(base_keyword)
        aggregated: Dict[str, List[Dict[str, Any]]] = {}
        for kw in keywords:
            results = self.search_by_keyword(kw, filters=filters)
            aggregated[kw] = results
            logger.info(f"[SearchAgent] Aggregated {len(results)} results for keyword: {kw}")
        return aggregated

    def deduplicate_results(self, aggregated: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        logger.info("[SearchAgent] Starting deduplication of results.")
        dedup: Dict[str, Dict[str, Any]] = {}
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
        logger.info("[SearchAgent] Optimizing result variations.")
        order_by = self.settings.get("order_by", "weight")
        order_direction = self.settings.get("order_direction", "desc")
        reverse = True if order_direction == "desc" else False
        try:
            optimized = sorted(results, key=lambda v: v.get(order_by, 0), reverse=reverse)
            logger.info(f"[SearchAgent] Optimized results using order_by='{order_by}' {order_direction}.")
            return optimized
        except Exception as e:
            logger.error(f"[SearchAgent] Error during optimization: {e}")
            return results

    def refine_results(self, results: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
        logger.info("[SearchAgent] Refining results.")
        if not self.settings.get("enable_refine", True):
            logger.info("[SearchAgent] Refinement disabled; returning original results.")
            return results
        if self.settings.get("enable_optimization", True):
            results = self.optimize_variations(results)
        refined = sorted(results, key=lambda v: v.get("publish_time", ""), reverse=True)
        logger.info(f"[SearchAgent] Refined results to top {top_n} items.")
        return refined[:top_n]

    def summarize_results(self, results: List[Dict[str, Any]]) -> str:
        logger.info("[SearchAgent] Generating summary of results.")
        if not self.settings.get("enable_summary", True):
            logger.info("[SearchAgent] Summary generation disabled.")
            return ""
        if not results:
            logger.info("[SearchAgent] No results found for summarization.")
            return "No videos found to summarize."
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
        prompt = self._get_prompt_content("structured_output", template_vars={"text": summary_input})
        try:
            summary_text = self.openai_service.completion(prompt=prompt)
            logger.info("[SearchAgent] Generated summary for aggregated results.")
        except Exception as e:
            logger.error(f"[SearchAgent] Error during summary generation: {e}")
            summary_text = "Summary generation failed."
        return summary_text

    async def execute_search(self, base_keyword: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        综合执行搜索流程：聚合搜索、去重、精炼，并返回最终结果数据。
        如果传入了数据库对象，则将搜索摘要记录到数据库中（异步写入）。
        """
        logger.info(f"[SearchAgent] Executing full search workflow for keyword '{base_keyword}'.")
        start_time = datetime.now()
        aggregated = self.aggregate_search(base_keyword, filters=filters)
        deduped = self.deduplicate_results(aggregated)
        refined = self.refine_results(deduped)
        summary_text = self.summarize_results(refined)
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[SearchAgent] Search workflow completed in {total_time} seconds.")
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
        # 异步记录搜索摘要到数据库（如果传入了支持异步操作的 DB 对象）
        if self.db:
            try:
                record = {
                    "keyword": base_keyword,
                    "critique": summary_text,
                    "total_views": len(refined),
                    "total_likes": 0,
                    "weighted_score": 0.0,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                if hasattr(self.db, "store_keyword_analysis_async"):
                    await self.db.store_keyword_analysis_async([record])
                else:
                    await asyncio.to_thread(self.db.store_keyword_analysis, [record])
                logger.info("[SearchAgent] Search summary recorded in database. 😊")
            except Exception as e:
                logger.error(f"[SearchAgent] Failed to record search summary: {e}")
        return result

    @property
    def last_search_result(self) -> Optional[Dict[str, Any]]:
        """
        返回最近一次完整搜索流程的结果。
        """
        return self._last_search_result
