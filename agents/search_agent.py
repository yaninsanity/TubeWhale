import logging
from typing import Dict, Any, Optional, List

from utils.openAIServices import OpenAIService
from utils.youtube import YouTubeService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SearchAgent:
    """
    SearchAgent 负责协调 YouTube 搜索与 OpenAI 文本生成，
    提供高度弹性配置和自愈机制，重点在于生成并优化关键词变体的顺序，
    并支持传入自定义过滤条件调用 YouTubeService 搜索。

    流程：
      1. 关键词生成：根据用户输入生成多个关键词变体（Brainstorm），可动态启用或禁用。
      2. 聚合搜索：对每个关键词调用 YouTubeService 搜索视频，
         结果以字典形式存储（keyword -> [results]），可传入自定义过滤条件。
      3. 结果去重：将各关键词搜索结果合并后，基于 video_id 去重，
         并累计各关键词返回的数量作为权重。
      4. 结果精炼：对去重后的结果进行排序（默认按照权重排序，也可配置其他排序规则）。
      5. 摘要生成：调用 OpenAIService 生成聚合结果摘要，反馈各关键词及视频汇总情况。
    """

    def __init__(
        self,
        youtube_service: YouTubeService,
        openai_service: OpenAIService,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        初始化 SearchAgent。

        :param youtube_service: 用于视频搜索的 YouTubeService 实例。
        :param openai_service: 用于文本生成的 OpenAIService 实例。
        :param settings: 全局设置，默认包含：
            - default_filter: 默认搜索过滤条件（可动态传入更多过滤参数）。
            - max_results: 每个关键词搜索返回的视频数上限。
            - enable_brainstorm: 是否启用关键词扩展（Brainstorm）。
            - brainstorm_prompt_template: 关键词扩展使用的 prompt 模板名称。
            - enable_refine: 是否启用结果精炼（排序）。
            - order_by: 排序依据（默认使用 "weight"）。
            - order_direction: 排序方向（"desc" 或 "asc"，默认 "desc"）。
            - enable_optimization: 是否启用最终排序优化（根据权重等指标）。
            - enable_summary: 是否启用摘要生成。
        """
        self.youtube_service = youtube_service
        self.openai_service = openai_service
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
        """
        获取格式化后的 prompt 文本。支持 YAML 中 prompt 定义为字典（取 "user" 字段）或直接为字符串。

        :param prompt_template: prompt 模板名称。
        :param template_vars: 格式化所需变量。
        :param extra_text: 额外附加文本。
        :return: 格式化后的 prompt 文本。
        """
        raw_prompt = self.openai_service.get_prompt(prompt_template, variables=template_vars)
        prompt_text = raw_prompt.get("user", "") if isinstance(raw_prompt, dict) else raw_prompt
        if extra_text:
            prompt_text += "\n" + extra_text
        return prompt_text

    def generate_keywords(self, base_keyword: str) -> List[str]:
        """
        根据 base_keyword 调用 OpenAIService 生成关键词变体。
        若禁用 brainstorm，则直接返回 [base_keyword]。

        :param base_keyword: 用户输入的原始关键词。
        :return: 关键词列表；若未生成则至少返回原始关键词。
        """
        if self.settings.get("enable_brainstorm") is False:
            logger.info("[SearchAgent] Brainstorm disabled; returning original keyword.")
            return [base_keyword]
        try:
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
        """
        针对单个关键词调用 YouTubeService 进行搜索，并返回结果列表。
        传入的 filters 优先级高于 settings 中 default_filter。

        :param keyword: 搜索关键词。
        :param filters: 可选过滤条件。
        :return: 搜索结果列表。
        """
        try:
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
        """
        根据原始关键词进行聚合搜索。如果启用关键词扩展，则生成多个关键词，
        分别进行搜索，并返回字典格式结果： {关键词: [搜索结果列表]}。

        :param base_keyword: 用户输入的原始关键词。
        :param filters: 可选过滤条件，若传入则覆盖 settings 默认值。
        :return: 聚合搜索结果字典。
        """
        keywords = self.generate_keywords(base_keyword)
        aggregated: Dict[str, List[Dict[str, Any]]] = {}
        for kw in keywords:
            results = self.search_by_keyword(kw, filters=filters)
            aggregated[kw] = results
        return aggregated

    def deduplicate_results(self, aggregated: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        将聚合的搜索结果合并，并基于 video_id 进行去重，同时累计各关键词返回的数量作为权重。

        :param aggregated: 聚合搜索结果字典。
        :return: 去重后的结果列表，每个结果包含一个 weight 字段。
        """
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
        """
        对去重后的结果进行排序优化，默认根据 settings 中指定的 order_by 和 order_direction 排序，
        例如按 "weight" 降序排列，让表现更好的关键词排在前面。

        :param results: 去重后的结果列表。
        :return: 排序优化后的结果列表。
        """
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
        """
        对去重后的结果先进行优化排序，再按发布时间排序（降序），并返回前 N 个结果。

        :param results: 去重后的结果列表。
        :param top_n: 返回结果数量上限。
        :return: 精炼后的结果列表。
        """
        if not self.settings.get("enable_refine", True):
            logger.info("[SearchAgent] Refinement disabled; returning original results.")
            return results
        if self.settings.get("enable_optimization", True):
            results = self.optimize_variations(results)
        refined = sorted(results, key=lambda v: v.get("publish_time", ""), reverse=True)
        logger.info(f"[SearchAgent] Refined results to top {top_n} items.")
        return refined[:top_n]

    def summarize_results(self, results: List[Dict[str, Any]]) -> str:
        """
        使用 OpenAIService 对去重后的结果生成摘要，
        反馈各关键词及视频汇总情况。使用 "structured_output" 模板生成摘要。

        :param results: 去重后的搜索结果列表。
        :return: 生成的摘要文本。
        """
        if not self.settings.get("enable_summary", True):
            logger.info("[SearchAgent] Summary generation disabled.")
            return ""
        if not results:
            return "No videos found to summarize."
        # 按关键词分组
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
        summary_text = self.openai_service.completion(prompt=prompt)
        logger.info("[SearchAgent] Generated summary for aggregated results.")
        return summary_text

    def execute_search(self, base_keyword: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        综合执行搜索流程：
          1. 聚合搜索（包含关键词扩展）。
          2. 去重。
          3. 结果精炼（优化排序+发布时间排序）。
          4. 返回最终结果数据，包括各步骤中间结果。

        :param base_keyword: 用户输入的原始关键词。
        :param filters: 可选过滤条件，覆盖 settings 中默认值。
        :return: 包含各步骤中间结果的字典，便于调试、入库及后续调用。
        """
        logger.info(f"[SearchAgent] Executing full search workflow for keyword '{base_keyword}'.")
        aggregated = self.aggregate_search(base_keyword, filters=filters)
        deduped = self.deduplicate_results(aggregated)
        refined = self.refine_results(deduped)
        result = {
            "keywords_searched": list(aggregated.keys()),
            "aggregated_by_keyword": aggregated,
            "deduplicated_results": deduped,
            "refined_results": refined,
            "total_unique_videos": len(deduped)
        }
        self._last_search_result = result
        return result

    @property
    def last_search_result(self) -> Optional[Dict[str, Any]]:
        """
        以属性方式返回最近一次完整搜索流程的结果。

        :return: 上次搜索结果字典；若未执行过搜索则返回 None。
        """
        return self._last_search_result
