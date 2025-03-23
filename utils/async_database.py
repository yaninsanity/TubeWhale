#!/usr/bin/env python3
import asyncio
import logging
from typing import Any, Dict, List, Optional

from utils.database import Database

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class AsyncDatabase:
    """
    异步数据库包装器，将同步 Database 类的操作包装为异步接口，
    以便在异步流程中调用而不阻塞事件循环。
    """
    def __init__(self, db: Database):
        self.db = db

    async def store_video_metadata(self, video_metadata: dict) -> None:
        logger.info("Async: storing video metadata...")
        await asyncio.to_thread(self.db.store_video_metadata, video_metadata)

    async def update_video_metadata(
        self, video_id: str, llm_summary: str, transcript: str,
        audio_summary: Optional[str] = None, ai_cost: float = 0.0
    ) -> None:
        logger.info(f"Async: updating metadata for video {video_id}...")
        await asyncio.to_thread(
            self.db.update_video_metadata,
            video_id, llm_summary, transcript, audio_summary, ai_cost
        )

    async def store_comments(self, video_id: str, comments: List[Dict[str, Any]]) -> None:
        logger.info(f"Async: storing comments for video {video_id}...")
        await asyncio.to_thread(self.db.store_comments, video_id, comments)

    async def store_brainstormed_topics(self, topics: List[str], critique: str, topic_score: float) -> None:
        logger.info("Async: storing brainstormed topics...")
        await asyncio.to_thread(self.db.store_brainstormed_topics, topics, critique, topic_score)

    async def store_transcript_summary(self, video_id: str, transcript: str, summary: str) -> None:
        logger.info(f"Async: storing transcript summary for video {video_id}...")
        await asyncio.to_thread(self.db.store_transcript_summary, video_id, transcript, summary)

    async def store_ai_interaction(
        self, input_data: Dict[str, Any], output_data: Dict[str, Any],
        interaction_type: str, tokens_used: int = 0, cost: float = 0.0,
        timestamp: Optional[str] = None
    ) -> None:
        logger.info(f"Async: storing AI interaction for {interaction_type}...")
        await asyncio.to_thread(
            self.db.store_ai_interaction,
            input_data, output_data, interaction_type, tokens_used, cost, timestamp
        )

    async def store_keyword_analysis(self, keyword_analysis: List[Dict[str, Any]]) -> None:
        logger.info("Async: storing keyword analysis...")
        await asyncio.to_thread(self.db.store_keyword_analysis, keyword_analysis)

    async def store_data(self, table_name: str, data_dict: Dict[str, Any]) -> None:
        logger.info(f"Async: storing data into table {table_name}...")
        await asyncio.to_thread(self.db.store_data, table_name, data_dict)

    async def close(self) -> None:
        logger.info("Async: closing database...")
        await asyncio.to_thread(self.db.close)
