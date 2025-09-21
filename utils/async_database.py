#!/usr/bin/env python3
"""
Async Database Wrapper
Industrial-grade async wrapper for TubeWhale database operations
Supports both async and sync interfaces with lean best practices
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from utils.database import Database

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class AsyncDatabase:
    """
    Industrial async database wrapper with lean framework integration
    Provides backward compatibility while supporting modern async patterns
    """
    def __init__(self, db: Database):
        self.db = db
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="AsyncDB")
        
        # Import lean components lazily to avoid circular imports
        self._lean_manager = None
        self._lean_cli_interface = None
    
    @property
    def lean_manager(self):
        """Lazy load lean database manager"""
        if self._lean_manager is None:
            from utils.database_manager import create_database_manager
            self._lean_manager, self._lean_cli_interface = create_database_manager()
        return self._lean_manager
    
    @property 
    def lean_cli_interface(self):
        """Lazy load lean CLI interface"""
        if self._lean_cli_interface is None:
            from utils.database_manager import create_database_manager
            self._lean_manager, self._lean_cli_interface = create_database_manager()
        return self._lean_cli_interface

    async def store_video_metadata(self, video_metadata: dict) -> None:
        logger.info("Async: storing video metadata...")
        await asyncio.to_thread(self.db.store_video_metadata, video_metadata)

    async def update_video_metadata(
        self, video_id: str, llm_summary: str, transcript: str,
        audio_summary: Optional[str] = None, ai_cost: float = 0.0
    ) -> None:
        logger.info("Async: updating metadata for video %s...", video_id)
        await asyncio.to_thread(self.db.update_video_metadata, video_id, llm_summary, transcript, audio_summary, ai_cost)

    async def store_comments(self, video_id: str, comments: List[Dict[str, Any]]) -> None:
        logger.info("Async: storing comments for video %s...", video_id)
        await asyncio.to_thread(self.db.store_comments, video_id, comments)

    async def store_brainstormed_topics(self, topics: List[str], critique: str, topic_score: float) -> None:
        logger.info("Async: storing brainstormed topics...")
        await asyncio.to_thread(self.db.store_brainstormed_topics, topics, critique, topic_score)

    async def store_transcript_summary(self, video_id: str, transcript: str, summary: str) -> None:
        logger.info("Async: storing transcript summary for video %s...", video_id)
        await asyncio.to_thread(self.db.store_transcript_summary, video_id, transcript, summary)

    async def store_ai_interaction(
        self, input_data: Dict[str, Any], output_data: Dict[str, Any],
        interaction_type: str, tokens_used: int = 0, cost: float = 0.0,
        timestamp: Optional[str] = None, duration_ms: int = 0
    ) -> None:
        logger.info("Async: storing AI interaction for %s...", interaction_type)
        await asyncio.to_thread(self.db.store_ai_interaction, input_data, output_data, interaction_type, tokens_used, cost, timestamp, duration_ms)

    async def store_keyword_analysis(self, keyword_analysis: List[Dict[str, Any]]) -> None:
        logger.info("Async: storing keyword analysis...")
        await asyncio.to_thread(self.db.store_keyword_analysis, keyword_analysis)

    async def store_data(self, table_name: str, data_dict: Dict[str, Any]) -> None:
        logger.info("Async: storing data into table %s...", table_name)
        await asyncio.to_thread(self.db.store_data, table_name, data_dict)

    async def store_video_process_log(self, video_id: str, step: str, status: str, details: str = "") -> None:
        logger.info("Async: storing process log for video %s, step '%s'.", video_id, step)
        await asyncio.to_thread(self.db.store_video_process_log, video_id, step, status, details)

    # =====================================================
    # LEAN FRAMEWORK INTEGRATION - Industrial Best Practices  
    # =====================================================
    
    async def get_video_analysis_summary(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Async analysis summary using lean interface"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.lean_cli_interface.get_video_analysis_summary,
            limit
        )
    
    async def get_analysis_stats(self) -> Dict[str, Any]:
        """Async analysis statistics using lean interface"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.lean_cli_interface.get_analysis_stats
        )
    
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Async performance metrics using lean manager"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.lean_manager.get_performance_stats
        )
    
    async def execute_optimized_query(self, query: str, params: tuple = (), cache_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Async optimized query execution using lean manager"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.lean_manager.execute_query,
            query,
            params,
            cache_key
        )
    
    # Sync passthrough methods for compatibility
    def get_lean_manager(self):
        """Get lean database manager for direct sync access"""
        return self.lean_manager
    
    def get_lean_cli_interface(self):
        """Get lean CLI interface for direct sync access"""
        return self.lean_cli_interface

    async def close(self) -> None:
        logger.info("Async: closing database with lean framework cleanup...")
        await asyncio.to_thread(self.db.close)
        
        # Clean up lean components
        if self._lean_manager:
            self._lean_manager.close()
        
        # Clean up thread pool
        self.executor.shutdown(wait=True)
        logger.info("✅ AsyncDatabase closed with full cleanup")
