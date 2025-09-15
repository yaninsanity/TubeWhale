#!/usr/bin/env python3
"""
Industrial Video Processing Pipeline
High-performance async video analysis with expert consultation integration
"""

import asyncio
import sys
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.search_agent import SearchAgent
from agents.transcript_agent import run_transcript
from agents.summarizer_agent import gpt_summarizer_agent
from utils.youtube import YouTubeService

from .message_queue import TaskType, TaskPriority, Task, get_message_queue
from .expert_customization import get_expert_engine, ExpertDomain, AnalysisDepth


class ProcessingStage(Enum):
    """Video processing pipeline stages"""
    INITIALIZATION = "initialization"
    METADATA_EXTRACTION = "metadata_extraction"
    TRANSCRIPT_GENERATION = "transcript_generation"
    CONTENT_PREPROCESSING = "content_preprocessing"
    EXPERT_ANALYSIS = "expert_analysis"
    QUALITY_VALIDATION = "quality_validation"
    OUTPUT_FORMATTING = "output_formatting"
    COMPLETION = "completion"


class ProcessingStatus(Enum):
    """Processing status for real-time tracking"""
    QUEUED = "queued"
    INITIALIZING = "initializing"
    PROCESSING = "processing"
    ANALYZING = "analyzing"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingConfig:
    """Comprehensive configuration for video processing"""
    # Core processing options
    enable_transcript: bool = True
    enable_audio_download: bool = True
    enable_metadata_extraction: bool = True
    
    # Quality settings
    transcript_quality: str = "high"  # low, medium, high
    max_transcript_length: int = 50000
    min_content_length: int = 100
    
    # Expert analysis configuration
    expert_domain: Optional[ExpertDomain] = None
    expert_pipeline_id: Optional[str] = None
    custom_prompt_injection: Optional[str] = None
    
    # Output configuration
    output_format: str = "structured_json"
    include_metadata: bool = True
    include_timestamps: bool = True
    include_confidence_scores: bool = True
    
    # Performance tuning
    max_concurrent_downloads: int = 5
    request_timeout: int = 300
    retry_attempts: int = 3
    
    # Advanced options
    preprocessing_hooks: List[Callable] = field(default_factory=list)
    postprocessing_hooks: List[Callable] = field(default_factory=list)
    validation_rules: List[Callable] = field(default_factory=list)


@dataclass
class ProcessingMetrics:
    """Comprehensive processing performance metrics"""
    start_time: float = field(default_factory=time.time)
    stage_times: Dict[ProcessingStage, float] = field(default_factory=dict)
    total_processing_time: Optional[float] = None
    transcript_length: int = 0
    metadata_size: int = 0
    analysis_length: int = 0
    error_count: int = 0
    warning_count: int = 0
    
    def mark_stage_start(self, stage: ProcessingStage):
        """Mark the start of a processing stage"""
        self.stage_times[stage] = time.time()
    
    def mark_stage_complete(self, stage: ProcessingStage) -> float:
        """Mark stage completion and return duration"""
        if stage in self.stage_times:
            duration = time.time() - self.stage_times[stage]
            self.stage_times[stage] = duration
            return duration
        return 0.0
    
    def mark_completion(self):
        """Mark overall processing completion"""
        self.total_processing_time = time.time() - self.start_time


@dataclass
class ProcessingResult:
    """Comprehensive processing result with metadata"""
    video_id: str
    status: ProcessingStatus
    config: ProcessingConfig
    metrics: ProcessingMetrics
    
    # Core content
    video_metadata: Dict[str, Any] = field(default_factory=dict)
    transcript: str = ""
    expert_analysis: str = ""
    
    # Quality indicators
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    validation_results: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    # Structured output
    structured_data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for API responses"""
        return {
            "video_id": self.video_id,
            "status": self.status.value,
            "processing_metrics": {
                "total_time": self.metrics.total_processing_time,
                "stage_times": {
                    stage.value: duration 
                    for stage, duration in self.metrics.stage_times.items()
                },
                "transcript_length": self.metrics.transcript_length,
                "analysis_length": self.metrics.analysis_length,
                "error_count": self.metrics.error_count,
                "warning_count": self.metrics.warning_count
            },
            "content": {
                "video_metadata": self.video_metadata,
                "transcript": self.transcript if self.config.output_format != "summary_only" else "",
                "expert_analysis": self.expert_analysis
            },
            "quality_indicators": {
                "confidence_scores": self.confidence_scores,
                "validation_results": self.validation_results,
                "warnings": self.warnings,
                "errors": self.errors
            },
            "structured_data": self.structured_data
        }


class VideoProcessor:
    """
    Industrial-grade video processing pipeline with expert consultation
    Optimized for high-throughput, reliable video analysis
    """
    
    def __init__(self, config: Optional[ProcessingConfig] = None):
        self.config = config or ProcessingConfig()
        self.youtube_service = YouTubeService()
        self.search_agent = SearchAgent()
        self.expert_engine = get_expert_engine()
        
        # Processing infrastructure
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_concurrent_downloads)
        
        # Register message queue handlers
        self._register_queue_handlers()
        
        # Processing statistics
        self.total_processed = 0
        self.total_errors = 0
        self.average_processing_time = 0.0
    
    def _register_queue_handlers(self):
        """Register async handlers for different task types"""
        queue = get_message_queue()
        
        # Register handlers for each task type
        queue.register_handler(TaskType.SINGLE_VIDEO, self._handle_single_video_task)
        queue.register_handler(TaskType.PLAYLIST_ANALYSIS, self._handle_playlist_task)
        queue.register_handler(TaskType.BATCH_SEARCH, self._handle_batch_search_task)
        queue.register_handler(TaskType.EXPERT_CONSULTATION, self._handle_expert_consultation_task)
        queue.register_handler(TaskType.CUSTOM_PIPELINE, self._handle_custom_pipeline_task)
    
    async def process_single_video(self, 
                                 video_id: str,
                                 config: Optional[ProcessingConfig] = None) -> ProcessingResult:
        """
        Process individual video with comprehensive analysis
        High-level API for single video processing
        """
        processing_config = config or self.config
        result = ProcessingResult(
            video_id=video_id,
            status=ProcessingStatus.INITIALIZING,
            config=processing_config,
            metrics=ProcessingMetrics()
        )
        
        try:
            # Stage 1: Initialization
            result.metrics.mark_stage_start(ProcessingStage.INITIALIZATION)
            await self._initialize_processing(result)
            result.metrics.mark_stage_complete(ProcessingStage.INITIALIZATION)
            
            # Stage 2: Metadata Extraction
            result.metrics.mark_stage_start(ProcessingStage.METADATA_EXTRACTION)
            await self._extract_metadata(result)
            result.metrics.mark_stage_complete(ProcessingStage.METADATA_EXTRACTION)
            
            # Stage 3: Transcript Generation
            if processing_config.enable_transcript:
                result.metrics.mark_stage_start(ProcessingStage.TRANSCRIPT_GENERATION)
                await self._generate_transcript(result)
                result.metrics.mark_stage_complete(ProcessingStage.TRANSCRIPT_GENERATION)
            
            # Stage 4: Content Preprocessing
            result.metrics.mark_stage_start(ProcessingStage.CONTENT_PREPROCESSING)
            await self._preprocess_content(result)
            result.metrics.mark_stage_complete(ProcessingStage.CONTENT_PREPROCESSING)
            
            # Stage 5: Expert Analysis
            if processing_config.expert_domain or processing_config.expert_pipeline_id:
                result.metrics.mark_stage_start(ProcessingStage.EXPERT_ANALYSIS)
                await self._perform_expert_analysis(result)
                result.metrics.mark_stage_complete(ProcessingStage.EXPERT_ANALYSIS)
            
            # Stage 6: Quality Validation
            result.metrics.mark_stage_start(ProcessingStage.QUALITY_VALIDATION)
            await self._validate_quality(result)
            result.metrics.mark_stage_complete(ProcessingStage.QUALITY_VALIDATION)
            
            # Stage 7: Output Formatting
            result.metrics.mark_stage_start(ProcessingStage.OUTPUT_FORMATTING)
            await self._format_output(result)
            result.metrics.mark_stage_complete(ProcessingStage.OUTPUT_FORMATTING)
            
            # Mark completion
            result.status = ProcessingStatus.COMPLETED
            result.metrics.mark_completion()
            
            # Update statistics
            self._update_processing_stats(result)
            
        except Exception as e:
            result.status = ProcessingStatus.FAILED
            result.errors.append(f"Processing failed: {str(e)}")
            result.metrics.error_count += 1
            logging.error(f"Video processing failed for {video_id}: {e}")
            
        return result
    
    async def process_playlist(self, 
                             playlist_id: str,
                             config: Optional[ProcessingConfig] = None,
                             max_videos: int = 50) -> Dict[str, ProcessingResult]:
        """
        Process entire playlist with parallel processing
        Optimized for high-throughput batch analysis
        """
        processing_config = config or self.config
        
        try:
            # Get playlist videos
            video_ids = await self._get_playlist_videos(playlist_id, max_videos)
            
            # Create processing tasks
            tasks = []
            for video_id in video_ids:
                task = asyncio.create_task(
                    self.process_single_video(video_id, processing_config)
                )
                tasks.append((video_id, task))
            
            # Process with controlled concurrency
            results = {}
            semaphore = asyncio.Semaphore(processing_config.max_concurrent_downloads)
            
            async def process_with_semaphore(video_id: str, task):
                async with semaphore:
                    return await task
            
            # Wait for all tasks to complete
            completed_tasks = await asyncio.gather(
                *[process_with_semaphore(vid, task) for vid, task in tasks],
                return_exceptions=True
            )
            
            # Collect results
            for i, (video_id, _) in enumerate(tasks):
                result = completed_tasks[i]
                if isinstance(result, Exception):
                    # Create error result
                    error_result = ProcessingResult(
                        video_id=video_id,
                        status=ProcessingStatus.FAILED,
                        config=processing_config,
                        metrics=ProcessingMetrics()
                    )
                    error_result.errors.append(f"Processing exception: {str(result)}")
                    results[video_id] = error_result
                else:
                    results[video_id] = result
            
            return results
            
        except Exception as e:
            logging.error(f"Playlist processing failed for {playlist_id}: {e}")
            raise
    
    async def _handle_single_video_task(self, task: Task) -> Dict[str, Any]:
        """Message queue handler for single video processing"""
        video_id = task.payload.get("video_id")
        if not video_id:
            raise ValueError("Missing video_id in task payload")
        
        # Extract configuration from task
        config_data = task.payload.get("config", {})
        config = ProcessingConfig(**config_data)
        
        # Set expert domain from task if specified
        if task.expert_domain:
            try:
                config.expert_domain = ExpertDomain(task.expert_domain)
            except ValueError:
                task.metadata["warning"] = f"Invalid expert domain: {task.expert_domain}"
        
        # Process video
        result = await self.process_single_video(video_id, config)
        
        return {
            "video_id": video_id,
            "status": result.status.value,
            "result": result.to_dict()
        }
    
    async def _handle_playlist_task(self, task: Task) -> Dict[str, Any]:
        """Message queue handler for playlist processing"""
        playlist_id = task.payload.get("playlist_id")
        max_videos = task.payload.get("max_videos", 50)
        
        if not playlist_id:
            raise ValueError("Missing playlist_id in task payload")
        
        # Extract configuration
        config_data = task.payload.get("config", {})
        config = ProcessingConfig(**config_data)
        
        # Process playlist
        results = await self.process_playlist(playlist_id, config, max_videos)
        
        return {
            "playlist_id": playlist_id,
            "total_videos": len(results),
            "successful": len([r for r in results.values() if r.status == ProcessingStatus.COMPLETED]),
            "failed": len([r for r in results.values() if r.status == ProcessingStatus.FAILED]),
            "results": {vid: result.to_dict() for vid, result in results.items()}
        }
    
    async def _handle_batch_search_task(self, task: Task) -> Dict[str, Any]:
        """Message queue handler for batch search processing"""
        search_query = task.payload.get("search_query")
        max_results = task.payload.get("max_results", 20)
        
        if not search_query:
            raise ValueError("Missing search_query in task payload")
        
        # Perform search
        search_results = await self._search_videos(search_query, max_results)
        
        # Extract configuration
        config_data = task.payload.get("config", {})
        config = ProcessingConfig(**config_data)
        
        # Process found videos
        video_ids = [result["video_id"] for result in search_results]
        processing_results = {}
        
        for video_id in video_ids:
            try:
                result = await self.process_single_video(video_id, config)
                processing_results[video_id] = result
            except Exception as e:
                logging.error(f"Failed to process search result {video_id}: {e}")
        
        return {
            "search_query": search_query,
            "found_videos": len(search_results),
            "processed_videos": len(processing_results),
            "search_results": search_results,
            "processing_results": {
                vid: result.to_dict() 
                for vid, result in processing_results.items()
            }
        }
    
    async def _handle_expert_consultation_task(self, task: Task) -> Dict[str, Any]:
        """Message queue handler for expert consultation"""
        video_id = task.payload.get("video_id")
        expert_pipeline_id = task.payload.get("expert_pipeline_id")
        
        if not video_id or not expert_pipeline_id:
            raise ValueError("Missing video_id or expert_pipeline_id in task payload")
        
        # Configure for expert analysis
        config = ProcessingConfig(
            expert_pipeline_id=expert_pipeline_id,
            output_format="expert_report",
            include_confidence_scores=True
        )
        
        # Process with expert consultation
        result = await self.process_single_video(video_id, config)
        
        return {
            "video_id": video_id,
            "expert_pipeline_id": expert_pipeline_id,
            "consultation_result": result.to_dict()
        }
    
    async def _handle_custom_pipeline_task(self, task: Task) -> Dict[str, Any]:
        """Message queue handler for custom processing pipelines"""
        video_id = task.payload.get("video_id")
        pipeline_steps = task.custom_pipeline or []
        
        if not video_id:
            raise ValueError("Missing video_id in task payload")
        
        # Execute custom pipeline
        result = await self._execute_custom_pipeline(video_id, pipeline_steps, task.payload)
        
        return {
            "video_id": video_id,
            "pipeline_steps": pipeline_steps,
            "custom_result": result
        }
    
    async def _initialize_processing(self, result: ProcessingResult):
        """Initialize processing session"""
        result.status = ProcessingStatus.INITIALIZING
        
        # Validate video ID format
        if not result.video_id or len(result.video_id) != 11:
            raise ValueError(f"Invalid YouTube video ID: {result.video_id}")
        
        # Initialize structured data
        result.structured_data = {
            "video_id": result.video_id,
            "processing_config": {
                "expert_domain": result.config.expert_domain.value if result.config.expert_domain else None,
                "output_format": result.config.output_format,
                "quality_settings": {
                    "transcript_quality": result.config.transcript_quality,
                    "max_transcript_length": result.config.max_transcript_length
                }
            },
            "timestamps": {
                "processing_started": time.time()
            }
        }
    
    async def _extract_metadata(self, result: ProcessingResult):
        """Extract comprehensive video metadata"""
        try:
            # Get video metadata using YouTube service
            metadata = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.youtube_service.get_video_info,
                result.video_id
            )
            
            result.video_metadata = metadata
            result.metrics.metadata_size = len(str(metadata))
            
            # Add to structured data
            result.structured_data["video_metadata"] = {
                "title": metadata.get("title", ""),
                "duration": metadata.get("duration", 0),
                "view_count": metadata.get("view_count", 0),
                "channel": metadata.get("channel", ""),
                "upload_date": metadata.get("upload_date", ""),
                "description_length": len(metadata.get("description", ""))
            }
            
        except Exception as e:
            result.warnings.append(f"Metadata extraction failed: {str(e)}")
            result.metrics.warning_count += 1
    
    async def _generate_transcript(self, result: ProcessingResult):
        """Generate high-quality transcript"""
        try:
            # Generate transcript using transcript agent
            transcript_data = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                run_transcript,
                result.video_id
            )
            
            if isinstance(transcript_data, dict):
                result.transcript = transcript_data.get("transcript", "")
                result.confidence_scores["transcript"] = transcript_data.get("confidence", 0.8)
            else:
                result.transcript = str(transcript_data)
                result.confidence_scores["transcript"] = 0.7
            
            # Validate transcript length
            if len(result.transcript) > result.config.max_transcript_length:
                result.transcript = result.transcript[:result.config.max_transcript_length]
                result.warnings.append("Transcript truncated to maximum length")
                result.metrics.warning_count += 1
            
            result.metrics.transcript_length = len(result.transcript)
            
            # Add to structured data
            result.structured_data["transcript_metadata"] = {
                "length": len(result.transcript),
                "confidence": result.confidence_scores.get("transcript", 0.0),
                "quality": result.config.transcript_quality
            }
            
        except Exception as e:
            result.errors.append(f"Transcript generation failed: {str(e)}")
            result.metrics.error_count += 1
    
    async def _preprocess_content(self, result: ProcessingResult):
        """Preprocess content for analysis"""
        # Apply preprocessing hooks
        for hook in result.config.preprocessing_hooks:
            try:
                await hook(result)
            except Exception as e:
                result.warnings.append(f"Preprocessing hook failed: {str(e)}")
                result.metrics.warning_count += 1
        
        # Content validation
        if len(result.transcript) < result.config.min_content_length:
            result.warnings.append("Content may be too short for meaningful analysis")
            result.metrics.warning_count += 1
    
    async def _perform_expert_analysis(self, result: ProcessingResult):
        """Perform expert domain analysis"""
        try:
            result.status = ProcessingStatus.ANALYZING
            
            # Determine analysis pipeline
            if result.config.expert_pipeline_id:
                pipeline = self.expert_engine.get_expert_pipeline(result.config.expert_pipeline_id)
                if not pipeline:
                    raise ValueError(f"Expert pipeline not found: {result.config.expert_pipeline_id}")
            elif result.config.expert_domain:
                # Create default pipeline for domain
                pipeline_id = self.expert_engine.create_expert_consultation(
                    domain=result.config.expert_domain,
                    analysis_depth=AnalysisDepth.EXPERT
                )
                pipeline = self.expert_engine.get_expert_pipeline(pipeline_id)
            else:
                return  # No expert analysis requested
            
            # Validate content for domain
            if pipeline:
                validation_errors = self.expert_engine.validate_content_for_domain(
                    pipeline.domain, result.transcript
                )
                if validation_errors:
                    result.warnings.extend(validation_errors)
                    result.metrics.warning_count += len(validation_errors)
            
            # Generate expert analysis prompt
            video_title = result.video_metadata.get("title", "Unknown Title")
            analysis_prompt = self.expert_engine.generate_analysis_prompt(
                pipeline.pipeline_id, video_title, result.transcript
            )
            
            # Add custom prompt injection if specified
            if result.config.custom_prompt_injection:
                analysis_prompt += f"\n\nADDITIONAL REQUIREMENTS:\n{result.config.custom_prompt_injection}"
            
            # Perform expert analysis using GPT
            expert_analysis = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                gpt_summarizer_agent,
                analysis_prompt,
                result.transcript
            )
            
            result.expert_analysis = expert_analysis
            result.metrics.analysis_length = len(expert_analysis)
            result.confidence_scores["expert_analysis"] = 0.85
            
            # Add to structured data
            result.structured_data["expert_analysis"] = {
                "domain": pipeline.domain.value,
                "pipeline_id": pipeline.pipeline_id,
                "analysis_length": len(expert_analysis),
                "confidence": result.confidence_scores.get("expert_analysis", 0.0)
            }
            
        except Exception as e:
            result.errors.append(f"Expert analysis failed: {str(e)}")
            result.metrics.error_count += 1
    
    async def _validate_quality(self, result: ProcessingResult):
        """Validate processing quality"""
        result.status = ProcessingStatus.VALIDATING
        
        # Apply validation rules
        for rule in result.config.validation_rules:
            try:
                validation_result = await rule(result)
                result.validation_results.append(validation_result)
            except Exception as e:
                result.warnings.append(f"Validation rule failed: {str(e)}")
                result.metrics.warning_count += 1
        
        # Basic quality checks
        quality_score = 1.0
        
        # Check transcript quality
        if result.metrics.transcript_length == 0:
            quality_score -= 0.3
            result.warnings.append("No transcript available")
        elif result.metrics.transcript_length < 500:
            quality_score -= 0.1
            result.warnings.append("Transcript is very short")
        
        # Check analysis quality
        if result.expert_analysis and result.metrics.analysis_length > 1000:
            quality_score += 0.1
        
        result.confidence_scores["overall_quality"] = max(0.0, min(1.0, quality_score))
    
    async def _format_output(self, result: ProcessingResult):
        """Format final output according to configuration"""
        # Apply postprocessing hooks
        for hook in result.config.postprocessing_hooks:
            try:
                await hook(result)
            except Exception as e:
                result.warnings.append(f"Postprocessing hook failed: {str(e)}")
                result.metrics.warning_count += 1
        
        # Format based on output format
        if result.config.output_format == "expert_report":
            result.structured_data["formatted_output"] = {
                "executive_summary": result.expert_analysis[:500] + "..." if len(result.expert_analysis) > 500 else result.expert_analysis,
                "full_analysis": result.expert_analysis,
                "confidence_indicators": result.confidence_scores,
                "quality_metrics": {
                    "processing_time": result.metrics.total_processing_time,
                    "content_length": result.metrics.transcript_length,
                    "analysis_depth": len(result.expert_analysis)
                }
            }
    
    def _update_processing_stats(self, result: ProcessingResult):
        """Update processing statistics"""
        self.total_processed += 1
        
        if result.status == ProcessingStatus.FAILED:
            self.total_errors += 1
        
        # Update average processing time
        if result.metrics.total_processing_time:
            if self.total_processed == 1:
                self.average_processing_time = result.metrics.total_processing_time
            else:
                alpha = 0.1
                self.average_processing_time = (
                    alpha * result.metrics.total_processing_time +
                    (1 - alpha) * self.average_processing_time
                )
    
    async def _get_playlist_videos(self, playlist_id: str, max_videos: int) -> List[str]:
        """Get video IDs from playlist"""
        # Implementation would use YouTube API to get playlist videos
        # For now, return empty list as placeholder
        return []
    
    async def _search_videos(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Search for videos using query"""
        # Implementation would use search agent
        # For now, return empty list as placeholder
        return []
    
    async def _execute_custom_pipeline(self, video_id: str, pipeline_steps: List[str], payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute custom processing pipeline"""
        # Implementation for custom pipeline execution
        # For now, return basic result
        return {
            "video_id": video_id,
            "custom_steps": pipeline_steps,
            "status": "completed"
        }
    
    async def get_processing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics"""
        return {
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "success_rate": (
                (self.total_processed - self.total_errors) / self.total_processed
                if self.total_processed > 0 else 0.0
            ),
            "average_processing_time": self.average_processing_time,
            "configuration": {
                "max_concurrent_downloads": self.config.max_concurrent_downloads,
                "request_timeout": self.config.request_timeout,
                "retry_attempts": self.config.retry_attempts
            }
        }


# Global video processor instance
_video_processor: Optional[VideoProcessor] = None

def get_video_processor(config: Optional[ProcessingConfig] = None) -> VideoProcessor:
    """Get or create global video processor instance"""
    global _video_processor
    if _video_processor is None:
        _video_processor = VideoProcessor(config)
    return _video_processor
