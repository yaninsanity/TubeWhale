#!/usr/bin/env python3
"""
Enterprise Message Queue System
Industrial-grade async message processing for scalable video analysis
"""

import asyncio
import uuid
import json
from typing import Dict, List, Optional, Any, Callable, Union
from enum import Enum
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor


class TaskPriority(Enum):
    """Task execution priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class TaskStatus(Enum):
    """Task lifecycle status"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskType(Enum):
    """Video analysis task types"""
    SINGLE_VIDEO = "single_video"
    PLAYLIST_ANALYSIS = "playlist_analysis"
    BATCH_SEARCH = "batch_search"
    EXPERT_CONSULTATION = "expert_consultation"
    CUSTOM_PIPELINE = "custom_pipeline"


@dataclass
class TaskMetrics:
    """Task performance metrics"""
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    processing_time: Optional[float] = None
    retry_count: int = 0
    error_count: int = 0
    
    def mark_started(self):
        self.started_at = datetime.now()
    
    def mark_completed(self):
        self.completed_at = datetime.now()
        if self.started_at:
            self.processing_time = (self.completed_at - self.started_at).total_seconds()


@dataclass
class Task:
    """Enterprise task definition with comprehensive metadata"""
    task_id: str
    task_type: TaskType
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    expert_domain: Optional[str] = None
    custom_pipeline: Optional[List[str]] = None
    retry_policy: Optional[Dict[str, Any]] = None
    timeout_seconds: int = 3600
    created_by: str = "system"
    metrics: TaskMetrics = field(default_factory=TaskMetrics)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize task for persistence"""
        data = asdict(self)
        # Convert enums to strings
        data['task_type'] = self.task_type.value
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """Deserialize task from persistence"""
        # Convert string enums back
        data['task_type'] = TaskType(data['task_type'])
        data['priority'] = TaskPriority(data['priority'])
        data['status'] = TaskStatus(data['status'])
        
        # Handle metrics
        if 'metrics' in data:
            metrics_data = data['metrics']
            # Convert datetime strings back to datetime objects
            for key in ['created_at', 'started_at', 'completed_at']:
                if key in metrics_data and metrics_data[key]:
                    metrics_data[key] = datetime.fromisoformat(metrics_data[key])
            data['metrics'] = TaskMetrics(**metrics_data)
        
        return cls(**data)


class MessageQueue:
    """
    Industrial-grade async message queue with priority scheduling
    Designed for high-throughput video analysis workloads
    """
    
    def __init__(self, 
                 max_concurrent_tasks: int = 10,
                 max_queue_size: int = 1000,
                 enable_persistence: bool = True,
                 persistence_path: str = "queue_state.json"):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_queue_size = max_queue_size
        self.enable_persistence = enable_persistence
        self.persistence_path = Path(persistence_path)
        
        # Core queue structures
        self.pending_tasks: Dict[TaskPriority, List[Task]] = {
            priority: [] for priority in TaskPriority
        }
        self.active_tasks: Dict[str, Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self.failed_tasks: Dict[str, Task] = {}
        
        # Processing infrastructure
        self.task_handlers: Dict[TaskType, Callable] = {}
        self.is_processing = False
        self.worker_pool = ThreadPoolExecutor(max_workers=max_concurrent_tasks)
        self.processing_task: Optional[asyncio.Task] = None
        
        # Monitoring and metrics
        self.total_processed = 0
        self.total_failed = 0
        self.average_processing_time = 0.0
        
        # Event hooks for monitoring
        self.on_task_started: Optional[Callable[[Task], None]] = None
        self.on_task_completed: Optional[Callable[[Task], None]] = None
        self.on_task_failed: Optional[Callable[[Task, Exception], None]] = None
        
        # Load persisted state
        if self.enable_persistence:
            self._load_persisted_state()
    
    def register_handler(self, task_type: TaskType, handler: Callable) -> None:
        """Register async handler for specific task type"""
        self.task_handlers[task_type] = handler
    
    async def enqueue_task(self, task: Task) -> bool:
        """
        Add task to priority queue with validation
        Returns True if successfully enqueued
        """
        if self._get_total_queue_size() >= self.max_queue_size:
            raise RuntimeError(f"Queue capacity exceeded: {self.max_queue_size}")
        
        if task.task_type not in self.task_handlers:
            raise ValueError(f"No handler registered for task type: {task.task_type}")
        
        # Add to appropriate priority queue
        self.pending_tasks[task.priority].append(task)
        task.status = TaskStatus.QUEUED
        
        # Persist state
        if self.enable_persistence:
            await self._persist_state()
        
        # Start processing if not already running
        if not self.is_processing:
            await self.start_processing()
        
        return True
    
    async def start_processing(self) -> None:
        """Start the main processing loop"""
        if self.is_processing:
            return
        
        self.is_processing = True
        self.processing_task = asyncio.create_task(self._processing_loop())
    
    async def stop_processing(self, wait_for_completion: bool = True) -> None:
        """Stop processing with optional graceful shutdown"""
        self.is_processing = False
        
        if wait_for_completion and self.processing_task:
            await self.processing_task
        elif self.processing_task:
            self.processing_task.cancel()
    
    async def _processing_loop(self) -> None:
        """Main async processing loop with priority scheduling"""
        while self.is_processing:
            try:
                # Check if we can process more tasks
                if len(self.active_tasks) >= self.max_concurrent_tasks:
                    await asyncio.sleep(0.1)
                    continue
                
                # Get next highest priority task
                next_task = self._get_next_task()
                if not next_task:
                    await asyncio.sleep(0.5)
                    continue
                
                # Start task processing
                await self._process_task(next_task)
                
            except Exception as e:
                logging.error(f"Processing loop error: {e}")
                await asyncio.sleep(1.0)
    
    def _get_next_task(self) -> Optional[Task]:
        """Get next task using priority scheduling"""
        # Check priorities from highest to lowest
        for priority in reversed(list(TaskPriority)):
            if self.pending_tasks[priority]:
                return self.pending_tasks[priority].pop(0)
        return None
    
    async def _process_task(self, task: Task) -> None:
        """Process individual task with error handling and metrics"""
        task.status = TaskStatus.PROCESSING
        task.metrics.mark_started()
        self.active_tasks[task.task_id] = task
        
        # Notify start
        if self.on_task_started:
            self.on_task_started(task)
        
        try:
            # Get handler and execute
            handler = self.task_handlers[task.task_type]
            
            # Create timeout context
            timeout = task.timeout_seconds
            result = await asyncio.wait_for(handler(task), timeout=timeout)
            
            # Mark success
            task.status = TaskStatus.COMPLETED
            task.metadata['result'] = result
            task.metrics.mark_completed()
            
            # Move to completed
            self.completed_tasks[task.task_id] = task
            self.total_processed += 1
            
            # Update average processing time
            if task.metrics.processing_time:
                self._update_average_processing_time(task.metrics.processing_time)
            
            # Notify completion
            if self.on_task_completed:
                self.on_task_completed(task)
                
        except asyncio.TimeoutError:
            await self._handle_task_failure(task, TimeoutError(f"Task timeout after {task.timeout_seconds}s"))
        except Exception as e:
            await self._handle_task_failure(task, e)
        finally:
            # Remove from active
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
            
            # Persist state
            if self.enable_persistence:
                await self._persist_state()
    
    async def _handle_task_failure(self, task: Task, error: Exception) -> None:
        """Handle task failure with retry logic"""
        task.metrics.error_count += 1
        
        # Check retry policy
        should_retry = False
        if task.retry_policy:
            max_retries = task.retry_policy.get('max_retries', 0)
            if task.metrics.retry_count < max_retries:
                should_retry = True
                task.metrics.retry_count += 1
                task.status = TaskStatus.RETRYING
                
                # Re-queue with delay
                retry_delay = task.retry_policy.get('retry_delay', 5.0)
                await asyncio.sleep(retry_delay)
                self.pending_tasks[task.priority].append(task)
        
        if not should_retry:
            task.status = TaskStatus.FAILED
            task.metadata['error'] = str(error)
            task.metadata['error_type'] = type(error).__name__
            self.failed_tasks[task.task_id] = task
            self.total_failed += 1
        
        # Notify failure
        if self.on_task_failed:
            self.on_task_failed(task, error)
    
    def _update_average_processing_time(self, processing_time: float) -> None:
        """Update rolling average processing time"""
        if self.total_processed == 1:
            self.average_processing_time = processing_time
        else:
            # Simple moving average
            alpha = 0.1  # Weight for new value
            self.average_processing_time = (
                alpha * processing_time + 
                (1 - alpha) * self.average_processing_time
            )
    
    def _get_total_queue_size(self) -> int:
        """Get total number of pending tasks"""
        return sum(len(tasks) for tasks in self.pending_tasks.values())
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of specific task"""
        # Check active tasks
        if task_id in self.active_tasks:
            return self.active_tasks[task_id].to_dict()
        
        # Check completed tasks
        if task_id in self.completed_tasks:
            return self.completed_tasks[task_id].to_dict()
        
        # Check failed tasks
        if task_id in self.failed_tasks:
            return self.failed_tasks[task_id].to_dict()
        
        # Check pending tasks
        for priority_tasks in self.pending_tasks.values():
            for task in priority_tasks:
                if task.task_id == task_id:
                    return task.to_dict()
        
        return None
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel pending or active task"""
        # Remove from pending queues
        for priority_tasks in self.pending_tasks.values():
            for i, task in enumerate(priority_tasks):
                if task.task_id == task_id:
                    task.status = TaskStatus.CANCELLED
                    priority_tasks.pop(i)
                    return True
        
        # Mark active task as cancelled (will be handled by processing loop)
        if task_id in self.active_tasks:
            self.active_tasks[task_id].status = TaskStatus.CANCELLED
            return True
        
        return False
    
    async def get_queue_metrics(self) -> Dict[str, Any]:
        """Get comprehensive queue performance metrics"""
        return {
            "queue_status": {
                "is_processing": self.is_processing,
                "pending_tasks": self._get_total_queue_size(),
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.completed_tasks),
                "failed_tasks": len(self.failed_tasks)
            },
            "performance_metrics": {
                "total_processed": self.total_processed,
                "total_failed": self.total_failed,
                "success_rate": (
                    self.total_processed / (self.total_processed + self.total_failed)
                    if (self.total_processed + self.total_failed) > 0 else 0.0
                ),
                "average_processing_time": self.average_processing_time
            },
            "capacity_metrics": {
                "max_concurrent_tasks": self.max_concurrent_tasks,
                "max_queue_size": self.max_queue_size,
                "queue_utilization": self._get_total_queue_size() / self.max_queue_size
            }
        }
    
    async def _persist_state(self) -> None:
        """Persist queue state to disk"""
        if not self.enable_persistence:
            return
        
        try:
            state = {
                "pending_tasks": {
                    priority.name: [task.to_dict() for task in tasks]
                    for priority, tasks in self.pending_tasks.items()
                },
                "active_tasks": {
                    task_id: task.to_dict() 
                    for task_id, task in self.active_tasks.items()
                },
                "metrics": {
                    "total_processed": self.total_processed,
                    "total_failed": self.total_failed,
                    "average_processing_time": self.average_processing_time
                }
            }
            
            with open(self.persistence_path, 'w') as f:
                json.dump(state, f, indent=2, default=str)
                
        except Exception as e:
            logging.error(f"Failed to persist queue state: {e}")
    
    def _load_persisted_state(self) -> None:
        """Load persisted queue state from disk"""
        if not self.persistence_path.exists():
            return
        
        try:
            with open(self.persistence_path, 'r') as f:
                state = json.load(f)
            
            # Restore pending tasks
            for priority_name, tasks_data in state.get("pending_tasks", {}).items():
                priority = TaskPriority[priority_name]
                self.pending_tasks[priority] = [
                    Task.from_dict(task_data) for task_data in tasks_data
                ]
            
            # Restore metrics
            metrics = state.get("metrics", {})
            self.total_processed = metrics.get("total_processed", 0)
            self.total_failed = metrics.get("total_failed", 0)
            self.average_processing_time = metrics.get("average_processing_time", 0.0)
            
        except Exception as e:
            logging.error(f"Failed to load persisted queue state: {e}")


# Global message queue instance
_message_queue: Optional[MessageQueue] = None

def get_message_queue(
    max_concurrent_tasks: int = 10,
    max_queue_size: int = 1000,
    enable_persistence: bool = True
) -> MessageQueue:
    """Get or create global message queue instance"""
    global _message_queue
    if _message_queue is None:
        _message_queue = MessageQueue(
            max_concurrent_tasks=max_concurrent_tasks,
            max_queue_size=max_queue_size,
            enable_persistence=enable_persistence
        )
    return _message_queue


async def create_task(
    task_type: TaskType,
    payload: Dict[str, Any],
    priority: TaskPriority = TaskPriority.NORMAL,
    expert_domain: Optional[str] = None,
    custom_pipeline: Optional[List[str]] = None,
    timeout_seconds: int = 3600,
    retry_policy: Optional[Dict[str, Any]] = None
) -> str:
    """
    Factory function to create and enqueue analysis task
    Returns task_id for tracking
    """
    task_id = str(uuid.uuid4())
    
    task = Task(
        task_id=task_id,
        task_type=task_type,
        payload=payload,
        priority=priority,
        expert_domain=expert_domain,
        custom_pipeline=custom_pipeline,
        timeout_seconds=timeout_seconds,
        retry_policy=retry_policy or {
            "max_retries": 2,
            "retry_delay": 5.0
        }
    )
    
    queue = get_message_queue()
    await queue.enqueue_task(task)
    
    return task_id
