#!/usr/bin/env python3
"""
Performance Monitor
Lean best practices for system monitoring, metrics collection, and optimization
"""

import time
import psutil
import threading
import json
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from collections import deque
import logging

@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    operation: str
    duration_ms: float
    memory_mb: float
    cpu_percent: float
    timestamp: str
    success: bool
    metadata: Dict[str, Any]

@dataclass
class SystemSnapshot:
    """System resource snapshot"""
    timestamp: str
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_usage_percent: float
    active_threads: int

class MetricsCollector:
    """Efficient metrics collection with rolling window"""
    
    def __init__(self, max_metrics: int = 1000):
        self.metrics: deque = deque(maxlen=max_metrics)
        self.system_snapshots: deque = deque(maxlen=100)  # Last 100 snapshots
        self.metrics_lock = threading.Lock()
        self.logger = logging.getLogger("performance.collector")
        
        # Start background monitoring
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_system, daemon=True)
        self.monitor_thread.start()
    
    def record_operation(self, operation: str, duration_ms: float, success: bool = True, 
                        metadata: Dict[str, Any] = None) -> PerformanceMetric:
        """Record operation performance"""
        
        # Get current system stats
        memory_info = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent()
        
        metric = PerformanceMetric(
            operation=operation,
            duration_ms=duration_ms,
            memory_mb=memory_info.used / (1024 * 1024),
            cpu_percent=cpu_percent,
            timestamp=datetime.now().isoformat(),
            success=success,
            metadata=metadata or {}
        )
        
        with self.metrics_lock:
            self.metrics.append(metric)
        
        # Log slow operations
        if duration_ms > 1000:  # > 1 second
            self.logger.warning(f"Slow operation detected: {operation} took {duration_ms:.2f}ms")
        
        return metric
    
    def _monitor_system(self):
        """Background system monitoring"""
        while self.monitoring:
            try:
                snapshot = SystemSnapshot(
                    timestamp=datetime.now().isoformat(),
                    cpu_percent=psutil.cpu_percent(interval=1),
                    memory_percent=psutil.virtual_memory().percent,
                    memory_available_mb=psutil.virtual_memory().available / (1024 * 1024),
                    disk_usage_percent=psutil.disk_usage('/').percent,
                    active_threads=threading.active_count()
                )
                
                with self.metrics_lock:
                    self.system_snapshots.append(snapshot)
                
                time.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                self.logger.error(f"System monitoring error: {e}")
                time.sleep(30)  # Wait longer on error
    
    def get_operation_stats(self, operation: str = None) -> Dict[str, Any]:
        """Get statistics for specific operation or all operations"""
        
        with self.metrics_lock:
            if operation:
                relevant_metrics = [m for m in self.metrics if m.operation == operation]
            else:
                relevant_metrics = list(self.metrics)
        
        if not relevant_metrics:
            return {"message": "No metrics available"}
        
        # Calculate statistics
        durations = [m.duration_ms for m in relevant_metrics if m.success]
        success_count = len([m for m in relevant_metrics if m.success])
        total_count = len(relevant_metrics)
        
        if durations:
            avg_duration = sum(durations) / len(durations)
            min_duration = min(durations)
            max_duration = max(durations)
            p95_duration = sorted(durations)[int(len(durations) * 0.95)] if len(durations) > 20 else max_duration
        else:
            avg_duration = min_duration = max_duration = p95_duration = 0
        
        return {
            "operation": operation or "ALL",
            "total_operations": total_count,
            "successful_operations": success_count,
            "success_rate": (success_count / total_count) * 100 if total_count > 0 else 0,
            "duration_stats": {
                "average_ms": round(avg_duration, 2),
                "min_ms": round(min_duration, 2),
                "max_ms": round(max_duration, 2),
                "p95_ms": round(p95_duration, 2)
            }
        }
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get current system health summary"""
        
        with self.metrics_lock:
            if not self.system_snapshots:
                return {"message": "No system data available"}
            
            latest = self.system_snapshots[-1]
            
            # Calculate trends if we have enough data
            if len(self.system_snapshots) >= 5:
                recent_snapshots = list(self.system_snapshots)[-5:]
                cpu_trend = recent_snapshots[-1].cpu_percent - recent_snapshots[0].cpu_percent
                memory_trend = recent_snapshots[-1].memory_percent - recent_snapshots[0].memory_percent
            else:
                cpu_trend = memory_trend = 0
        
        # Determine health status
        health_status = "healthy"
        warnings = []
        
        if latest.cpu_percent > 80:
            health_status = "warning"
            warnings.append("High CPU usage")
        
        if latest.memory_percent > 85:
            health_status = "critical" if health_status != "critical" else health_status
            warnings.append("High memory usage")
        
        if latest.disk_usage_percent > 90:
            health_status = "critical"
            warnings.append("Low disk space")
        
        return {
            "status": health_status,
            "warnings": warnings,
            "current": asdict(latest),
            "trends": {
                "cpu_change": round(cpu_trend, 2),
                "memory_change": round(memory_trend, 2)
            }
        }
    
    def get_top_operations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top operations by various metrics"""
        
        with self.metrics_lock:
            operations_data = {}
            
            for metric in self.metrics:
                op = metric.operation
                if op not in operations_data:
                    operations_data[op] = {
                        "operation": op,
                        "count": 0,
                        "total_duration": 0,
                        "max_duration": 0,
                        "avg_memory": 0,
                        "success_rate": 0
                    }
                
                operations_data[op]["count"] += 1
                operations_data[op]["total_duration"] += metric.duration_ms
                operations_data[op]["max_duration"] = max(operations_data[op]["max_duration"], metric.duration_ms)
                operations_data[op]["avg_memory"] += metric.memory_mb
        
        # Calculate averages and success rates
        for op_data in operations_data.values():
            count = op_data["count"]
            op_data["avg_duration"] = round(op_data["total_duration"] / count, 2)
            op_data["avg_memory"] = round(op_data["avg_memory"] / count, 2)
            
            # Calculate success rate
            successful = len([m for m in self.metrics if m.operation == op_data["operation"] and m.success])
            op_data["success_rate"] = round((successful / count) * 100, 2)
        
        # Sort by total duration and return top operations
        sorted_operations = sorted(
            operations_data.values(),
            key=lambda x: x["total_duration"],
            reverse=True
        )
        
        return sorted_operations[:limit]
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

class PerformanceTimer:
    """Context manager for timing operations"""
    
    def __init__(self, collector: MetricsCollector, operation: str, metadata: Dict[str, Any] = None):
        self.collector = collector
        self.operation = operation
        self.metadata = metadata or {}
        self.start_time = None
        self.success = True
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        
        if exc_type is not None:
            self.success = False
            self.metadata["error"] = str(exc_val)
        
        self.collector.record_operation(
            self.operation,
            duration_ms,
            self.success,
            self.metadata
        )

def monitor_performance(collector: MetricsCollector, operation: str, metadata: Dict[str, Any] = None):
    """Decorator for automatic performance monitoring"""
    
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            with PerformanceTimer(collector, operation, metadata):
                return func(*args, **kwargs)
        return wrapper
    return decorator

class PerformanceOptimizer:
    """Performance optimization recommendations"""
    
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self.logger = logging.getLogger("performance.optimizer")
    
    def analyze_performance(self) -> Dict[str, Any]:
        """Analyze performance and provide recommendations"""
        
        system_health = self.collector.get_system_health()
        top_operations = self.collector.get_top_operations()
        
        recommendations = []
        
        # System resource recommendations
        if system_health.get("status") != "healthy":
            for warning in system_health.get("warnings", []):
                if "CPU" in warning:
                    recommendations.append("Consider reducing concurrent operations or optimizing CPU-intensive tasks")
                elif "memory" in warning:
                    recommendations.append("Monitor memory usage and consider implementing data streaming for large operations")
                elif "disk" in warning:
                    recommendations.append("Clean up temporary files or consider expanding disk space")
        
        # Operation-specific recommendations
        for op in top_operations[:3]:  # Top 3 operations
            if op["avg_duration"] > 1000:  # > 1 second
                recommendations.append(f"Optimize '{op['operation']}' operation - average duration is {op['avg_duration']}ms")
            
            if op["success_rate"] < 95:  # < 95% success rate
                recommendations.append(f"Improve reliability of '{op['operation']}' operation - success rate is {op['success_rate']}%")
        
        return {
            "system_health": system_health,
            "top_operations": top_operations,
            "recommendations": recommendations,
            "analysis_timestamp": datetime.now().isoformat()
        }

def create_performance_monitor() -> tuple[MetricsCollector, PerformanceOptimizer]:
    """Factory function to create performance monitoring components"""
    
    collector = MetricsCollector()
    optimizer = PerformanceOptimizer(collector)
    
    return collector, optimizer