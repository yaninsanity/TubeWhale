#!/usr/bin/env python3
"""
Database Management
Lean best practices for SQLite operations, connection pooling, and performance
"""

import sqlite3
import threading
import json
import time
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
from queue import Queue, Empty
import logging

@dataclass
class DatabaseConfig:
    """Database configuration with optimization settings"""
    database_path: str
    pool_size: int = 5
    timeout: float = 30.0
    enable_wal: bool = True
    enable_foreign_keys: bool = True
    cache_size: int = 2000  # 2MB cache
    busy_timeout: int = 30000  # 30 seconds
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"

@dataclass 
class QueryMetrics:
    """Query performance metrics"""
    query: str
    duration_ms: float
    rows_affected: int
    timestamp: str
    success: bool
    error_message: Optional[str] = None

class ConnectionPool:
    """Thread-safe SQLite connection pool"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool = Queue(maxsize=config.pool_size)
        self.active_connections = 0
        self.pool_lock = threading.Lock()
        self.logger = logging.getLogger("database.pool")
        
        # Initialize pool
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool with optimized connections"""
        for _ in range(self.config.pool_size):
            conn = self._create_connection()
            self.pool.put(conn)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create optimized SQLite connection"""
        conn = sqlite3.connect(
            self.config.database_path,
            timeout=self.config.timeout,
            check_same_thread=False
        )
        
        # Enable row factory for dict-like results
        conn.row_factory = sqlite3.Row
        
        # Optimize connection
        cursor = conn.cursor()
        
        if self.config.enable_wal:
            cursor.execute(f"PRAGMA journal_mode = {self.config.journal_mode}")
        
        if self.config.enable_foreign_keys:
            cursor.execute("PRAGMA foreign_keys = ON")
        
        cursor.execute(f"PRAGMA cache_size = -{self.config.cache_size}")
        cursor.execute(f"PRAGMA busy_timeout = {self.config.busy_timeout}")
        cursor.execute(f"PRAGMA synchronous = {self.config.synchronous}")
        cursor.execute("PRAGMA temp_store = memory")
        cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        
        return conn
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with automatic return"""
        conn = None
        try:
            # Get connection from pool
            conn = self.pool.get(timeout=self.config.timeout)
            yield conn
        except Empty:
            raise Exception("Database connection pool exhausted")
        finally:
            if conn:
                self.pool.put(conn)
    
    def close_all(self):
        """Close all connections in pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except Empty:
                break

class QueryCache:
    """Simple TTL-based query cache"""
    
    def __init__(self, default_ttl: int = 300):  # 5 minutes default
        self.cache: Dict[str, Tuple[Any, datetime]] = {}
        self.default_ttl = default_ttl
        self.cache_lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached result if not expired"""
        with self.cache_lock:
            if key in self.cache:
                result, timestamp = self.cache[key]
                if datetime.now() - timestamp < timedelta(seconds=self.default_ttl):
                    return result
                else:
                    del self.cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Cache result with TTL"""
        with self.cache_lock:
            self.cache[key] = (value, datetime.now())
    
    def clear(self):
        """Clear all cached results"""
        with self.cache_lock:
            self.cache.clear()

class DatabaseManager:
    """Industrial database manager with optimization and monitoring"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool = ConnectionPool(config)
        self.cache = QueryCache()
        self.metrics: List[QueryMetrics] = []
        self.metrics_lock = threading.Lock()
        self.logger = logging.getLogger("database.manager")
        
        # Performance tracking
        self.slow_query_threshold = 1000  # 1 second in ms
        
    def execute_query(self, query: str, params: tuple = (), cache_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Execute query with metrics and caching"""
        
        # Check cache first
        if cache_key:
            cached_result = self.cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        start_time = time.time()
        success = True
        error_message = None
        rows_affected = 0
        result = []
        
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                # Get results for SELECT queries
                if query.strip().upper().startswith('SELECT'):
                    result = [dict(row) for row in cursor.fetchall()]
                    rows_affected = len(result)
                else:
                    conn.commit()
                    rows_affected = cursor.rowcount
                
                # Cache SELECT results
                if cache_key and result:
                    self.cache.set(cache_key, result)
                
        except Exception as e:
            success = False
            error_message = str(e)
            self.logger.error(f"Query failed: {query[:100]}... Error: {e}")
            raise
        
        finally:
            # Record metrics
            duration_ms = (time.time() - start_time) * 1000
            
            metric = QueryMetrics(
                query=query[:200],  # Truncate long queries
                duration_ms=duration_ms,
                rows_affected=rows_affected,
                timestamp=datetime.now().isoformat(),
                success=success,
                error_message=error_message
            )
            
            with self.metrics_lock:
                self.metrics.append(metric)
                
                # Log slow queries
                if duration_ms > self.slow_query_threshold:
                    self.logger.warning(f"Slow query detected: {duration_ms:.2f}ms - {query[:100]}...")
        
        return result
    
    def execute_transaction(self, queries: List[Tuple[str, tuple]]) -> bool:
        """Execute multiple queries in a transaction"""
        
        start_time = time.time()
        success = True
        
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Begin transaction
                cursor.execute("BEGIN TRANSACTION")
                
                for query, params in queries:
                    cursor.execute(query, params)
                
                # Commit transaction
                conn.commit()
                
        except Exception as e:
            success = False
            try:
                conn.rollback()
            except:
                pass
            self.logger.error(f"Transaction failed: {e}")
            raise
        
        finally:
            duration_ms = (time.time() - start_time) * 1000
            self.logger.info(f"Transaction completed: {duration_ms:.2f}ms, Success: {success}")
        
        return success
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get database performance statistics"""
        
        with self.metrics_lock:
            if not self.metrics:
                return {"message": "No metrics available"}
            
            # Calculate statistics
            total_queries = len(self.metrics)
            successful_queries = len([m for m in self.metrics if m.success])
            failed_queries = total_queries - successful_queries
            
            durations = [m.duration_ms for m in self.metrics if m.success]
            
            avg_duration = sum(durations) / len(durations) if durations else 0
            slow_queries = len([d for d in durations if d > self.slow_query_threshold])
            
            return {
                "total_queries": total_queries,
                "successful_queries": successful_queries,
                "failed_queries": failed_queries,
                "success_rate": (successful_queries / total_queries) * 100 if total_queries > 0 else 0,
                "average_duration_ms": round(avg_duration, 2),
                "slow_queries": slow_queries,
                "cache_entries": len(self.cache.cache)
            }
    
    def optimize_database(self):
        """Run database optimization commands"""
        
        optimization_queries = [
            "VACUUM",
            "ANALYZE",
            "PRAGMA optimize"
        ]
        
        for query in optimization_queries:
            try:
                self.execute_query(query)
                self.logger.info(f"Optimization completed: {query}")
            except Exception as e:
                self.logger.error(f"Optimization failed for {query}: {e}")
    
    def close(self):
        """Close database manager and cleanup resources"""
        self.cache.clear()
        self.pool.close_all()
        self.logger.info("Database manager closed")

class CLIDatabaseInterface:
    """CLI-optimized database interface for TubeWhale operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = logging.getLogger("database.cli")
    
    def get_video_analysis_summary(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent video analysis summary for CLI display"""
        
        query = """
        SELECT 
            v.video_id,
            v.title,
            v.channel_name,
            v.duration,
            v.view_count,
            a.analysis_type,
            a.created_at,
            json_extract(a.results, '$.overall_score') as score
        FROM videos v
        LEFT JOIN analysis a ON v.video_id = a.video_id
        ORDER BY a.created_at DESC
        LIMIT ?
        """
        
        return self.db.execute_query(
            query, 
            (limit,), 
            cache_key=f"video_summary_{limit}"
        )
    
    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get analysis statistics for CLI dashboard"""
        
        stats_query = """
        SELECT 
            COUNT(DISTINCT video_id) as total_videos,
            COUNT(*) as total_analyses,
            AVG(json_extract(results, '$.overall_score')) as avg_score,
            analysis_type,
            COUNT(*) as count_by_type
        FROM analysis 
        GROUP BY analysis_type
        """
        
        results = self.db.execute_query(stats_query, cache_key="analysis_stats")
        
        # Format for CLI display
        stats = {
            "total_videos": 0,
            "total_analyses": 0,
            "average_score": 0,
            "by_type": {}
        }
        
        for row in results:
            stats["total_videos"] = max(stats["total_videos"], row.get("total_videos", 0))
            stats["total_analyses"] += row.get("count_by_type", 0)
            stats["by_type"][row.get("analysis_type", "unknown")] = row.get("count_by_type", 0)
            
            if row.get("avg_score"):
                stats["average_score"] = round(row["avg_score"], 2)
        
        return stats
    
    def get_recent_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent errors for troubleshooting"""
        
        query = """
        SELECT error_type, error_message, created_at, COUNT(*) as count
        FROM errors 
        WHERE created_at >= datetime('now', '-{} hours')
        GROUP BY error_type, error_message
        ORDER BY count DESC, created_at DESC
        """.format(hours)
        
        return self.db.execute_query(query, cache_key=f"recent_errors_{hours}")

def create_database_manager(database_path: str = "tubewhale.db") -> Tuple[DatabaseManager, CLIDatabaseInterface]:
    """Factory function to create database manager and CLI interface"""
    
    config = DatabaseConfig(
        database_path=database_path,
        pool_size=5,
        timeout=30.0,
        enable_wal=True,
        enable_foreign_keys=True,
        cache_size=2000
    )
    
    db_manager = DatabaseManager(config)
    cli_interface = CLIDatabaseInterface(db_manager)
    
    return db_manager, cli_interface