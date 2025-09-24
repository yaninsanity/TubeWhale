"""
🎯 工业级数据库事件监听器
实时监控数据库变化，确保前端立即获得更新通知
"""

import logging
import threading
import time
import sqlite3
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
from pathlib import Path
import json

from django.db.models.signals import post_save, post_delete  # 🎯 正确的导入路径
from django.dispatch import receiver
from django.db import models
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

logger = logging.getLogger(__name__)

class DatabaseEventMonitor:
    """数据库事件监控器 - 工业级实现"""
    
    def __init__(self, db_path: str = None):
        """初始化数据库事件监控器"""
        self.db_path = db_path or "tubewhale.db"
        self.channel_layer = get_channel_layer()
        self.listeners = {}
        self.is_monitoring = False
        self.monitor_thread = None
        self._lock = threading.RLock()
        
        # 监控的表列表
        self.monitored_tables = [
            'videos',
            'analyses', 
            'transcripts',
            'summaries',
            'audio_analyses',
            'search_results'
        ]
        
        # 最后检查的时间戳
        self.last_check_times = {}
        for table in self.monitored_tables:
            self.last_check_times[table] = datetime.now()
        
        logger.info("📊 数据库事件监控器已初始化")

    def start_monitoring(self):
        """开始监控数据库变化"""
        if self.is_monitoring:
            logger.warning("⚠️ 监控已在运行中")
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("🎯 数据库事件监控已启动")

    def stop_monitoring(self):
        """停止监控数据库变化"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("⏹️ 数据库事件监控已停止")

    def add_listener(self, event_type: str, callback: Callable):
        """添加事件监听器"""
        if event_type not in self.listeners:
            self.listeners[event_type] = []
        self.listeners[event_type].append(callback)
        logger.info(f"📝 添加监听器: {event_type}")

    def remove_listener(self, event_type: str, callback: Callable):
        """移除事件监听器"""
        if event_type in self.listeners:
            try:
                self.listeners[event_type].remove(callback)
                logger.info(f"🗑️ 移除监听器: {event_type}")
            except ValueError:
                pass

    def _monitor_loop(self):
        """监控主循环"""
        logger.info("🔄 开始数据库监控循环")
        
        while self.is_monitoring:
            try:
                self._check_database_changes()
                time.sleep(1)  # 每秒检查一次
            except Exception as e:
                logger.error(f"❌ 监控循环异常: {e}")
                time.sleep(5)  # 错误时等待更长时间

    def _check_database_changes(self):
        """检查数据库变化"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                for table_name in self.monitored_tables:
                    self._check_table_changes(conn, table_name)
                    
        except Exception as e:
            logger.error(f"❌ 检查数据库变化失败: {e}")

    def _check_table_changes(self, conn: sqlite3.Connection, table_name: str):
        """检查特定表的变化"""
        try:
            # 检查表是否存在
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if not cursor.fetchone():
                return
            
            # 查询最近的变化（假设表有updated_at或created_at字段）
            last_check = self.last_check_times[table_name]
            
            # 尝试不同的时间戳字段名
            timestamp_fields = ['updated_at', 'created_at', 'timestamp', 'modified_at']
            query_executed = False
            
            for timestamp_field in timestamp_fields:
                try:
                    # 检查字段是否存在
                    cursor = conn.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    if timestamp_field not in columns:
                        continue
                    
                    # 查询最近的变化
                    query = f"""
                        SELECT * FROM {table_name} 
                        WHERE {timestamp_field} > ? 
                        ORDER BY {timestamp_field} DESC
                    """
                    
                    cursor = conn.execute(query, (last_check.isoformat(),))
                    changes = cursor.fetchall()
                    
                    if changes:
                        logger.info(f"📊 检测到 {len(changes)} 个 {table_name} 表的变化")
                        self._process_table_changes(table_name, changes)
                    
                    query_executed = True
                    break
                    
                except sqlite3.OperationalError:
                    continue
            
            if not query_executed:
                # 如果没有时间戳字段，使用 rowid 方式检查
                self._check_by_rowid(conn, table_name)
            
            # 更新最后检查时间
            self.last_check_times[table_name] = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ 检查表 {table_name} 变化失败: {e}")

    def _check_by_rowid(self, conn: sqlite3.Connection, table_name: str):
        """通过rowid检查变化（后备方法）"""
        try:
            # 获取表的最大rowid
            cursor = conn.execute(f"SELECT MAX(rowid) FROM {table_name}")
            max_rowid = cursor.fetchone()[0]
            
            if max_rowid is None:
                return
            
            # 检查是否有新的记录
            last_rowid_key = f"{table_name}_last_rowid"
            last_rowid = getattr(self, last_rowid_key, 0)
            
            if max_rowid > last_rowid:
                # 有新记录
                cursor = conn.execute(
                    f"SELECT * FROM {table_name} WHERE rowid > ? ORDER BY rowid",
                    (last_rowid,)
                )
                changes = cursor.fetchall()
                
                if changes:
                    logger.info(f"📊 通过rowid检测到 {len(changes)} 个 {table_name} 表的变化")
                    self._process_table_changes(table_name, changes)
                
                setattr(self, last_rowid_key, max_rowid)
                
        except Exception as e:
            logger.error(f"❌ 通过rowid检查表 {table_name} 失败: {e}")

    def _process_table_changes(self, table_name: str, changes: List[sqlite3.Row]):
        """处理表变化"""
        try:
            for change in changes:
                change_data = dict(change)
                
                # 创建事件数据
                event_data = {
                    'table': table_name,
                    'action': 'insert',  # 简化为insert，实际应用中可以区分insert/update/delete
                    'data': change_data,
                    'timestamp': datetime.now().isoformat()
                }
                
                # 触发监听器
                self._trigger_listeners('database_change', event_data)
                self._trigger_listeners(f'{table_name}_change', event_data)
                
                # 发送WebSocket通知
                self._send_websocket_notification(event_data)
                
                # 特殊处理视频相关表
                if table_name in ['videos', 'analyses']:
                    self._handle_video_related_change(table_name, change_data)
                    
        except Exception as e:
            logger.error(f"❌ 处理表变化失败: {e}")

    def _trigger_listeners(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件监听器"""
        if event_type in self.listeners:
            for callback in self.listeners[event_type]:
                try:
                    callback(event_data)
                except Exception as e:
                    logger.error(f"❌ 监听器回调失败: {e}")

    def _send_websocket_notification(self, event_data: Dict[str, Any]):
        """发送WebSocket通知"""
        if self.channel_layer:
            try:
                async_to_sync(self.channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "database_update",
                        "table": event_data['table'],
                        "action": event_data['action'],
                        "data": event_data['data'],
                        "timestamp": event_data['timestamp']
                    }
                )
                logger.info(f"📡 发送WebSocket通知: {event_data['table']}")
            except Exception as e:
                logger.error(f"❌ 发送WebSocket通知失败: {e}")

    def _handle_video_related_change(self, table_name: str, change_data: Dict[str, Any]):
        """处理视频相关的变化"""
        try:
            if table_name == 'videos':
                # 视频表变化
                video_id = change_data.get('id') or change_data.get('video_id')
                if video_id:
                    self._notify_video_status_change(video_id, 'video_added', change_data)
            
            elif table_name == 'analyses':
                # 分析表变化 
                video_id = change_data.get('video_id')
                if video_id:
                    self._notify_video_status_change(video_id, 'analysis_completed', change_data)
                    
        except Exception as e:
            logger.error(f"❌ 处理视频相关变化失败: {e}")

    def _notify_video_status_change(self, video_id: str, change_type: str, data: Dict[str, Any]):
        """通知视频状态变化"""
        try:
            # 导入状态管理器（避免循环导入）
            from .video_status_manager import status_manager
            
            if status_manager:
                if change_type == 'video_added':
                    # 视频添加到数据库
                    status_manager.update_video_status(video_id, 
                                                     database_saved=True,
                                                     database_id=data.get('id'))
                elif change_type == 'analysis_completed':
                    # 分析完成并保存到数据库
                    status_manager.update_video_status(video_id,
                                                     analysis_saved=True,
                                                     analysis_id=data.get('id'))
                    
                logger.info(f"📊 更新视频状态: {video_id} - {change_type}")
                
        except Exception as e:
            logger.error(f"❌ 通知视频状态变化失败: {e}")

    def manually_check_changes(self):
        """手动检查数据库变化"""
        logger.info("🔍 手动检查数据库变化")
        self._check_database_changes()

    def get_monitoring_stats(self) -> Dict[str, Any]:
        """获取监控统计信息"""
        return {
            'is_monitoring': self.is_monitoring,
            'monitored_tables': self.monitored_tables,
            'last_check_times': {
                table: time.isoformat() 
                for table, time in self.last_check_times.items()
            },
            'listener_counts': {
                event_type: len(callbacks) 
                for event_type, callbacks in self.listeners.items()
            }
        }


# Django信号处理器（如果使用Django ORM）
@receiver(post_save)
def handle_model_save(sender, instance, created, **kwargs):
    """处理Django模型保存事件"""
    try:
        if hasattr(instance, '_meta'):
            table_name = instance._meta.db_table
            
            event_data = {
                'table': table_name,
                'action': 'insert' if created else 'update',
                'data': {
                    'id': getattr(instance, 'id', None),
                    'model': sender.__name__,
                    'created': created
                },
                'timestamp': datetime.now().isoformat()
            }
            
            # 发送WebSocket通知
            channel_layer = get_channel_layer()
            if channel_layer:
                async_to_sync(channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "django_model_change",
                        "event_data": event_data
                    }
                )
                
            logger.info(f"📊 Django模型变化: {sender.__name__} ({'创建' if created else '更新'})")
            
    except Exception as e:
        logger.error(f"❌ 处理Django模型保存事件失败: {e}")


@receiver(post_delete)
def handle_model_delete(sender, instance, **kwargs):
    """处理Django模型删除事件"""
    try:
        if hasattr(instance, '_meta'):
            table_name = instance._meta.db_table
            
            event_data = {
                'table': table_name,
                'action': 'delete',
                'data': {
                    'id': getattr(instance, 'id', None),
                    'model': sender.__name__
                },
                'timestamp': datetime.now().isoformat()
            }
            
            # 发送WebSocket通知
            channel_layer = get_channel_layer()
            if channel_layer:
                async_to_sync(channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "django_model_change",
                        "event_data": event_data
                    }
                )
                
            logger.info(f"📊 Django模型删除: {sender.__name__}")
            
    except Exception as e:
        logger.error(f"❌ 处理Django模型删除事件失败: {e}")


# 全局监控器实例
database_monitor = DatabaseEventMonitor()

# 自动启动监控
def start_database_monitoring():
    """启动数据库监控"""
    try:
        database_monitor.start_monitoring()
        logger.info("🎯 数据库监控已启动")
    except Exception as e:
        logger.error(f"❌ 启动数据库监控失败: {e}")

def stop_database_monitoring():
    """停止数据库监控"""
    try:
        database_monitor.stop_monitoring()
        logger.info("⏹️ 数据库监控已停止")
    except Exception as e:
        logger.error(f"❌ 停止数据库监控失败: {e}")


# 在Django应用启动时自动启动监控
from django.apps import AppConfig

class DatabaseMonitorConfig(AppConfig):
    """数据库监控应用配置"""
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'database_monitor'
    
    def ready(self):
        """应用准备就绪时启动监控"""
        start_database_monitoring()