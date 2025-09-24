"""
🎯 工业级视频处理状态管理器
支持多进程、分布式状态同步和实时追踪
"""

import redis
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from django.conf import settings
from django.core.cache import cache
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import threading
import time

logger = logging.getLogger(__name__)

class VideoProcessingStatusManager:
    """视频处理状态管理器 - 工业级实现"""
    
    def __init__(self):
        """初始化状态管理器"""
        try:
            self.redis_client = redis.Redis.from_url(
                settings.REDIS_URL, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # 测试连接
            self.redis_client.ping()
            logger.info("✅ Redis连接成功建立")
        except Exception as e:
            logger.error(f"❌ Redis连接失败: {e}")
            # 使用本地缓存作为后备
            self.redis_client = None
        
        self.channel_layer = get_channel_layer()
        self._lock = threading.RLock()
        
        # 状态常量
        self.STATUS_PENDING = "pending"
        self.STATUS_DOWNLOADING = "downloading"
        self.STATUS_PROCESSING = "processing"
        self.STATUS_TRANSCRIBING = "transcribing"
        self.STATUS_ANALYZING = "analyzing"
        self.STATUS_SUMMARIZING = "summarizing"
        self.STATUS_COMPLETED = "completed"
        self.STATUS_ERROR = "error"
        self.STATUS_PAUSED = "paused"
        self.STATUS_CANCELLED = "cancelled"

    def create_video_status(self, video_id: str, url: str, title: str = "", user_id: str = None) -> Dict[str, Any]:
        """创建新的视频处理状态"""
        status_data = {
            'video_id': video_id,
            'url': url,
            'title': title or f"视频_{video_id[:8]}",
            'user_id': user_id,
            'status': self.STATUS_PENDING,
            'progress': 0,
            'current_step': '准备中',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'start_time': None,
            'end_time': None,
            'error_message': None,
            'retry_count': 0,
            'max_retries': 3,
            'file_path': None,
            'file_size': 0,
            'duration': 0,
            'agents_status': {
                'download_agent': 'pending',
                'transcript_agent': 'pending',
                'summarizer_agent': 'pending',
                'audio_agent': 'pending',
                'search_agent': 'pending'
            },
            'results': {
                'transcript': None,
                'summary': None,
                'audio_analysis': None,
                'search_data': None
            },
            'metrics': {
                'download_time': 0,
                'processing_time': 0,
                'total_time': 0,
                'file_size_mb': 0
            }
        }
        
        # 保存到Redis
        if self._save_status(video_id, status_data):
            logger.info(f"📝 创建视频状态: {video_id} - {title}")
            self._notify_status_change(video_id, status_data)
            return status_data
        else:
            logger.error(f"❌ 创建视频状态失败: {video_id}")
            return None

    def update_video_status(self, video_id: str, **updates) -> bool:
        """更新视频处理状态"""
        with self._lock:
            try:
                current_status = self.get_video_status(video_id)
                if not current_status:
                    logger.error(f"❌ 视频状态不存在: {video_id}")
                    return False
                
                # 更新字段
                current_status.update(updates)
                current_status['updated_at'] = datetime.now().isoformat()
                
                # 特殊处理
                if 'status' in updates:
                    if updates['status'] == self.STATUS_PROCESSING and not current_status.get('start_time'):
                        current_status['start_time'] = datetime.now().isoformat()
                    elif updates['status'] in [self.STATUS_COMPLETED, self.STATUS_ERROR, self.STATUS_CANCELLED]:
                        current_status['end_time'] = datetime.now().isoformat()
                        if current_status.get('start_time'):
                            start_time = datetime.fromisoformat(current_status['start_time'])
                            end_time = datetime.now()
                            current_status['metrics']['total_time'] = (end_time - start_time).total_seconds()
                
                # 保存更新
                if self._save_status(video_id, current_status):
                    logger.info(f"🔄 更新视频状态: {video_id} -> {updates}")
                    self._notify_status_change(video_id, current_status)
                    return True
                else:
                    logger.error(f"❌ 保存视频状态失败: {video_id}")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ 更新视频状态异常: {video_id} - {e}")
                return False

    def update_progress(self, video_id: str, progress: int, current_step: str = None) -> bool:
        """更新处理进度"""
        updates = {'progress': min(max(progress, 0), 100)}
        if current_step:
            updates['current_step'] = current_step
        
        if self.update_video_status(video_id, **updates):
            # 发送进度更新通知
            self._notify_progress_update(video_id, progress, current_step)
            return True
        return False

    def update_agent_status(self, video_id: str, agent_name: str, status: str, result_data: Any = None) -> bool:
        """更新智能助手状态"""
        current_status = self.get_video_status(video_id)
        if not current_status:
            return False
        
        # 更新助手状态
        current_status['agents_status'][agent_name] = status
        
        # 如果有结果数据，保存到results中
        if result_data is not None:
            result_key = agent_name.replace('_agent', '')
            current_status['results'][result_key] = result_data
        
        return self.update_video_status(video_id, 
                                      agents_status=current_status['agents_status'],
                                      results=current_status['results'])

    def set_error(self, video_id: str, error_message: str, retry: bool = True) -> bool:
        """设置错误状态"""
        current_status = self.get_video_status(video_id)
        if not current_status:
            return False
        
        retry_count = current_status.get('retry_count', 0)
        max_retries = current_status.get('max_retries', 3)
        
        updates = {
            'error_message': error_message,
            'retry_count': retry_count + 1 if retry else retry_count
        }
        
        # 判断是否需要重试
        if retry and retry_count < max_retries:
            updates['status'] = self.STATUS_PENDING  # 重置为待处理
            updates['current_step'] = f'错误重试 ({retry_count + 1}/{max_retries})'
            logger.warning(f"⚠️ 视频处理错误，准备重试: {video_id} - {error_message}")
        else:
            updates['status'] = self.STATUS_ERROR
            updates['current_step'] = '处理失败'
            logger.error(f"❌ 视频处理失败: {video_id} - {error_message}")
        
        if self.update_video_status(video_id, **updates):
            self._notify_error(video_id, error_message)
            return True
        return False

    def mark_completed(self, video_id: str, final_results: Dict[str, Any] = None) -> bool:
        """标记为完成状态"""
        updates = {
            'status': self.STATUS_COMPLETED,
            'progress': 100,
            'current_step': '处理完成'
        }
        
        if final_results:
            current_status = self.get_video_status(video_id)
            if current_status:
                current_status['results'].update(final_results)
                updates['results'] = current_status['results']
        
        if self.update_video_status(video_id, **updates):
            self._notify_completion(video_id, final_results or {})
            logger.info(f"✅ 视频处理完成: {video_id}")
            return True
        return False

    def get_video_status(self, video_id: str) -> Optional[Dict[str, Any]]:
        """获取视频状态"""
        try:
            if self.redis_client:
                status_data = self.redis_client.hgetall(f"video_status:{video_id}")
                if status_data:
                    # 解析JSON字段
                    for key in ['agents_status', 'results', 'metrics']:
                        if key in status_data and status_data[key]:
                            try:
                                status_data[key] = json.loads(status_data[key])
                            except json.JSONDecodeError:
                                status_data[key] = {}
                    return status_data
            else:
                # 使用Django缓存作为后备
                return cache.get(f"video_status:{video_id}")
        except Exception as e:
            logger.error(f"❌ 获取视频状态失败: {video_id} - {e}")
        return None

    def get_all_active_videos(self) -> List[Dict[str, Any]]:
        """获取所有活跃的视频状态"""
        try:
            if self.redis_client:
                video_keys = self.redis_client.keys("video_status:*")
                active_videos = []
                
                for key in video_keys:
                    video_data = self.redis_client.hgetall(key)
                    if video_data and video_data.get('status') not in [self.STATUS_COMPLETED, self.STATUS_CANCELLED]:
                        # 解析JSON字段
                        for json_key in ['agents_status', 'results', 'metrics']:
                            if json_key in video_data and video_data[json_key]:
                                try:
                                    video_data[json_key] = json.loads(video_data[json_key])
                                except json.JSONDecodeError:
                                    video_data[json_key] = {}
                        active_videos.append(video_data)
                
                return active_videos
            else:
                # 使用Django缓存作为后备
                return cache.get("all_active_videos", [])
        except Exception as e:
            logger.error(f"❌ 获取活跃视频列表失败: {e}")
            return []

    def get_processing_statistics(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        try:
            if self.redis_client:
                all_keys = self.redis_client.keys("video_status:*")
                stats = {
                    'total_videos': len(all_keys),
                    'status_counts': {
                        self.STATUS_PENDING: 0,
                        self.STATUS_PROCESSING: 0,
                        self.STATUS_COMPLETED: 0,
                        self.STATUS_ERROR: 0,
                        self.STATUS_PAUSED: 0
                    },
                    'average_processing_time': 0,
                    'success_rate': 0
                }
                
                processing_times = []
                for key in all_keys:
                    video_data = self.redis_client.hgetall(key)
                    if video_data:
                        status = video_data.get('status', 'unknown')
                        if status in stats['status_counts']:
                            stats['status_counts'][status] += 1
                        
                        # 计算处理时间
                        if video_data.get('metrics'):
                            try:
                                metrics = json.loads(video_data['metrics'])
                                if metrics.get('total_time', 0) > 0:
                                    processing_times.append(metrics['total_time'])
                            except:
                                pass
                
                # 计算平均处理时间
                if processing_times:
                    stats['average_processing_time'] = sum(processing_times) / len(processing_times)
                
                # 计算成功率
                completed = stats['status_counts'][self.STATUS_COMPLETED]
                total_processed = completed + stats['status_counts'][self.STATUS_ERROR]
                if total_processed > 0:
                    stats['success_rate'] = (completed / total_processed) * 100
                
                return stats
            
        except Exception as e:
            logger.error(f"❌ 获取处理统计失败: {e}")
        
        return {'total_videos': 0, 'status_counts': {}, 'average_processing_time': 0, 'success_rate': 0}

    def cleanup_old_statuses(self, days: int = 7) -> int:
        """清理旧的状态记录"""
        try:
            if not self.redis_client:
                return 0
                
            cutoff_date = datetime.now() - timedelta(days=days)
            all_keys = self.redis_client.keys("video_status:*")
            cleaned_count = 0
            
            for key in all_keys:
                video_data = self.redis_client.hgetall(key)
                if video_data and video_data.get('created_at'):
                    try:
                        created_at = datetime.fromisoformat(video_data['created_at'])
                        if created_at < cutoff_date and video_data.get('status') in [self.STATUS_COMPLETED, self.STATUS_ERROR, self.STATUS_CANCELLED]:
                            self.redis_client.delete(key)
                            cleaned_count += 1
                    except:
                        continue
            
            logger.info(f"🧹 清理了 {cleaned_count} 个旧状态记录")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ 清理旧状态失败: {e}")
            return 0

    def _save_status(self, video_id: str, status_data: Dict[str, Any]) -> bool:
        """保存状态到存储"""
        try:
            # 序列化复杂字段
            serialized_data = status_data.copy()
            for key in ['agents_status', 'results', 'metrics']:
                if key in serialized_data and isinstance(serialized_data[key], dict):
                    serialized_data[key] = json.dumps(serialized_data[key])
            
            if self.redis_client:
                # 保存到Redis，设置过期时间
                self.redis_client.hset(f"video_status:{video_id}", mapping=serialized_data)
                self.redis_client.expire(f"video_status:{video_id}", 86400 * 30)  # 30天过期
                return True
            else:
                # 保存到Django缓存
                cache.set(f"video_status:{video_id}", status_data, 86400 * 30)
                return True
                
        except Exception as e:
            logger.error(f"❌ 保存状态失败: {video_id} - {e}")
            return False

    def _notify_status_change(self, video_id: str, status_data: Dict[str, Any]):
        """通知状态变化"""
        if self.channel_layer:
            try:
                async_to_sync(self.channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "video_status_update",
                        "video": status_data,
                        "timestamp": datetime.now().isoformat()
                    }
                )
            except Exception as e:
                logger.error(f"❌ 发送状态更新通知失败: {video_id} - {e}")

    def _notify_progress_update(self, video_id: str, progress: int, current_step: str):
        """通知进度更新"""
        if self.channel_layer:
            try:
                async_to_sync(self.channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "video_progress_update",
                        "video_id": video_id,
                        "progress": progress,
                        "current_step": current_step,
                        "timestamp": datetime.now().isoformat()
                    }
                )
            except Exception as e:
                logger.error(f"❌ 发送进度更新通知失败: {video_id} - {e}")

    def _notify_error(self, video_id: str, error_message: str):
        """通知错误"""
        if self.channel_layer:
            try:
                async_to_sync(self.channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "video_error",
                        "video_id": video_id,
                        "error": error_message,
                        "timestamp": datetime.now().isoformat()
                    }
                )
            except Exception as e:
                logger.error(f"❌ 发送错误通知失败: {video_id} - {e}")

    def _notify_completion(self, video_id: str, results: Dict[str, Any]):
        """通知完成"""
        if self.channel_layer:
            try:
                async_to_sync(self.channel_layer.group_send)(
                    "video_processing_live",
                    {
                        "type": "video_completed",
                        "video_id": video_id,
                        "results": results,
                        "timestamp": datetime.now().isoformat()
                    }
                )
            except Exception as e:
                logger.error(f"❌ 发送完成通知失败: {video_id} - {e}")


# 全局单例实例
status_manager = VideoProcessingStatusManager()