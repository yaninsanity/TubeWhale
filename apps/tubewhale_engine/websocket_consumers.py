"""
🎯 Industrial-grade real-time video processing tracking WebSocket consumer
Supports multi-client real-time status synchronization for optimal user experience
"""

import json
import asyncio
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.core.cache import cache
from datetime import datetime
import redis
from django.conf import settings

logger = logging.getLogger(__name__)

class VideoProcessingConsumer(AsyncWebsocketConsumer):
    """视频处理实时状态WebSocket消费者"""
    
    async def connect(self):
        """建立WebSocket连接"""
        self.user = self.scope.get("user")
        self.room_group_name = "video_processing_live"
        
        # 加入房间组
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        
        await self.accept()
        
        # 发送当前所有视频状态
        await self.send_current_status()
        logger.info(f"🔗 用户 {self.user} 已连接到实时视频处理追踪")

    async def disconnect(self, close_code):
        """断开WebSocket连接"""
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )
        logger.info(f"❌ 用户 {self.user} 已断开实时追踪连接 (代码: {close_code})")

    async def receive(self, text_data):
        """接收来自WebSocket的消息"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type')
            
            if message_type == 'request_status':
                # 客户端请求当前状态
                await self.send_current_status()
            elif message_type == 'pause_video':
                # 暂停视频处理
                video_id = data.get('video_id')
                await self.pause_video_processing(video_id)
            elif message_type == 'resume_video':
                # 恢复视频处理
                video_id = data.get('video_id')
                await self.resume_video_processing(video_id)
                
        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': '无效的JSON格式'
            }))

    async def send_current_status(self):
        """发送当前所有视频的处理状态"""
        try:
            # 从Redis获取所有视频状态
            redis_client = redis.Redis.from_url(settings.REDIS_URL)
            video_keys = redis_client.keys("video_status:*")
            
            current_videos = []
            for key in video_keys:
                video_data = redis_client.hgetall(key)
                if video_data:
                    # 解码Redis数据
                    decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in video_data.items()}
                    current_videos.append(decoded_data)
            
            await self.send(text_data=json.dumps({
                'type': 'current_status',
                'videos': current_videos,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ 获取当前状态失败: {e}")
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'获取状态失败: {str(e)}'
            }))

    # 处理来自群组的消息
    async def video_status_update(self, event):
        """视频状态更新"""
        await self.send(text_data=json.dumps({
            'type': 'video_update',
            'video': event['video'],
            'timestamp': event['timestamp']
        }))

    async def video_progress_update(self, event):
        """视频处理进度更新"""
        await self.send(text_data=json.dumps({
            'type': 'progress_update',
            'video_id': event['video_id'],
            'progress': event['progress'],
            'current_step': event['current_step'],
            'timestamp': event['timestamp']
        }))

    async def video_error(self, event):
        """视频处理错误"""
        await self.send(text_data=json.dumps({
            'type': 'video_error',
            'video_id': event['video_id'],
            'error': event['error'],
            'timestamp': event['timestamp']
        }))

    async def video_completed(self, event):
        """视频处理完成"""
        await self.send(text_data=json.dumps({
            'type': 'video_completed',
            'video_id': event['video_id'],
            'results': event['results'],
            'timestamp': event['timestamp']
        }))

    async def system_status(self, event):
        """系统状态更新"""
        await self.send(text_data=json.dumps({
            'type': 'system_status',
            'status': event['status'],
            'metrics': event['metrics'],
            'timestamp': event['timestamp']
        }))

    @database_sync_to_async
    def pause_video_processing(self, video_id):
        """暂停视频处理"""
        try:
            redis_client = redis.Redis.from_url(settings.REDIS_URL)
            redis_client.hset(f"video_status:{video_id}", "status", "paused")
            logger.info(f"⏸️ 视频 {video_id} 已暂停处理")
            return True
        except Exception as e:
            logger.error(f"❌ 暂停视频 {video_id} 失败: {e}")
            return False

    @database_sync_to_async
    def resume_video_processing(self, video_id):
        """恢复视频处理"""
        try:
            redis_client = redis.Redis.from_url(settings.REDIS_URL)
            redis_client.hset(f"video_status:{video_id}", "status", "processing")
            logger.info(f"▶️ 视频 {video_id} 已恢复处理")
            return True
        except Exception as e:
            logger.error(f"❌ 恢复视频 {video_id} 失败: {e}")
            return False


class SystemMonitoringConsumer(AsyncWebsocketConsumer):
    """系统监控WebSocket消费者"""
    
    async def connect(self):
        """建立系统监控连接"""
        self.room_group_name = "system_monitoring"
        
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        
        await self.accept()
        
        # 开始定期发送系统状态
        self.monitoring_task = asyncio.create_task(self.send_system_metrics())
        logger.info("📊 系统监控WebSocket已连接")

    async def disconnect(self, close_code):
        """断开系统监控连接"""
        if hasattr(self, 'monitoring_task'):
            self.monitoring_task.cancel()
        
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )
        logger.info(f"📊 系统监控WebSocket已断开 (代码: {close_code})")

    async def send_system_metrics(self):
        """定期发送系统度量"""
        while True:
            try:
                # 获取系统度量
                from .ai_agents import get_real_time_agent_status
                metrics = await database_sync_to_async(get_real_time_agent_status)()
                
                await self.send(text_data=json.dumps({
                    'type': 'system_metrics',
                    'metrics': metrics,
                    'timestamp': datetime.now().isoformat()
                }))
                
                # 每5秒更新一次
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 发送系统度量失败: {e}")
                await asyncio.sleep(10)  # 错误时等待更长时间

    async def agent_status_update(self, event):
        """智能助手状态更新"""
        await self.send(text_data=json.dumps({
            'type': 'agent_status',
            'agent_id': event['agent_id'],
            'status': event['status'],
            'timestamp': event['timestamp']
        }))