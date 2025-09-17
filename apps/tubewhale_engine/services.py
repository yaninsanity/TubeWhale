"""
TubeWhale Engine Services
整合原有的agents和CLI功能到Django后端

这个模块提供了统一的接口来调用TubeWhale的核心功能：
- 音频下载和转录
- 字幕获取和处理  
- 视频搜索和分析
- 内容摘要和标准化
"""

import os
import asyncio
import logging
from typing import Optional, Dict, Any, List
from django.conf import settings

# 导入TubeWhale原有组件
try:
    from agents.audio_agent import AudioProcessingAgent
    from agents.transcript_agent import TranscriptAgent
    from agents.summarizer_agent import SummarizerAgent
    from agents.search_agent import SearchAgent
    from agents.standardizer_agent import StandardizerAgent
    from utils.youtube import YouTubeService
    from utils.openAIServices import OpenAIService
except ImportError as e:
    logging.warning(f"Could not import TubeWhale components: {e}")
    AudioProcessingAgent = None
    TranscriptAgent = None
    SummarizerAgent = None
    SearchAgent = None
    StandardizerAgent = None
    YouTubeService = None
    OpenAIService = None


class TubeWhaleEngine:
    """
    TubeWhale引擎主类
    统一管理所有TubeWhale功能模块
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._youtube_service = None
        self._openai_service = None
        self._audio_agent = None
        self._transcript_agent = None
        self._summarizer_agent = None
        self._search_agent = None
        self._standardizer_agent = None
        
        # 从Django设置或环境变量获取配置
        self.openai_api_key = getattr(settings, 'OPENAI_API_KEY', os.environ.get('OPENAI_API_KEY'))
        self.downloads_dir = getattr(settings, 'TUBEWHALE_DOWNLOADS_DIR', 'downloads')
        
        self._initialize_services()
    
    def _initialize_services(self):
        """初始化所有服务组件"""
        try:
            if not all([YouTubeService, OpenAIService, AudioProcessingAgent, 
                       TranscriptAgent, SummarizerAgent, SearchAgent, StandardizerAgent]):
                self.logger.warning("Some TubeWhale components not available")
                return
                
            # 初始化基础服务
            self._youtube_service = YouTubeService()
            self._openai_service = OpenAIService(api_key=self.openai_api_key)
            
            # 初始化agents
            self._transcript_agent = TranscriptAgent(
                openai_service=self._openai_service,
                youtube_service=self._youtube_service,
                logger=self.logger
            )
            
            self._audio_agent = AudioProcessingAgent(
                transcript_agent=self._transcript_agent,
                logger=self.logger
            )
            
            self._summarizer_agent = SummarizerAgent(
                openai_service=self._openai_service,
                logger=self.logger
            )
            
            self._search_agent = SearchAgent(
                youtube_service=self._youtube_service,
                logger=self.logger
            )
            
            self._standardizer_agent = StandardizerAgent(
                logger=self.logger
            )
            
            self.logger.info("✅ TubeWhale Engine initialized successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize TubeWhale Engine: {e}")
    
    def is_available(self) -> bool:
        """检查TubeWhale引擎是否可用"""
        return all([
            self._youtube_service is not None,
            self._openai_service is not None,
            self._audio_agent is not None,
            self._transcript_agent is not None
        ])
    
    async def process_video(self, video_id: str, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        处理YouTube视频 - 核心功能
        
        Args:
            video_id: YouTube视频ID
            options: 处理选项 {
                'transcript_only': bool,    # 仅获取字幕
                'audio_only': bool,         # 仅处理音频
                'summarize': bool,          # 生成摘要
                'standardize': bool,        # 标准化输出
                'search_related': bool      # 搜索相关视频
            }
        
        Returns:
            Dict包含处理结果
        """
        if not self.is_available():
            return {
                'success': False,
                'error': 'TubeWhale Engine not available',
                'video_id': video_id
            }
        
        options = options or {}
        result = {
            'success': True,
            'video_id': video_id,
            'transcript': None,
            'audio_summary': None,
            'video_info': None,
            'related_videos': None,
            'standardized_output': None
        }
        
        try:
            # 1. 获取视频信息
            if hasattr(self._youtube_service, 'get_video_info'):
                result['video_info'] = await self._get_video_info(video_id)
            
            # 2. 获取字幕
            if not options.get('audio_only', False):
                result['transcript'] = await self._get_transcript(video_id)
            
            # 3. 处理音频（如果字幕获取失败或强制音频处理）
            if not result['transcript'] or options.get('audio_only', False):
                result['audio_summary'] = await self._process_audio(video_id)
            
            # 4. 生成摘要
            if options.get('summarize', True):
                content = result['transcript'] or result['audio_summary']
                if content:
                    result['summary'] = await self._generate_summary(content)
            
            # 5. 搜索相关视频
            if options.get('search_related', False):
                result['related_videos'] = await self._search_related_videos(video_id)
            
            # 6. 标准化输出
            if options.get('standardize', False):
                result['standardized_output'] = await self._standardize_output(result)
            
            self.logger.info(f"✅ Video {video_id} processed successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Error processing video {video_id}: {e}")
            result['success'] = False
            result['error'] = str(e)
        
        return result
    
    async def _get_video_info(self, video_id: str) -> Optional[Dict[str, Any]]:
        """获取视频基本信息"""
        try:
            return await asyncio.get_event_loop().run_in_executor(
                None, self._youtube_service.get_video_info, video_id
            )
        except Exception as e:
            self.logger.error(f"Failed to get video info for {video_id}: {e}")
            return None
    
    async def _get_transcript(self, video_id: str) -> Optional[str]:
        """获取视频字幕"""
        try:
            if hasattr(self._transcript_agent, 'fetch_transcript'):
                return await self._transcript_agent.fetch_transcript(video_id)
            return None
        except Exception as e:
            self.logger.error(f"Failed to get transcript for {video_id}: {e}")
            return None
    
    async def _process_audio(self, video_id: str) -> Optional[str]:
        """处理视频音频"""
        try:
            return await self._audio_agent.process_video_audio(video_id)
        except Exception as e:
            self.logger.error(f"Failed to process audio for {video_id}: {e}")
            return None
    
    async def _generate_summary(self, content: str) -> Optional[str]:
        """生成内容摘要"""
        try:
            return await self._summarizer_agent.generate_summary(content)
        except Exception as e:
            self.logger.error(f"Failed to generate summary: {e}")
            return None
    
    async def _search_related_videos(self, video_id: str) -> Optional[List[Dict[str, Any]]]:
        """搜索相关视频"""
        try:
            return await self._search_agent.search_related_videos(video_id)
        except Exception as e:
            self.logger.error(f"Failed to search related videos for {video_id}: {e}")
            return None
    
    async def _standardize_output(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """标准化输出格式"""
        try:
            return await self._standardizer_agent.standardize_result(result)
        except Exception as e:
            self.logger.error(f"Failed to standardize output: {e}")
            return None


# 全局单例实例
_tubewhale_engine = None

def get_tubewhale_engine() -> TubeWhaleEngine:
    """获取TubeWhale引擎单例实例"""
    global _tubewhale_engine
    if _tubewhale_engine is None:
        _tubewhale_engine = TubeWhaleEngine()
    return _tubewhale_engine
