"""
Complete User Interaction Flow System
完整用户交互流程系统

从Welcome开始的点击式交互体验，支持视频链接、播放列表、主题搜索
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from django.utils.translation import gettext as _
import re

class InputType(Enum):
    """输入类型枚举"""
    VIDEO_URL = "video_url"          # YouTube视频完整链接
    VIDEO_ID = "video_id"            # YouTube视频ID
    PLAYLIST_URL = "playlist_url"    # YouTube播放列表链接
    SEARCH_TOPIC = "search_topic"    # 搜索主题
    CHANNEL_URL = "channel_url"      # YouTube频道链接

@dataclass
class InteractionStep:
    """交互步骤数据结构"""
    id: str
    title: str
    description: str
    component_type: str              # 'welcome', 'scenario_selector', 'input_handler', 'confirmation'
    options: List[Dict[str, Any]]    # 可选项
    next_steps: List[str]            # 下一步可能的步骤
    validation_rules: List[str]      # 验证规则
    help_text: str                   # 帮助文本
    estimated_time: str              # 预估时间

class UserInteractionFlowManager:
    """用户交互流程管理器 - 完整的点击式体验"""
    
    def __init__(self):
        self.flow_steps = self._initialize_flow_steps()
        self.input_patterns = self._initialize_input_patterns()
    
    def _initialize_flow_steps(self) -> Dict[str, InteractionStep]:
        """初始化交互流程步骤"""
        return {
            "welcome": InteractionStep(
                id="welcome",
                title=_("Welcome to TubeWhale - AI-Powered YouTube Analysis"),
                description=_("Transform any YouTube content into actionable insights with AI. Choose your analysis goal and start discovering valuable information from videos, playlists, or topics."),
                component_type="welcome",
                options=[
                    {
                        "id": "quick_start",
                        "title": _("🚀 Quick Start"),
                        "description": _("I have a specific video or topic to analyze"),
                        "icon": "🚀",
                        "action": "goto_input_selection",
                        "recommended": True
                    },
                    {
                        "id": "explore_scenarios",
                        "title": _("🎯 Explore Analysis Types"),
                        "description": _("Show me what kind of analysis I can do"),
                        "icon": "🎯", 
                        "action": "goto_scenario_selection",
                        "recommended": False
                    },
                    {
                        "id": "see_examples",
                        "title": _("💡 See Examples"),
                        "description": _("Show me sample analyses and results"),
                        "icon": "💡",
                        "action": "goto_examples",
                        "recommended": False
                    }
                ],
                next_steps=["input_selection", "scenario_selection", "examples"],
                validation_rules=[],
                help_text=_("Choose how you'd like to start. Most users prefer Quick Start for immediate results."),
                estimated_time=_("30 seconds")
            ),
            
            "input_selection": InteractionStep(
                id="input_selection",
                title=_("What would you like to analyze?"),
                description=_("We support multiple input types. Choose the one that matches what you have:"),
                component_type="input_selector",
                options=[
                    {
                        "id": "single_video",
                        "title": _("📹 Single Video"),
                        "description": _("Analyze one YouTube video"),
                        "icon": "📹",
                        "input_placeholder": _("Paste YouTube URL or Video ID"),
                        "examples": [
                            "https://youtube.com/watch?v=dQw4w9WgXcQ",
                            "youtu.be/dQw4w9WgXcQ", 
                            "dQw4w9WgXcQ"
                        ],
                        "action": "process_single_video"
                    },
                    {
                        "id": "playlist",
                        "title": _("📚 Playlist/Channel"),
                        "description": _("Analyze multiple videos from playlist or channel"),
                        "icon": "📚",
                        "input_placeholder": _("Paste playlist or channel URL"),
                        "examples": [
                            "https://youtube.com/playlist?list=PLrxxx",
                            "https://youtube.com/@username",
                            "https://youtube.com/channel/UCxxx"
                        ],
                        "action": "process_playlist"
                    },
                    {
                        "id": "search_topic",
                        "title": _("🔍 Search Topic"),
                        "description": _("Find and analyze videos about a topic"),
                        "icon": "🔍",
                        "input_placeholder": _("Enter topic or keywords"),
                        "examples": [
                            "Python programming tutorials",
                            "Climate change 2024",
                            "Italian cooking recipes"
                        ],
                        "action": "process_search_topic"
                    }
                ],
                next_steps=["scenario_selection", "input_processing"],
                validation_rules=["non_empty", "valid_input_format"],
                help_text=_("Don't worry about the exact format - our AI will understand what you provide!"),
                estimated_time=_("1 minute")
            ),
            
            "scenario_selection": InteractionStep(
                id="scenario_selection",
                title=_("Choose Your Analysis Focus"),
                description=_("Select the type of insights you want to extract:"),
                component_type="scenario_selector",
                options=[
                    {
                        "id": "education_research",
                        "title": _("📚 Education Research"),
                        "description": _("Extract knowledge, teaching methods, learning outcomes"),
                        "icon": "📚",
                        "difficulty": "beginner",
                        "use_cases": [_("Course analysis"), _("Tutorial evaluation"), _("Learning assessment")],
                        "output_preview": _("Key concepts, teaching quality, learning outcomes")
                    },
                    {
                        "id": "market_research", 
                        "title": _("💼 Business Intelligence"),
                        "description": _("Market trends, competitor analysis, consumer insights"),
                        "icon": "💼",
                        "difficulty": "intermediate",
                        "use_cases": [_("Market analysis"), _("Product research"), _("Competitor study")],
                        "output_preview": _("Market trends, consumer sentiment, business opportunities")
                    },
                    {
                        "id": "content_creator_study",
                        "title": _("🎬 Content Strategy"),
                        "description": _("Creator strategies, audience engagement, growth patterns"),
                        "icon": "🎬", 
                        "difficulty": "beginner",
                        "use_cases": [_("Creator analysis"), _("Content optimization"), _("Audience research")],
                        "output_preview": _("Content strategies, engagement metrics, growth insights")
                    }
                ],
                next_steps=["analysis_confirmation"],
                validation_rules=["scenario_selected"],
                help_text=_("Each scenario provides different types of insights. Choose based on your goals."),
                estimated_time=_("30 seconds")
            ),
            
            "analysis_confirmation": InteractionStep(
                id="analysis_confirmation",
                title=_("Ready to Analyze!"),
                description=_("Review your analysis setup and start processing:"),
                component_type="confirmation",
                options=[
                    {
                        "id": "start_analysis",
                        "title": _("🚀 Start Analysis"),
                        "description": _("Begin AI processing"),
                        "icon": "🚀",
                        "action": "start_analysis",
                        "primary": True
                    },
                    {
                        "id": "modify_settings",
                        "title": _("⚙️ Modify Settings"),
                        "description": _("Change scenario or input"),
                        "icon": "⚙️",
                        "action": "goto_scenario_selection",
                        "primary": False
                    }
                ],
                next_steps=["analysis_processing"],
                validation_rules=["user_confirmed"],
                help_text=_("Analysis typically takes 5-10 minutes depending on content length."),
                estimated_time=_("5-10 minutes")
            )
        }
    
    def _initialize_input_patterns(self) -> Dict[str, Dict[str, Any]]:
        """初始化输入模式识别"""
        return {
            "patterns": {
                # YouTube完整链接
                "youtube_watch": {
                    "regex": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})",
                    "type": InputType.VIDEO_URL,
                    "extract_id": lambda match: match.group(1)
                },
                # YouTube播放列表
                "youtube_playlist": {
                    "regex": r"(?:https?://)?(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)",
                    "type": InputType.PLAYLIST_URL,
                    "extract_id": lambda match: match.group(1)
                },
                # YouTube频道
                "youtube_channel": {
                    "regex": r"(?:https?://)?(?:www\.)?youtube\.com/(?:channel/|@|c/)([a-zA-Z0-9_-]+)",
                    "type": InputType.CHANNEL_URL,
                    "extract_id": lambda match: match.group(1)
                },
                # 纯视频ID
                "video_id_only": {
                    "regex": r"^[a-zA-Z0-9_-]{11}$",
                    "type": InputType.VIDEO_ID,
                    "extract_id": lambda match: match.group(0)
                },
                # 搜索主题（任何其他文本）
                "search_topic": {
                    "regex": r".+",
                    "type": InputType.SEARCH_TOPIC,
                    "extract_id": lambda match: match.group(0)
                }
            }
        }
    
    def analyze_user_input(self, user_input: str) -> Dict[str, Any]:
        """分析用户输入，识别类型并提取信息"""
        user_input = user_input.strip()
        
        if not user_input:
            return {
                "type": None,
                "valid": False,
                "error": _("Input cannot be empty"),
                "suggestions": [
                    _("Try a YouTube video URL"),
                    _("Enter a search topic like 'Python tutorials'"),
                    _("Paste a playlist link")
                ]
            }
        
        # 按优先级检查模式
        pattern_priority = ["youtube_watch", "youtube_playlist", "youtube_channel", "video_id_only", "search_topic"]
        
        for pattern_name in pattern_priority:
            pattern_info = self.input_patterns["patterns"][pattern_name]
            match = re.search(pattern_info["regex"], user_input, re.IGNORECASE)
            
            if match:
                extracted_id = pattern_info["extract_id"](match)
                input_type = pattern_info["type"]
                
                return {
                    "input_type": input_type.value,
                    "type": input_type.value,  # Keep both for compatibility
                    "valid": True,
                    "original_input": user_input,
                    "extracted_id": extracted_id,
                    "processed_input": self._process_input_by_type(input_type, extracted_id),
                    "confidence": self._calculate_confidence(input_type, user_input),
                    "suggestions": self._generate_input_suggestions(input_type)
                }
        
        # 如果没有匹配任何模式，作为搜索主题处理
        return {
            "input_type": InputType.SEARCH_TOPIC.value,
            "type": InputType.SEARCH_TOPIC.value,  # Keep both for compatibility
            "valid": True,
            "original_input": user_input,
            "extracted_id": user_input,
            "processed_input": {
                "search_query": user_input,
                "suggested_keywords": self._extract_keywords(user_input)
            },
            "confidence": 80,
            "suggestions": [
                _("We'll search YouTube for: '{}'").format(user_input),
                _("Add more specific keywords for better results"),
                _("Try different phrasings if needed")
            ]
        }
    
    def _process_input_by_type(self, input_type: InputType, extracted_id: str) -> Dict[str, Any]:
        """根据输入类型处理数据"""
        if input_type == InputType.VIDEO_URL or input_type == InputType.VIDEO_ID:
            return {
                "video_id": extracted_id,
                "youtube_url": f"https://www.youtube.com/watch?v={extracted_id}",
                "processing_type": "single_video"
            }
        elif input_type == InputType.PLAYLIST_URL:
            return {
                "playlist_id": extracted_id,
                "youtube_url": f"https://www.youtube.com/playlist?list={extracted_id}",
                "processing_type": "playlist"
            }
        elif input_type == InputType.CHANNEL_URL:
            return {
                "channel_id": extracted_id,
                "youtube_url": f"https://www.youtube.com/@{extracted_id}",
                "processing_type": "channel"
            }
        elif input_type == InputType.SEARCH_TOPIC:
            return {
                "search_query": extracted_id,
                "keywords": self._extract_keywords(extracted_id),
                "processing_type": "search"
            }
        
        return {"processing_type": "unknown"}
    
    def _calculate_confidence(self, input_type: InputType, user_input: str) -> float:
        """计算输入识别的置信度"""
        if input_type == InputType.VIDEO_URL and "youtube.com" in user_input.lower():
            return 0.95
        elif input_type == InputType.PLAYLIST_URL and "playlist" in user_input.lower():
            return 0.95
        elif input_type == InputType.CHANNEL_URL and ("@" in user_input or "channel" in user_input.lower()):
            return 0.90
        elif input_type == InputType.VIDEO_ID and len(user_input) == 11:
            return 0.85
        elif input_type == InputType.SEARCH_TOPIC:
            return 0.80
        
        return 0.70
    
    def _extract_keywords(self, text: str) -> List[str]:
        """从搜索主题中提取关键词"""
        # 简单的关键词提取，实际使用时可以使用NLP库
        import string
        
        # 移除标点符号并分割单词
        translator = str.maketrans('', '', string.punctuation)
        clean_text = text.translate(translator).lower()
        words = clean_text.split()
        
        # 过滤常见停用词
        stopwords = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'a', 'an'}
        keywords = [word for word in words if word not in stopwords and len(word) > 2]
        
        return keywords[:5]  # 返回最多5个关键词
    
    def _generate_input_suggestions(self, input_type: InputType) -> List[str]:
        """根据输入类型生成建议"""
        suggestions = {
            InputType.VIDEO_URL: [
                _("Great! We'll analyze this YouTube video"),
                _("Processing will take 3-8 minutes depending on video length"),
                _("You can close this page and come back later for results")
            ],
            InputType.PLAYLIST_URL: [
                _("Perfect! We'll analyze all videos in this playlist"),
                _("Large playlists may take 15-30 minutes to process"),
                _("Premium users get priority processing")
            ],
            InputType.CHANNEL_URL: [
                _("Excellent! We'll analyze recent videos from this channel"),
                _("We'll focus on the most popular/recent content"),
                _("Channel analysis provides creator insights and trends")
            ],
            InputType.VIDEO_ID: [
                _("Video ID recognized! We'll fetch and analyze the content"),
                _("Make sure this is a valid YouTube video ID"),
                _("Processing time: 3-8 minutes")
            ],
            InputType.SEARCH_TOPIC: [
                _("We'll search YouTube and analyze the best matching content"),
                _("More specific topics give better results"),
                _("We'll analyze multiple videos on this topic")
            ]
        }
        
        return suggestions.get(input_type, [_("Input processed successfully")])
    
    def get_flow_step(self, step_id: str) -> Optional[InteractionStep]:
        """获取特定的流程步骤"""
        return self.flow_steps.get(step_id)
    
    def get_next_step_recommendations(self, current_step: str, user_context: Dict[str, Any]) -> List[str]:
        """根据当前步骤和用户上下文推荐下一步"""
        step = self.get_flow_step(current_step)
        if not step:
            return ["welcome"]
        
        # 根据用户上下文智能推荐
        if user_context.get('is_new_user', True):
            if current_step == "welcome":
                return ["input_selection"]  # 新用户直接到输入选择
            elif current_step == "input_selection":
                return ["scenario_selection"]
        else:
            # 有经验用户可能直接跳过某些步骤
            if current_step == "welcome" and user_context.get('has_analysis_history', False):
                return ["input_selection", "scenario_selection"]
        
        return step.next_steps
    
    def validate_step_completion(self, step_id: str, user_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证步骤完成情况"""
        step = self.get_flow_step(step_id)
        if not step:
            return False, [_("Invalid step")]
        
        errors = []
        
        for rule in step.validation_rules:
            if rule == "non_empty":
                if not user_data.get('input', '').strip():
                    errors.append(_("Input cannot be empty"))
            elif rule == "valid_input_format":
                input_analysis = self.analyze_user_input(user_data.get('input', ''))
                if not input_analysis['valid']:
                    errors.append(input_analysis.get('error', _("Invalid input format")))
            elif rule == "scenario_selected":
                if not user_data.get('selected_scenario'):
                    errors.append(_("Please select an analysis scenario"))
            elif rule == "user_confirmed":
                if not user_data.get('confirmed', False):
                    errors.append(_("Please confirm to proceed"))
        
        return len(errors) == 0, errors
    
    def generate_progress_info(self, current_step: str) -> Dict[str, Any]:
        """生成进度信息"""
        step_order = ["welcome", "input_selection", "scenario_selection", "analysis_confirmation", "analysis_processing"]
        
        try:
            current_index = step_order.index(current_step)
            total_steps = len(step_order)
            
            return {
                "current_step": current_index + 1,
                "total_steps": total_steps,
                "percentage": int((current_index + 1) / total_steps * 100),
                "step_name": current_step,
                "next_step": step_order[current_index + 1] if current_index < total_steps - 1 else None,
                "is_final_step": current_index == total_steps - 1
            }
        except ValueError:
            return {
                "current_step": 1,
                "total_steps": 5,
                "percentage": 20,
                "step_name": current_step,
                "next_step": "input_selection",
                "is_final_step": False
            }

# 全局交互流程管理器实例
interaction_flow_manager = UserInteractionFlowManager()