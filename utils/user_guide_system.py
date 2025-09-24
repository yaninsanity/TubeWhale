"""
用户引导和提示系统 - YouTube级别的用户体验
User Guidance and Tooltip System - YouTube-level User Experience

提供智能的用户引导、提示和帮助，让任何看过YouTube的用户都能轻松使用
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum
from django.utils.translation import gettext as _

class GuideType(Enum):
    """引导类型"""
    ONBOARDING = "onboarding"        # 新用户引导
    FEATURE_INTRO = "feature_intro"  # 功能介绍
    TOOLTIP = "tooltip"              # 工具提示
    CONTEXTUAL_HELP = "contextual"   # 上下文帮助
    SUCCESS_TIP = "success_tip"      # 成功提示
    ERROR_HELP = "error_help"        # 错误帮助

@dataclass
class GuideContent:
    """引导内容数据结构"""
    id: str
    type: GuideType
    title: str
    content: str
    visual: Optional[str] = None      # 图片/GIF/视频URL
    action_button: Optional[Dict[str, str]] = None  # 行动按钮
    dismiss_button: Optional[Dict[str, str]] = None # 忽略按钮
    auto_show_conditions: List[str] = None          # 自动显示条件
    position: Optional[str] = None    # 显示位置
    priority: int = 1                 # 优先级 (1-10)
    max_show_times: int = 3           # 最大显示次数

class UserGuideManager:
    """用户引导管理器 - 智能化用户体验系统"""
    
    def __init__(self):
        self.guides = self._initialize_guides()
    
    def _initialize_guides(self) -> Dict[str, GuideContent]:
        """初始化所有引导内容"""
        return {
            # 新用户引导
            "welcome_new_user": GuideContent(
                id="welcome_new_user",
                type=GuideType.ONBOARDING,
                title=_("🎉 Welcome to TubeWhale!"),
                content=_("Simple as using YouTube! Just 3 steps: Choose Analysis Scenario → Input Video/Topic → Get AI Insights.\n\nSupported inputs:\n• Single video: youtube.com/watch?v=abc123\n• Playlist: youtube.com/playlist?list=xyz\n• Video ID: abc123\n• Search topic: \"Python tutorials\"\n\nLet's start your first analysis!"),
                visual="/static/guides/welcome-animation.gif",
                action_button={"text": _("Start First Analysis"), "action": "start_first_analysis"},
                dismiss_button={"text": _("Maybe Later"), "action": "dismiss"},
                auto_show_conditions=["is_new_user", "first_visit"],
                position="center",
                priority=10,
                max_show_times=1
            ),
            
            "scenario_selection_guide": GuideContent(
                id="scenario_selection_guide", 
                type=GuideType.FEATURE_INTRO,
                title=_("📋 How to Choose Analysis Scenario?"),
                content=_("Each scenario is optimized for specific needs! 🎯\n\n📚 Education Research: Analyze learning content\n💼 Business Insights: Market and product research\n🎬 Content Creator: Study creator strategies\n\nNot sure? Try 'Education Content Research' - easiest to start with!\n\nAll scenarios support:\n• Single videos\n• Playlists\n• Video IDs\n• Search topics"),
                visual="/static/guides/scenario-selection.mp4",
                action_button={"text": _("Got It"), "action": "dismiss"},
                auto_show_conditions=["viewing_scenario_list", "show_count < 3"],
                position="right-sidebar",
                priority=8,
                max_show_times=3
            ),
            
            "universal_input_guide": GuideContent(
                id="universal_input_guide",
                type=GuideType.TOOLTIP,
                title=_("🎯 Universal Input - Multiple Ways to Analyze"),
                content=_("We support ALL input types:\n\n📹 YouTube Links:\n✅ Full URL: youtube.com/watch?v=dQw4w9WgXcQ\n✅ Short URL: youtu.be/dQw4w9WgXcQ\n✅ Playlist: youtube.com/playlist?list=PLxxx\n\n🆔 Video ID Only:\n✅ Just the ID: dQw4w9WgXcQ\n\n🔍 Search Topics:\n✅ \"Python programming tutorials\"\n✅ \"Market analysis 2024\"\n✅ \"Cooking recipes Italian\"\n\n💡 Tip: Copy-paste from browser or just type what you want to analyze!"),
                action_button={"text": _("Understand"), "action": "dismiss"},
                auto_show_conditions=["focus_url_input"],
                position="bottom",
                priority=9,
                max_show_times=3
            ),
            
            "premium_benefits_intro": GuideContent(
                id="premium_benefits_intro",
                type=GuideType.FEATURE_INTRO,
                title=_("⚡ Premium User Exclusive Benefits"),
                content=_("🚀 Unlimited analysis\n🔧 Auto API configuration\n⚡ Batch analysis support\n🎯 Advanced analysis templates\n📊 Detailed data export\n🔄 History management\n🌍 Priority processing\n\nYour API keys are auto-configured. Start enjoying Premium experience!"),
                visual="/static/guides/premium-features.png",
                action_button={"text": _("Explore Premium Features"), "action": "explore_premium"},
                auto_show_conditions=["is_premium_user", "first_premium_visit"],
                position="center",
                priority=9,
                max_show_times=1
            ),
            
            "analysis_in_progress": GuideContent(
                id="analysis_in_progress",
                type=GuideType.SUCCESS_TIP,
                title="🤖 AI正在认真分析中...",
                content="分析通常需要5-10分钟，具体时间取决于视频长度。\n\n🔍 我们的AI正在：\n• 转录音频内容\n• 分析视觉元素\n• 提取关键信息\n• 生成智能洞察\n\n💡 您可以关闭此页面，稍后回来查看结果！",
                visual="/static/guides/analysis-progress.gif",
                auto_show_conditions=["analysis_started"],
                position="center",
                priority=7,
                max_show_times=1
            ),
            
            "first_analysis_complete": GuideContent(
                id="first_analysis_complete",
                type=GuideType.SUCCESS_TIP,
                title="🎉 恭喜！您的第一份分析报告完成了！",
                content="太棒了！您已经掌握了基本流程。\n\n🎯 接下来您可以：\n• 尝试其他分析场景\n• 批量分析多个视频\n• 导出分析结果\n• 查看历史记录\n\n继续探索更多功能吧！",
                visual="/static/guides/first-success.png", 
                action_button={"text": "继续探索", "action": "explore_more"},
                auto_show_conditions=["first_analysis_completed"],
                position="center",
                priority=8,
                max_show_times=1
            ),
            
            # 错误处理和帮助
            "invalid_youtube_url": GuideContent(
                id="invalid_youtube_url",
                type=GuideType.ERROR_HELP,
                title="❌ YouTube链接格式不正确",
                content="请确保输入有效的YouTube链接：\n\n✅ 正确格式示例：\n• https://www.youtube.com/watch?v=dQw4w9WgXcQ\n• https://youtu.be/dQw4w9WgXcQ\n• https://www.youtube.com/playlist?list=PLrAXtmRdnEQy6nuLvVUF\n\n🔧 常见问题：\n• 链接是否完整？\n• 视频是否设为私有？\n• 网络连接是否正常？",
                action_button={"text": "重试", "action": "retry_input"},
                position="inline-error",
                priority=10,
                max_show_times=5
            ),
            
            "quota_exceeded": GuideContent(
                id="quota_exceeded",
                type=GuideType.ERROR_HELP,
                title="⚠️ 分析次数已达到限制",
                content="基础用户每月可进行10次分析。\n\n🚀 升级到Premium解锁：\n• ♾️ 无限分析次数\n• ⚡ 更快处理速度\n• 🎯 高级分析功能\n• 📊 数据导出权限\n\n💡 或等待下月配额重置。",
                action_button={"text": "升级Premium", "action": "upgrade_premium"},
                dismiss_button={"text": "稍后升级", "action": "dismiss"},
                position="center",
                priority=9,
                max_show_times=3
            ),
            
            # 功能提示
            "bulk_analysis_tip": GuideContent(
                id="bulk_analysis_tip",
                type=GuideType.TOOLTIP,
                title="💡 批量分析技巧",
                content="Premium用户可以同时分析多个视频！\n\n🎯 适用场景：\n• 分析整个播放列表\n• 对比多个创作者\n• 研究系列视频\n\n只需在URL输入框中每行放一个链接即可。",
                auto_show_conditions=["is_premium", "multiple_urls_detected"],
                position="tooltip-right",
                priority=5,
                max_show_times=2
            ),
            
            "export_results_tip": GuideContent(
                id="export_results_tip", 
                type=GuideType.TOOLTIP,
                title="📊 导出分析结果",
                content="您可以导出分析结果用于：\n• 📝 制作报告或演示\n• 📈 进一步数据分析\n• 💾 本地存储备份\n\n支持PDF、Excel、JSON等多种格式。",
                auto_show_conditions=["analysis_completed", "is_premium"],
                position="tooltip-left",
                priority=4,
                max_show_times=2
            )
        }
    
    def get_contextual_guides(self, context: Dict[str, Any]) -> List[GuideContent]:
        """根据上下文获取相关引导内容"""
        relevant_guides = []
        
        for guide in self.guides.values():
            if self._should_show_guide(guide, context):
                relevant_guides.append(guide)
        
        # 按优先级排序
        relevant_guides.sort(key=lambda g: g.priority, reverse=True)
        
        return relevant_guides
    
    def _should_show_guide(self, guide: GuideContent, context: Dict[str, Any]) -> bool:
        """判断是否应该显示某个引导"""
        if not guide.auto_show_conditions:
            return False
        
        for condition in guide.auto_show_conditions:
            if not self._evaluate_condition(condition, context):
                return False
        
        # 检查显示次数限制
        show_count = context.get('guide_show_counts', {}).get(guide.id, 0)
        if show_count >= guide.max_show_times:
            return False
        
        return True
    
    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """评估显示条件"""
        # 简单条件评估
        if condition == "is_new_user":
            return context.get('is_new_user', False)
        elif condition == "first_visit":
            return context.get('visit_count', 0) <= 1
        elif condition == "is_premium":
            return context.get('user_tier') == 'premium'
        elif condition == "is_premium_user":
            return context.get('user_tier') == 'premium'
        elif condition == "first_premium_visit":
            return context.get('is_premium') and context.get('premium_visit_count', 0) <= 1
        elif condition == "viewing_scenario_list":
            return context.get('current_page') == 'scenario_list'
        elif condition == "focus_url_input":
            return context.get('focused_element') == 'url_input'
        elif condition == "analysis_started":
            return context.get('analysis_status') == 'started'
        elif condition == "first_analysis_completed":
            return context.get('analysis_count') == 1 and context.get('last_analysis_status') == 'completed'
        elif condition == "analysis_completed":
            return context.get('analysis_status') == 'completed'
        elif condition == "multiple_urls_detected":
            return context.get('input_url_count', 0) > 1
        elif "show_count <" in condition:
            # 处理显示次数条件，如 "show_count < 3"
            try:
                threshold = int(condition.split('<')[1].strip())
                guide_id = context.get('current_guide_id')
                show_count = context.get('guide_show_counts', {}).get(guide_id, 0)
                return show_count < threshold
            except:
                return False
        
        return False
    
    def get_tooltips_for_element(self, element_id: str, context: Dict[str, Any]) -> List[GuideContent]:
        """获取特定UI元素的工具提示"""
        tooltips = []
        
        # 根据元素ID匹配相关的工具提示
        element_tooltip_map = {
            'url_input': ['youtube_url_input'],
            'scenario_selector': ['scenario_selection_guide'],
            'premium_badge': ['premium_benefits_intro'],
            'bulk_input': ['bulk_analysis_tip'],
            'export_button': ['export_results_tip']
        }
        
        relevant_tooltip_ids = element_tooltip_map.get(element_id, [])
        
        for tooltip_id in relevant_tooltip_ids:
            if tooltip_id in self.guides:
                guide = self.guides[tooltip_id]
                if self._should_show_guide(guide, context):
                    tooltips.append(guide)
        
        return tooltips
    
    def get_error_help(self, error_type: str, context: Dict[str, Any]) -> Optional[GuideContent]:
        """获取错误相关的帮助内容"""
        error_guide_map = {
            'invalid_youtube_url': 'invalid_youtube_url',
            'quota_exceeded': 'quota_exceeded',
            'analysis_failed': 'analysis_error_help',
            'network_error': 'network_error_help'
        }
        
        guide_id = error_guide_map.get(error_type)
        if guide_id and guide_id in self.guides:
            return self.guides[guide_id]
        
        return None
    
    def generate_onboarding_flow(self, user_context: Dict[str, Any]) -> List[GuideContent]:
        """生成个性化的新用户引导流程"""
        onboarding_flow = []
        
        # 基础欢迎引导
        if user_context.get('is_new_user'):
            onboarding_flow.append(self.guides['welcome_new_user'])
        
        # Premium用户特殊引导
        if user_context.get('is_premium'):
            onboarding_flow.append(self.guides['premium_benefits_intro'])
        
        # 功能介绍引导
        onboarding_flow.append(self.guides['scenario_selection_guide'])
        
        return onboarding_flow
    
    def format_guide_for_frontend(self, guide: GuideContent, context: Dict[str, Any]) -> Dict[str, Any]:
        """为前端格式化引导内容"""
        return {
            'id': guide.id,
            'type': guide.type.value,
            'title': guide.title,
            'content': guide.content,
            'visual': guide.visual,
            'actions': {
                'primary': guide.action_button,
                'secondary': guide.dismiss_button
            },
            'display': {
                'position': guide.position or 'center',
                'priority': guide.priority,
                'auto_dismiss_seconds': 15 if guide.type == GuideType.TOOLTIP else None
            },
            'meta': {
                'show_count': context.get('guide_show_counts', {}).get(guide.id, 0),
                'max_shows': guide.max_show_times
            }
        }

# 全局用户引导管理器实例
user_guide_manager = UserGuideManager()