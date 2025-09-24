"""
场景化分析模板系统
Scenario-based Analysis Template System

基于具体使用场景而非用户等级的智能分析模板系统
让任何看过YouTube的用户都能轻松使用我们的分析框架
"""

from enum import Enum
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from django.utils.translation import gettext as _

class AnalysisScenario(Enum):
    """分析场景枚举"""
    # 教育内容分析
    EDUCATION_RESEARCH = "education_research"
    LEARNING_CONTENT = "learning_content" 
    TUTORIAL_ANALYSIS = "tutorial_analysis"
    
    # 商业研究分析  
    MARKET_RESEARCH = "market_research"
    COMPETITOR_ANALYSIS = "competitor_analysis"
    PRODUCT_REVIEW = "product_review"
    
    # 娱乐内容分析
    ENTERTAINMENT_TRENDS = "entertainment_trends"
    CONTENT_CREATOR_STUDY = "content_creator_study"
    VIRAL_CONTENT = "viral_content"
    
    # 新闻媒体分析
    NEWS_MONITORING = "news_monitoring" 
    PUBLIC_OPINION = "public_opinion"
    FACT_CHECKING = "fact_checking"
    
    # 技术内容分析
    TECH_REVIEW = "tech_review"
    PROGRAMMING_TUTORIAL = "programming_tutorial"
    TECH_TRENDS = "tech_trends"
    
    # 健康生活分析
    HEALTH_CONTENT = "health_content"
    FITNESS_TUTORIAL = "fitness_tutorial" 
    WELLNESS_RESEARCH = "wellness_research"

@dataclass
class ScenarioTemplate:
    """场景模板数据结构"""
    scenario: AnalysisScenario
    display_name: str
    description: str
    icon: str
    difficulty: str  # 'beginner', 'intermediate', 'advanced'
    estimated_time: str  # 预估分析时间
    use_cases: List[str]  # 具体使用案例
    ai_focus_areas: List[str]  # AI重点分析领域
    output_format: Dict[str, Any]  # 输出格式配置
    step_by_step_guide: List[Dict[str, str]]  # 分步指导
    tooltips: Dict[str, str]  # 提示信息
    examples: List[Dict[str, Any]]  # 示例案例
    best_practices: List[str]  # 最佳实践建议

class ScenarioTemplateManager:
    """场景模板管理器 - HCI最佳实践实现"""
    
    def __init__(self):
        self.templates = self._initialize_templates()
    
    def _initialize_templates(self) -> Dict[AnalysisScenario, ScenarioTemplate]:
        """初始化所有场景模板"""
        return {
            AnalysisScenario.EDUCATION_RESEARCH: ScenarioTemplate(
                scenario=AnalysisScenario.EDUCATION_RESEARCH,
                display_name=_("📚 Educational Content Research"),
                description=_("Analyze educational YouTube videos, extract knowledge points, teaching methods and learning effectiveness"),
                icon="📚",
                difficulty="beginner",
                estimated_time=_("5-10 minutes"),
                use_cases=[
                    _("Research online education trends"),
                    _("Analyze educational video quality"),
                    _("Extract course key knowledge points"),
                    _("Evaluate educational content effectiveness")
                ],
                ai_focus_areas=[
                    _("Knowledge point extraction"),
                    _("Teaching method analysis"), 
                    _("Learner interaction assessment"),
                    _("Content difficulty rating"),
                    _("Educational value scoring")
                ],
                output_format={
                    "summary": _("Educational content summary"),
                    "knowledge_points": _("Key knowledge points list"), 
                    "teaching_methods": _("Teaching method analysis"),
                    "difficulty_level": _("Content difficulty rating"),
                    "learning_outcomes": _("Expected learning outcomes")
                },
                step_by_step_guide=[
                    {
                        "step": _("Step 1: Enter YouTube video link"),
                        "description": _("As simple as pasting a link in YouTube search box"),
                        "tip": _("Supports individual videos or playlist links")
                    },
                    {
                        "step": _("Step 2: AI automatically analyzes educational content"),  
                        "description": _("Our AI analyzes videos like an excellent education expert"),
                        "tip": _("Includes speech-to-text, knowledge extraction, teaching method recognition")
                    },
                    {
                        "step": _("Step 3: Get intelligent analysis report"),
                        "description": _("Receive an easy-to-understand educational content analysis report"), 
                        "tip": _("Report includes knowledge points, teaching quality, learning suggestions, etc.")
                    }
                ],
                tooltips={
                    "knowledge_extraction": "AI会自动识别视频中的关键概念、定理、公式等知识点",
                    "teaching_quality": "评估讲解清晰度、逻辑性、互动性等教学质量指标",
                    "learning_outcomes": "预测观看此视频后学习者能获得的知识和技能"
                },
                examples=[
                    {
                        "title": "Khan Academy数学课程分析",
                        "description": "分析微积分教学视频的知识点结构",
                        "expected_output": "提取数学公式、解题步骤、难点解释"
                    },
                    {
                        "title": "编程教程效果评估", 
                        "description": "评估Python入门教程的教学质量",
                        "expected_output": "代码示例分析、学习路径建议、难度评级"
                    }
                ],
                best_practices=[
                    "选择完整的教学视频而非片段",
                    "确保视频音频清晰便于AI分析", 
                    "可批量分析同一主题的多个视频进行对比"
                ]
            ),
            
            AnalysisScenario.MARKET_RESEARCH: ScenarioTemplate(
                scenario=AnalysisScenario.MARKET_RESEARCH,
                display_name=_("📊 Market Research Analysis"), 
                description=_("Analyze business, product, market-related YouTube content, gain industry insights"),
                icon="📊",
                difficulty="intermediate", 
                estimated_time=_("10-15 minutes"),
                use_cases=[
                    _("Understand industry development trends"),
                    _("Analyze competitor strategies"),
                    _("Research consumer feedback"),
                    _("Discover business opportunities")
                ],
                ai_focus_areas=[
                    "市场趋势识别",
                    "竞品分析",
                    "用户反馈分析", 
                    "商业模式研究",
                    "价格策略分析"
                ],
                output_format={
                    "market_trends": "市场趋势洞察",
                    "competitor_analysis": "竞争对手分析",
                    "consumer_insights": "消费者洞察", 
                    "business_opportunities": "商业机会发现",
                    "risk_assessment": "市场风险评估"
                },
                step_by_step_guide=[
                    {
                        "step": "第1步：选择研究目标",
                        "description": "明确您想研究的市场或产品领域", 
                        "tip": "可以是特定品牌、产品类别或整个行业"
                    },
                    {
                        "step": "第2步：收集相关视频",
                        "description": "输入相关YouTube视频链接或关键词",
                        "tip": "建议包含产品评测、行业分析、用户评价等多类型视频"
                    },
                    {
                        "step": "第3步：AI智能商业分析", 
                        "description": "系统会像商业分析师一样深度解析内容",
                        "tip": "识别市场信号、竞争态势、用户需求等关键信息"
                    },
                    {
                        "step": "第4步：获取商业洞察报告",
                        "description": "收到专业的市场研究分析报告",
                        "tip": "包含趋势图表、SWOT分析、机会识别等"
                    }
                ],
                tooltips={
                    "trend_analysis": "AI分析视频中提到的市场变化、技术发展、消费习惯等趋势",
                    "sentiment_analysis": "评估用户对产品/服务的整体情感倾向和满意度",
                    "competitive_landscape": "识别竞争对手、对比优劣势、发现差异化机会"
                },
                examples=[
                    {
                        "title": "智能手机市场调研",
                        "description": "分析iPhone vs Android用户评价视频",
                        "expected_output": "用户偏好分析、功能对比、价格敏感度研究"
                    },
                    {
                        "title": "新能源汽车趋势研究",
                        "description": "研究Tesla、BYD等品牌的市场表现", 
                        "expected_output": "技术趋势、消费者接受度、政策影响分析"
                    }
                ],
                best_practices=[
                    "选择最新的视频内容以获得时效性洞察",
                    "包含不同观点的视频以获得全面分析",
                    "定期重复分析以跟踪市场变化趋势"
                ]
            ),
            
            AnalysisScenario.CONTENT_CREATOR_STUDY: ScenarioTemplate(
                scenario=AnalysisScenario.CONTENT_CREATOR_STUDY,
                display_name=_("🎬 Content Creator Analysis"),
                description=_("Research YouTube creators' content strategies, growth patterns and audience interaction"), 
                icon="🎬",
                difficulty="beginner",
                estimated_time="8-12分钟",
                use_cases=[
                    "学习成功创作者的策略",
                    "分析视频内容表现",
                    "研究受众互动模式", 
                    "优化内容创作方向"
                ],
                ai_focus_areas=[
                    "内容策略分析",
                    "受众参与度研究",
                    "视频制作技巧",
                    "增长模式识别",
                    "变现策略分析"
                ],
                output_format={
                    "content_strategy": "内容策略分析",
                    "audience_engagement": "受众互动分析", 
                    "growth_patterns": "增长模式研究",
                    "monetization_insights": "变现策略洞察",
                    "success_factors": "成功要素提取"
                },
                step_by_step_guide=[
                    {
                        "step": "第1步：选择要研究的创作者",
                        "description": "输入您感兴趣的YouTube创作者频道或视频",
                        "tip": "可以是单个创作者或多个同领域创作者对比"
                    },
                    {
                        "step": "第2步：AI深度内容分析",
                        "description": "系统分析视频内容、制作风格、互动策略",
                        "tip": "包括标题优化、封面设计、内容结构等要素"
                    },
                    {
                        "step": "第3步：受众行为洞察",
                        "description": "分析评论、点赞、分享等互动数据",
                        "tip": "识别哪些内容最受欢迎、什么时候发布效果最好"
                    },
                    {
                        "step": "第4步：获得创作指导建议",
                        "description": "收到个性化的内容创作优化建议",
                        "tip": "基于成功案例提供可执行的改进方案"
                    }
                ],
                tooltips={
                    "content_analysis": "AI会分析视频主题、结构、节奏、视觉效果等创作要素",
                    "engagement_metrics": "评估观众互动质量，包括评论情感、观看时长、回访率等",
                    "growth_tracking": "识别频道增长的关键节点和成功因素"
                },
                examples=[
                    {
                        "title": "美食博主成长策略研究",
                        "description": "分析知名美食频道的内容演进",
                        "expected_output": "菜谱选择策略、拍摄技巧进化、粉丝互动方式"
                    },
                    {
                        "title": "科技评测UP主分析",
                        "description": "研究科技频道的评测方法和观众反馈",
                        "expected_output": "评测标准、观众关注点、商业合作模式"
                    }
                ],
                best_practices=[
                    "选择不同发展阶段的视频进行对比分析",
                    "关注创作者的互动回复方式和频率",
                    "分析热门视频的共同特征和成功要素"
                ]
            )
        }
    
    def get_all_scenarios(self) -> List[ScenarioTemplate]:
        """获取所有场景模板"""
        return list(self.templates.values())
    
    def get_scenario_by_type(self, scenario: AnalysisScenario) -> Optional[ScenarioTemplate]:
        """根据场景类型获取模板"""
        return self.templates.get(scenario)
    
    def get_scenarios_by_difficulty(self, difficulty: str) -> List[ScenarioTemplate]:
        """根据难度获取场景模板"""
        return [template for template in self.templates.values() 
                if template.difficulty == difficulty]
    
    def get_recommended_scenarios_for_user(self, user_context: Dict[str, Any]) -> List[ScenarioTemplate]:
        """基于用户上下文推荐场景模板"""
        # 基于用户历史、偏好、经验水平推荐合适的分析场景
        recommendations = []
        
        # 新用户推荐简单易用的场景
        if user_context.get('is_new_user', True):
            recommendations.extend(self.get_scenarios_by_difficulty('beginner'))
        
        # 基于用户兴趣推荐
        user_interests = user_context.get('interests', [])
        for template in self.templates.values():
            if any(interest.lower() in template.description.lower() 
                   for interest in user_interests):
                recommendations.append(template)
        
        return recommendations[:6]  # 最多推荐6个，去重后返回
    
    def generate_user_guide(self, scenario: AnalysisScenario) -> Dict[str, Any]:
        """为特定场景生成完整的用户指导"""
        template = self.get_scenario_by_type(scenario)
        if not template:
            return {}
        
        return {
            'scenario_info': {
                'name': template.display_name,
                'description': template.description,
                'icon': template.icon,
                'difficulty': template.difficulty,
                'estimated_time': template.estimated_time
            },
            'quick_start': {
                'title': '快速开始 - 就像使用YouTube一样简单！',
                'steps': template.step_by_step_guide
            },
            'use_cases': {
                'title': '适用场景',
                'cases': template.use_cases
            },
            'ai_capabilities': {
                'title': 'AI分析能力',
                'areas': template.ai_focus_areas
            },
            'tooltips': template.tooltips,
            'examples': template.examples,
            'best_practices': {
                'title': '最佳实践建议',
                'tips': template.best_practices
            },
            'output_preview': {
                'title': '分析结果预览',
                'format': template.output_format
            }
        }

# 全局实例
scenario_manager = ScenarioTemplateManager()