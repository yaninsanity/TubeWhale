"""
TubeWhale YouTube 总结模板系统
每个模板包含三个核心思考问题，用于生成更有针对性的分析
"""

from dataclasses import dataclass
from typing import List, Dict, Any
import json

@dataclass
class ThinkingQuestion:
    """思考问题数据结构"""
    id: str
    question: str
    purpose: str  # 问题的目的/意图
    weight: float = 1.0  # 权重

@dataclass
class YouTubeTemplate:
    """YouTube分析模板"""
    id: str
    name: str
    description: str
    role: str  # content-creator, marketing-expert, data-analyst
    category: str
    thinking_questions: List[ThinkingQuestion]
    analysis_focus: List[str]
    output_format: str
    is_active: bool = True

class YouTubeTemplateManager:
    """YouTube模板管理器"""
    
    def __init__(self):
        self.templates = self._initialize_templates()
    
    def _initialize_templates(self) -> Dict[str, YouTubeTemplate]:
        """初始化所有YouTube分析模板"""
        templates = {}
        
        # 内容创作者模板
        templates['content-engagement'] = YouTubeTemplate(
            id='content-engagement',
            name='观众参与度优化分析',
            description='深度分析视频的观众参与策略和互动效果',
            role='content-creator',
            category='engagement',
            thinking_questions=[
                ThinkingQuestion(
                    id='hook-analysis',
                    question='这个视频的开头15秒如何抓住观众注意力？使用了什么样的钩子(hook)策略？',
                    purpose='分析开头吸引力和留存策略'
                ),
                ThinkingQuestion(
                    id='engagement-triggers', 
                    question='视频中哪些时刻促使观众点赞、评论或分享？这些触发点的共同特征是什么？',
                    purpose='识别参与度触发机制'
                ),
                ThinkingQuestion(
                    id='retention-strategy',
                    question='创作者如何在整个视频中维持观众兴趣？使用了哪些防止观众流失的技巧？',
                    purpose='分析观众留存技巧'
                )
            ],
            analysis_focus=['hook_effectiveness', 'engagement_patterns', 'retention_techniques', 'call_to_action'],
            output_format='engagement_optimization_report'
        )
        
        templates['content-trending'] = YouTubeTemplate(
            id='content-trending',
            name='病毒传播潜力分析',
            description='评估视频的病毒传播可能性和热门因素',
            role='content-creator',
            category='viral',
            thinking_questions=[
                ThinkingQuestion(
                    id='viral-elements',
                    question='这个视频包含哪些具有病毒传播潜力的元素？（情感、争议、实用性、娱乐性）',
                    purpose='识别病毒传播要素'
                ),
                ThinkingQuestion(
                    id='trending-timing',
                    question='视频发布的时机和内容是否契合当前热点话题或趋势？',
                    purpose='分析趋势契合度'
                ),
                ThinkingQuestion(
                    id='shareability-factor',
                    question='观众为什么会主动分享这个视频？分享的动机是什么？',
                    purpose='评估分享动机'
                )
            ],
            analysis_focus=['viral_potential', 'trend_alignment', 'shareability', 'emotional_impact'],
            output_format='viral_potential_report'
        )
        
        templates['content-storytelling'] = YouTubeTemplate(
            id='content-storytelling',
            name='叙事结构深度分析',
            description='分析视频的叙事技巧和故事架构',
            role='content-creator',
            category='storytelling',
            thinking_questions=[
                ThinkingQuestion(
                    id='narrative-arc',
                    question='这个视频遵循什么样的叙事弧线？开头-发展-高潮-结尾的结构如何安排？',
                    purpose='分析叙事结构'
                ),
                ThinkingQuestion(
                    id='emotional-journey',
                    question='观众在观看过程中经历了怎样的情感变化？哪些时刻情感强度最高？',
                    purpose='追踪情感曲线'
                ),
                ThinkingQuestion(
                    id='story-devices',
                    question='创作者使用了哪些叙事技巧？（悬念、回放、对比、隐喻等）',
                    purpose='识别叙事技巧'
                )
            ],
            analysis_focus=['narrative_structure', 'emotional_arc', 'storytelling_devices', 'pacing'],
            output_format='storytelling_analysis_report'
        )
        
        # 营销专家模板
        templates['marketing-brand'] = YouTubeTemplate(
            id='marketing-brand',
            name='品牌价值传播分析',
            description='分析视频的品牌信息传递和价值主张',
            role='marketing-expert',
            category='branding',
            thinking_questions=[
                ThinkingQuestion(
                    id='brand-message',
                    question='这个视频传达了什么样的品牌核心信息？与品牌定位是否一致？',
                    purpose='分析品牌信息一致性'
                ),
                ThinkingQuestion(
                    id='target-alignment',
                    question='视频内容如何与目标受众的需求、兴趣和价值观产生共鸣？',
                    purpose='评估受众契合度'
                ),
                ThinkingQuestion(
                    id='differentiation',
                    question='视频如何展现品牌与竞争对手的差异化价值？独特卖点是什么？',
                    purpose='识别差异化优势'
                )
            ],
            analysis_focus=['brand_consistency', 'value_proposition', 'audience_alignment', 'differentiation'],
            output_format='brand_analysis_report'
        )
        
        templates['marketing-conversion'] = YouTubeTemplate(
            id='marketing-conversion',
            name='转化漏斗优化分析',
            description='分析视频的转化路径和销售漏斗效果',
            role='marketing-expert',
            category='conversion',
            thinking_questions=[
                ThinkingQuestion(
                    id='conversion-path',
                    question='视频如何引导观众从认知到考虑再到决策？转化路径是否清晰？',
                    purpose='分析转化路径设计'
                ),
                ThinkingQuestion(
                    id='objection-handling',
                    question='视频如何预见并处理潜在客户的疑虑和反对意见？',
                    purpose='评估异议处理策略'
                ),
                ThinkingQuestion(
                    id='urgency-creation',
                    question='创作者使用了哪些技巧来创造紧迫感和促进立即行动？',
                    purpose='分析行动促进机制'
                )
            ],
            analysis_focus=['conversion_funnel', 'cta_effectiveness', 'objection_handling', 'urgency_tactics'],
            output_format='conversion_optimization_report'
        )
        
        # 数据分析师模板
        templates['data-performance'] = YouTubeTemplate(
            id='data-performance',
            name='数据表现深度分析',
            description='基于数据指标分析视频表现和预测趋势',
            role='data-analyst',
            category='metrics',
            thinking_questions=[
                ThinkingQuestion(
                    id='metrics-correlation',
                    question='观看时长、点赞率、评论率之间存在什么样的关联性？哪个指标是主导因素？',
                    purpose='分析指标相关性'
                ),
                ThinkingQuestion(
                    id='audience-behavior',
                    question='从数据角度看，观众行为模式有什么特征？在哪些时间点流失率最高？',
                    purpose='分析观众行为模式'
                ),
                ThinkingQuestion(
                    id='performance-prediction',
                    question='基于当前数据趋势，这个视频的长期表现预期如何？增长潜力在哪里？',
                    purpose='预测性能表现'
                )
            ],
            analysis_focus=['metrics_analysis', 'behavioral_patterns', 'performance_forecasting', 'optimization_opportunities'],
            output_format='data_analysis_report'
        )
        
        templates['data-audience'] = YouTubeTemplate(
            id='data-audience',
            name='受众洞察分析',
            description='深度分析受众特征和行为模式',
            role='data-analyst',
            category='audience',
            thinking_questions=[
                ThinkingQuestion(
                    id='demographic-insights',
                    question='从视频内容和互动数据来看，核心受众的人口统计特征是什么？',
                    purpose='分析受众画像'
                ),
                ThinkingQuestion(
                    id='engagement-segments',
                    question='不同受众群体的参与行为有何差异？哪个群体的价值最高？',
                    purpose='细分受众价值'
                ),
                ThinkingQuestion(
                    id='growth-opportunities',
                    question='数据显示哪些受众群体有扩展潜力？如何吸引更多类似用户？',
                    purpose='识别增长机会'
                )
            ],
            analysis_focus=['audience_segmentation', 'demographic_analysis', 'engagement_patterns', 'growth_potential'],
            output_format='audience_insights_report'
        )
        
        return templates
    
    def get_template(self, template_id: str) -> YouTubeTemplate:
        """获取指定模板"""
        return self.templates.get(template_id)
    
    def get_templates_by_role(self, role: str) -> List[YouTubeTemplate]:
        """根据角色获取模板列表"""
        return [template for template in self.templates.values() 
                if template.role == role and template.is_active]
    
    def get_all_templates(self) -> List[YouTubeTemplate]:
        """获取所有活跃模板"""
        return [template for template in self.templates.values() if template.is_active]
    
    def customize_template_questions(self, template_id: str, custom_questions: List[str]) -> YouTubeTemplate:
        """自定义模板的思考问题"""
        template = self.get_template(template_id)
        if not template:
            return None
            
        # 创建自定义思考问题
        custom_thinking_questions = []
        for i, question in enumerate(custom_questions[:3]):  # 限制最多3个问题
            custom_thinking_questions.append(
                ThinkingQuestion(
                    id=f'custom-{i+1}',
                    question=question,
                    purpose='用户自定义分析角度'
                )
            )
        
        # 创建新的模板实例
        customized_template = YouTubeTemplate(
            id=f'{template.id}-custom',
            name=f'{template.name} (自定义)',
            description=f'{template.description} - 包含用户自定义思考问题',
            role=template.role,
            category=template.category,
            thinking_questions=custom_thinking_questions,
            analysis_focus=template.analysis_focus,
            output_format=template.output_format
        )
        
        return customized_template
    
    def export_template_config(self, template_id: str) -> Dict[str, Any]:
        """导出模板配置为字典格式"""
        template = self.get_template(template_id)
        if not template:
            return {}
            
        return {
            'template_id': template.id,
            'template_name': template.name,
            'role': template.role,
            'thinking_questions': [
                {
                    'id': q.id,
                    'question': q.question,
                    'purpose': q.purpose,
                    'weight': q.weight
                }
                for q in template.thinking_questions
            ],
            'analysis_focus': template.analysis_focus,
            'output_format': template.output_format
        }

# 全局模板管理器实例
youtube_template_manager = YouTubeTemplateManager()

def get_youtube_templates():
    """获取YouTube模板管理器实例"""
    return youtube_template_manager