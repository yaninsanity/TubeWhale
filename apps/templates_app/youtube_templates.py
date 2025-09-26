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
        """Initialize all YouTube analysis templates - English-first intelligent platform"""
        templates = {}
        
        # ========== ENGLISH-FIRST PROFESSIONAL TEMPLATES ==========
        
        # 1. Comprehensive Video Analysis - Premium Template
        templates['comprehensive_analysis'] = YouTubeTemplate(
            id='comprehensive_analysis',
            name='Comprehensive Video Analysis',
            description='Complete multi-dimensional analysis covering performance, content quality, audience engagement, and strategic recommendations',
            role='content-creator',
            category='comprehensive',
            thinking_questions=[
                ThinkingQuestion(
                    id='content-quality-assessment',
                    question='What is the overall production quality and content value of this video? How does it compare to industry standards?',
                    purpose='Evaluate technical and creative quality benchmarks'
                ),
                ThinkingQuestion(
                    id='audience-engagement-analysis',
                    question='How effectively does this video engage its target audience? What engagement patterns and retention strategies are evident?',
                    purpose='Analyze viewer interaction and engagement effectiveness'
                ),
                ThinkingQuestion(
                    id='strategic-optimization-opportunities',
                    question='What are the primary optimization opportunities for improving performance, reach, and monetization potential?',
                    purpose='Identify actionable improvement strategies'
                )
            ],
            analysis_focus=['content_quality', 'audience_engagement', 'performance_metrics', 'optimization_opportunities', 'strategic_recommendations'],
            output_format='comprehensive_analysis_report'
        )
        
        # 2. Performance Benchmark Analysis - Free Template
        templates['performance_benchmark'] = YouTubeTemplate(
            id='performance_benchmark',
            name='Performance Benchmark Analysis',
            description='Quick performance evaluation with scoring metrics and competitive benchmarking',
            role='content-creator',
            category='performance',
            thinking_questions=[
                ThinkingQuestion(
                    id='performance-metrics-evaluation',
                    question='How do the key performance indicators (views, engagement rate, retention) compare to channel averages and industry benchmarks?',
                    purpose='Benchmark performance against standards'
                ),
                ThinkingQuestion(
                    id='competitive-positioning',
                    question='Where does this video rank compared to similar content in the niche? What competitive advantages or disadvantages are evident?',
                    purpose='Assess competitive market position'
                ),
                ThinkingQuestion(
                    id='improvement-priorities',
                    question='Based on performance data, what are the top 3 areas for immediate improvement to boost results?',
                    purpose='Prioritize optimization efforts'
                )
            ],
            analysis_focus=['performance_scoring', 'benchmark_comparison', 'competitive_analysis', 'improvement_recommendations'],
            output_format='performance_benchmark_report'
        )
        
        # 3. Competitive Intelligence Report - Premium Template
        templates['competitive_intelligence'] = YouTubeTemplate(
            id='competitive_intelligence',
            name='Competitive Intelligence Report',
            description='In-depth competitor analysis, market positioning, and strategic differentiation opportunities',
            role='marketing-expert',
            category='competitive',
            thinking_questions=[
                ThinkingQuestion(
                    id='competitor-strategy-analysis',
                    question='What content strategies, formats, and approaches are top competitors using successfully in this niche?',
                    purpose='Map competitive landscape and successful strategies'
                ),
                ThinkingQuestion(
                    id='differentiation-opportunities',
                    question='What unique angles, underserved topics, or content gaps exist that this creator could exploit for competitive advantage?',
                    purpose='Identify strategic differentiation opportunities'
                ),
                ThinkingQuestion(
                    id='market-positioning-strategy',
                    question='How should this content be positioned relative to competitors to maximize market share and audience capture?',
                    purpose='Develop competitive positioning strategy'
                )
            ],
            analysis_focus=['competitor_analysis', 'market_gaps', 'differentiation_strategy', 'positioning_recommendations'],
            output_format='competitive_intelligence_report'
        )
        
        # 4. Trend Forecasting & Market Timing - Premium Template  
        templates['trend_forecasting'] = YouTubeTemplate(
            id='trend_forecasting',
            name='Trend Forecasting & Market Timing',
            description='Advanced trend analysis, market timing predictions, and strategic content planning for viral potential',
            role='data-analyst',
            category='trends',
            thinking_questions=[
                ThinkingQuestion(
                    id='trend-lifecycle-analysis',
                    question='What stage of the trend lifecycle is this topic/format in? Is it emerging, peaking, or declining?',
                    purpose='Determine optimal timing for trend capitalization'
                ),
                ThinkingQuestion(
                    id='market-timing-predictions',
                    question='Based on current data patterns, when will this trend reach maximum virality and when should similar content be published?',
                    purpose='Predict optimal market timing windows'
                ),
                ThinkingQuestion(
                    id='future-trend-opportunities',
                    question='What emerging trends or topics should this creator prepare content for in the next 30-90 days?',
                    purpose='Forecast future content opportunities'
                )
            ],
            analysis_focus=['trend_analysis', 'timing_optimization', 'viral_prediction', 'future_planning'],
            output_format='trend_forecasting_report'
        )
        
        # 5. Audience Psychographic Profile - Premium Template
        templates['audience_psychographic'] = YouTubeTemplate(
            id='audience_psychographic',
            name='Audience Psychographic Profile',
            description='Deep psychological analysis of audience behavior, motivations, preferences, and content consumption patterns',
            role='data-analyst',
            category='audience',
            thinking_questions=[
                ThinkingQuestion(
                    id='psychological-drivers',
                    question='What psychological needs, desires, and motivations drive this audience to consume this type of content?',
                    purpose='Understand core audience psychology'
                ),
                ThinkingQuestion(
                    id='behavioral-patterns',
                    question='What specific viewing behaviors, engagement patterns, and content preferences characterize this audience segment?',
                    purpose='Map audience behavioral characteristics'
                ),
                ThinkingQuestion(
                    id='content-optimization-psychology',
                    question='How can content be psychologically optimized to better resonate with this audiences emotional and rational triggers?',
                    purpose='Develop psychologically-targeted content strategy'
                )
            ],
            analysis_focus=['psychological_analysis', 'behavioral_mapping', 'audience_segmentation', 'psychological_optimization'],
            output_format='audience_psychographic_report'
        )
        
        # 6. YouTube SEO Audit & Optimization - Free Template
        templates['seo_audit'] = YouTubeTemplate(
            id='seo_audit',
            name='YouTube SEO Audit & Optimization',
            description='Complete SEO analysis covering keyword optimization, discoverability, and search ranking factors',
            role='marketing-expert',
            category='seo',
            thinking_questions=[
                ThinkingQuestion(
                    id='keyword-optimization-analysis',
                    question='How effectively are relevant keywords integrated in the title, description, tags, and content for maximum discoverability?',
                    purpose='Evaluate keyword optimization effectiveness'
                ),
                ThinkingQuestion(
                    id='search-ranking-factors',
                    question='Which YouTube ranking factors (watch time, CTR, engagement) are optimized and which need improvement for better search visibility?',
                    purpose='Assess search ranking optimization'
                ),
                ThinkingQuestion(
                    id='discoverability-enhancement',
                    question='What specific SEO improvements would have the highest impact on organic discovery and suggested video placement?',
                    purpose='Prioritize SEO optimization efforts'
                )
            ],
            analysis_focus=['keyword_optimization', 'search_rankings', 'discoverability', 'seo_recommendations'],
            output_format='seo_audit_report'
        )
        
        # 7. Monetization Strategy & Revenue Optimization - Premium Template
        templates['monetization_strategy'] = YouTubeTemplate(
            id='monetization_strategy',
            name='Monetization Strategy & Revenue Optimization',
            description='Advanced revenue analysis, monetization opportunities, and strategic income diversification planning',
            role='marketing-expert',
            category='monetization',
            thinking_questions=[
                ThinkingQuestion(
                    id='revenue-stream-analysis',
                    question='What monetization opportunities (ads, sponsorships, products, memberships) are currently being leveraged or missed?',
                    purpose='Identify revenue optimization opportunities'
                ),
                ThinkingQuestion(
                    id='audience-monetization-potential',
                    question='Based on audience demographics and engagement, what monetization strategies would be most effective and profitable?',
                    purpose='Match monetization to audience characteristics'
                ),
                ThinkingQuestion(
                    id='revenue-scaling-strategy',
                    question='What systematic approach should be implemented to scale revenue while maintaining content quality and audience satisfaction?',
                    purpose='Develop sustainable revenue growth strategy'
                )
            ],
            analysis_focus=['revenue_analysis', 'monetization_opportunities', 'audience_value', 'scaling_strategy'],
            output_format='monetization_strategy_report'
        )
        
        # 8. Strategic Content Planning & Series Development - Free Template
        templates['content_planning'] = YouTubeTemplate(
            id='content_planning',
            name='Strategic Content Planning & Series Development',
            description='Long-term content strategy, series planning, and systematic content development for sustained growth',
            role='content-creator',
            category='strategy',
            thinking_questions=[
                ThinkingQuestion(
                    id='content-series-potential',
                    question='How can this video concept be expanded into a series or content pillar for sustained audience growth and engagement?',
                    purpose='Identify series and content expansion opportunities'
                ),
                ThinkingQuestion(
                    id='strategic-content-gaps',
                    question='What content gaps exist in the creators current strategy that should be filled to create a comprehensive content ecosystem?',
                    purpose='Map content strategy completeness'
                ),
                ThinkingQuestion(
                    id='long-term-planning',
                    question='What 90-day content roadmap would maximize growth potential while maintaining consistent quality and audience satisfaction?',
                    purpose='Develop strategic content calendar'
                )
            ],
            analysis_focus=['series_development', 'content_gaps', 'strategic_planning', 'growth_roadmap'],
            output_format='content_planning_report'
        )
        
        # ========== LEGACY CHINESE TEMPLATES (for backward compatibility) ==========
        
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