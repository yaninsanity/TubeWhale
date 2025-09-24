# -*- coding: utf-8 -*-
"""
用户场景配置系统
定义不同用户角色的预设配置和模板推荐
"""

SCENARIO_CONFIGS = {
    'content_creator': {
        'name': '内容创作者',
        'name_en': 'Content Creator',
        'description': '为YouTube创作者提供内容优化和趋势分析',
        'icon': 'fas fa-video',
        'color': '#ff6b6b',
        'engine_defaults': {
            'keyword': 'viral content',
            'max_n': 20,
            'top_k': 15,
            'filter_type': 'trending',
            'concurrency': 3,
            'analysis_depth': 'detailed'
        },
        'recommended_templates': [
            'viral_content_analysis',
            'content_optimization',
            'audience_engagement'
        ],
        'custom_prompts': {
            'keyword_generation': {
                'system': """你是一位经验丰富的YouTube内容创作专家。你的任务是为内容创作者生成能够提高视频曝光度和观看量的关键词变化。
重点关注：
- 病毒式传播潜力
- 观众参与度指标
- 内容趋势识别
- 竞争分析洞察""",
                'example': "基础关键词：'cooking tutorial' → 生成：'easy cooking hacks', 'viral cooking trends', 'beginner cooking tips', '10 minute meals'"
            },
            'summarization': {
                'system': """你是一位专业的内容策略师，专门为YouTube创作者提供可执行的内容洞察。
分析重点：
- 观众留存策略
- 内容差异化机会
- 变现潜力评估
- 创作者增长策略""",
                'example': "分析视频转录后，提供：观众兴趣点、内容改进建议、趋势机会、竞争对手分析"
            }
        }
    },
    
    'market_researcher': {
        'name': '市场研究员',
        'name_en': 'Market Researcher',
        'description': '深度市场分析和竞争情报收集',
        'icon': 'fas fa-chart-line',
        'color': '#4ecdc4',
        'engine_defaults': {
            'keyword': 'market trends',
            'max_n': 50,
            'top_k': 25,
            'filter_type': 'comprehensive',
            'concurrency': 5,
            'analysis_depth': 'comprehensive'
        },
        'recommended_templates': [
            'market_intelligence',
            'competitor_analysis',
            'trend_forecasting'
        ],
        'custom_prompts': {
            'keyword_generation': {
                'system': """你是一位资深市场研究分析师。生成关键词时要考虑：
- 市场细分机会
- 竞争格局分析
- 消费者行为洞察
- 行业发展趋势""",
                'example': "基础关键词：'sustainable fashion' → 生成：'eco-friendly clothing brands', 'sustainable fashion trends 2024', 'green textile innovations'"
            },
            'summarization': {
                'system': """你是一位专业的市场情报分析师，专注于提供战略性市场洞察。
分析框架：
- SWOT分析要素
- 市场机会识别
- 风险评估
- 竞争优势分析""",
                'example': "提供：市场规模评估、关键成功因素、竞争威胁、增长机会"
            }
        }
    },
    
    'academic_researcher': {
        'name': '学术研究者',
        'name_en': 'Academic Researcher',
        'description': '学术研究和数据驱动的深度分析',
        'icon': 'fas fa-graduation-cap',
        'color': '#45b7d1',
        'engine_defaults': {
            'keyword': 'research data',
            'max_n': 100,
            'top_k': 50,
            'filter_type': 'scholarly',
            'concurrency': 8,
            'analysis_depth': 'exhaustive'
        },
        'recommended_templates': [
            'academic_analysis',
            'data_mining',
            'research_synthesis'
        ],
        'custom_prompts': {
            'keyword_generation': {
                'system': """你是一位学术研究专家。生成的关键词应该：
- 具备学术严谨性
- 涵盖多个研究维度
- 支持系统性文献综述
- 包含跨学科视角""",
                'example': "基础关键词：'climate change' → 生成：'climate change mitigation strategies', 'anthropogenic climate factors', 'climate adaptation policies'"
            },
            'summarization': {
                'system': """你是一位学术分析师，专注于生成具有学术价值的研究洞察。
分析标准：
- 证据质量评估
- 方法论严谨性
- 理论框架应用
- 研究空白识别""",
                'example': "提供：研究假设、方法论评估、数据质量分析、理论贡献"
            }
        }
    },
    
    'business_analyst': {
        'name': '商业分析师',
        'name_en': 'Business Analyst',
        'description': '商业智能和战略决策支持',
        'icon': 'fas fa-briefcase',
        'color': '#f7b731',
        'engine_defaults': {
            'keyword': 'business intelligence',
            'max_n': 30,
            'top_k': 20,
            'filter_type': 'business',
            'concurrency': 4,
            'analysis_depth': 'strategic'
        },
        'recommended_templates': [
            'business_intelligence',
            'strategic_analysis',
            'performance_metrics'
        ],
        'custom_prompts': {
            'keyword_generation': {
                'system': """你是一位商业战略顾问。关键词生成要关注：
- 商业价值创造
- 运营效率优化
- 风险管理
- 投资回报分析""",
                'example': "基础关键词：'digital transformation' → 生成：'digital ROI optimization', 'business process automation', 'digital customer experience'"
            },
            'summarization': {
                'system': """你是一位商业咨询专家，专注于提供可执行的商业洞察。
分析重点：
- KPI影响评估
- 成本效益分析
- 实施可行性
- 风险缓解策略""",
                'example': "提供：业务影响评估、实施建议、资源需求、预期ROI"
            }
        }
    }
}

def get_scenario_config(scenario_key):
    """获取指定场景的配置"""
    return SCENARIO_CONFIGS.get(scenario_key, SCENARIO_CONFIGS['content_creator'])

def get_all_scenarios():
    """获取所有可用场景"""
    return SCENARIO_CONFIGS

def get_scenario_engine_defaults(scenario_key):
    """获取场景的引擎默认配置"""
    config = get_scenario_config(scenario_key)
    return config.get('engine_defaults', {})

def get_scenario_custom_prompts(scenario_key):
    """获取场景的自定义prompt"""
    config = get_scenario_config(scenario_key)
    return config.get('custom_prompts', {})