#!/usr/bin/env python3
"""
Standalone Template Manager for TubeWhale CLI
独立的模板管理器，不依赖Django，支持前端injection
"""

import json
import os
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

class StandaloneTemplateManager:
    """
    独立模板管理器，支持：
    1. 不依赖Django的独立运行
    2. 前端模板injection机制
    3. 预设模板扩展
    """
    
    def __init__(self):
        self.templates_cache = {}
        self.role_prompts = self._load_builtin_roles()
        self.injected_templates = {}
        self._load_builtin_templates()
        
    def _load_builtin_roles(self) -> Dict[str, Dict[str, str]]:
        """Load built-in role templates for professional analysis"""
        return {
            "content_creator": {
                "system_prompt": """You are a professional content creator specializing in YouTube video analysis. Your expertise includes:
- Video performance optimization and audience engagement analysis
- Content strategy and viral potential assessment
- Thumbnail, title, and description optimization
- Audience retention and interaction analysis
- Creator monetization and growth strategies

Analyze the provided video content from a content creator's perspective, focusing on:
1. Content quality and production value
2. Audience engagement potential
3. Optimization opportunities
4. Growth strategy recommendations
5. Monetization potential

Provide practical insights that can directly improve content performance.""",
                
                "analysis_focus": "engagement metrics, content optimization, viral potential, audience retention",
                "output_style": "practical, actionable recommendations with specific improvement suggestions"
            },
            
            "marketing_expert": {
                "system_prompt": """You are a marketing expert analyzing video content for brand performance and strategy. Your expertise includes:
- Brand messaging and positioning analysis
- Audience targeting and segmentation
- Marketing funnel optimization
- Conversion rate analysis
- ROI assessment and campaign effectiveness
- Competitive analysis and market positioning

Analyze the provided video content from a marketing perspective, focusing on:
1. Brand message effectiveness
2. Audience targeting accuracy
3. Marketing funnel performance
4. Conversion opportunities
5. Competitive positioning
6. ROI optimization strategies

Provide strategic marketing insights with measurable recommendations.""",
                
                "analysis_focus": "brand messaging, audience targeting, conversion rates, marketing ROI",
                "output_style": "strategic insights with measurable KPIs and conversion optimization recommendations"
            },
            
            "data_analyst": {
                "system_prompt": """You are a data analyst specializing in statistical analysis of YouTube content insights and patterns. Your expertise includes:
- Statistical analysis and data interpretation
- Performance metrics and KPI analysis
- Trend identification and forecasting
- A/B testing and experimentation
- Data visualization and reporting
- Predictive modeling and analytics

Analyze the provided video content from a data-driven perspective, focusing on:
1. Performance metrics and statistical significance
2. Trend analysis and pattern recognition
3. Correlation analysis between variables
4. Predictive insights and forecasting
5. Data-driven optimization recommendations
6. Statistical validation of hypotheses

Provide data-driven insights with statistical backing and measurable metrics.""",
                
                "analysis_focus": "statistical patterns, performance metrics, trend analysis, data correlations",
                "output_style": "data-driven analysis with statistical evidence, charts, and quantifiable insights"
            }
        }
    
    def _load_builtin_templates(self):
        """Load professional English templates aligned with TubeWhale's OpenAI analysis objectives"""
        self.templates_cache = {
            "comprehensive_analysis": {
                "id": "comprehensive_analysis",
                "name": "Comprehensive Video Analysis",
                "description": "Complete multi-dimensional analysis covering content quality, audience engagement, performance metrics, and optimization opportunities",
                "category": "analysis",
                "tier": "premium",
                "prompt": """Conduct a comprehensive analysis of the YouTube video "{video_title}":

## 📊 Content Analysis
{role_analysis_focus}
Evaluate content structure, pacing, value delivery, and production quality.

## 🎯 Performance Metrics Assessment
- Watch time retention: {watch_time}
- Engagement rate: {engagement_rate}
- Subscriber conversion: {subscription_rate}
- Click-through rate analysis
- Audience demographics breakdown

## 💡 Optimization Recommendations
Based on {role} expertise, provide specific improvement strategies:
{role_specific_prompt}

## 📈 Growth Potential Analysis
Predict performance improvements after implementing recommendations.
Include viral potential assessment and scaling opportunities.

## 🔍 Technical Insights
{custom_questions}

Provide actionable, data-driven recommendations for immediate implementation.""",
                "variables": ["video_title", "watch_time", "engagement_rate", "subscription_rate", "custom_questions"],
                "parameters": {
                    "max_tokens": 2000,
                    "temperature": 0.7,
                    "role_adaptable": True
                }
            },
            
            "performance_benchmark": {
                "id": "performance_benchmark",
                "name": "Performance Benchmark Analysis",
                "description": "Quick performance evaluation with scoring metrics, ideal for batch analysis and initial screening",
                "category": "performance",
                "tier": "free",
                "prompt": """Performance benchmark analysis for "{video_title}":

## ⚡ Key Performance Indicators
- Title effectiveness: ___/10 (clickability, keyword optimization)
- Thumbnail impact: ___/10 (visual appeal, CTR potential)
- Content quality score: ___/10 (value delivery, production)
- Audience match: ___/10 (target demographic alignment)
- SEO optimization: ___/10 (discoverability factors)

## 🚀 Immediate Action Items
{role_specific_insights}
Prioritized recommendations for quick wins.

## 📊 Competitive Positioning
{key_metrics}
Compare against industry benchmarks and similar content.

## 🎯 Next Steps
{additional_context}
Specific action plan with timeline and expected impact.""",
                "variables": ["video_title", "key_metrics", "additional_context"],
                "parameters": {
                    "max_tokens": 800,
                    "temperature": 0.5,
                    "role_adaptable": True
                }
            },
            
            "competitive_intelligence": {
                "id": "competitive_intelligence",
                "name": "Competitive Intelligence Report",
                "description": "In-depth competitor analysis identifying market gaps, positioning opportunities, and strategic advantages",
                "category": "strategy",
                "tier": "premium",
                "prompt": """Competitive intelligence analysis for "{video_title}":

## 🔍 Market Landscape Analysis
{competitor_context}
Map the competitive environment and identify key players.

## 📊 Performance Comparison Matrix
- Our strengths: {our_strengths}
- Competitor advantages: {competitor_strengths}
- Market opportunities: {opportunity_gaps}
- Differentiation factors: List unique value propositions

## 🎯 Strategic Positioning
From {role} perspective, develop positioning strategy:
{role_specific_strategy}

## 📈 Market Entry Strategy
{action_items}
Detailed roadmap for competitive advantage.

## 🚀 Implementation Timeline
Prioritized action plan with resource requirements and success metrics.""",
                "variables": ["video_title", "competitor_context", "our_strengths", "competitor_strengths", "opportunity_gaps", "action_items"],
                "parameters": {
                    "max_tokens": 1500,
                    "temperature": 0.6,
                    "role_adaptable": True
                }
            },
            
            "trend_forecasting": {
                "id": "trend_forecasting",
                "name": "Trend Forecasting & Market Timing",
                "description": "Advanced trend analysis with market timing predictions and content strategy alignment",
                "category": "market",
                "tier": "premium",
                "prompt": """Trend forecasting analysis for "{video_title}":

## 📈 Trend Identification & Validation
{trend_context}
Analyze current trend momentum and sustainability.

## ⏰ Market Timing Analysis
- Trend heat index: {trend_heat} (1-100 scale)
- Market saturation level: {market_saturation}
- Competition intensity: {competition_level}
- Trend lifecycle stage: {trend_duration}
- Optimal entry window: Calculate ideal timing

## 🎯 Content Strategy Alignment
{role_specific_insights}
Align content with trend trajectory for maximum impact.

## 🚀 Execution Roadmap
- Content angles: {content_angles}
- Publishing schedule: {optimal_timing}
- Distribution strategy: {promotion_strategy}
- Success metrics: Define KPIs for trend capitalization

## 🔮 Future Opportunities
{additional_insights}
Predict emerging trends and prepare content pipeline.""",
                "variables": ["video_title", "trend_context", "trend_heat", "market_saturation", "competition_level", "trend_duration", "content_angles", "optimal_timing", "promotion_strategy", "additional_insights"],
                "parameters": {
                    "max_tokens": 1800,
                    "temperature": 0.7,
                    "role_adaptable": True
                }
            },
            
            "audience_psychographics": {
                "id": "audience_psychographics",
                "name": "Audience Psychographic Profile",
                "description": "Deep psychological and behavioral analysis of target audience segments with actionable insights",
                "category": "audience",
                "tier": "premium",
                "prompt": """Audience psychographic analysis for "{video_title}":

## 👥 Demographic Foundation
{audience_demographics}
Build comprehensive audience persona with statistical backing.

## 🧠 Behavioral Pattern Analysis
- Viewing preferences: {viewing_preferences}
- Engagement patterns: {engagement_patterns}
- Content consumption timing: {consumption_timing}
- Device usage patterns: {device_usage}
- Social sharing behavior: Analyze virality factors

## 💭 Psychological Insights
{role} professional analysis of audience motivations:
{role_specific_insights}

## 🎯 Segmentation Strategy
- Primary audience: {core_audience}
- Secondary segments: {potential_audience}
- Content preferences by segment: {content_preferences}
- Communication style optimization: {communication_style}

## 📊 Data-Driven Validation
{audience_data}
Statistical evidence supporting audience insights.

## 💡 Personalization Strategy
{optimization_recommendations}
Tailored content strategies for each audience segment.""",
                "variables": ["video_title", "audience_demographics", "viewing_preferences", "engagement_patterns", "consumption_timing", "device_usage", "core_audience", "potential_audience", "content_preferences", "communication_style", "audience_data", "optimization_recommendations"],
                "parameters": {
                    "max_tokens": 2200,
                    "temperature": 0.6,
                    "role_adaptable": True
                }
            },
            
            "seo_audit": {
                "id": "seo_audit",
                "name": "YouTube SEO Audit & Optimization",
                "description": "Complete YouTube SEO analysis with keyword research, optimization recommendations, and discoverability enhancement",
                "category": "optimization",
                "tier": "free",
                "prompt": """YouTube SEO audit for "{video_title}":

## 🔍 Keyword Intelligence
- Primary keywords: {primary_keywords}
- Long-tail opportunities: {long_tail_keywords}
- Search volume analysis: {search_volume}
- Keyword difficulty assessment: {keyword_difficulty}
- Semantic keyword clusters: Map related terms

## 📝 Content Optimization Analysis
{role_specific_insights}
Technical SEO improvements for better discoverability.

## 🎯 Title Optimization
- Current title score: {title_score}/10
- Optimization recommendations: {title_suggestions}
- CTR prediction model: {ctr_prediction}
- A/B testing variations: Suggest alternatives

## 📄 Description Enhancement
- Description quality assessment: {description_quality}
- Keyword density optimization: {keyword_density}
- Improvement recommendations: {description_improvements}
- Call-to-action optimization: Enhance engagement prompts

## 🏷️ Tag Strategy Optimization
- Recommended tags: {recommended_tags}
- Tag weight distribution: {tag_weights}
- Tag combination analysis: {tag_combinations}
- Competitor tag analysis: Identify gaps

## 📊 Performance Projections
{seo_projections}
Expected improvement metrics after optimization implementation.""",
                "variables": ["video_title", "primary_keywords", "long_tail_keywords", "search_volume", "keyword_difficulty", "title_score", "title_suggestions", "ctr_prediction", "description_quality", "keyword_density", "description_improvements", "recommended_tags", "tag_weights", "tag_combinations", "seo_projections"],
                "parameters": {
                    "max_tokens": 1600,
                    "temperature": 0.5,
                    "role_adaptable": True
                }
            },
            
            "monetization_strategy": {
                "id": "monetization_strategy",
                "name": "Monetization Strategy & Revenue Optimization",
                "description": "Comprehensive revenue analysis with multiple income stream development and optimization strategies",
                "category": "business",
                "tier": "premium",
                "prompt": """Monetization strategy analysis for "{video_title}":

## 💰 Revenue Stream Analysis
- Current revenue: {current_revenue}
- CPM estimation: {estimated_cpm}
- Ad revenue optimization: {ad_revenue}
- Alternative income sources: {other_revenue}
- Revenue per view calculation: Analyze efficiency

## 🎯 Monetization Opportunity Matrix
{role_specific_strategy}
Identify and prioritize revenue opportunities.

## 📈 Multi-Stream Revenue Strategy
- Ad revenue optimization: {ad_optimization}
- Sponsorship opportunities: {sponsorship_opportunities}
- Product integration: {product_promotion}
- Membership/subscription model: {membership_content}
- Affiliate marketing potential: Assess commission opportunities

## 🛍️ Business Model Development
- Optimal monetization mix: {monetization_methods}
- Revenue forecasting: {revenue_projection}
- Implementation priority matrix: {implementation_priority}
- ROI calculations: Expected returns per strategy

## 📊 Performance Tracking Framework
{tracking_metrics}
KPIs for monitoring monetization success.

## 🚀 90-Day Action Plan
{action_plan}
Detailed implementation roadmap with milestones and success metrics.""",
                "variables": ["video_title", "current_revenue", "estimated_cpm", "ad_revenue", "other_revenue", "ad_optimization", "sponsorship_opportunities", "product_promotion", "membership_content", "monetization_methods", "revenue_projection", "implementation_priority", "tracking_metrics", "action_plan"],
                "parameters": {
                    "max_tokens": 2000,
                    "temperature": 0.6,
                    "role_adaptable": True
                }
            },
            
            "content_strategy": {
                "id": "content_strategy",
                "name": "Strategic Content Planning & Series Development",
                "description": "Systematic content planning with series development, publishing optimization, and audience growth strategies",
                "category": "planning",
                "tier": "free",
                "prompt": """Strategic content planning based on "{video_title}":

## 📅 Content Calendar Development
{content_calendar}
Strategic publishing schedule aligned with audience behavior and market trends.

## 🎬 Series Architecture
- Theme expansion opportunities: {theme_extensions}
- Content depth scaling: {content_depth}
- Publishing frequency optimization: {update_frequency}
- Seasonal content integration: {seasonal_considerations}
- Cross-platform content adaptation: Multi-channel strategy

## 🎯 Content Strategy Framework
{role_specific_strategy}
Systematic approach to content creation and audience building.

## 📊 Performance Forecasting
- Expected view projections: {expected_views}
- Engagement rate predictions: {engagement_forecast}
- Growth trajectory modeling: {growth_potential}
- Subscriber acquisition forecast: Calculate growth rates

## 💡 Creative Development Pipeline
{creative_ideas}
Systematic idea generation and content variation strategies.

## 🔄 Content Ecosystem Design
{content_cycle}
Interconnected content strategy for maximum audience retention and growth.""",
                "variables": ["video_title", "content_calendar", "theme_extensions", "content_depth", "update_frequency", "seasonal_considerations", "expected_views", "engagement_forecast", "growth_potential", "creative_ideas", "content_cycle"],
                "parameters": {
                    "max_tokens": 1400,
                    "temperature": 0.8,
                    "role_adaptable": True
                }
            }
        }

    def inject_template(self, template_data: Dict[str, Any]) -> bool:
        """
        前端模板injection接口
        允许前端动态注入自定义模板
        """
        try:
            template_id = template_data.get('id')
            if not template_id:
                return False
                
            # 验证模板结构
            required_fields = ['id', 'name', 'prompt']
            if not all(field in template_data for field in required_fields):
                return False
            
            # 添加注入时间戳
            template_data['injected_at'] = datetime.now().isoformat()
            template_data['source'] = 'frontend_injection'
            
            # 注入到缓存
            self.injected_templates[template_id] = template_data
            self.templates_cache[template_id] = template_data
            
            return True
            
        except Exception as e:
            print(f"❌ Template injection failed: {e}")
            return False
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """获取模板"""
        return self.templates_cache.get(template_id)
    
    def list_templates(self, category: Optional[str] = None, tier: Optional[str] = None) -> List[Dict[str, Any]]:
        """列出模板"""
        templates = list(self.templates_cache.values())
        
        if category:
            templates = [t for t in templates if t.get('category') == category]
        
        if tier:
            templates = [t for t in templates if t.get('tier') == tier]
            
        return templates
    
    def get_role_prompt(self, role: str) -> Optional[Dict[str, str]]:
        """获取角色提示"""
        return self.role_prompts.get(role)
    
    def compile_template(self, template_id: str, variables: Dict[str, Any], role: str = 'content_creator') -> Optional[str]:
        """编译模板，支持角色注入"""
        template = self.get_template(template_id)
        if not template:
            return None
            
        try:
            # 获取角色特定信息
            role_info = self.get_role_prompt(role)
            if role_info:
                variables.update({
                    'role': role,
                    'role_specific_prompt': role_info.get('system_prompt', ''),
                    'role_analysis_focus': role_info.get('analysis_focus', ''),
                    'role_specific_insights': f"基于{role}角色的专业见解...",
                    'role_specific_strategy': f"从{role}角度的策略建议..."
                })
            
            # 编译模板
            compiled = template['prompt'].format(**variables)
            return compiled
            
        except KeyError as e:
            print(f"❌ Missing variable for template compilation: {e}")
            return None
        except Exception as e:
            print(f"❌ Template compilation failed: {e}")
            return None
    
    def get_template_variables(self, template_id: str) -> List[str]:
        """获取模板所需变量"""
        template = self.get_template(template_id)
        if not template:
            return []
        
        return template.get('variables', [])
    
    def search_templates(self, query: str) -> List[Dict[str, Any]]:
        """搜索模板"""
        if not query:
            return self.list_templates()
            
        results = []
        query_lower = query.lower()
        
        for template in self.templates_cache.values():
            if (query_lower in template.get('name', '').lower() or 
                query_lower in template.get('description', '').lower() or
                query_lower in template.get('category', '').lower()):
                results.append(template)
                
        return results
    
    def get_injected_templates(self) -> Dict[str, Any]:
        """获取所有前端注入的模板"""
        return self.injected_templates.copy()
    
    def clear_injected_templates(self):
        """清除所有注入的模板"""
        for template_id in self.injected_templates:
            if template_id in self.templates_cache:
                del self.templates_cache[template_id]
        self.injected_templates.clear()


def get_standalone_template_manager() -> StandaloneTemplateManager:
    """获取独立模板管理器实例"""
    if not hasattr(get_standalone_template_manager, '_instance'):
        get_standalone_template_manager._instance = StandaloneTemplateManager()
    return get_standalone_template_manager._instance