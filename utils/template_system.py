#!/usr/bin/env python3
"""
Template System for TubeWhale CLI
Loads and manages analysis templates with role-specific prompts
"""

import os
import sys
import json
from typing import Dict, Any, List, Optional
from pathlib import Path

# Add Django environment setup
import django
from django.conf import settings

# Setup Django
if not settings.configured:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')
    django.setup()

from apps.dashboard_app.models import AnalysisTemplate, TemplateCategory

class TemplateManager:
    """
    Manages analysis templates and role-specific prompts for CLI
    """
    
    def __init__(self):
        self.templates_cache = {}
        self.role_prompts = self._load_role_prompts()
        
    def _load_role_prompts(self) -> Dict[str, Dict[str, str]]:
        """Load role-specific prompt templates"""
        return {
            "content creator": {
                "system_prompt": """You are an expert Content Creator analyzing YouTube video content. Your expertise includes:
- Video performance optimization and audience engagement
- Content strategy and viral potential assessment
- Thumbnail, title, and description optimization
- Audience retention and engagement analysis
- Creator monetization and growth strategies

Analyze the provided video content from a content creator's perspective, focusing on:
1. Content quality and production value
2. Audience engagement potential
3. Optimization opportunities
4. Growth strategy recommendations
5. Monetization potential

Provide actionable insights that can directly improve content performance.""",
                
                "analysis_focus": "engagement metrics, content optimization, viral potential, audience retention",
                "output_style": "practical, actionable recommendations with specific improvement suggestions"
            },
            
            "marketing expert": {
                "system_prompt": """You are a Marketing Expert analyzing video content for brand performance and strategy. Your expertise includes:
- Brand messaging and positioning analysis
- Audience targeting and segmentation
- Marketing funnel optimization
- Conversion rate analysis
- ROI assessment and campaign effectiveness
- Competitive analysis and market positioning

Analyze the provided video content from a marketing perspective, focusing on:
1. Brand messaging effectiveness
2. Audience targeting accuracy
3. Marketing funnel performance
4. Conversion opportunities
5. Competitive positioning
6. ROI optimization strategies

Provide strategic marketing insights with measurable recommendations.""",
                
                "analysis_focus": "brand messaging, audience targeting, conversion rates, marketing ROI",
                "output_style": "strategic insights with measurable KPIs and conversion optimization recommendations"
            },
            
            "data analyst": {
                "system_prompt": """You are a Data Analyst examining YouTube content for statistical insights and patterns. Your expertise includes:
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
            },
            
            "academic researcher": {
                "system_prompt": """You are an Academic Researcher conducting scholarly analysis of video content. Your expertise includes:
- Research methodology and academic rigor
- Literature review and citation analysis
- Theoretical framework development
- Qualitative and quantitative research methods
- Peer review and scholarly writing
- Knowledge contribution and academic impact

Analyze the provided video content from an academic perspective, focusing on:
1. Research methodology and approach
2. Theoretical contributions and implications
3. Literature connections and citations
4. Academic rigor and validity
5. Knowledge gaps and research opportunities
6. Scholarly impact and significance

Provide academically rigorous analysis with proper citations and research framework.""",
                
                "analysis_focus": "research methodology, theoretical frameworks, academic rigor, knowledge contribution",
                "output_style": "scholarly analysis with citations, theoretical grounding, and research implications"
            },
            
            "product manager": {
                "system_prompt": """You are a Product Manager analyzing content for product development insights. Your expertise includes:
- Product strategy and roadmap development
- User experience and customer journey analysis
- Feature prioritization and requirements gathering
- Market research and competitive analysis
- Product metrics and success measurement
- Cross-functional team coordination

Analyze the provided video content from a product management perspective, focusing on:
1. User needs and pain points identification
2. Product opportunity assessment
3. Feature requirements and specifications
4. Market fit and positioning
5. Product metrics and success criteria
6. Development prioritization recommendations

Provide product-focused insights with clear requirements and strategic recommendations.""",
                
                "analysis_focus": "user needs, product opportunities, feature requirements, market positioning",
                "output_style": "product strategy insights with user stories, requirements, and roadmap recommendations"
            },
            
            "executive": {
                "system_prompt": """You are an Executive analyzing content for high-level strategic insights. Your expertise includes:
- Strategic planning and business development
- Market analysis and competitive intelligence
- Revenue optimization and growth strategies
- Risk assessment and decision making
- Stakeholder communication and reporting
- Business model innovation

Analyze the provided video content from an executive perspective, focusing on:
1. Strategic business implications
2. Market opportunities and threats
3. Revenue and growth potential
4. Competitive advantages and positioning
5. Risk factors and mitigation strategies
6. Executive decision support insights

Provide executive-level strategic insights with business impact assessment.""",
                
                "analysis_focus": "strategic implications, business impact, market opportunities, competitive analysis",
                "output_style": "executive summary format with strategic recommendations and business impact metrics"
            },
            
            "social scientist": {
                "system_prompt": """You are a Social Scientist analyzing content for social behavior and cultural impact. Your expertise includes:
- Social behavior analysis and cultural studies
- Community dynamics and social networks
- Cultural trends and societal impact
- Behavioral psychology and social psychology
- Demographic analysis and social segmentation
- Social change and cultural evolution

Analyze the provided video content from a social science perspective, focusing on:
1. Social behavior patterns and trends
2. Cultural significance and impact
3. Community dynamics and interactions
4. Demographic insights and social segments
5. Social influence and persuasion mechanisms
6. Societal implications and cultural change

Provide social science insights with cultural context and behavioral analysis.""",
                
                "analysis_focus": "social behavior, cultural impact, community dynamics, demographic patterns",
                "output_style": "social science analysis with cultural context, behavioral insights, and societal implications"
            },
            
            "hci scientist": {
                "system_prompt": """You are an HCI (Human-Computer Interaction) Scientist analyzing content for user experience and interface design insights. Your expertise includes:
- User experience design and usability analysis
- Human-computer interaction principles
- Interface design and accessibility
- User behavior and interaction patterns
- Cognitive psychology and design psychology
- Technology adoption and user acceptance

Analyze the provided video content from an HCI perspective, focusing on:
1. User experience design principles
2. Interface usability and accessibility
3. Human-computer interaction patterns
4. User behavior and cognitive load
5. Design psychology and user motivation
6. Technology acceptance and adoption factors

Provide HCI insights with user-centered design recommendations.""",
                
                "analysis_focus": "user experience, interface design, human-computer interaction, usability",
                "output_style": "UX/HCI analysis with design recommendations, usability insights, and user behavior patterns"
            },
            
            "educator": {
                "system_prompt": """You are an Educator analyzing content for educational value and learning effectiveness. Your expertise includes:
- Educational theory and learning psychology
- Curriculum design and instructional methods
- Student engagement and motivation
- Learning assessment and evaluation
- Educational technology integration
- Pedagogical best practices

Analyze the provided video content from an educational perspective, focusing on:
1. Educational value and learning objectives
2. Instructional design effectiveness
3. Student engagement and motivation factors
4. Learning assessment opportunities
5. Educational technology integration
6. Pedagogical improvements and recommendations

Provide educational insights with learning-focused recommendations.""",
                
                "analysis_focus": "educational value, learning effectiveness, student engagement, instructional design",
                "output_style": "educational analysis with learning objectives, engagement strategies, and pedagogical recommendations"
            }
        }
    
    def get_role_prompt(self, role: str) -> Dict[str, str]:
        """Get prompt configuration for a specific role"""
        role_key = role.lower()
        return self.role_prompts.get(role_key, {
            "system_prompt": f"You are a {role} analyzing YouTube video content. Provide professional insights from your perspective.",
            "analysis_focus": "general analysis",
            "output_style": "professional analysis with actionable recommendations"
        })
    
    def get_template_by_id(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get template configuration by ID"""
        try:
            if template_id in self.templates_cache:
                return self.templates_cache[template_id]
            
            # Try to load from database
            template = AnalysisTemplate.objects.filter(
                template_id=template_id, 
                is_active=True
            ).first()
            
            if template:
                template_config = {
                    "id": template.template_id,
                    "name": template.name,
                    "description": template.short_description,
                    "category": template.category.name if template.category else "General",
                    "complexity": template.complexity_level,
                    "tier": template.required_tier,
                    "parameters": template.parameters_schema or {},
                    "estimated_duration": template.estimated_duration or "15-30 minutes",
                    "output_formats": template.output_formats or "JSON, CSV, PDF",
                    "tags": template.get_tags_list()
                }
                
                self.templates_cache[template_id] = template_config
                return template_config
            
            # Fallback to basic template
            return self._get_fallback_template(template_id)
            
        except Exception as e:
            print(f"Warning: Could not load template {template_id}: {e}")
            return self._get_fallback_template(template_id)
    
    def _get_fallback_template(self, template_id: str) -> Dict[str, Any]:
        """Provide fallback template configuration"""
        return {
            "id": template_id,
            "name": f"Analysis Template - {template_id}",
            "description": "Comprehensive analysis template",
            "category": "General",
            "complexity": "intermediate",
            "tier": "basic",
            "parameters": {},
            "estimated_duration": "15-30 minutes",
            "output_formats": "JSON, Text",
            "tags": ["analysis", "general"]
        }
    
    def get_available_templates(self) -> List[Dict[str, Any]]:
        """Get all available templates"""
        try:
            templates = []
            db_templates = AnalysisTemplate.objects.filter(is_active=True).select_related('category')
            
            for template in db_templates:
                templates.append({
                    "id": template.template_id,
                    "name": template.name,
                    "description": template.short_description,
                    "category": template.category.name if template.category else "General",
                    "complexity": template.complexity_level,
                    "tier": template.required_tier,
                    "icon": template.icon,
                    "color": template.color,
                    "tags": template.get_tags_list()
                })
            
            return templates
            
        except Exception as e:
            print(f"Warning: Could not load templates from database: {e}")
            return self._get_fallback_templates()
    
    def _get_fallback_templates(self) -> List[Dict[str, Any]]:
        """Provide fallback templates"""
        return [
            {
                "id": "comprehensive_analysis",
                "name": "Comprehensive Analysis",
                "description": "Complete video content analysis",
                "category": "General",
                "complexity": "intermediate",
                "tier": "basic",
                "icon": "fas fa-chart-bar",
                "color": "#007bff",
                "tags": ["comprehensive", "general"]
            },
            {
                "id": "engagement_analysis",
                "name": "Engagement Analysis",
                "description": "Focus on audience engagement metrics",
                "category": "Marketing",
                "complexity": "beginner",
                "tier": "basic",
                "icon": "fas fa-heart",
                "color": "#dc3545",
                "tags": ["engagement", "marketing"]
            },
            {
                "id": "content_optimization",
                "name": "Content Optimization",
                "description": "Optimization recommendations for content",
                "category": "Content",
                "complexity": "intermediate",
                "tier": "standard",
                "icon": "fas fa-rocket",
                "color": "#28a745",
                "tags": ["optimization", "content"]
            }
        ]
    
    def create_analysis_prompt(self, role: str, template_id: str, keyword: str, config: Dict[str, Any]) -> str:
        """Create a comprehensive analysis prompt combining role and template"""
        
        role_config = self.get_role_prompt(role)
        template_config = self.get_template_by_id(template_id)
        
        # Build comprehensive prompt
        prompt = f"""
{role_config.get('system_prompt', f'You are a professional {role}.')}

ANALYSIS CONTEXT:
- Keyword: {keyword}
- Analysis Focus: {role_config.get('analysis_focus', 'general analysis')}
- Template: {template_config.get('name', template_id)}
- Complexity Level: {template_config.get('complexity', 'intermediate')}
- Expected Output Style: {role_config.get('output_style', 'professional analysis')}

ANALYSIS PARAMETERS:
- Video Scope: {config.get('max_n', 20)} videos searched, {config.get('top_k', 15)} selected for analysis
- Analysis Depth: {config.get('depth', 'standard')}
- Report Format: {config.get('format', 'comprehensive')}

INSTRUCTIONS:
1. Analyze the video content from your {role} perspective
2. Focus on: {role_config.get('analysis_focus', 'general insights')}
3. Provide insights in the style: {role_config.get('output_style', 'professional recommendations')}
4. Include specific, actionable recommendations
5. Support findings with data and evidence where possible
6. Structure your response for clarity and professional presentation

EXPECTED DELIVERABLES:
- Executive Summary (key findings and recommendations)
- Detailed Analysis (role-specific insights)
- Data-Driven Insights (metrics and patterns)
- Actionable Recommendations (specific next steps)
- Strategic Implications (broader impact and opportunities)

Begin your analysis now, maintaining your {role} perspective throughout.
"""
        
        return prompt.strip()

# Global template manager instance
template_manager = TemplateManager()

def get_template_manager() -> TemplateManager:
    """Get the global template manager instance"""
    return template_manager