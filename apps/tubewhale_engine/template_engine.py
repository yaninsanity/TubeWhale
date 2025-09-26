"""
TubeWhale Dynamic Template Engine
动态模板系统 - 工业级最佳实践
"""

import json
import logging
from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass
from django.conf import settings

logger = logging.getLogger(__name__)


class AnalysisLevel(Enum):
    """Analysis depth levels"""
    BASIC = "basic"
    DETAILED = "detailed"
    EXPERT = "expert"
    COMPREHENSIVE = "comprehensive"


class ExpertRole(Enum):
    """Expert role types"""
    CONTENT_CREATOR = "content_creator"
    MARKETING_EXPERT = "marketing_expert"
    DATA_ANALYST = "data_analyst"
    EDUCATIONAL_SPECIALIST = "educational_specialist"
    BUSINESS_ANALYST = "business_analyst"
    RESEARCH_SCIENTIST = "research_scientist"
    SOCIAL_SCIENTIST = "social_scientist"
    HCI_SPECIALIST = "hci_specialist"
    MUSIC_EDUCATOR = "music_educator"


@dataclass
class AnalysisTemplate:
    """Dynamic analysis template configuration"""
    id: str
    name: str
    level: AnalysisLevel
    compatible_roles: List[ExpertRole]
    ai_system_prompt: str
    analysis_sections: List[str]
    output_format: Dict[str, Any]
    visualization_config: Dict[str, Any]
    processing_time_estimate: int  # seconds
    max_tokens: int
    temperature: float


class DynamicTemplateEngine:
    """
    Industrial-grade dynamic template engine
    Core value: Video content summarization from expert perspectives
    """
    
    def __init__(self):
        self.templates = {}
        self.role_configurations = {}
        self._initialize_templates()
        self._initialize_role_configs()
    
    def _initialize_templates(self):
        """Initialize predefined templates with industrial best practices"""
        
        # Basic Performance Template
        self.templates["basic_performance"] = AnalysisTemplate(
            id="basic_performance",
            name="Basic Performance Analysis",
            level=AnalysisLevel.BASIC,
            compatible_roles=list(ExpertRole),
            ai_system_prompt="""You are a professional video analyst. Provide concise, actionable insights focusing on key performance metrics and immediate improvement opportunities. Keep analysis practical and results-oriented.""",
            analysis_sections=[
                "performance_metrics",
                "audience_engagement", 
                "content_quality",
                "optimization_opportunities"
            ],
            output_format={
                "summary": "2-3 sentence executive summary",
                "key_metrics": "Quantitative performance indicators",
                "recommendations": "3-5 actionable improvement suggestions",
                "confidence_score": "Analysis reliability (0-1)"
            },
            visualization_config={
                "charts": ["engagement_timeline", "performance_radar"],
                "thumbnails": True,
                "youtube_integration": True
            },
            processing_time_estimate=30,
            max_tokens=1000,
            temperature=0.3
        )
        
        # Detailed Analysis Template
        self.templates["detailed_analysis"] = AnalysisTemplate(
            id="detailed_analysis",
            name="Detailed Expert Analysis",
            level=AnalysisLevel.DETAILED,
            compatible_roles=list(ExpertRole),
            ai_system_prompt="""You are a senior expert analyst with deep domain knowledge. Provide comprehensive analysis with detailed reasoning, supporting evidence, and strategic insights. Include both quantitative and qualitative assessments.""",
            analysis_sections=[
                "executive_summary",
                "content_analysis",
                "audience_psychology",
                "performance_benchmarking",
                "strategic_recommendations",
                "implementation_roadmap"
            ],
            output_format={
                "executive_summary": "Comprehensive overview with key findings",
                "detailed_analysis": "In-depth section-by-section analysis",
                "competitive_positioning": "Market context and benchmarking",
                "audience_insights": "Demographic and behavioral analysis",
                "action_plan": "Prioritized implementation steps",
                "success_metrics": "KPIs for measuring improvement"
            },
            visualization_config={
                "charts": ["comprehensive_dashboard", "trend_analysis", "competitive_comparison"],
                "thumbnails": True,
                "youtube_integration": True,
                "expert_annotations": True
            },
            processing_time_estimate=120,
            max_tokens=3000,
            temperature=0.4
        )
        
        # Expert Deep Dive Template
        self.templates["expert_deep_dive"] = AnalysisTemplate(
            id="expert_deep_dive",
            name="Expert Deep Dive Analysis",
            level=AnalysisLevel.EXPERT,
            compatible_roles=[
                ExpertRole.RESEARCH_SCIENTIST,
                ExpertRole.BUSINESS_ANALYST,
                ExpertRole.SOCIAL_SCIENTIST,
                ExpertRole.HCI_SPECIALIST
            ],
            ai_system_prompt="""You are a leading expert researcher with specialized domain knowledge. Provide scholarly-level analysis with theoretical frameworks, methodological rigor, and evidence-based insights. Include literature connections and future research directions.""",
            analysis_sections=[
                "theoretical_framework",
                "methodology_assessment",
                "empirical_findings",
                "comparative_analysis",  
                "implications_discussion",
                "future_directions",
                "expert_recommendations"
            ],
            output_format={
                "abstract": "Research-style summary of key findings",
                "theoretical_analysis": "Academic framework application",
                "empirical_evidence": "Data-driven insights with statistical context",
                "comparative_study": "Benchmarking against industry standards",
                "expert_opinion": "Professional judgment and predictions",
                "methodology_notes": "Analysis approach and limitations"
            },
            visualization_config={
                "charts": ["research_dashboard", "statistical_analysis", "trend_forecasting"],
                "academic_formatting": True,
                "citation_ready": True,
                "expert_profile": True
            },
            processing_time_estimate=300,
            max_tokens=5000,
            temperature=0.2
        )
        
        # Comprehensive Intelligence Template
        self.templates["comprehensive_intelligence"] = AnalysisTemplate(
            id="comprehensive_intelligence",
            name="Comprehensive Business Intelligence",
            level=AnalysisLevel.COMPREHENSIVE,
            compatible_roles=list(ExpertRole),
            ai_system_prompt="""You are a C-level strategic consultant combining multiple expert perspectives. Provide enterprise-grade analysis that integrates technical, business, and strategic insights. Focus on actionable intelligence for decision-makers.""",
            analysis_sections=[
                "strategic_overview",
                "multi_perspective_analysis",
                "risk_assessment",
                "opportunity_identification",
                "resource_requirements",
                "roi_projections",
                "implementation_strategy",
                "success_framework"
            ],
            output_format={
                "executive_briefing": "C-level summary with key decisions needed",
                "strategic_analysis": "Multi-dimensional business perspective",
                "risk_opportunity_matrix": "Comprehensive SWOT-style analysis",
                "resource_planning": "Budget, timeline, and team requirements",
                "success_roadmap": "Phased implementation with milestones",
                "kpi_framework": "Measurement and monitoring system"
            },
            visualization_config={
                "charts": ["executive_dashboard", "strategic_matrix", "roi_projections", "timeline_roadmap"],
                "executive_summary": True,
                "presentation_ready": True,
                "multi_format_export": True
            },
            processing_time_estimate=600,
            max_tokens=8000,
            temperature=0.3
        )
    
    def _initialize_role_configs(self):
        """Initialize role-specific configurations"""
        
        self.role_configurations = {
            ExpertRole.CONTENT_CREATOR: {
                "focus_areas": ["creativity", "engagement", "storytelling", "audience_retention"],
                "key_metrics": ["watch_time", "click_through_rate", "subscriber_growth", "engagement_rate"],
                "specialization": "Content optimization and audience engagement",
                "ai_enhancement": "Focus on creative insights and audience psychology"
            },
            
            ExpertRole.MARKETING_EXPERT: {
                "focus_areas": ["brand_positioning", "conversion_optimization", "campaign_performance", "roi_analysis"],
                "key_metrics": ["conversion_rate", "cost_per_acquisition", "brand_awareness", "market_share"],
                "specialization": "Marketing strategy and performance optimization",
                "ai_enhancement": "Marketing funnel analysis and conversion optimization"
            },
            
            ExpertRole.DATA_ANALYST: {
                "focus_areas": ["statistical_analysis", "trend_identification", "predictive_modeling", "performance_metrics"],
                "key_metrics": ["statistical_significance", "correlation_analysis", "trend_strength", "prediction_accuracy"],
                "specialization": "Data-driven insights and statistical analysis",
                "ai_enhancement": "Advanced analytics and predictive insights"
            },
            
            ExpertRole.BUSINESS_ANALYST: {
                "focus_areas": ["business_value", "process_optimization", "strategic_alignment", "competitive_analysis"],
                "key_metrics": ["roi", "market_position", "operational_efficiency", "strategic_value"],
                "specialization": "Business strategy and operational excellence",
                "ai_enhancement": "Strategic business intelligence and competitive positioning"
            }
            # Add more role configurations as needed
        }
    
    def get_template(self, template_id: str) -> Optional[AnalysisTemplate]:
        """Get template by ID"""
        return self.templates.get(template_id)
    
    def get_compatible_templates(self, role: ExpertRole) -> List[AnalysisTemplate]:
        """Get templates compatible with a specific role"""
        return [
            template for template in self.templates.values()
            if role in template.compatible_roles
        ]
    
    def build_dynamic_prompt(
        self, 
        template: AnalysisTemplate, 
        role: ExpertRole, 
        video_data: Dict[str, Any],
        custom_requirements: Optional[str] = None
    ) -> str:
        """Build dynamic AI prompt combining template and role"""
        
        role_config = self.role_configurations.get(role, {})
        
        prompt = f"""
{template.ai_system_prompt}

EXPERT ROLE: {role.value.replace('_', ' ').title()}
SPECIALIZATION: {role_config.get('specialization', 'Professional analysis')}
ANALYSIS LEVEL: {template.level.value.upper()}

VIDEO INFORMATION:
- Title: {video_data.get('title', 'Unknown')}
- Duration: {video_data.get('duration', 'Unknown')}
- Views: {video_data.get('view_count', 0):,}
- Likes: {video_data.get('like_count', 0):,}
- Comments: {video_data.get('comment_count', 0):,}
- Channel: {video_data.get('channel_title', 'Unknown')}
- Description: {video_data.get('description', '')[:500]}...

FOCUS AREAS FOR THIS ANALYSIS:
{chr(10).join(f"- {area.replace('_', ' ').title()}" for area in role_config.get('focus_areas', []))}

KEY METRICS TO EVALUATE:
{chr(10).join(f"- {metric.replace('_', ' ').title()}" for metric in role_config.get('key_metrics', []))}

REQUIRED ANALYSIS SECTIONS:
{chr(10).join(f"- {section.replace('_', ' ').title()}" for section in template.analysis_sections)}

OUTPUT FORMAT REQUIREMENTS:
{json.dumps(template.output_format, indent=2)}

{f"ADDITIONAL REQUIREMENTS: {custom_requirements}" if custom_requirements else ""}

Provide a comprehensive analysis in valid JSON format that addresses all required sections with professional expertise from the {role.value.replace('_', ' ')} perspective.
"""
        
        return prompt
    
    def get_processing_estimate(self, template_id: str) -> Dict[str, Any]:
        """Get processing time and resource estimates"""
        template = self.templates.get(template_id)
        if not template:
            return {"error": "Template not found"}
        
        return {
            "estimated_time_seconds": template.processing_time_estimate,
            "estimated_time_human": self._format_duration(template.processing_time_estimate),
            "complexity_level": template.level.value,
            "max_tokens": template.max_tokens,
            "ai_calls_estimated": 1 if template.max_tokens <= 4000 else 2
        }
    
    def _format_duration(self, seconds: int) -> str:
        """Format duration in human readable format"""
        if seconds < 60:
            return f"{seconds} seconds"
        elif seconds < 3600:
            return f"{seconds // 60} minutes"
        else:
            return f"{seconds // 3600} hours {(seconds % 3600) // 60} minutes"
    
    def get_template_list(self) -> List[Dict[str, Any]]:
        """Get list of all available templates"""
        return [
            {
                "id": template.id,
                "name": template.name,
                "level": template.level.value,
                "description": f"{template.level.value.title()} level analysis with {len(template.analysis_sections)} sections",
                "compatible_roles": [role.value for role in template.compatible_roles],
                "estimated_time": self._format_duration(template.processing_time_estimate),
                "sections": template.analysis_sections
            }
            for template in self.templates.values()
        ]
    
    def validate_combination(self, template_id: str, role: str) -> Dict[str, Any]:
        """Validate template and role combination"""
        template = self.templates.get(template_id)
        if not template:
            return {"valid": False, "error": "Template not found"}
        
        try:
            role_enum = ExpertRole(role)
        except ValueError:
            return {"valid": False, "error": "Invalid expert role"}
        
        if role_enum not in template.compatible_roles:
            return {
                "valid": False,
                "error": f"Role {role} not compatible with template {template_id}",
                "compatible_roles": [r.value for r in template.compatible_roles]
            }
        
        return {
            "valid": True,
            "template": template.name,
            "role": role_enum.value,
            "estimated_processing": self.get_processing_estimate(template_id)
        }


# Global template engine instance
template_engine = DynamicTemplateEngine()