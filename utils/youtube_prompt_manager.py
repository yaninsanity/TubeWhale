#!/usr/bin/env python3
"""
YouTube-specific Prompt Management System
Ensures all prompts are targeted for YouTube content analysis, providing meaningful and professional analytical capabilities
"""

import json
import os
import logging
from typing import Dict, Optional, Any, List
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

class PromptType(Enum):
    """YouTube content analysis prompt types"""
    TRANSCRIPT_ANALYSIS = "transcript_analysis"
    TECHNICAL_REVIEW = "technical_review"
    EDUCATIONAL_ASSESSMENT = "educational_assessment"
    CONTENT_QUALITY = "content_quality"
    AUDIENCE_TARGETING = "audience_targeting"
    TREND_ANALYSIS = "trend_analysis"
    COMPETITIVE_ANALYSIS = "competitive_analysis"

@dataclass
class YouTubePromptTemplate:
    """YouTube-specific prompt template structure"""
    id: str
    name: str
    type: PromptType
    template: str
    variables: List[str]
    target_audience: str
    expected_output: str
    quality_metrics: List[str]
    created_at: str
    version: str

class YouTubePromptManager:
    """YouTube content analysis dedicated Prompt manager"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.templates: Dict[str, YouTubePromptTemplate] = {}
        self._load_default_templates()
    
    def _load_default_templates(self):
        """Load default YouTube-specific analysis templates"""
        
        # Technical Content Deep Analysis Template
        self.templates["tech_deep_analysis"] = YouTubePromptTemplate(
            id="tech_deep_analysis",
            name="Technical Content Deep Analysis",
            type=PromptType.TECHNICAL_REVIEW,
            template="""
Please provide a comprehensive professional analysis of the following YouTube technical video:

## Analysis Objectives
- Technical accuracy and depth assessment
- Practical utility and actionability analysis
- Target audience alignment evaluation
- Technology trends and cutting-edge assessment

## Video Basic Information
- Title: {title}
- Channel: {channel}
- Duration: {duration}
- Published: {publish_date}
- Keywords: {keyword}

## Transcript Content Analysis
{transcript}

## Expected Output Format
Please provide analysis following this structure:

### 1. Technical Depth Score (1-10 scale)
- Theoretical Depth: X/10
- Practical Value: X/10
- Cutting-edge Level: X/10

### 2. Content Quality Assessment
- Information Accuracy: [Accurate/Partially Accurate/Questionable]
- Logical Structure: [Clear/Average/Confusing]
- Example Quality: [Excellent/Good/Average]

### 3. Audience Alignment
- Target Audience: [Beginner/Intermediate/Advanced/Expert]
- Prerequisites: [List required knowledge]
- Learning Outcomes: [Specific description]

### 4. Key Technical Points
- Core Concepts: [List 3-5 concepts]
- Technology Stack: [Related technologies]
- Application Scenarios: [Real-world applications]

### 5. Improvement Recommendations
- Content Suggestions: [Specific recommendations]
- Presentation Optimization: [Areas for improvement]
- Supplementary Resources: [Recommended resources]
            """.strip(),
            variables=["title", "channel", "duration", "publish_date", "keyword", "transcript"],
            target_audience="Technical learners, developers, technology decision makers",
            expected_output="Structured technical content analysis report",
            quality_metrics=["Technical accuracy", "Depth scoring", "Practical utility", "Audience alignment"],
            created_at=datetime.now().isoformat(),
            version="1.0"
        )
        
        # Educational Content Assessment Template
        self.templates["educational_assessment"] = YouTubePromptTemplate(
            id="educational_assessment",
            name="Educational Content Assessment",
            type=PromptType.EDUCATIONAL_ASSESSMENT,
            template="""
Please conduct a comprehensive educational quality assessment of the following YouTube educational video:

## Educational Assessment Dimensions
- Learning objective clarity
- Content organization logic
- Knowledge transfer effectiveness
- Learning experience user-friendliness

## Video Information
- Title: {title}
- Educational Topic: {keyword}
- Video Duration: {duration}
- Key Knowledge Points: {transcript}

## Assessment Requirements
Please conduct a professional evaluation from an educational perspective, focusing on:
1. Scientific soundness of instructional design
2. Completeness and accuracy of knowledge points
3. Reasonableness of learning curve
4. Interactivity and engagement level
5. Actionability of practical applications

## Output Format
### Educational Quality Scoring
- Content Accuracy: X/10
- Teaching Logic: X/10
- Expression Clarity: X/10
- Practical Value: X/10

### Learning Effectiveness Prediction
- Suitable Learning Stage: [Description]
- Expected Learning Outcomes: [Specific description]
- Reinforcement Practice Recommendations: [Suggestions]

### Teaching Optimization Recommendations
- Structural Improvements: [Recommendations]
- Content Supplements: [Areas needing additional content]
- Expression Optimization: [Areas for improvement]
            """.strip(),
            variables=["title", "keyword", "duration", "transcript"],
            target_audience="Learners, educators, trainers",
            expected_output="Educational quality assessment report",
            quality_metrics=["Teaching effectiveness", "Content completeness", "Learning friendliness"],
            created_at=datetime.now().isoformat(),
            version="1.0"
        )
        
        # Comprehensive Content Quality Assessment Template
        self.templates["content_quality_comprehensive"] = YouTubePromptTemplate(
            id="content_quality_comprehensive",
            name="Comprehensive Content Quality Assessment",
            type=PromptType.CONTENT_QUALITY,
            template="""
Please conduct a comprehensive content quality assessment of the following YouTube video:

## Video Basic Information
- Title: {title}
- Channel: {channel}
- Category: {category}
- Keywords: {keyword}
- Viewing Data: {view_count} views, {like_count} likes

## Content Analysis Dimensions
### 1. Information Value Assessment
- Information density and depth
- Originality and uniqueness
- Timeliness and relevance

### 2. Expression Quality Assessment
- Language expression clarity
- Logical structure completeness
- Visual presentation professionalism

### 3. Audience Service Quality
- Target audience positioning accuracy
- Practicality and actionability
- Educational/entertainment value

## Transcript Content
{transcript}

## Expected Output
Please provide a comprehensive assessment report including:
- Overall quality score (1-100 points)
- Detailed analysis of each dimension
- Competitive advantage identification
- Improvement recommendations
- Recommendation index and rationale
            """.strip(),
            variables=["title", "channel", "category", "keyword", "view_count", "like_count", "transcript"],
            target_audience="Content creators, marketing professionals, viewers",
            expected_output="Comprehensive quality assessment report",
            quality_metrics=["Overall quality", "Information value", "Expression quality", "Audience service"],
            created_at=datetime.now().isoformat(),
            version="1.0"
        )
    
    def get_template(self, template_id: str) -> Optional[YouTubePromptTemplate]:
        """Get specified prompt template"""
        return self.templates.get(template_id)
    
    def get_tech_analysis_prompt(self) -> str:
        """Get technical analysis prompt template"""
        template = self.get_template("tech_deep_analysis")
        return template.template if template else "Technical analysis template not found"
    
    def get_educational_assessment_prompt(self) -> str:
        """Get educational assessment prompt template"""
        template = self.get_template("educational_assessment")
        return template.template if template else "Educational assessment template not found"
    
    def get_comprehensive_quality_prompt(self) -> str:
        """Get comprehensive quality prompt template"""
        template = self.get_template("quality_comprehensive")
        return template.template if template else "Comprehensive quality template not found"
    
    def list_templates(self) -> Dict[str, str]:
        """List all available templates"""
        return {tid: template.name for tid, template in self.templates.items()}
    
    def compose_prompt(self, template_id: str, variables: Dict[str, Any]) -> Optional[str]:
        """
        Compose final prompt using specified template and variables
        
        Args:
            template_id: Template ID
            variables: Variable dictionary containing video metadata and content
            
        Returns:
            Composed prompt string
        """
        template = self.get_template(template_id)
        if not template:
            self.logger.error(f"Template not found: {template_id}")
            return None
        
        try:
            # Validate required variables
            missing_vars = []
            for var in template.variables:
                if var not in variables:
                    missing_vars.append(var)
            
            if missing_vars:
                self.logger.warning(f"Missing variables for template {template_id}: {missing_vars}")
                # Provide default values for missing variables
                for var in missing_vars:
                    variables[var] = f"[{var}_not_provided]"
            
            # Compose prompt
            formatted_prompt = template.template.format(**variables)
            
            # Add quality requirements
            quality_section = """
## Analysis Quality Requirements
- Ensure analysis is based on transcript content facts
- Maintain objective and professional analytical attitude
- Provide specific actionable recommendations

Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """.strip()
            
            return f"{formatted_prompt}\n\n{quality_section}"
            
        except Exception as e:
            self.logger.error(f"Error composing prompt for {template_id}: {e}")
            return None
    
    def validate_prompt_quality(self, prompt: str) -> Dict[str, Any]:
        """
        Validate prompt quality, ensuring compliance with YouTube content analysis standards
        
        Returns:
            Quality assessment results
        """
        checks = {
            "length_appropriate": len(prompt) >= 200,
            "youtube_focused": "YouTube" in prompt or "video" in prompt,
            "analysis_structured": "analysis" in prompt.lower() and "assessment" in prompt.lower(),
            "actionable_output": "recommendation" in prompt.lower() or "suggestion" in prompt.lower(),
            "professional_tone": not any(word in prompt.lower() for word in ["bad", "terrible", "awful"])
        }
        
        quality_score = sum(checks.values()) / len(checks)
        
        return {
            "quality_score": quality_score,
            "checks": checks,
            "recommendations": self._generate_improvement_recommendations(checks)
        }
    
    def _generate_improvement_recommendations(self, checks: Dict[str, bool]) -> List[str]:
        """Generate improvement recommendations based on quality check results"""
        recommendations = []
        
        if not checks["length_appropriate"]:
            recommendations.append("Increase prompt detail level, provide more specific analytical guidance")
        
        if not checks["youtube_focused"]:
            recommendations.append("Strengthen YouTube platform characteristics, clarify video content analysis focus")
        
        if not checks["analysis_structured"]:
            recommendations.append("Add structured analysis framework, ensure systematic analysis")
        
        if not checks["actionable_output"]:
            recommendations.append("Include actionable recommendations and suggestions, improve practical value")
        
        if not checks["professional_tone"]:
            recommendations.append("Maintain professional tone, avoid subjective negative expressions")
        
        return recommendations
    
    def get_template_for_keyword(self, keyword: str) -> str:
        """Automatically select appropriate template based on keyword"""
        keyword_lower = keyword.lower()
        
        # Technical content keywords
        tech_keywords = ["programming", "coding", "development", "technical", "algorithm", "api", 
                        "framework", "library", "software", "engineering", "data science", 
                        "machine learning", "ai", "artificial intelligence"]
        
        # Educational content keywords
        edu_keywords = ["tutorial", "course", "learning", "education", "teaching", "training",
                       "beginner", "guide", "how to", "lesson", "class", "instruction"]
        
        if any(tech_word in keyword_lower for tech_word in tech_keywords):
            return "tech_deep_analysis"
        elif any(edu_word in keyword_lower for edu_word in edu_keywords):
            return "educational_assessment"
        else:
            return "content_quality_comprehensive"
    
    def export_templates(self, file_path: str) -> bool:
        """Export all templates to JSON file"""
        try:
            export_data = {}
            for tid, template in self.templates.items():
                export_data[tid] = {
                    "id": template.id,
                    "name": template.name,
                    "type": template.type.value,
                    "template": template.template,
                    "variables": template.variables,
                    "target_audience": template.target_audience,
                    "expected_output": template.expected_output,
                    "quality_metrics": template.quality_metrics,
                    "created_at": template.created_at,
                    "version": template.version
                }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Templates exported to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting templates: {e}")
            return False
    
    def import_templates(self, file_path: str) -> bool:
        """Import templates from JSON file"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"Template file not found: {file_path}")
                return False
            
            with open(file_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)
            
            for tid, template_data in import_data.items():
                template = YouTubePromptTemplate(
                    id=template_data["id"],
                    name=template_data["name"],
                    type=PromptType(template_data["type"]),
                    template=template_data["template"],
                    variables=template_data["variables"],
                    target_audience=template_data["target_audience"],
                    expected_output=template_data["expected_output"],
                    quality_metrics=template_data["quality_metrics"],
                    created_at=template_data["created_at"],
                    version=template_data["version"]
                )
                self.templates[tid] = template
            
            self.logger.info(f"Templates imported from {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing templates: {e}")
            return False