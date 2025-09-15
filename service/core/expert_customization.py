#!/usr/bin/env python3
"""
Expert Domain Customization Framework
Advanced system for domain experts to create sophisticated analysis pipelines
"""

from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import json
import yaml
from pathlib import Path


class ExpertDomain(Enum):
    """Professional domains with specialized analysis requirements"""
    GENERAL = "general"
    BUSINESS_INTELLIGENCE = "business_intelligence" 
    MEDICAL_RESEARCH = "medical_research"
    ACADEMIC_RESEARCH = "academic_research"
    TECHNOLOGY_INNOVATION = "technology_innovation"
    EDUCATIONAL_CONTENT = "educational_content"
    SOCIAL_SCIENCE = "social_science"
    DATA_SCIENCE = "data_science"
    LEGAL_ANALYSIS = "legal_analysis"
    FINANCIAL_MARKETS = "financial_markets"
    ENVIRONMENTAL_SCIENCE = "environmental_science"
    PSYCHOLOGICAL_ANALYSIS = "psychological_analysis"


class AnalysisDepth(Enum):
    """Analysis complexity and depth levels"""
    OVERVIEW = "overview"           # Quick summary
    STANDARD = "standard"           # Comprehensive analysis
    EXPERT = "expert"               # Deep professional analysis
    RESEARCH_GRADE = "research_grade"  # Publication-quality analysis


class OutputFormat(Enum):
    """Structured output formats for different use cases"""
    MARKDOWN = "markdown"
    STRUCTURED_JSON = "structured_json"
    ACADEMIC_PAPER = "academic_paper"
    EXECUTIVE_SUMMARY = "executive_summary"
    TECHNICAL_REPORT = "technical_report"
    RESEARCH_NOTES = "research_notes"


@dataclass
class ExpertQuestion:
    """Sophisticated question structure for expert consultation"""
    question: str
    context: str = ""
    focus_areas: List[str] = field(default_factory=list)
    expected_format: OutputFormat = OutputFormat.STRUCTURED_JSON
    weight: float = 1.0  # Question importance weighting
    evaluation_criteria: List[str] = field(default_factory=list)
    follow_up_questions: List[str] = field(default_factory=list)


@dataclass
class ExpertPipeline:
    """Complete analysis pipeline for domain experts"""
    pipeline_id: str
    name: str
    domain: ExpertDomain
    description: str
    questions: List[ExpertQuestion]
    analysis_depth: AnalysisDepth
    output_format: OutputFormat
    preprocessing_steps: List[str] = field(default_factory=list)
    postprocessing_steps: List[str] = field(default_factory=list)
    quality_checks: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> List[str]:
        """Validate pipeline configuration"""
        errors = []
        
        if not self.questions:
            errors.append("Pipeline must contain at least one expert question")
        
        for i, question in enumerate(self.questions):
            if not question.question.strip():
                errors.append(f"Question {i+1} cannot be empty")
            
            if question.weight <= 0:
                errors.append(f"Question {i+1} weight must be positive")
        
        return errors


class ExpertTemplate(ABC):
    """Abstract base for domain-specific expert templates"""
    
    @abstractmethod
    def get_domain(self) -> ExpertDomain:
        """Return the expert domain this template serves"""
        pass
    
    @abstractmethod
    def get_default_questions(self) -> List[ExpertQuestion]:
        """Return default expert questions for this domain"""
        pass
    
    @abstractmethod
    def get_analysis_framework(self) -> str:
        """Return the analysis framework prompt template"""
        pass
    
    @abstractmethod
    def validate_input(self, content: str) -> List[str]:
        """Validate input content for domain-specific requirements"""
        pass
    
    def customize_pipeline(self, 
                          base_questions: List[ExpertQuestion],
                          custom_requirements: Dict[str, Any]) -> ExpertPipeline:
        """Customize pipeline based on expert requirements"""
        # Default implementation - subclasses can override
        return ExpertPipeline(
            pipeline_id=f"{self.get_domain().value}_custom",
            name=f"Custom {self.get_domain().value.title()} Analysis",
            domain=self.get_domain(),
            description="Customized expert analysis pipeline",
            questions=base_questions,
            analysis_depth=AnalysisDepth.EXPERT,
            output_format=OutputFormat.STRUCTURED_JSON
        )


class BusinessIntelligenceTemplate(ExpertTemplate):
    """Business intelligence and market analysis template"""
    
    def get_domain(self) -> ExpertDomain:
        return ExpertDomain.BUSINESS_INTELLIGENCE
    
    def get_default_questions(self) -> List[ExpertQuestion]:
        return [
            ExpertQuestion(
                question="What are the key strategic insights and market opportunities presented?",
                context="Strategic business analysis",
                focus_areas=["strategy", "market_analysis", "competitive_advantage"],
                weight=2.0,
                evaluation_criteria=["actionability", "strategic_relevance", "market_impact"]
            ),
            ExpertQuestion(
                question="How does this content relate to current industry trends and disruptions?",
                context="Industry trend analysis",
                focus_areas=["industry_trends", "disruption", "innovation"],
                weight=1.8,
                evaluation_criteria=["trend_alignment", "disruption_potential", "timing"]
            ),
            ExpertQuestion(
                question="What are the financial implications and ROI considerations?",
                context="Financial impact assessment",
                focus_areas=["financial_impact", "roi", "cost_benefit"],
                weight=1.5,
                evaluation_criteria=["financial_viability", "risk_assessment", "return_potential"]
            ),
            ExpertQuestion(
                question="What competitive advantages or strategic risks are identified?",
                context="Competitive landscape analysis",
                focus_areas=["competitive_advantage", "risk_assessment", "market_position"],
                weight=1.7,
                evaluation_criteria=["competitive_differentiation", "risk_severity", "mitigation_strategies"]
            )
        ]
    
    def get_analysis_framework(self) -> str:
        return """
BUSINESS INTELLIGENCE ANALYSIS FRAMEWORK

Video Title: {title}
Duration: {duration}
Analysis Depth: {analysis_depth}

STRATEGIC ANALYSIS:

1. MARKET INTELLIGENCE
   - Industry landscape and competitive dynamics
   - Market size, growth potential, and saturation
   - Key players and market share analysis
   - Emerging trends and disruption signals

2. BUSINESS MODEL EVALUATION
   - Revenue streams and monetization strategies
   - Value proposition and customer segments
   - Operational efficiency and scalability
   - Innovation capabilities and R&D focus

3. FINANCIAL ASSESSMENT
   - Revenue and profitability indicators
   - Investment requirements and funding
   - Cost structure and margin analysis
   - Risk factors and financial sustainability

4. STRATEGIC RECOMMENDATIONS
   - Actionable strategic initiatives
   - Investment priorities and resource allocation
   - Timeline and implementation roadmap
   - Success metrics and KPIs

EXPERT QUESTIONS:
{expert_questions}

CONTENT ANALYSIS:
{transcript}

Provide comprehensive business intelligence analysis addressing all expert questions with strategic recommendations.
"""
    
    def validate_input(self, content: str) -> List[str]:
        """Validate business content requirements"""
        errors = []
        
        # Check for business-relevant keywords
        business_keywords = [
            "business", "market", "strategy", "revenue", "profit", 
            "customer", "competition", "growth", "investment"
        ]
        
        content_lower = content.lower()
        if not any(keyword in content_lower for keyword in business_keywords):
            errors.append("Content may not be relevant for business intelligence analysis")
        
        if len(content) < 100:
            errors.append("Content too short for meaningful business analysis")
        
        return errors


class MedicalResearchTemplate(ExpertTemplate):
    """Medical research and healthcare analysis template"""
    
    def get_domain(self) -> ExpertDomain:
        return ExpertDomain.MEDICAL_RESEARCH
    
    def get_default_questions(self) -> List[ExpertQuestion]:
        return [
            ExpertQuestion(
                question="What is the clinical evidence quality and research methodology?",
                context="Evidence-based medicine evaluation",
                focus_areas=["clinical_evidence", "methodology", "study_design"],
                weight=2.5,
                evaluation_criteria=["evidence_level", "study_quality", "bias_assessment"]
            ),
            ExpertQuestion(
                question="Are there patient safety considerations or contraindications?",
                context="Patient safety assessment",
                focus_areas=["patient_safety", "contraindications", "adverse_effects"],
                weight=3.0,
                evaluation_criteria=["safety_profile", "risk_severity", "monitoring_requirements"]
            ),
            ExpertQuestion(
                question="How does this align with current clinical guidelines and best practices?",
                context="Clinical practice integration",
                focus_areas=["guidelines", "best_practices", "standard_of_care"],
                weight=2.0,
                evaluation_criteria=["guideline_adherence", "practice_relevance", "implementation_feasibility"]
            ),
            ExpertQuestion(
                question="What are the therapeutic implications and clinical outcomes?",
                context="Clinical impact assessment",
                focus_areas=["therapeutic_efficacy", "clinical_outcomes", "patient_benefit"],
                weight=2.2,
                evaluation_criteria=["efficacy_evidence", "outcome_significance", "clinical_utility"]
            )
        ]
    
    def get_analysis_framework(self) -> str:
        return """
MEDICAL RESEARCH ANALYSIS FRAMEWORK

Content Title: {title}
Duration: {duration}
Analysis Depth: {analysis_depth}

CLINICAL EVALUATION:

1. EVIDENCE ASSESSMENT
   - Study design and methodology quality
   - Sample size and statistical power
   - Control groups and randomization
   - Bias assessment and limitations

2. CLINICAL RELEVANCE
   - Patient population and demographics
   - Clinical endpoints and outcomes
   - Therapeutic significance
   - Real-world applicability

3. SAFETY PROFILE
   - Adverse events and side effects
   - Contraindications and warnings
   - Drug interactions and precautions
   - Monitoring requirements

4. PRACTICE IMPLICATIONS
   - Integration with current guidelines
   - Clinical decision-making impact
   - Implementation considerations
   - Future research directions

EXPERT QUESTIONS:
{expert_questions}

MEDICAL CONTENT:
{transcript}

Provide evidence-based medical analysis addressing all expert questions with clinical recommendations.
Note: This analysis is for educational and research purposes only and does not constitute medical advice.
"""
    
    def validate_input(self, content: str) -> List[str]:
        """Validate medical content requirements"""
        errors = []
        
        medical_keywords = [
            "patient", "clinical", "treatment", "therapy", "diagnosis",
            "medical", "health", "disease", "study", "research"
        ]
        
        content_lower = content.lower()
        if not any(keyword in content_lower for keyword in medical_keywords):
            errors.append("Content may not be relevant for medical research analysis")
        
        # Check for potential misinformation flags
        warning_phrases = [
            "miracle cure", "guaranteed results", "doctors hate this",
            "pharmaceutical conspiracy", "natural cure they don't want you to know"
        ]
        
        if any(phrase in content_lower for phrase in warning_phrases):
            errors.append("Content contains potential medical misinformation flags")
        
        return errors


class TechnologyInnovationTemplate(ExpertTemplate):
    """Technology innovation and technical analysis template"""
    
    def get_domain(self) -> ExpertDomain:
        return ExpertDomain.TECHNOLOGY_INNOVATION
    
    def get_default_questions(self) -> List[ExpertQuestion]:
        return [
            ExpertQuestion(
                question="What is the technical innovation level and breakthrough potential?",
                context="Innovation assessment",
                focus_areas=["innovation", "breakthrough_potential", "technical_novelty"],
                weight=2.5,
                evaluation_criteria=["novelty_score", "technical_complexity", "innovation_impact"]
            ),
            ExpertQuestion(
                question="How feasible is the technical implementation and scalability?",
                context="Technical feasibility analysis",
                focus_areas=["implementation", "scalability", "technical_barriers"],
                weight=2.0,
                evaluation_criteria=["implementation_complexity", "scalability_potential", "resource_requirements"]
            ),
            ExpertQuestion(
                question="What are the security, performance, and reliability implications?",
                context="System quality assessment",
                focus_areas=["security", "performance", "reliability"],
                weight=1.8,
                evaluation_criteria=["security_robustness", "performance_metrics", "reliability_standards"]
            ),
            ExpertQuestion(
                question="How does this technology disrupt existing solutions or create new markets?",
                context="Market disruption analysis",
                focus_areas=["disruption", "market_creation", "competitive_advantage"],
                weight=2.2,
                evaluation_criteria=["disruption_potential", "market_size", "adoption_barriers"]
            )
        ]
    
    def get_analysis_framework(self) -> str:
        return """
TECHNOLOGY INNOVATION ANALYSIS FRAMEWORK

Content Title: {title}
Duration: {duration}
Analysis Depth: {analysis_depth}

TECHNICAL ASSESSMENT:

1. INNOVATION EVALUATION
   - Technical novelty and breakthrough assessment
   - Comparison with existing solutions
   - Patent landscape and IP considerations
   - Innovation readiness level (TRL)

2. ARCHITECTURE & IMPLEMENTATION
   - System design and architecture patterns
   - Implementation complexity and requirements
   - Scalability and performance characteristics
   - Integration capabilities and standards

3. QUALITY & SECURITY
   - Security architecture and threat model
   - Performance benchmarks and optimization
   - Reliability and fault tolerance
   - Maintenance and operational considerations

4. MARKET DISRUPTION
   - Competitive landscape analysis
   - Market timing and adoption curve
   - Business model implications
   - Ecosystem and partnership opportunities

EXPERT QUESTIONS:
{expert_questions}

TECHNICAL CONTENT:
{transcript}

Provide comprehensive technology analysis addressing all expert questions with technical and strategic insights.
"""
    
    def validate_input(self, content: str) -> List[str]:
        """Validate technology content requirements"""
        errors = []
        
        tech_keywords = [
            "technology", "software", "algorithm", "system", "platform",
            "architecture", "framework", "api", "database", "cloud"
        ]
        
        content_lower = content.lower()
        if not any(keyword in content_lower for keyword in tech_keywords):
            errors.append("Content may not be relevant for technology innovation analysis")
        
        return errors


class ExpertCustomizationEngine:
    """
    Advanced engine for domain experts to create sophisticated analysis pipelines
    Supports HCI-optimized interfaces for expert consultation
    """
    
    def __init__(self, templates_dir: str = "expert_templates"):
        self.templates_dir = Path(templates_dir)
        self.templates_dir.mkdir(exist_ok=True)
        
        # Registry of available expert templates
        self.expert_templates: Dict[ExpertDomain, ExpertTemplate] = {
            ExpertDomain.BUSINESS_INTELLIGENCE: BusinessIntelligenceTemplate(),
            ExpertDomain.MEDICAL_RESEARCH: MedicalResearchTemplate(),
            ExpertDomain.TECHNOLOGY_INNOVATION: TechnologyInnovationTemplate(),
        }
        
        # Custom pipelines created by experts
        self.custom_pipelines: Dict[str, ExpertPipeline] = {}
        
        # Load existing custom pipelines
        self._load_custom_pipelines()
    
    def get_available_domains(self) -> List[Dict[str, Any]]:
        """Get list of available expert domains with capabilities"""
        return [
            {
                "domain": domain.value,
                "name": domain.value.replace("_", " ").title(),
                "description": self._get_domain_description(domain),
                "default_questions": len(template.get_default_questions()),
                "analysis_framework": bool(template.get_analysis_framework())
            }
            for domain, template in self.expert_templates.items()
        ]
    
    def create_expert_consultation(self,
                                 domain: ExpertDomain,
                                 custom_questions: Optional[List[ExpertQuestion]] = None,
                                 analysis_depth: AnalysisDepth = AnalysisDepth.EXPERT,
                                 output_format: OutputFormat = OutputFormat.STRUCTURED_JSON,
                                 pipeline_name: Optional[str] = None) -> str:
        """
        Create sophisticated expert consultation pipeline
        Returns pipeline_id for execution
        """
        if domain not in self.expert_templates:
            raise ValueError(f"Unsupported expert domain: {domain}")
        
        template = self.expert_templates[domain]
        
        # Use custom questions or default
        questions = custom_questions or template.get_default_questions()
        
        # Create pipeline
        pipeline_id = f"{domain.value}_{len(self.custom_pipelines)}"
        pipeline = ExpertPipeline(
            pipeline_id=pipeline_id,
            name=pipeline_name or f"Expert {domain.value.title()} Consultation",
            domain=domain,
            description=f"Professional {domain.value} analysis with expert consultation",
            questions=questions,
            analysis_depth=analysis_depth,
            output_format=output_format
        )
        
        # Validate pipeline
        errors = pipeline.validate()
        if errors:
            raise ValueError(f"Pipeline validation failed: {errors}")
        
        # Store pipeline
        self.custom_pipelines[pipeline_id] = pipeline
        
        # Persist pipeline
        self._save_custom_pipeline(pipeline)
        
        return pipeline_id
    
    def get_expert_pipeline(self, pipeline_id: str) -> Optional[ExpertPipeline]:
        """Retrieve expert pipeline by ID"""
        return self.custom_pipelines.get(pipeline_id)
    
    def generate_analysis_prompt(self, 
                               pipeline_id: str,
                               title: str,
                               transcript: str) -> str:
        """Generate complete analysis prompt for expert pipeline"""
        pipeline = self.get_expert_pipeline(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        template = self.expert_templates[pipeline.domain]
        framework = template.get_analysis_framework()
        
        # Format expert questions
        expert_questions_text = "\n".join([
            f"{i+1}. {q.question}"
            f"\n   Context: {q.context}"
            f"\n   Focus Areas: {', '.join(q.focus_areas)}"
            f"\n   Weight: {q.weight}"
            f"\n   Expected Format: {q.expected_format.value}"
            for i, q in enumerate(pipeline.questions)
        ])
        
        # Format complete prompt
        return framework.format(
            title=title,
            duration="N/A",  # Will be filled by video processor
            analysis_depth=pipeline.analysis_depth.value,
            expert_questions=expert_questions_text,
            transcript=transcript
        )
    
    def validate_content_for_domain(self, 
                                  domain: ExpertDomain,
                                  content: str) -> List[str]:
        """Validate content suitability for specific expert domain"""
        if domain not in self.expert_templates:
            return [f"Unsupported domain: {domain}"]
        
        template = self.expert_templates[domain]
        return template.validate_input(content)
    
    def get_domain_expertise_summary(self, domain: ExpertDomain) -> Dict[str, Any]:
        """Get comprehensive summary of domain expertise capabilities"""
        if domain not in self.expert_templates:
            return {}
        
        template = self.expert_templates[domain]
        default_questions = template.get_default_questions()
        
        return {
            "domain": domain.value,
            "name": domain.value.replace("_", " ").title(),
            "description": self._get_domain_description(domain),
            "default_questions_count": len(default_questions),
            "focus_areas": list(set(
                area for q in default_questions for area in q.focus_areas
            )),
            "evaluation_criteria": list(set(
                criteria for q in default_questions for criteria in q.evaluation_criteria
            )),
            "average_question_weight": sum(q.weight for q in default_questions) / len(default_questions),
            "framework_available": bool(template.get_analysis_framework())
        }
    
    def _get_domain_description(self, domain: ExpertDomain) -> str:
        """Get human-readable description for expert domain"""
        descriptions = {
            ExpertDomain.BUSINESS_INTELLIGENCE: "Strategic business analysis and market intelligence",
            ExpertDomain.MEDICAL_RESEARCH: "Evidence-based medical research and clinical analysis",
            ExpertDomain.TECHNOLOGY_INNOVATION: "Technical innovation and engineering analysis",
            ExpertDomain.ACADEMIC_RESEARCH: "Scholarly research and academic methodology",
            ExpertDomain.EDUCATIONAL_CONTENT: "Pedagogical analysis and learning effectiveness",
            ExpertDomain.SOCIAL_SCIENCE: "Behavioral analysis and societal implications",
            ExpertDomain.DATA_SCIENCE: "Statistical analysis and machine learning insights",
            ExpertDomain.LEGAL_ANALYSIS: "Legal framework and regulatory compliance",
            ExpertDomain.FINANCIAL_MARKETS: "Financial analysis and market evaluation",
            ExpertDomain.ENVIRONMENTAL_SCIENCE: "Environmental impact and sustainability analysis"
        }
        return descriptions.get(domain, "Professional domain analysis")
    
    def _load_custom_pipelines(self) -> None:
        """Load existing custom pipelines from disk"""
        for pipeline_file in self.templates_dir.glob("*.yaml"):
            try:
                with open(pipeline_file, 'r') as f:
                    data = yaml.safe_load(f)
                
                # Reconstruct pipeline object
                pipeline = self._dict_to_pipeline(data)
                self.custom_pipelines[pipeline.pipeline_id] = pipeline
                
            except Exception as e:
                print(f"Failed to load pipeline {pipeline_file}: {e}")
    
    def _save_custom_pipeline(self, pipeline: ExpertPipeline) -> None:
        """Save custom pipeline to disk"""
        try:
            pipeline_file = self.templates_dir / f"{pipeline.pipeline_id}.yaml"
            with open(pipeline_file, 'w') as f:
                yaml.dump(self._pipeline_to_dict(pipeline), f, default_flow_style=False)
        except Exception as e:
            print(f"Failed to save pipeline {pipeline.pipeline_id}: {e}")
    
    def _pipeline_to_dict(self, pipeline: ExpertPipeline) -> Dict[str, Any]:
        """Convert pipeline to serializable dictionary"""
        return {
            "pipeline_id": pipeline.pipeline_id,
            "name": pipeline.name,
            "domain": pipeline.domain.value,
            "description": pipeline.description,
            "analysis_depth": pipeline.analysis_depth.value,
            "output_format": pipeline.output_format.value,
            "questions": [
                {
                    "question": q.question,
                    "context": q.context,
                    "focus_areas": q.focus_areas,
                    "expected_format": q.expected_format.value,
                    "weight": q.weight,
                    "evaluation_criteria": q.evaluation_criteria,
                    "follow_up_questions": q.follow_up_questions
                }
                for q in pipeline.questions
            ],
            "preprocessing_steps": pipeline.preprocessing_steps,
            "postprocessing_steps": pipeline.postprocessing_steps,
            "quality_checks": pipeline.quality_checks,
            "metadata": pipeline.metadata
        }
    
    def _dict_to_pipeline(self, data: Dict[str, Any]) -> ExpertPipeline:
        """Convert dictionary to pipeline object"""
        questions = [
            ExpertQuestion(
                question=q["question"],
                context=q.get("context", ""),
                focus_areas=q.get("focus_areas", []),
                expected_format=OutputFormat(q.get("expected_format", "structured_json")),
                weight=q.get("weight", 1.0),
                evaluation_criteria=q.get("evaluation_criteria", []),
                follow_up_questions=q.get("follow_up_questions", [])
            )
            for q in data["questions"]
        ]
        
        return ExpertPipeline(
            pipeline_id=data["pipeline_id"],
            name=data["name"],
            domain=ExpertDomain(data["domain"]),
            description=data["description"],
            questions=questions,
            analysis_depth=AnalysisDepth(data["analysis_depth"]),
            output_format=OutputFormat(data["output_format"]),
            preprocessing_steps=data.get("preprocessing_steps", []),
            postprocessing_steps=data.get("postprocessing_steps", []),
            quality_checks=data.get("quality_checks", []),
            metadata=data.get("metadata", {})
        )


# Global expert customization engine
_expert_engine: Optional[ExpertCustomizationEngine] = None

def get_expert_engine() -> ExpertCustomizationEngine:
    """Get or create global expert customization engine"""
    global _expert_engine
    if _expert_engine is None:
        _expert_engine = ExpertCustomizationEngine()
    return _expert_engine
