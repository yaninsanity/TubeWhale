#!/usr/bin/env python3
"""
Specialized Domain Templates
Professional templates for specific research and analysis domains
"""

from typing import Dict, Any

DOMAIN_TEMPLATES = {
    "medical_health": {
        "name": "Medical & Health Analysis",
        "domain": "healthcare",
        "description": "Medical and health content analysis for healthcare professionals",
        "prompt": """
MEDICAL & HEALTH VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Medical and health content evaluation

MEDICAL ANALYSIS FRAMEWORK:

1. CLINICAL CONTENT ASSESSMENT
   - Medical accuracy and evidence base
   - Treatment approaches and protocols
   - Diagnostic methods and procedures
   - Patient safety considerations

2. HEALTH INFORMATION QUALITY
   - Scientific rigor and peer review status
   - Source credibility and expert credentials
   - Potential misinformation or contraindications
   - Regulatory compliance considerations

3. PATIENT IMPACT EVALUATION
   - Patient education value and clarity
   - Accessibility for different health literacy levels
   - Cultural sensitivity and inclusivity
   - Ethical considerations and informed consent

4. CLINICAL APPLICATIONS
   - Practice integration opportunities
   - Continuing education value
   - Professional development insights
   - Quality improvement implications

TRANSCRIPT:
{transcript}

Provide medical and health analysis with emphasis on accuracy, safety, and clinical relevance.
Note: This analysis is for educational purposes and should not replace professional medical advice.
""",
        "parameters": {
            "temperature": 0.3,
            "max_tokens": 2500,
            "model": "gpt-4"
        }
    },

    "social_science": {
        "name": "Social Science Research",
        "domain": "social_science",
        "description": "Social science analysis for behavioral and societal research",
        "prompt": """
SOCIAL SCIENCE VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Social science research perspective

SOCIAL SCIENCE ANALYSIS FRAMEWORK:

1. BEHAVIORAL PATTERNS
   - Individual and group behaviors observed
   - Social dynamics and interactions
   - Cultural factors and influences
   - Psychological mechanisms at play

2. SOCIETAL IMPLICATIONS
   - Community impact and social change
   - Policy implications and governance
   - Economic and social justice considerations
   - Demographic trends and disparities

3. RESEARCH METHODOLOGY
   - Data collection and analysis methods
   - Sample characteristics and generalizability
   - Validity and reliability considerations
   - Ethical research practices

4. THEORETICAL CONTRIBUTIONS
   - Social theories and frameworks applied
   - Interdisciplinary connections
   - Empirical evidence and findings
   - Future research opportunities

TRANSCRIPT:
{transcript}

Provide comprehensive social science analysis with focus on behavioral patterns, societal impact, and research methodology.
""",
        "parameters": {
            "temperature": 0.6,
            "max_tokens": 2800,
            "model": "gpt-4"
        }
    },

    "data_science": {
        "name": "Data Science Analysis",
        "domain": "data_science",
        "description": "Technical analysis for data science and analytics content",
        "prompt": """
DATA SCIENCE VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Data science and analytics evaluation

DATA SCIENCE ANALYSIS FRAMEWORK:

1. TECHNICAL METHODOLOGY
   - Algorithms and techniques discussed
   - Data preprocessing and cleaning approaches
   - Model architecture and implementation
   - Performance metrics and validation methods

2. DATA QUALITY & ETHICS
   - Data sources and collection methods
   - Bias detection and mitigation strategies
   - Privacy and security considerations
   - Reproducibility and transparency

3. ANALYTICAL INSIGHTS
   - Statistical significance and confidence levels
   - Feature importance and interpretation
   - Visualization effectiveness
   - Scalability and computational efficiency

4. PRACTICAL APPLICATIONS
   - Real-world implementation challenges
   - Business value and ROI considerations
   - Technology stack and infrastructure needs
   - Future trends and emerging technologies

TRANSCRIPT:
{transcript}

Provide technical data science analysis with emphasis on methodology, ethics, and practical implementation.
""",
        "parameters": {
            "temperature": 0.4,
            "max_tokens": 2500,
            "model": "gpt-4"
        }
    },

    "educational_content": {
        "name": "Educational Content Analysis",
        "domain": "education",
        "description": "Pedagogical analysis for educational content and learning materials",
        "prompt": """
EDUCATIONAL CONTENT VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Educational effectiveness and pedagogical value

EDUCATIONAL ANALYSIS FRAMEWORK:

1. PEDAGOGICAL DESIGN
   - Learning objectives clarity and alignment
   - Instructional design principles applied
   - Content structure and progression
   - Engagement strategies and techniques

2. LEARNING EFFECTIVENESS
   - Cognitive load and complexity management
   - Multiple learning styles accommodation
   - Assessment and feedback mechanisms
   - Knowledge retention strategies

3. ACCESSIBILITY & INCLUSIVITY
   - Universal design for learning principles
   - Language accessibility and clarity
   - Cultural responsiveness and sensitivity
   - Technology accessibility features

4. EDUCATIONAL IMPACT
   - Skill development and competency building
   - Critical thinking and problem-solving
   - Real-world application opportunities
   - Lifelong learning value

TRANSCRIPT:
{transcript}

Provide educational analysis focused on pedagogical effectiveness, learning outcomes, and student engagement.
""",
        "parameters": {
            "temperature": 0.5,
            "max_tokens": 2300,
            "model": "gpt-4"
        }
    },

    "technology_innovation": {
        "name": "Technology Innovation Analysis",
        "domain": "technology",
        "description": "Technical innovation analysis for emerging technologies and trends",
        "prompt": """
TECHNOLOGY INNOVATION VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Technology innovation and emerging trends

TECHNOLOGY ANALYSIS FRAMEWORK:

1. INNOVATION ASSESSMENT
   - Technological novelty and breakthrough potential
   - Technical feasibility and maturity level
   - Competitive advantages and differentiation
   - Market disruption potential

2. TECHNICAL ARCHITECTURE
   - System design and implementation approach
   - Scalability and performance considerations
   - Security and reliability factors
   - Integration capabilities and standards

3. MARKET DYNAMICS
   - Adoption barriers and enablers
   - Economic impact and business models
   - Regulatory considerations and compliance
   - Ecosystem development and partnerships

4. FUTURE IMPLICATIONS
   - Long-term technology roadmap
   - Societal impact and ethical considerations
   - Investment and development priorities
   - Skills and workforce implications

TRANSCRIPT:
{transcript}

Provide comprehensive technology analysis with focus on innovation potential, technical merit, and market impact.
""",
        "parameters": {
            "temperature": 0.6,
            "max_tokens": 2600,
            "model": "gpt-4"
        }
    }
}
