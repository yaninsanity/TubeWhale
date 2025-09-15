#!/usr/bin/env python3
"""
Core Analysis Templates
Professional analysis templates for different research domains
"""

from typing import Dict, Any

# Core template structure
CORE_TEMPLATES = {
    "default": {
        "name": "General Analysis",
        "domain": "general",
        "description": "Comprehensive video content analysis for general purposes",
        "prompt": """
GENERAL VIDEO CONTENT ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: General comprehensive review

ANALYSIS FRAMEWORK:

1. CONTENT OVERVIEW
   - Summarize main topics and key points
   - Identify the primary message or purpose
   - Assess content structure and organization

2. KEY INSIGHTS
   - Extract actionable information
   - Highlight important facts or findings
   - Note any unique perspectives or approaches

3. QUALITY ASSESSMENT
   - Evaluate information credibility and sources
   - Assess presentation clarity and effectiveness
   - Consider target audience alignment

4. PRACTICAL VALUE
   - Identify actionable takeaways
   - Suggest potential applications
   - Recommend follow-up topics or resources

TRANSCRIPT:
{transcript}

Provide a structured analysis following the framework above.
""",
        "parameters": {
            "temperature": 0.7,
            "max_tokens": 2000,
            "model": "gpt-4"
        }
    },

    "business_intelligence": {
        "name": "Business Intelligence Analysis",
        "domain": "business",
        "description": "Strategic business analysis for market insights and decision making",
        "prompt": """
BUSINESS INTELLIGENCE VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Strategic business insights

BUSINESS ANALYSIS FRAMEWORK:

1. MARKET INTELLIGENCE
   - Industry trends and market dynamics
   - Competitive landscape insights
   - Market opportunities and threats

2. STRATEGIC INSIGHTS
   - Business models and strategies discussed
   - Innovation patterns and technological trends
   - Growth opportunities and scalability factors

3. OPERATIONAL INTELLIGENCE
   - Process improvements and efficiency gains
   - Resource allocation and optimization
   - Risk factors and mitigation strategies

4. ACTIONABLE RECOMMENDATIONS
   - Strategic decision points
   - Implementation considerations
   - ROI potential and success metrics

TRANSCRIPT:
{transcript}

Provide strategic business intelligence analysis with actionable insights.
""",
        "parameters": {
            "temperature": 0.6,
            "max_tokens": 2500,
            "model": "gpt-4"
        }
    },

    "research_academic": {
        "name": "Academic Research Analysis",
        "domain": "research",
        "description": "Scholarly analysis for academic research and literature review",
        "prompt": """
ACADEMIC RESEARCH VIDEO ANALYSIS

Video Title: {title}
Duration: {duration}
Analysis Focus: Scholarly research perspective

ACADEMIC ANALYSIS FRAMEWORK:

1. THEORETICAL FOUNDATION
   - Key theories and concepts presented
   - Methodological approaches discussed
   - Literature connections and citations

2. RESEARCH CONTRIBUTIONS
   - Novel findings or insights
   - Empirical evidence and data analysis
   - Methodological innovations

3. CRITICAL EVALUATION
   - Strengths and limitations of arguments
   - Evidence quality and reliability
   - Potential biases or gaps

4. SCHOLARLY IMPLICATIONS
   - Contribution to field knowledge
   - Future research directions
   - Interdisciplinary connections

TRANSCRIPT:
{transcript}

Provide scholarly analysis suitable for academic research and literature review.
""",
        "parameters": {
            "temperature": 0.5,
            "max_tokens": 3000,
            "model": "gpt-4"
        }
    }
}
