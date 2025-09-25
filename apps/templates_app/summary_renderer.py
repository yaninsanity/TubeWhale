"""
TubeWhale YouTube 总结模板渲染引擎
确保思考问题能正确整合到YouTube总结过程中
"""

import json
import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from apps.templates_app.youtube_templates import get_youtube_templates, YouTubeTemplate, ThinkingQuestion

@dataclass
class AnalysisContext:
    """分析上下文"""
    video_title: str
    video_description: str
    transcript: str
    metadata: Dict[str, Any]
    thinking_questions: List[str]
    template_id: str
    role: str
    language: str = 'zh-CN'

class YouTubeSummaryRenderer:
    """YouTube总结渲染器"""
    
    def __init__(self):
        self.template_manager = get_youtube_templates()
        
        # 多语言提示词模板
        self.prompts = {
            'zh-CN': {
                'system_role': "你是一位专业的YouTube视频分析师，擅长从多个角度深度分析视频内容。",
                'analysis_instruction': "请基于以下思考问题对这个YouTube视频进行深度分析：",
                'question_prefix': "思考问题",
                'analysis_sections': {
                    'summary': '📋 视频总结',
                    'key_insights': '💡 核心洞察',
                    'thinking_analysis': '🤔 深度思考',
                    'recommendations': '🎯 优化建议',
                    'conclusion': '📝 结论'
                }
            },
            'en': {
                'system_role': "You are a professional YouTube video analyst skilled in deep content analysis from multiple perspectives.",
                'analysis_instruction': "Please conduct an in-depth analysis of this YouTube video based on the following thinking questions:",
                'question_prefix': "Thinking Question",
                'analysis_sections': {
                    'summary': '📋 Video Summary',
                    'key_insights': '💡 Key Insights',
                    'thinking_analysis': '🤔 Deep Analysis',
                    'recommendations': '🎯 Recommendations',
                    'conclusion': '📝 Conclusion'
                }
            }
        }
    
    def render_analysis_prompt(self, context: AnalysisContext) -> str:
        """渲染分析提示词"""
        lang = context.language
        prompts = self.prompts.get(lang, self.prompts['zh-CN'])
        
        # 获取模板信息
        template = self.template_manager.get_template(context.template_id)
        if not template:
            template = self._get_default_template(context.role)
        
        # 构建思考问题列表
        questions_to_use = context.thinking_questions if context.thinking_questions else [
            q.question for q in template.thinking_questions
        ]
        
        # 构建提示词
        prompt_parts = [
            f"{prompts['system_role']}\n",
            f"**分析角色**: {self._get_role_name(context.role, lang)}",
            f"**分析模板**: {template.name}",
            f"**视频标题**: {context.video_title}",
            f"**视频描述**: {context.video_description[:500]}{'...' if len(context.video_description) > 500 else ''}",
            "\n" + "="*80 + "\n",
            prompts['analysis_instruction'],
            ""
        ]
        
        # 添加思考问题
        for i, question in enumerate(questions_to_use[:3], 1):  # 最多3个问题
            prompt_parts.append(f"**{prompts['question_prefix']} {i}**: {question}")
        
        prompt_parts.extend([
            "\n" + "="*80 + "\n",
            "**视频文本内容**:",
            f"```\n{context.transcript[:3000]}{'...' if len(context.transcript) > 3000 else ''}\n```",
            "\n" + "="*80 + "\n",
            self._get_output_format_instruction(template, lang),
        ])
        
        return "\n".join(prompt_parts)
    
    def _get_role_name(self, role: str, lang: str) -> str:
        """获取角色名称"""
        role_names = {
            'zh-CN': {
                'content-creator': '内容创作者',
                'marketing-expert': '营销专家',
                'data-analyst': '数据分析师'
            },
            'en': {
                'content-creator': 'Content Creator',
                'marketing-expert': 'Marketing Expert',
                'data-analyst': 'Data Analyst'
            }
        }
        return role_names.get(lang, role_names['zh-CN']).get(role, role)
    
    def _get_default_template(self, role: str) -> YouTubeTemplate:
        """获取默认模板"""
        templates_by_role = self.template_manager.get_templates_by_role(role)
        return templates_by_role[0] if templates_by_role else self.template_manager.get_all_templates()[0]
    
    def _get_output_format_instruction(self, template: YouTubeTemplate, lang: str) -> str:
        """获取输出格式指令"""
        prompts = self.prompts.get(lang, self.prompts['zh-CN'])
        sections = prompts['analysis_sections']
        
        if lang == 'zh-CN':
            return f"""
请按照以下结构输出分析报告：

## {sections['summary']}
简明扼要地总结视频的主要内容和核心信息（150-200字）

## {sections['key_insights']}
基于专业角度提取的关键洞察点（3-5个要点）

## {sections['thinking_analysis']}
针对上述思考问题的深度分析：
- 问题1的分析和见解
- 问题2的分析和见解  
- 问题3的分析和见解

## {sections['recommendations']}
基于分析结果提出的具体优化建议（3-5条）

## {sections['conclusion']}
综合结论和后续行动建议

**分析要求**:
1. 保持专业和客观的分析视角
2. 提供具体、可执行的建议
3. 结合行业最佳实践
4. 注重实用性和可操作性
"""
        else:
            return f"""
Please output the analysis report in the following structure:

## {sections['summary']}
Concisely summarize the main content and core information of the video (150-200 words)

## {sections['key_insights']}
Key insights extracted from a professional perspective (3-5 points)

## {sections['thinking_analysis']}
In-depth analysis of the thinking questions above:
- Analysis and insights for Question 1
- Analysis and insights for Question 2
- Analysis and insights for Question 3

## {sections['recommendations']}
Specific optimization recommendations based on analysis results (3-5 items)

## {sections['conclusion']}
Comprehensive conclusion and next action recommendations

**Analysis Requirements**:
1. Maintain professional and objective analytical perspective
2. Provide specific, actionable recommendations
3. Incorporate industry best practices
4. Focus on practicality and operability
"""
    
    def create_analysis_config(self, 
                             role: str, 
                             template_id: str, 
                             custom_questions: List[str] = None,
                             language: str = 'zh-CN') -> Dict[str, Any]:
        """创建分析配置"""
        template = self.template_manager.get_template(template_id)
        if not template:
            template = self._get_default_template(role)
        
        # 使用自定义问题或模板默认问题
        questions = custom_questions if custom_questions else [
            q.question for q in template.thinking_questions
        ]
        
        return {
            'template_id': template.id,
            'template_name': template.name,
            'role': role,
            'role_name': self._get_role_name(role, language),
            'thinking_questions': questions[:3],  # 最多3个问题
            'analysis_focus': template.analysis_focus,
            'output_format': template.output_format,
            'language': language,
            'created_at': str(datetime.datetime.now())
        }
    
    def validate_analysis_input(self, video_data: Dict[str, Any], 
                              questions: List[str]) -> Dict[str, Any]:
        """验证分析输入数据"""
        errors = []
        warnings = []
        
        # 检查视频数据完整性
        required_fields = ['title', 'description', 'transcript']
        for field in required_fields:
            if not video_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # 检查思考问题
        if not questions:
            warnings.append("No thinking questions provided, using template defaults")
        elif len(questions) > 3:
            warnings.append("Too many questions provided, using first 3")
        
        # 检查文本长度
        if video_data.get('transcript') and len(video_data['transcript']) < 100:
            warnings.append("Transcript seems too short for meaningful analysis")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    def post_process_analysis(self, analysis_result: str, 
                            context: AnalysisContext) -> Dict[str, Any]:
        """后处理分析结果"""
        # 提取结构化信息
        sections = self._extract_sections(analysis_result, context.language)
        
        # 计算分析质量指标
        quality_metrics = self._calculate_quality_metrics(analysis_result, context)
        
        return {
            'raw_analysis': analysis_result,
            'structured_sections': sections,
            'quality_metrics': quality_metrics,
            'metadata': {
                'template_used': context.template_id,
                'role': context.role,
                'language': context.language,
                'questions_count': len(context.thinking_questions),
                'analysis_length': len(analysis_result)
            }
        }
    
    def _extract_sections(self, analysis: str, language: str) -> Dict[str, str]:
        """提取分析结果的各个部分"""
        sections = {}
        prompts = self.prompts.get(language, self.prompts['zh-CN'])
        section_names = prompts['analysis_sections']
        
        # 使用正则表达式提取各部分内容
        for key, title in section_names.items():
            pattern = rf"## {re.escape(title)}(.*?)(?=## |$)"
            match = re.search(pattern, analysis, re.DOTALL)
            if match:
                sections[key] = match.group(1).strip()
        
        return sections
    
    def _calculate_quality_metrics(self, analysis: str, context: AnalysisContext) -> Dict[str, Any]:
        """计算分析质量指标"""
        return {
            'completeness': len(analysis) > 1000,  # 基本长度检查
            'structure_score': len(re.findall(r'##\s+', analysis)),  # 结构化程度
            'question_coverage': len([q for q in context.thinking_questions if q.lower() in analysis.lower()]),
            'actionability': len(re.findall(r'建议|推荐|应该|可以|需要', analysis)) if context.language == 'zh-CN' else len(re.findall(r'recommend|suggest|should|could|need', analysis.lower()))
        }

# 全局渲染器实例
youtube_summary_renderer = YouTubeSummaryRenderer()

def get_youtube_summary_renderer():
    """获取YouTube总结渲染器实例"""
    return youtube_summary_renderer

# 导入必要的模块
import datetime