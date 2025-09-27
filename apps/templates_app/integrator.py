"""
Template-OpenAI Integration Service
精准连接admin管理的template与OpenAI prompt系统
"""

from typing import Dict, Any, Optional, List
from django.conf import settings
from django.db.models import Q
import logging
import json

from apps.templates_app.models import (
    CustomTemplate, ExpertPrompt, TemplateInfo,
    TemplateDomain, ExpertRole
)

logger = logging.getLogger(__name__)


class TemplateOpenAIIntegrator:
    """
    模板OpenAI集成器
    负责将admin管理的template注入到OpenAI prompt构建流程
    """
    
    def __init__(self):
        self.cache = {}
        self._expert_prompts_cache = {}
    
    def get_integrated_prompt(
        self, 
        template_id: str,
        role: str = None,
        domain: str = "general",
        video_data: Dict[str, Any] = None,
        custom_variables: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        获取完整集成的prompt配置
        Returns: {
            "system_prompt": str,
            "user_prompt": str, 
            "json_schema": dict,
            "template_config": dict
        }
        """
        try:
            # 1. 获取基础模板
            base_template = self._get_base_template(template_id)
            if not base_template:
                logger.warning(f"Template {template_id} not found, using fallback")
                base_template = self._get_fallback_template()
            
            # 2. 获取专家提示
            expert_prompt = self._get_expert_prompt(role, domain) if role else None
            
            # 3. 构建完整prompt
            integrated_prompt = self._build_integrated_prompt(
                base_template, expert_prompt, video_data, custom_variables
            )
            
            # 4. 确保JSON schema
            json_schema = self._extract_json_schema(expert_prompt, base_template)
            
            return {
                "system_prompt": integrated_prompt["system"],
                "user_prompt": integrated_prompt["user"],
                "json_schema": json_schema,
                "template_config": {
                    "template_id": template_id,
                    "role": role,
                    "domain": domain,
                    "parameters": base_template.get("parameters", {}),
                    "max_tokens": base_template.get("max_tokens", 2000),
                    "temperature": base_template.get("temperature", 0.7)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract JSON schema: {e}")
            return ""
    
    def get_template_info(self, template_id: str = None, expert_role: str = None) -> Dict[str, Any]:
        """
        Get comprehensive template information for validation and debugging
        
        Args:
            template_id: Template identifier
            expert_role: Expert role for the template
            
        Returns:
            Dict containing template information and status
        """
        try:
            info = {
                'template_found': False,
                'expert_prompt_found': False,
                'template_name': None,
                'expert_role': expert_role,
                'domain': None,
                'analysis_focus': [],
                'available_expert_roles': [],
                'integration_status': 'unknown'
            }
            
            # Find template
            template = None
            if template_id:
                template = CustomTemplate.objects.filter(
                    template_id=template_id,
                    is_active=True
                ).first()
                
                if not template:
                    # Try by name
                    template = CustomTemplate.objects.filter(
                        name__icontains=template_id.replace('_', ' ').replace('-', ' '),
                        is_active=True
                    ).first()
            
            if template:
                info.update({
                    'template_found': True,
                    'template_name': template.name,
                    'domain': template.domain,
                    'analysis_focus': template.analysis_focus or [],
                    'available_expert_roles': list(
                        template.expert_prompts.filter(is_active=True).values_list('role', flat=True)
                    )
                })
                
                # Check for expert prompt
                if expert_role:
                    expert_prompt = template.expert_prompts.filter(
                        role=expert_role,
                        is_active=True
                    ).first()
                    
                    if expert_prompt:
                        info.update({
                            'expert_prompt_found': True,
                            'integration_status': 'fully_integrated'
                        })
                    else:
                        info['integration_status'] = 'template_only'
                else:
                    info['integration_status'] = 'template_available'
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get template info: {e}")
            return {
                'template_found': False,
                'expert_prompt_found': False,
                'error': str(e),
                'integration_status': 'error'
            }
    
    def _get_base_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """获取基础模板"""
        try:
            # 从CustomTemplate模型获取
            custom_template = CustomTemplate.objects.filter(
                template_id=template_id
            ).first()
            
            if custom_template:
                return {
                    "id": custom_template.template_id,
                    "name": custom_template.name,
                    "prompt": custom_template.prompt,
                    "parameters": custom_template.parameters or {},
                    "domain": custom_template.domain,
                    "description": custom_template.description,
                    "max_tokens": 2000,  # 默认值
                    "temperature": 0.7   # 默认值
                }
            
            # 从TemplateInfo模型获取
            template_info = TemplateInfo.objects.filter(
                template_id=template_id,
                is_active=True
            ).first()
            
            if template_info:
                return {
                    "id": template_info.template_id,
                    "name": template_info.title,
                    "prompt": f"Analyze the content based on {template_info.theme}. {template_info.description}",
                    "parameters": {},
                    "domain": "general",
                    "description": template_info.description,
                    "max_tokens": 2000,
                    "temperature": 0.7
                }
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to get base template {template_id}: {e}")
            return None
    
    def _get_expert_prompt(self, role: str, domain: str = "general") -> Optional[ExpertPrompt]:
        """获取专家提示"""
        cache_key = f"{role}_{domain}"
        
        if cache_key in self._expert_prompts_cache:
            return self._expert_prompts_cache[cache_key]
        
        try:
            # 精确匹配：role + domain
            expert_prompt = ExpertPrompt.objects.filter(
                role=role,
                domain=domain,
                active=True
            ).order_by('weight').first()
            
            # 回退到general domain
            if not expert_prompt and domain != "general":
                expert_prompt = ExpertPrompt.objects.filter(
                    role=role,
                    domain="general",
                    active=True
                ).order_by('weight').first()
            
            # 缓存结果
            self._expert_prompts_cache[cache_key] = expert_prompt
            return expert_prompt
            
        except Exception as e:
            logger.error(f"Failed to get expert prompt {role}/{domain}: {e}")
            return None
    
    def _build_integrated_prompt(
        self,
        base_template: Dict[str, Any],
        expert_prompt: Optional[ExpertPrompt],
        video_data: Dict[str, Any] = None,
        custom_variables: Dict[str, Any] = None
    ) -> Dict[str, str]:
        """构建集成的prompt"""
        
        # 基础prompt
        base_prompt = base_template.get("prompt", "")
        
        # 变量替换
        if video_data:
            for key, value in video_data.items():
                placeholder = f"{{{key}}}"
                base_prompt = base_prompt.replace(placeholder, str(value))
        
        if custom_variables:
            for key, value in custom_variables.items():
                placeholder = f"{{{key}}}"
                base_prompt = base_prompt.replace(placeholder, str(value))
        
        # 专家层注入
        if expert_prompt:
            final_prompt = expert_prompt.apply_to(base_prompt)
        else:
            final_prompt = base_prompt
        
        # 分离system和user prompts
        prompt_parts = final_prompt.split("USER:", 1)
        if len(prompt_parts) == 2:
            system_prompt = prompt_parts[0].replace("SYSTEM:", "").strip()
            user_prompt = prompt_parts[1].strip()
        else:
            # 如果没有明确分离，使用智能分割
            lines = final_prompt.split('\n')
            if len(lines) > 5:
                system_prompt = '\n'.join(lines[:3])
                user_prompt = '\n'.join(lines[3:])
            else:
                system_prompt = "You are a professional analyst. Provide detailed, accurate analysis."
                user_prompt = final_prompt
        
        return {
            "system": system_prompt,
            "user": user_prompt
        }
    
    def _extract_json_schema(
        self, 
        expert_prompt: Optional[ExpertPrompt], 
        base_template: Dict[str, Any]
    ) -> Dict[str, Any]:
        """提取JSON schema以确保结构化输出"""
        
        # 从expert_prompt的outro中提取JSON schema
        if expert_prompt and expert_prompt.prompt_outro:
            outro = expert_prompt.prompt_outro
            
            # 寻找JSON schema模式
            if "JSON:" in outro:
                try:
                    json_part = outro.split("JSON:")[1].strip()
                    # 提取{}内容
                    if "{" in json_part and "}" in json_part:
                        start = json_part.find("{")
                        end = json_part.rfind("}") + 1
                        json_str = json_part[start:end]
                        # 尝试解析
                        return json.loads(json_str)
                except:
                    pass
            
            # 如果有明确的字段定义
            if "keys:" in outro.lower():
                return {"structured_output": True, "format": "json"}
        
        # 默认JSON schema
        return {
            "analysis": "string",
            "key_insights": "array",
            "recommendations": "array", 
            "confidence_score": "number"
        }
    
    def _get_fallback_template(self) -> Dict[str, Any]:
        """获取回退模板"""
        return {
            "id": "fallback_analysis",
            "name": "Fallback Analysis Template",
            "prompt": "Provide a comprehensive analysis of the content. Focus on key insights, main themes, and actionable recommendations.",
            "parameters": {},
            "domain": "general",
            "description": "Default fallback template for analysis",
            "max_tokens": 2000,
            "temperature": 0.7
        }
    
    def _get_emergency_fallback(self) -> Dict[str, Any]:
        """紧急回退配置"""
        return {
            "system_prompt": "You are a professional content analyst. Provide structured analysis in JSON format.",
            "user_prompt": "Analyze the provided content and return insights in JSON format with fields: analysis, key_insights, recommendations.",
            "json_schema": {
                "analysis": "string",
                "key_insights": "array",
                "recommendations": "array"
            },
            "template_config": {
                "template_id": "emergency_fallback",
                "max_tokens": 1500,
                "temperature": 0.7
            }
        }
    
    def list_available_templates(self, role: str = None, domain: str = None) -> List[Dict[str, Any]]:
        """列出可用模板"""
        templates = []
        
        # CustomTemplate
        custom_qs = CustomTemplate.objects.all()
        if domain:
            custom_qs = custom_qs.filter(domain=domain)
        
        for template in custom_qs:
            templates.append({
                "id": template.template_id,
                "name": template.name,
                "description": template.description,
                "domain": template.domain,
                "type": "custom",
                "active": True
            })
        
        # TemplateInfo
        info_qs = TemplateInfo.objects.filter(is_active=True)
        
        for template_info in info_qs:
            templates.append({
                "id": template_info.template_id,
                "name": template_info.title,
                "description": template_info.description,
                "domain": "general",
                "type": "info",
                "active": template_info.is_active,
                "tier": template_info.required_tier,
                "difficulty": template_info.difficulty_level
            })
        
        return templates
    
    def validate_template_integration(self, template_id: str, role: str = None) -> Dict[str, Any]:
        """验证模板集成"""
        try:
            result = self.get_integrated_prompt(template_id, role)
            
            validation = {
                "valid": True,
                "template_found": bool(result.get("template_config")),
                "expert_prompt_found": bool(role),
                "json_schema_valid": bool(result.get("json_schema")),
                "system_prompt_length": len(result.get("system_prompt", "")),
                "user_prompt_length": len(result.get("user_prompt", "")),
                "issues": []
            }
            
            # 验证检查
            if validation["system_prompt_length"] < 10:
                validation["issues"].append("System prompt too short")
            
            if validation["user_prompt_length"] < 10:
                validation["issues"].append("User prompt too short")
            
            if not result.get("json_schema"):
                validation["issues"].append("No JSON schema defined")
            
            validation["valid"] = len(validation["issues"]) == 0
            
            return validation
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e),
                "issues": [f"Integration failed: {e}"]
            }


# 全局实例
template_openai_integrator = TemplateOpenAIIntegrator()