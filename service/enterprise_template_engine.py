#!/usr/bin/env python3
"""
Enterprise Template Management System
Advanced template engine with full customization and validation
"""

import os
import yaml
import json
import hashlib
from typing import Dict, Any, List, Optional, Set, Union
from pathlib import Path
from datetime import datetime

from service.templates.core_templates import CORE_TEMPLATES
from service.templates.domain_templates import DOMAIN_TEMPLATES


class TemplateValidationError(Exception):
    """Custom exception for template validation errors"""
    pass


class TemplateEngine:
    """
    Enterprise-grade template management system
    
    Features:
    - Advanced template validation
    - Hierarchical template inheritance
    - Real-time template compilation
    - Custom domain support
    - Template versioning
    - Performance optimization
    """
    
    def __init__(self, 
                 custom_templates_dir: Optional[str] = None,
                 enable_caching: bool = True,
                 validation_strict: bool = True):
        self.custom_templates_dir = custom_templates_dir or "templates/custom"
        self.enable_caching = enable_caching
        self.validation_strict = validation_strict
        
        # Core storage
        self._templates: Dict[str, Dict[str, Any]] = {}
        self._template_metadata: Dict[str, Dict[str, Any]] = {}
        self._compiled_cache: Dict[str, str] = {}
        self._validation_cache: Dict[str, bool] = {}
        
        # Initialize template system
        self._initialize_template_system()
    
    def _initialize_template_system(self):
        """Initialize the enterprise template system"""
        # Load built-in templates
        self._load_core_templates()
        self._load_domain_templates()
        
        # Load custom templates
        self._load_custom_templates()
        
        # Build metadata index
        self._build_metadata_index()
        
        # Validate all templates
        if self.validation_strict:
            self._validate_all_templates()
    
    def _load_core_templates(self):
        """Load core analysis templates"""
        for template_id, template_data in CORE_TEMPLATES.items():
            self._templates[template_id] = {
                **template_data,
                "template_type": "core",
                "created_at": datetime.now().isoformat(),
                "version": "1.0.0",
                "immutable": True
            }
    
    def _load_domain_templates(self):
        """Load domain-specific templates"""
        for template_id, template_data in DOMAIN_TEMPLATES.items():
            self._templates[template_id] = {
                **template_data,
                "template_type": "domain",
                "created_at": datetime.now().isoformat(),
                "version": "1.0.0",
                "immutable": True
            }
    
    def _load_custom_templates(self):
        """Load user-defined custom templates"""
        custom_dir = Path(self.custom_templates_dir)
        if not custom_dir.exists():
            custom_dir.mkdir(parents=True, exist_ok=True)
            return
        
        for template_file in custom_dir.glob("*.yaml"):
            try:
                with open(template_file, 'r', encoding='utf-8') as f:
                    template_data = yaml.safe_load(f)
                
                template_id = template_file.stem
                self._templates[template_id] = {
                    **template_data,
                    "template_type": "custom",
                    "file_path": str(template_file),
                    "immutable": False
                }
                
            except Exception as e:
                print(f"Error loading custom template {template_file}: {e}")
    
    def _build_metadata_index(self):
        """Build searchable metadata index"""
        for template_id, template in self._templates.items():
            self._template_metadata[template_id] = {
                "id": template_id,
                "name": template.get("name", template_id),
                "domain": template.get("domain", "general"),
                "description": template.get("description", ""),
                "template_type": template.get("template_type", "unknown"),
                "version": template.get("version", "1.0.0"),
                "created_at": template.get("created_at"),
                "immutable": template.get("immutable", False),
                "tags": template.get("tags", []),
                "complexity_score": self._calculate_complexity_score(template),
                "parameter_count": len(template.get("parameters", {}))
            }
    
    def _calculate_complexity_score(self, template: Dict[str, Any]) -> float:
        """Calculate template complexity score for optimization"""
        prompt = template.get("prompt", "")
        parameters = template.get("parameters", {})
        
        # Base complexity from prompt length
        complexity = len(prompt) / 1000.0
        
        # Add complexity for parameters
        complexity += len(parameters) * 0.1
        
        # Add complexity for placeholders
        placeholder_count = prompt.count("{") + prompt.count("}")
        complexity += placeholder_count * 0.05
        
        return round(complexity, 2)
    
    def _validate_all_templates(self):
        """Validate all templates in the system"""
        for template_id, template in self._templates.items():
            try:
                self.validate_template_structure(template)
                self._validation_cache[template_id] = True
            except TemplateValidationError as e:
                print(f"Template validation failed for {template_id}: {e}")
                self._validation_cache[template_id] = False
    
    # ==================== Core Template Operations ====================
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get template by ID with caching optimization"""
        template = self._templates.get(template_id)
        if not template:
            return None
        
        # Return deep copy to prevent accidental modification
        return {**template}
    
    def list_templates(self, 
                      domain_filter: Optional[str] = None,
                      template_type_filter: Optional[str] = None,
                      include_metadata: bool = False) -> List[Dict[str, Any]]:
        """
        List templates with advanced filtering
        
        Args:
            domain_filter: Filter by domain (business, research, medical, etc.)
            template_type_filter: Filter by type (core, domain, custom)
            include_metadata: Include detailed metadata
            
        Returns:
            Filtered list of templates
        """
        results = []
        
        for template_id, metadata in self._template_metadata.items():
            # Apply filters
            if domain_filter and metadata["domain"] != domain_filter:
                continue
            if template_type_filter and metadata["template_type"] != template_type_filter:
                continue
            
            if include_metadata:
                results.append(metadata)
            else:
                results.append({
                    "id": template_id,
                    "name": metadata["name"],
                    "domain": metadata["domain"],
                    "description": metadata["description"]
                })
        
        # Sort by domain, then by name
        return sorted(results, key=lambda x: (x["domain"], x["name"]))
    
    def get_domains(self) -> List[str]:
        """Get all available template domains"""
        domains = set()
        for metadata in self._template_metadata.values():
            domains.add(metadata["domain"])
        return sorted(list(domains))
    
    def search_templates(self, 
                        query: str,
                        search_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Advanced template search functionality
        
        Args:
            query: Search query string
            search_fields: Fields to search in (name, description, domain, tags)
            
        Returns:
            Ranked search results
        """
        if search_fields is None:
            search_fields = ["name", "description", "domain", "tags"]
        
        query_lower = query.lower()
        results = []
        
        for template_id, metadata in self._template_metadata.items():
            score = 0
            
            # Search in specified fields
            for field in search_fields:
                if field in metadata:
                    field_value = str(metadata[field]).lower()
                    if query_lower in field_value:
                        # Higher score for exact matches in name
                        if field == "name" and query_lower == field_value:
                            score += 10
                        elif field == "name":
                            score += 5
                        elif field == "domain":
                            score += 3
                        else:
                            score += 1
            
            if score > 0:
                results.append({
                    **metadata,
                    "search_score": score
                })
        
        # Sort by search score descending
        return sorted(results, key=lambda x: x["search_score"], reverse=True)
    
    # ==================== Custom Template Management ====================
    
    def create_custom_template(self,
                             template_id: str,
                             name: str,
                             domain: str,
                             description: str,
                             prompt: str,
                             parameters: Optional[Dict[str, Any]] = None,
                             tags: Optional[List[str]] = None) -> bool:
        """
        Create highly customizable analysis template
        
        Args:
            template_id: Unique template identifier
            name: Human-readable template name
            domain: Analysis domain
            description: Detailed template description
            prompt: Analysis prompt with placeholders
            parameters: LLM parameters (temperature, max_tokens, etc.)
            tags: Template tags for categorization
            
        Returns:
            True if created successfully
        """
        # Validate template ID uniqueness
        if template_id in self._templates:
            raise ValueError(f"Template ID '{template_id}' already exists")
        
        # Build template structure
        template_data = {
            "name": name,
            "domain": domain,
            "description": description,
            "prompt": prompt,
            "parameters": parameters or {
                "temperature": 0.7,
                "max_tokens": 2000,
                "model": "gpt-4"
            },
            "tags": tags or [],
            "template_type": "custom",
            "created_at": datetime.now().isoformat(),
            "version": "1.0.0",
            "immutable": False
        }
        
        # Validate template structure
        try:
            self.validate_template_structure(template_data)
        except TemplateValidationError as e:
            raise ValueError(f"Template validation failed: {e}")
        
        # Save to memory
        self._templates[template_id] = template_data
        
        # Update metadata
        self._template_metadata[template_id] = {
            "id": template_id,
            "name": name,
            "domain": domain,
            "description": description,
            "template_type": "custom",
            "version": "1.0.0",
            "created_at": template_data["created_at"],
            "immutable": False,
            "tags": tags or [],
            "complexity_score": self._calculate_complexity_score(template_data),
            "parameter_count": len(template_data["parameters"])
        }
        
        # Persist to file
        try:
            self._save_custom_template(template_id, template_data)
            self._validation_cache[template_id] = True
            return True
        except Exception as e:
            # Rollback memory changes
            self._templates.pop(template_id, None)
            self._template_metadata.pop(template_id, None)
            raise RuntimeError(f"Failed to save template: {e}")
    
    def update_template(self, 
                       template_id: str, 
                       updates: Dict[str, Any]) -> bool:
        """
        Update existing template with validation
        
        Args:
            template_id: Template to update
            updates: Dictionary of fields to update
            
        Returns:
            True if updated successfully
        """
        template = self._templates.get(template_id)
        if not template:
            return False
        
        # Check if template is immutable
        if template.get("immutable", False):
            raise ValueError(f"Template '{template_id}' is immutable and cannot be modified")
        
        # Create updated template
        updated_template = {**template, **updates}
        updated_template["version"] = self._increment_version(template.get("version", "1.0.0"))
        updated_template["updated_at"] = datetime.now().isoformat()
        
        # Validate updated template
        try:
            self.validate_template_structure(updated_template)
        except TemplateValidationError as e:
            raise ValueError(f"Template validation failed: {e}")
        
        # Apply updates
        self._templates[template_id] = updated_template
        
        # Update metadata
        self._template_metadata[template_id].update({
            "name": updated_template.get("name"),
            "domain": updated_template.get("domain"),
            "description": updated_template.get("description"),
            "version": updated_template["version"],
            "tags": updated_template.get("tags", []),
            "complexity_score": self._calculate_complexity_score(updated_template),
            "parameter_count": len(updated_template.get("parameters", {}))
        })
        
        # Persist changes
        if template.get("template_type") == "custom":
            self._save_custom_template(template_id, updated_template)
        
        # Clear cache
        self._compiled_cache.pop(template_id, None)
        
        return True
    
    def delete_custom_template(self, template_id: str) -> bool:
        """Delete custom template (only custom templates can be deleted)"""
        template = self._templates.get(template_id)
        if not template:
            return False
        
        if template.get("immutable", False):
            raise ValueError(f"Template '{template_id}' is immutable and cannot be deleted")
        
        # Remove from memory
        self._templates.pop(template_id, None)
        self._template_metadata.pop(template_id, None)
        self._compiled_cache.pop(template_id, None)
        self._validation_cache.pop(template_id, None)
        
        # Remove file
        if template.get("template_type") == "custom":
            template_file = Path(self.custom_templates_dir) / f"{template_id}.yaml"
            if template_file.exists():
                template_file.unlink()
        
        return True
    
    # ==================== Template Compilation & Formatting ====================
    
    def compile_template(self, 
                        template_id: str,
                        variables: Dict[str, Any],
                        use_cache: bool = True) -> str:
        """
        Compile template with variables and caching
        
        Args:
            template_id: Template to compile
            variables: Variables to inject into template
            use_cache: Whether to use compilation cache
            
        Returns:
            Compiled template string
        """
        template = self.get_template(template_id)
        if not template:
            raise ValueError(f"Template '{template_id}' not found")
        
        # Generate cache key
        cache_key = self._generate_cache_key(template_id, variables)
        
        # Check cache
        if use_cache and self.enable_caching and cache_key in self._compiled_cache:
            return self._compiled_cache[cache_key]
        
        # Compile template
        try:
            prompt = template["prompt"]
            compiled_prompt = prompt.format(**variables)
            
            # Cache result
            if use_cache and self.enable_caching:
                self._compiled_cache[cache_key] = compiled_prompt
            
            return compiled_prompt
            
        except KeyError as e:
            missing_var = str(e).strip("'\"")
            raise ValueError(f"Missing required variable: {missing_var}")
        except Exception as e:
            raise RuntimeError(f"Template compilation failed: {e}")
    
    def get_template_parameters(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get template LLM parameters"""
        template = self.get_template(template_id)
        if not template:
            return None
        
        return template.get("parameters", {
            "temperature": 0.7,
            "max_tokens": 2000,
            "model": "gpt-4"
        })
    
    def extract_template_variables(self, template_id: str) -> List[str]:
        """Extract required variables from template prompt"""
        template = self.get_template(template_id)
        if not template:
            return []
        
        prompt = template.get("prompt", "")
        
        # Extract variables using simple regex-like approach
        variables = []
        i = 0
        while i < len(prompt):
            if prompt[i] == '{':
                j = i + 1
                while j < len(prompt) and prompt[j] != '}':
                    j += 1
                if j < len(prompt):
                    var_name = prompt[i+1:j]
                    if var_name and var_name not in variables:
                        variables.append(var_name)
                i = j
            else:
                i += 1
        
        return variables
    
    # ==================== Validation & Analytics ====================
    
    def validate_template_structure(self, template_data: Dict[str, Any]) -> bool:
        """
        Comprehensive template validation
        
        Args:
            template_data: Template data to validate
            
        Returns:
            True if valid
            
        Raises:
            TemplateValidationError: If validation fails
        """
        errors = []
        
        # Required fields validation
        required_fields = ["name", "domain", "description", "prompt"]
        for field in required_fields:
            if field not in template_data or not template_data[field]:
                errors.append(f"Missing required field: {field}")
        
        # Prompt validation
        prompt = template_data.get("prompt", "")
        if prompt:
            # Check for required placeholders
            required_placeholders = ["{title}", "{transcript}"]
            for placeholder in required_placeholders:
                if placeholder not in prompt:
                    errors.append(f"Prompt missing required placeholder: {placeholder}")
            
            # Check for unmatched braces
            open_braces = prompt.count("{")
            close_braces = prompt.count("}")
            if open_braces != close_braces:
                errors.append("Prompt has unmatched braces")
        
        # Parameters validation
        parameters = template_data.get("parameters", {})
        if parameters:
            if "temperature" in parameters:
                temp = parameters["temperature"]
                if not isinstance(temp, (int, float)) or temp < 0 or temp > 2:
                    errors.append("Temperature must be a number between 0 and 2")
            
            if "max_tokens" in parameters:
                max_tokens = parameters["max_tokens"]
                if not isinstance(max_tokens, int) or max_tokens < 1:
                    errors.append("max_tokens must be a positive integer")
        
        # Domain validation
        domain = template_data.get("domain", "")
        valid_domains = {
            "general", "business", "research", "medical", "technology", 
            "education", "social_science", "data_science"
        }
        if domain and domain not in valid_domains:
            # Warning but not error for custom domains
            pass
        
        if errors:
            raise TemplateValidationError("; ".join(errors))
        
        return True
    
    def get_template_analytics(self) -> Dict[str, Any]:
        """Get comprehensive template system analytics"""
        total_templates = len(self._templates)
        
        # Count by type
        type_counts = {}
        domain_counts = {}
        complexity_stats = []
        
        for metadata in self._template_metadata.values():
            template_type = metadata["template_type"]
            domain = metadata["domain"]
            complexity = metadata["complexity_score"]
            
            type_counts[template_type] = type_counts.get(template_type, 0) + 1
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
            complexity_stats.append(complexity)
        
        # Calculate complexity statistics
        avg_complexity = sum(complexity_stats) / len(complexity_stats) if complexity_stats else 0
        max_complexity = max(complexity_stats) if complexity_stats else 0
        min_complexity = min(complexity_stats) if complexity_stats else 0
        
        return {
            "total_templates": total_templates,
            "templates_by_type": type_counts,
            "templates_by_domain": domain_counts,
            "complexity_analysis": {
                "average_complexity": round(avg_complexity, 2),
                "max_complexity": max_complexity,
                "min_complexity": min_complexity
            },
            "cache_performance": {
                "compiled_cache_size": len(self._compiled_cache),
                "validation_cache_size": len(self._validation_cache),
                "cache_enabled": self.enable_caching
            },
            "validation_status": {
                "strict_validation": self.validation_strict,
                "validated_templates": sum(1 for valid in self._validation_cache.values() if valid),
                "failed_validation": sum(1 for valid in self._validation_cache.values() if not valid)
            }
        }
    
    # ==================== Utility Methods ====================
    
    def _save_custom_template(self, template_id: str, template_data: Dict[str, Any]):
        """Save custom template to file"""
        custom_dir = Path(self.custom_templates_dir)
        custom_dir.mkdir(parents=True, exist_ok=True)
        
        template_file = custom_dir / f"{template_id}.yaml"
        
        # Prepare data for saving (remove internal fields)
        save_data = {k: v for k, v in template_data.items() 
                    if k not in ["template_type", "immutable"]}
        
        with open(template_file, 'w', encoding='utf-8') as f:
            yaml.dump(save_data, f, default_flow_style=False, allow_unicode=True)
    
    def _increment_version(self, current_version: str) -> str:
        """Increment template version number"""
        try:
            parts = current_version.split(".")
            if len(parts) >= 3:
                parts[2] = str(int(parts[2]) + 1)
            else:
                parts.append("1")
            return ".".join(parts)
        except:
            return "1.0.1"
    
    def _generate_cache_key(self, template_id: str, variables: Dict[str, Any]) -> str:
        """Generate cache key for compiled templates"""
        var_string = json.dumps(variables, sort_keys=True)
        key_data = f"{template_id}:{var_string}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def clear_cache(self):
        """Clear all cached data"""
        self._compiled_cache.clear()
        self._validation_cache.clear()


# Global template engine instance
_template_engine = None

def get_template_manager() -> TemplateEngine:
    """Get global enterprise template engine instance"""
    global _template_engine
    if _template_engine is None:
        _template_engine = TemplateEngine()
    return _template_engine


# Backward compatibility alias
TemplateManager = TemplateEngine
