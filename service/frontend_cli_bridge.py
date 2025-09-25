#!/usr/bin/env python3
"""
Frontend-CLI Bridge API
前端到CLI的桥接API，支持模板injection和完整功能调用
"""

import json
import subprocess
import os
import tempfile
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path


class FrontendCLIBridge:
    """
    前端-CLI桥接器
    支持：
    1. 前端直接调用CLI功能
    2. 模板动态injection
    3. 结果实时返回
    4. 错误处理和日志
    """
    
    def __init__(self, cli_path: str = None):
        self.cli_path = cli_path or self._find_cli_path()
        self.temp_dir = Path(tempfile.gettempdir()) / 'tubewhale_bridge'
        self.temp_dir.mkdir(exist_ok=True)
        
    def _find_cli_path(self) -> str:
        """查找CLI入口文件"""
        possible_paths = [
            'main.py',
            os.path.join(os.getcwd(), 'main.py'),
            os.path.join(os.path.dirname(__file__), '..', 'main.py')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return os.path.abspath(path)
                
        raise FileNotFoundError("Cannot find main.py CLI entry point")
    
    def inject_and_analyze(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        注入模板并执行分析
        支持前端传递的完整请求
        """
        try:
            # 提取请求参数
            video_id = request_data.get('video_id')
            role_id = request_data.get('role_id', 'content_creator')
            template_data = request_data.get('template_data')
            custom_questions = request_data.get('custom_questions', [])
            output_format = request_data.get('output_format', 'text')
            
            if not video_id:
                return {
                    'success': False,
                    'error': 'video_id is required'
                }
            
            # 准备CLI参数
            cli_args = [
                'python3', self.cli_path,
                'video', video_id,
                '--role', role_id,
                '--output-format', output_format,
                '--monitor'
            ]
            
            # 处理模板injection
            if template_data:
                template_file = self._save_injected_template(template_data)
                cli_args.extend(['--injected-template-file', template_file])
            
            # 处理自定义问题
            if custom_questions:
                questions_file = self._save_custom_questions(custom_questions)
                cli_args.extend(['--custom-questions', questions_file])
            
            # 执行CLI命令
            result = self._execute_cli(cli_args)
            
            # 清理临时文件
            self._cleanup_temp_files([template_file, questions_file] if 'template_file' in locals() else [])
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def list_available_templates(self) -> Dict[str, Any]:
        """获取可用模板列表"""
        try:
            cli_args = [
                'python3', self.cli_path,
                'templates', '--list',
                '--output-format', 'json'
            ]
            
            result = self._execute_cli(cli_args)
            
            if result['success']:
                # 解析模板列表
                try:
                    templates = json.loads(result['output'])
                    return {
                        'success': True,
                        'templates': templates,
                        'total': len(templates) if isinstance(templates, list) else 0
                    }
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'templates': [],
                        'raw_output': result['output']
                    }
            else:
                return result
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_template_details(self, template_id: str) -> Dict[str, Any]:
        """获取模板详细信息"""
        try:
            cli_args = [
                'python3', self.cli_path,
                'template-info', template_id,
                '--output-format', 'json'
            ]
            
            return self._execute_cli(cli_args)
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def validate_template(self, template_data: Dict[str, Any]) -> Dict[str, Any]:
        """验证模板格式和内容"""
        try:
            # 基本验证
            required_fields = ['id', 'name', 'prompt']
            missing_fields = [field for field in required_fields if field not in template_data]
            
            if missing_fields:
                return {
                    'success': False,
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                }
            
            # 保存临时模板文件
            template_file = self._save_injected_template(template_data)
            
            # CLI验证
            cli_args = [
                'python3', self.cli_path,
                'validate-template', template_file,
                '--output-format', 'json'
            ]
            
            result = self._execute_cli(cli_args)
            
            # 清理临时文件
            self._cleanup_temp_files([template_file])
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def batch_analyze(self, batch_request: Dict[str, Any]) -> Dict[str, Any]:
        """批量分析多个视频"""
        try:
            video_ids = batch_request.get('video_ids', [])
            if not video_ids:
                return {
                    'success': False,
                    'error': 'video_ids list is required'
                }
            
            results = []
            for video_id in video_ids:
                request_data = {
                    **batch_request,
                    'video_id': video_id
                }
                result = self.inject_and_analyze(request_data)
                results.append({
                    'video_id': video_id,
                    'result': result
                })
            
            return {
                'success': True,
                'batch_results': results,
                'total_processed': len(results),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _save_injected_template(self, template_data: Dict[str, Any]) -> str:
        """保存注入的模板到临时文件"""
        template_file = self.temp_dir / f"injected_template_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(template_file, 'w', encoding='utf-8') as f:
            json.dump(template_data, f, ensure_ascii=False, indent=2)
            
        return str(template_file)
    
    def _save_custom_questions(self, questions: List[str]) -> str:
        """保存自定义问题到临时文件"""
        questions_file = self.temp_dir / f"custom_questions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(questions_file, 'w', encoding='utf-8') as f:
            for question in questions:
                f.write(f"{question}\n")
                
        return str(questions_file)
    
    def _execute_cli(self, cli_args: List[str]) -> Dict[str, Any]:
        """执行CLI命令并返回结果"""
        try:
            # 设置环境变量
            env = os.environ.copy()
            env['PYTHONPATH'] = os.path.dirname(self.cli_path)
            
            # 执行命令
            result = subprocess.run(
                cli_args,
                capture_output=True,
                text=True,
                timeout=300,  # 5分钟超时
                env=env,
                cwd=os.path.dirname(self.cli_path)
            )
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'output': result.stdout,
                'error': result.stderr,
                'command': ' '.join(cli_args),
                'timestamp': datetime.now().isoformat()
            }
            
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': 'CLI command timed out (5 minutes)',
                'command': ' '.join(cli_args),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'command': ' '.join(cli_args),
                'timestamp': datetime.now().isoformat()
            }
    
    def _cleanup_temp_files(self, file_paths: List[str]):
        """清理临时文件"""
        for file_path in file_paths:
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Warning: Failed to cleanup temp file {file_path}: {e}")


# Django View Integration
if __name__ != "__main__":
    try:
        from django.http import JsonResponse
        from django.views.decorators.csrf import csrf_exempt
        from django.views.decorators.http import require_http_methods
        import json
        
        # Global bridge instance
        bridge = FrontendCLIBridge()
        
        @csrf_exempt
        @require_http_methods(["POST"])
        def frontend_cli_analyze(request):
            """前端分析API端点"""
            try:
                data = json.loads(request.body)
                result = bridge.inject_and_analyze(data)
                return JsonResponse(result)
            except Exception as e:
                return JsonResponse({'success': False, 'error': str(e)}, status=500)
        
        @require_http_methods(["GET"])
        def frontend_cli_templates(request):
            """获取模板列表API端点"""
            result = bridge.list_available_templates()
            return JsonResponse(result)
        
        @csrf_exempt
        @require_http_methods(["POST"])
        def frontend_cli_validate_template(request):
            """验证模板API端点"""
            try:
                data = json.loads(request.body)
                result = bridge.validate_template(data)
                return JsonResponse(result)
            except Exception as e:
                return JsonResponse({'success': False, 'error': str(e)}, status=500)
        
        @csrf_exempt
        @require_http_methods(["POST"])
        def frontend_cli_batch_analyze(request):
            """批量分析API端点"""
            try:
                data = json.loads(request.body)
                result = bridge.batch_analyze(data)
                return JsonResponse(result)
            except Exception as e:
                return JsonResponse({'success': False, 'error': str(e)}, status=500)
                
    except ImportError:
        # Django not available - standalone mode
        pass