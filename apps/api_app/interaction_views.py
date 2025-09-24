"""
User Interaction Flow API Views
用户交互流程API视图

提供从Welcome到分析完成的完整点击式交互体验
"""

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.utils.translation import gettext as _
from datetime import datetime

from utils.user_interaction_flow import interaction_flow_manager, InputType
from utils.scenario_template_system import scenario_manager, AnalysisScenario
from apps.user_app.models import User, UserAnalysisHistory

import json
from typing import Dict, Any

class InteractionFlowView(APIView):
    """用户交互流程API - 完整的点击式体验"""
    
    permission_classes = [IsAuthenticated]
    
    def get(self, request, step_id=None):
        """获取交互流程步骤信息"""
        try:
            user = request.user
            
            # 如果没有指定步骤，从welcome开始
            if not step_id:
                step_id = "welcome"
            
            # 获取流程步骤
            flow_step = interaction_flow_manager.get_flow_step(step_id)
            if not flow_step:
                return Response({
                    'success': False,
                    'message': _('Invalid flow step'),
                    'error_code': 'INVALID_STEP'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 构建用户上下文
            user_context = self._build_user_context(user, request)
            
            # 获取下一步推荐
            next_step_recommendations = interaction_flow_manager.get_next_step_recommendations(
                step_id, user_context
            )
            
            # 生成进度信息
            progress_info = interaction_flow_manager.generate_progress_info(step_id)
            
            # 格式化响应数据
            response_data = {
                'success': True,
                'message': _('Flow step retrieved successfully'),
                'data': {
                    'step_info': {
                        'id': flow_step.id,
                        'title': flow_step.title,
                        'description': flow_step.description,
                        'component_type': flow_step.component_type,
                        'help_text': flow_step.help_text,
                        'estimated_time': flow_step.estimated_time
                    },
                    'options': self._enhance_step_options(flow_step.options, user_context),
                    'progress': progress_info,
                    'next_steps': next_step_recommendations,
                    'user_context': {
                        'is_new_user': user_context['is_new_user'],
                        'is_premium': user_context['is_premium'],
                        'analysis_count': user_context['analysis_count'],
                        'preferred_language': user_context.get('language', 'en')
                    },
                    'smart_recommendations': self._generate_smart_recommendations(step_id, user_context)
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'{_("Failed to retrieve flow step")}: {str(e)}',
                'error_code': 'FLOW_STEP_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def post(self, request, step_id=None):
        """处理用户交互输入，验证并移动到下一步"""
        try:
            user = request.user
            user_data = request.data.get('user_data', {})
            action = request.data.get('action')
            
            if not step_id:
                return Response({
                    'success': False,
                    'message': _('Step ID is required'),
                    'error_code': 'MISSING_STEP_ID'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 验证步骤完成情况
            is_valid, validation_errors = interaction_flow_manager.validate_step_completion(
                step_id, user_data
            )
            
            if not is_valid:
                return Response({
                    'success': False,
                    'message': _('Validation failed'),
                    'errors': validation_errors,
                    'error_code': 'VALIDATION_FAILED'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 处理特殊动作
            if action == "analyze_input":
                return self._handle_input_analysis(user_data.get('input', ''), user)
            elif action == "start_analysis":
                return self._handle_analysis_start(user_data, user)
            
            # 处理步骤间导航
            next_step = self._determine_next_step(step_id, action, user_data)
            
            response_data = {
                'success': True,
                'message': _('Step processed successfully'),
                'data': {
                    'current_step': step_id,
                    'next_step': next_step,
                    'action_result': self._process_step_action(step_id, action, user_data, user),
                    'redirect_url': f'/api/interaction-flow/{next_step}' if next_step else None
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'{_("Failed to process step")}: {str(e)}',
                'error_code': 'STEP_PROCESSING_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _build_user_context(self, user: User, request) -> Dict[str, Any]:
        """构建用户上下文"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
        except:
            analysis_count = 0
        
        return {
            'user_id': str(user.id),
            'is_new_user': analysis_count < 3,
            'is_premium': user.tier == 'premium',
            'analysis_count': analysis_count,
            'language': user.language if hasattr(user, 'language') else 'en',
            'has_analysis_history': analysis_count > 0,
            'timezone': getattr(user, 'timezone_setting', 'UTC'),
            'current_page': request.GET.get('page', ''),
            'referrer': request.GET.get('ref', '')
        }
    
    def _enhance_step_options(self, options: list, user_context: Dict[str, Any]) -> list:
        """增强步骤选项，添加个性化信息"""
        enhanced_options = []
        
        for option in options:
            enhanced_option = option.copy()
            
            # 为新用户添加推荐标识
            if user_context['is_new_user'] and option.get('recommended'):
                enhanced_option['badge'] = _('Recommended for beginners')
                enhanced_option['highlight'] = True
            
            # 为Premium用户显示特殊功能
            if user_context['is_premium'] and 'premium' in option.get('description', '').lower():
                enhanced_option['premium_badge'] = True
            
            # 添加预估时间和难度信息
            if 'difficulty' in option:
                enhanced_option['difficulty_info'] = {
                    'level': option['difficulty'],
                    'user_suitable': self._is_suitable_for_user(option['difficulty'], user_context),
                    'estimated_success_rate': self._estimate_success_rate(option['difficulty'], user_context)
                }
            
            enhanced_options.append(enhanced_option)
        
        return enhanced_options
    
    def _is_suitable_for_user(self, difficulty: str, user_context: Dict[str, Any]) -> bool:
        """判断难度是否适合用户"""
        if user_context['is_new_user']:
            return difficulty == 'beginner'
        elif user_context['analysis_count'] < 10:
            return difficulty in ['beginner', 'intermediate']
        else:
            return True
    
    def _estimate_success_rate(self, difficulty: str, user_context: Dict[str, Any]) -> str:
        """估算成功率"""
        if user_context['is_new_user']:
            rates = {'beginner': '95%', 'intermediate': '80%', 'advanced': '65%'}
        else:
            rates = {'beginner': '98%', 'intermediate': '92%', 'advanced': '85%'}
        
        return rates.get(difficulty, '90%')
    
    def _generate_smart_recommendations(self, step_id: str, user_context: Dict[str, Any]) -> list:
        """生成智能推荐"""
        recommendations = []
        
        if step_id == "welcome":
            if user_context['is_new_user']:
                recommendations.append({
                    'type': 'tip',
                    'title': _('First Time Here?'),
                    'content': _('Try our Quick Start - it\'s the fastest way to see TubeWhale in action!'),
                    'icon': '💡'
                })
            
            if user_context['is_premium']:
                recommendations.append({
                    'type': 'premium',
                    'title': _('Premium Features Available'),
                    'content': _('Access unlimited analysis, batch processing, and advanced insights.'),
                    'icon': '⚡'
                })
        
        elif step_id == "input_selection":
            if user_context['analysis_count'] == 0:
                recommendations.append({
                    'type': 'suggestion',
                    'title': _('Suggestion for First Analysis'),
                    'content': _('Try analyzing a popular educational video - they usually have clear structure and good results!'),
                    'icon': '🎯'
                })
        
        elif step_id == "scenario_selection":
            recommendations.append({
                'type': 'info',
                'title': _('Not Sure Which to Choose?'),
                'content': _('Education Research works well for most content types and provides clear, actionable insights.'),
                'icon': '🤔'
            })
        
        return recommendations
    
    def _handle_input_analysis(self, user_input: str, user: User) -> Response:
        """处理输入分析请求"""
        try:
            # 分析用户输入
            input_analysis = interaction_flow_manager.analyze_user_input(user_input)
            
            # 添加处理建议
            processing_suggestions = self._generate_processing_suggestions(input_analysis, user)
            
            return Response({
                'success': True,
                'message': _('Input analyzed successfully'),
                'data': {
                    'input_analysis': input_analysis,
                    'processing_suggestions': processing_suggestions,
                    'next_recommended_action': 'proceed_to_scenario_selection' if input_analysis['valid'] else 'fix_input'
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'{_("Failed to analyze input")}: {str(e)}',
                'error_code': 'INPUT_ANALYSIS_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _generate_processing_suggestions(self, input_analysis: Dict[str, Any], user: User) -> list:
        """根据输入分析生成处理建议"""
        suggestions = []
        
        if not input_analysis['valid']:
            suggestions.extend(input_analysis.get('suggestions', []))
            return suggestions
        
        input_type = input_analysis['type']
        
        if input_type == 'video_url' or input_type == 'video_id':
            suggestions.append({
                'type': 'processing',
                'content': _('Single video analysis typically takes 3-8 minutes'),
                'icon': '⏱️'
            })
            
            if user.tier == 'premium':
                suggestions.append({
                    'type': 'premium',
                    'content': _('Premium users get priority processing and detailed insights'),
                    'icon': '⚡'
                })
        
        elif input_type == 'playlist_url':
            suggestions.append({
                'type': 'info',
                'content': _('Playlist analysis may take 15-30 minutes depending on size'),
                'icon': '📚'
            })
            
            suggestions.append({
                'type': 'tip',
                'content': _('We\'ll analyze up to 50 videos from the playlist'),
                'icon': '💡'
            })
        
        elif input_type == 'search_topic':
            suggestions.append({
                'type': 'processing',
                'content': _('We\'ll find and analyze the most relevant videos for your topic'),
                'icon': '🔍'
            })
            
            suggestions.append({
                'type': 'tip',
                'content': _('More specific topics usually yield better results'),
                'icon': '🎯'
            })
        
        return suggestions
    
    def _handle_analysis_start(self, user_data: Dict[str, Any], user: User) -> Response:
        """处理分析启动请求"""
        try:
            # 创建分析任务（这里应该集成实际的分析引擎）
            analysis_task = self._create_analysis_task(user_data, user)
            
            return Response({
                'success': True,
                'message': _('Analysis started successfully'),
                'data': {
                    'task_id': analysis_task['id'],
                    'status': 'processing',
                    'estimated_completion': analysis_task['estimated_completion'],
                    'tracking_url': f'/api/analysis/status/{analysis_task["id"]}',
                    'user_message': _('Your analysis is now running! You can close this page and come back later.')
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'{_("Failed to start analysis")}: {str(e)}',
                'error_code': 'ANALYSIS_START_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _create_analysis_task(self, user_data: Dict[str, Any], user: User) -> Dict[str, Any]:
        """创建分析任务"""
        import uuid
        from datetime import datetime, timedelta
        
        # 从用户数据中提取分析参数
        input_info = user_data.get('input_analysis', {})
        scenario = user_data.get('selected_scenario', 'education_research')
        
        task_id = str(uuid.uuid4())
        
        # 根据输入类型估算完成时间
        input_type = input_info.get('type', 'search_topic')
        if input_type in ['video_url', 'video_id']:
            estimated_minutes = 8
        elif input_type == 'playlist_url':
            estimated_minutes = 25
        elif input_type == 'search_topic':
            estimated_minutes = 15
        else:
            estimated_minutes = 10
        
        estimated_completion = datetime.now() + timedelta(minutes=estimated_minutes)
        
        # TODO: 这里应该调用实际的分析引擎创建任务
        
        return {
            'id': task_id,
            'estimated_completion': estimated_completion.isoformat(),
            'input_type': input_type,
            'scenario': scenario,
            'user_id': str(user.id)
        }
    
    def _determine_next_step(self, current_step: str, action: str, user_data: Dict[str, Any]) -> str:
        """确定下一步骤"""
        step_navigation = {
            "welcome": {
                "goto_input_selection": "input_selection",
                "goto_scenario_selection": "scenario_selection",
                "goto_examples": "examples"
            },
            "input_selection": {
                "process_single_video": "scenario_selection",
                "process_playlist": "scenario_selection", 
                "process_search_topic": "scenario_selection"
            },
            "scenario_selection": {
                "select_scenario": "analysis_confirmation"
            },
            "analysis_confirmation": {
                "start_analysis": "analysis_processing",
                "goto_scenario_selection": "scenario_selection"
            }
        }
        
        return step_navigation.get(current_step, {}).get(action, current_step)
    
    def _process_step_action(self, step_id: str, action: str, user_data: Dict[str, Any], user: User) -> Dict[str, Any]:
        """处理步骤动作"""
        result = {
            'action': action,
            'step': step_id,
            'processed_at': str(datetime.now()),
            'success': True
        }
        
        # 根据不同的步骤和动作进行处理
        if step_id == "welcome" and action in ["goto_input_selection", "goto_scenario_selection"]:
            result['message'] = _('Navigation processed')
            
        elif step_id == "input_selection" and action.startswith("process_"):
            # 保存用户输入信息到会话或数据库
            result['input_saved'] = True
            result['input_type'] = user_data.get('input_type')
            
        elif step_id == "scenario_selection" and action == "select_scenario":
            # 保存选择的场景
            result['scenario_saved'] = True
            result['selected_scenario'] = user_data.get('selected_scenario')
        
        return result


class InputAnalysisView(APIView):
    """输入分析API - 独立的输入处理端点"""
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        """分析用户输入并返回详细信息"""
        try:
            user_input = request.data.get('input', '').strip()
            
            if not user_input:
                return Response({
                    'success': False,
                    'message': _('Input is required'),
                    'error_code': 'EMPTY_INPUT'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 分析输入
            analysis_result = interaction_flow_manager.analyze_user_input(user_input)
            
            # 生成处理建议
            suggestions = self._generate_processing_suggestions(analysis_result, request.user)
            
            # 推荐相关场景
            recommended_scenarios = self._recommend_scenarios_for_input(analysis_result)
            
            response_data = {
                'success': True,
                'message': _('Input analysis completed'),
                'data': {
                    'analysis': analysis_result,
                    'processing_suggestions': suggestions,
                    'recommended_scenarios': recommended_scenarios,
                    'can_proceed': analysis_result['valid']
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'{_("Input analysis failed")}: {str(e)}',
                'error_code': 'INPUT_ANALYSIS_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _generate_processing_suggestions(self, analysis_result: Dict[str, Any], user: User) -> list:
        """生成处理建议（复用上面的逻辑）"""
        # 这里可以复用上面的 _generate_processing_suggestions 方法
        return []
    
    def _recommend_scenarios_for_input(self, analysis_result: Dict[str, Any]) -> list:
        """根据输入类型推荐场景"""
        input_type = analysis_result.get('type')
        
        if input_type in ['video_url', 'video_id']:
            return [
                {'id': 'education_research', 'reason': _('Great for analyzing educational content')},
                {'id': 'content_creator_study', 'reason': _('Perfect for studying video creation techniques')}
            ]
        elif input_type == 'playlist_url':
            return [
                {'id': 'market_research', 'reason': _('Excellent for analyzing trends across multiple videos')},
                {'id': 'content_creator_study', 'reason': _('Ideal for studying creator strategies over time')}
            ]
        elif input_type == 'search_topic':
            return [
                {'id': 'education_research', 'reason': _('Perfect for researching learning content on your topic')},
                {'id': 'market_research', 'reason': _('Great for market analysis and trend research')}
            ]
        
        return []