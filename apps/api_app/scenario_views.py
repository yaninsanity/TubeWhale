"""
场景选择API视图 - YouTube级别的用户体验
Scenario Selection API Views - YouTube-level User Experience

让任何看过YouTube的用户都能直观理解和使用我们的分析功能
"""

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.utils.translation import gettext_lazy as _
from django.utils.translation import gettext

from utils.scenario_template_system import scenario_manager, AnalysisScenario
from apps.user_app.models import User, UserAnalysisHistory

import json
from typing import Dict, Any

class ScenarioListView(APIView):
    """场景列表API - 就像YouTube首页推荐一样智能"""
    
    permission_classes = [IsAuthenticated]
    
    @method_decorator(cache_page(60 * 15))  # 15分钟缓存
    def get(self, request):
        """获取场景列表，根据用户情况智能推荐"""
        try:
            user = request.user
            
            # 构建用户上下文
            user_context = self._build_user_context(user)
            
            # 获取所有场景模板
            all_scenarios = scenario_manager.get_all_scenarios()
            
            # 获取推荐场景
            recommended_scenarios = scenario_manager.get_recommended_scenarios_for_user(user_context)
            
            response_data = {
                'success': True,
                'message': str(_('Scenario list retrieved successfully')),
                'data': {
                    'user_info': {
                        'is_premium': user.tier == 'premium',
                        'analysis_count': getattr(user, 'analysis_count', 0),
                        'is_new_user': self._is_new_user(user),
                        'experience_level': self._get_user_experience_level(user)
                    },
                    'recommendations': {
                        'title': str(_('Recommended for You 📈')),
                        'subtitle': str(_('Based on your usage habits, these scenarios are most suitable for you')),
                        'scenarios': [self._format_scenario_card(template) for template in recommended_scenarios]
                    },
                    'all_scenarios': {
                        'title': str(_('All Analysis Scenarios 🎯')),
                        'subtitle': str(_('Choose the analysis scenario that best fits your needs')),
                        'categories': self._organize_scenarios_by_category(all_scenarios)
                    },
                    'quick_tips': self._generate_quick_tips(user_context)
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'获取场景列表失败: {str(e)}',
                'error_code': 'SCENARIO_LIST_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _build_user_context(self, user: User) -> Dict[str, Any]:
        """构建用户上下文信息"""
        return {
            'is_new_user': self._is_new_user(user),
            'tier': user.tier,
            'analysis_count': getattr(user, 'analysis_count', 0),
            'interests': self._extract_user_interests(user),
            'experience_level': self._get_user_experience_level(user)
        }
    
    def _is_new_user(self, user: User) -> bool:
        """判断是否为新用户"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
            return analysis_count < 3
        except:
            return True
    
    def _get_user_experience_level(self, user: User) -> str:
        """获取用户经验水平"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
            if analysis_count < 5:
                return 'beginner'
            elif analysis_count < 20:
                return 'intermediate'
            else:
                return 'advanced'
        except:
            return 'beginner'
    
    def _extract_user_interests(self, user: User) -> list:
        """提取用户兴趣偏好"""
        # TODO: 基于用户历史分析记录提取兴趣标签
        return ['education', 'technology', 'business']  # 默认兴趣
    
    def _format_scenario_card(self, template) -> Dict[str, Any]:
        """格式化场景卡片信息 - YouTube卡片风格"""
        return {
            'scenario_id': template.scenario.value,
            'title': template.display_name,
            'description': template.description,
            'icon': template.icon,
            'difficulty': {
                'level': template.difficulty,
                'display': self._get_difficulty_display(template.difficulty),
                'color': self._get_difficulty_color(template.difficulty)
            },
            'estimated_time': template.estimated_time,
            'use_cases': template.use_cases[:3],  # 只显示前3个用例
            'popularity': self._calculate_scenario_popularity(template.scenario),
            'success_rate': '95%',  # TODO: 基于实际数据计算
            'thumbnail': f'/static/scenario-thumbnails/{template.scenario.value}.png',
            'quick_preview': {
                'what_you_get': template.ai_focus_areas[:3],
                'perfect_for': template.use_cases[:2]
            }
        }
    
    def _get_difficulty_display(self, difficulty: str) -> str:
        """获取难度显示文本"""
        difficulty_map = {
            'beginner': str(_('Beginner Friendly 🌟')),
            'intermediate': str(_('Intermediate ⭐⭐')),
            'advanced': str(_('Professional Level ⭐⭐⭐'))
        }
        return difficulty_map.get(difficulty, str(_('Moderate')))
    
    def _get_difficulty_color(self, difficulty: str) -> str:
        """获取难度颜色"""
        color_map = {
            'beginner': '#4CAF50',    # 绿色
            'intermediate': '#FF9800',  # 橙色
            'advanced': '#F44336'     # 红色
        }
        return color_map.get(difficulty, '#2196F3')
    
    def _calculate_scenario_popularity(self, scenario: AnalysisScenario) -> str:
        """计算场景受欢迎程度"""
        # TODO: 基于实际使用数据计算
        popularity_map = {
            AnalysisScenario.EDUCATION_RESEARCH: 'high',
            AnalysisScenario.MARKET_RESEARCH: 'high', 
            AnalysisScenario.CONTENT_CREATOR_STUDY: 'medium',
        }
        return popularity_map.get(scenario, 'medium')
    
    def _organize_scenarios_by_category(self, scenarios) -> Dict[str, list]:
        """按类别组织场景"""
        categories = {
            'learning': {
                'title': str(_('📚 Learning & Research')),
                'description': str(_('Educational content analysis, knowledge extraction, learning effectiveness evaluation')),
                'scenarios': []
            },
            'business': {
                'title': str(_('💼 Business Insights')), 
                'description': str(_('Market research, competitor analysis, business opportunity discovery')),
                'scenarios': []
            },
            'content': {
                'title': str(_('🎬 Content Creation')),
                'description': str(_('Creator analysis, content strategy, audience research')),
                'scenarios': []
            },
            'lifestyle': {
                'title': str(_('🌱 Lifestyle')),
                'description': str(_('Health content, life tips, hobbies and interests')),
                'scenarios': []
            }
        }
        
        # 根据场景类型分类
        for template in scenarios:
            scenario_card = self._format_scenario_card(template)
            
            if template.scenario in [AnalysisScenario.EDUCATION_RESEARCH, 
                                   AnalysisScenario.LEARNING_CONTENT,
                                   AnalysisScenario.TUTORIAL_ANALYSIS]:
                categories['learning']['scenarios'].append(scenario_card)
            elif template.scenario in [AnalysisScenario.MARKET_RESEARCH,
                                     AnalysisScenario.COMPETITOR_ANALYSIS, 
                                     AnalysisScenario.PRODUCT_REVIEW]:
                categories['business']['scenarios'].append(scenario_card)
            elif template.scenario in [AnalysisScenario.CONTENT_CREATOR_STUDY,
                                     AnalysisScenario.ENTERTAINMENT_TRENDS,
                                     AnalysisScenario.VIRAL_CONTENT]:
                categories['content']['scenarios'].append(scenario_card)
            else:
                categories['lifestyle']['scenarios'].append(scenario_card)
        
        return categories
    
    def _generate_quick_tips(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        """生成快速提示信息"""
        if user_context['is_new_user']:
            return {
                'title': str(_('💡 Beginner Tips')),
                'tips': [
                    str(_('Choose scenarios marked with "Beginner Friendly 🌟" to start')),
                    str(_('Each scenario has detailed step-by-step guidance, as simple as YouTube tutorials')),
                    str(_('Not sure which to choose? Try "Educational Content Research" - easiest to get started!')),
                    str(_('Premium users can run multiple analysis tasks simultaneously ⚡'))
                ]
            }
        else:
            return {
                'title': str(_('🚀 Usage Tips')),
                'tips': [
                    str(_('Try batch analysis of multiple related videos for more comprehensive insights')),
                    str(_('Use different scenarios to analyze the same video, discover multi-dimensional information')),
                    str(_('Regularly analyze latest content in the same topic to track trend changes')),
                    str(_('View analysis history to build your professional knowledge base'))
                ]
            }

class ScenarioDetailView(APIView):
    """场景详情API - 就像YouTube视频详情页一样详细"""
    
    permission_classes = [IsAuthenticated]
    
    def get(self, request, scenario_id):
        """获取特定场景的详细信息和使用指导"""
        try:
            # 验证场景ID
            try:
                scenario = AnalysisScenario(scenario_id)
            except ValueError:
                return Response({
                    'success': False,
                    'message': '场景ID无效',
                    'error_code': 'INVALID_SCENARIO_ID'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 生成用户指导
            user_guide = scenario_manager.generate_user_guide(scenario)
            
            if not user_guide:
                return Response({
                    'success': False, 
                    'message': '场景详情不存在',
                    'error_code': 'SCENARIO_NOT_FOUND'
                }, status=status.HTTP_404_NOT_FOUND)
            
            # 增强用户指导信息
            enhanced_guide = self._enhance_user_guide(user_guide, request.user)
            
            response_data = {
                'success': True,
                'message': str(_('Scenario details retrieved successfully')),
                'data': enhanced_guide
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'获取场景详情失败: {str(e)}',
                'error_code': 'SCENARIO_DETAIL_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _enhance_user_guide(self, guide: Dict[str, Any], user: User) -> Dict[str, Any]:
        """增强用户指导信息"""
        guide['user_specific'] = {
            'can_use': self._check_user_permissions(user),
            'recommendations': self._get_personalized_recommendations(user, guide),
            'related_scenarios': self._get_related_scenarios(guide['scenario_info']['name'])
        }
        
        guide['interactive_demo'] = {
            'available': True,
            'demo_video_url': f'/static/demo-videos/{guide["scenario_info"]["name"]}.mp4',
            'try_sample_url': f'/api/scenarios/demo/{guide["scenario_info"]["name"]}',
            'description': '观看2分钟演示视频，或直接体验示例分析'
        }
        
        return guide
    
    def _check_user_permissions(self, user: User) -> Dict[str, Any]:
        """检查用户权限"""
        return {
            'has_access': True,  # 所有注册用户都可以使用基础功能
            'premium_features': user.tier == 'premium',
            'analysis_quota': self._get_user_quota(user),
            'upgrade_suggestion': None if user.tier == 'premium' else 
                '升级到Premium享受无限分析次数和高级功能'
        }
    
    def _get_user_quota(self, user: User) -> Dict[str, Any]:
        """获取用户配额信息"""
        if user.tier == 'premium':
            return {
                'current_usage': 0,
                'limit': '无限制',
                'reset_time': None,
                'is_unlimited': True
            }
        else:
            # TODO: 实现基础用户的配额管理
            return {
                'current_usage': 2,
                'limit': 10,
                'reset_time': '2024-01-01 00:00:00',
                'is_unlimited': False
            }
    
    def _get_personalized_recommendations(self, user: User, guide: Dict[str, Any]) -> list:
        """获取个性化推荐"""
        recommendations = []
        
        if self._is_new_user(user):
            recommendations.append({
                'type': 'tip',
                'title': '新手建议',
                'content': '建议从一个简单的教育视频开始，比如TED演讲或Khan Academy的课程'
            })
        
        if user.tier != 'premium':
            recommendations.append({
                'type': 'upgrade',
                'title': 'Premium特权',
                'content': '升级到Premium可以批量分析多个视频，节省时间提高效率'
            })
        
        return recommendations
    
    def _get_related_scenarios(self, current_scenario_name: str) -> list:
        """获取相关场景推荐"""
        # TODO: 基于场景相似度推荐相关场景
        return [
            {
                'name': '📊 市场研究分析',
                'reason': '同样适合商业用户，可以从不同角度分析内容'
            },
            {
                'name': '🎬 内容创作者分析', 
                'reason': '了解内容制作者的策略和技巧'
            }
        ]
    
    def _is_new_user(self, user: User) -> bool:
        """判断是否为新用户"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
            return analysis_count < 3
        except:
            return True

class ScenarioStartAnalysisView(APIView):
    """开始场景分析API - YouTube播放按钮一样简单"""
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        """启动分析任务"""
        try:
            scenario_id = request.data.get('scenario_id')
            youtube_url = request.data.get('youtube_url')
            custom_options = request.data.get('options', {})
            
            # 验证输入
            if not scenario_id or not youtube_url:
                return Response({
                    'success': False,
                    'message': '请选择分析场景并输入YouTube链接',
                    'error_code': 'MISSING_REQUIRED_FIELDS'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 验证场景
            try:
                scenario = AnalysisScenario(scenario_id)
            except ValueError:
                return Response({
                    'success': False,
                    'message': '无效的分析场景',
                    'error_code': 'INVALID_SCENARIO'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 检查用户权限和配额
            permission_check = self._check_analysis_permission(request.user)
            if not permission_check['allowed']:
                return Response({
                    'success': False,
                    'message': permission_check['reason'],
                    'error_code': 'PERMISSION_DENIED'
                }, status=status.HTTP_403_FORBIDDEN)
            
            # 创建分析任务
            analysis_task = self._create_analysis_task(
                user=request.user,
                scenario=scenario,
                youtube_url=youtube_url,
                options=custom_options
            )
            
            response_data = {
                'success': True,
                'message': '分析任务已启动，就像YouTube加载一样快！',
                'data': {
                    'task_id': analysis_task['id'],
                    'status': 'processing',
                    'estimated_completion': analysis_task['estimated_completion'],
                    'progress': {
                        'current_step': 1,
                        'total_steps': 4,
                        'step_description': '正在获取视频信息...',
                        'percentage': 25
                    },
                    'tracking_url': f'/api/analysis/status/{analysis_task["id"]}',
                    'user_friendly_message': '我们的AI正在认真分析您的视频，请稍等片刻 🤖✨'
                }
            }
            
            return Response(response_data, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'启动分析失败: {str(e)}',
                'error_code': 'ANALYSIS_START_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _check_analysis_permission(self, user: User) -> Dict[str, Any]:
        """检查分析权限"""
        # Premium用户无限制
        if user.tier == 'premium':
            return {'allowed': True, 'reason': None}
        
        # 基础用户检查配额
        # TODO: 实现配额检查逻辑
        return {'allowed': True, 'reason': None}
    
    def _create_analysis_task(self, user: User, scenario: AnalysisScenario, 
                            youtube_url: str, options: Dict[str, Any]) -> Dict[str, Any]:
        """创建分析任务"""
        # TODO: 集成实际的分析任务创建逻辑
        import uuid
        from datetime import datetime, timedelta
        
        task_id = str(uuid.uuid4())
        estimated_completion = datetime.now() + timedelta(minutes=10)
        
        # 这里应该调用实际的分析引擎
        
        return {
            'id': task_id,
            'estimated_completion': estimated_completion.isoformat(),
            'status': 'processing'
        }