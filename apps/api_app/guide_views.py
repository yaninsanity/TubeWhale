"""
用户引导API视图 - 智能引导系统
User Guide API Views - Intelligent Guidance System

提供上下文相关的用户引导、提示和帮助内容
"""

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page

from utils.user_guide_system import user_guide_manager
from apps.user_app.models import User, UserAnalysisHistory

import json
from typing import Dict, Any

class UserGuideView(APIView):
    """用户引导API - 智能化用户体验"""
    
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        """获取当前上下文相关的用户引导内容"""
        try:
            user = request.user
            
            # 构建用户上下文
            context = self._build_user_context(user, request)
            
            # 获取相关引导内容
            contextual_guides = user_guide_manager.get_contextual_guides(context)
            
            # 获取新用户引导流程
            onboarding_flow = []
            if context.get('is_new_user'):
                onboarding_flow = user_guide_manager.generate_onboarding_flow(context)
            
            # 格式化响应数据
            response_data = {
                'success': True,
                'message': '用户引导内容获取成功',
                'data': {
                    'user_context': {
                        'is_new_user': context['is_new_user'],
                        'is_premium': context['is_premium'],
                        'analysis_count': context['analysis_count'],
                        'experience_level': context['experience_level']
                    },
                    'contextual_guides': [
                        user_guide_manager.format_guide_for_frontend(guide, context)
                        for guide in contextual_guides
                    ],
                    'onboarding_flow': [
                        user_guide_manager.format_guide_for_frontend(guide, context)
                        for guide in onboarding_flow
                    ],
                    'quick_actions': self._generate_quick_actions(context)
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'获取用户引导失败: {str(e)}',
                'error_code': 'USER_GUIDE_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def post(self, request):
        """记录用户引导交互，更新显示状态"""
        try:
            guide_id = request.data.get('guide_id')
            action = request.data.get('action')  # 'shown', 'dismissed', 'completed'
            context_data = request.data.get('context', {})
            
            if not guide_id or not action:
                return Response({
                    'success': False,
                    'message': '缺少必要参数',
                    'error_code': 'MISSING_PARAMETERS'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 更新用户引导状态
            self._update_guide_status(request.user, guide_id, action, context_data)
            
            return Response({
                'success': True,
                'message': '用户引导状态更新成功'
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'更新用户引导状态失败: {str(e)}',
                'error_code': 'GUIDE_STATUS_UPDATE_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _build_user_context(self, user: User, request) -> Dict[str, Any]:
        """构建用户上下文"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
        except:
            analysis_count = 0
        
        # 从请求中获取页面上下文
        page_context = request.GET.get('page', '')
        focused_element = request.GET.get('focus', '')
        
        return {
            'user_id': str(user.id),
            'is_new_user': analysis_count < 3,
            'is_premium': user.tier == 'premium',
            'user_tier': user.tier,
            'analysis_count': analysis_count,
            'experience_level': self._get_experience_level(analysis_count),
            'current_page': page_context,
            'focused_element': focused_element,
            'visit_count': self._get_visit_count(user),
            'premium_visit_count': self._get_premium_visit_count(user) if user.tier == 'premium' else 0,
            'guide_show_counts': self._get_guide_show_counts(user)
        }
    
    def _get_experience_level(self, analysis_count: int) -> str:
        """根据分析次数确定经验水平"""
        if analysis_count < 5:
            return 'beginner'
        elif analysis_count < 20:
            return 'intermediate'
        else:
            return 'advanced'
    
    def _get_visit_count(self, user: User) -> int:
        """获取用户访问次数（简化实现）"""
        # TODO: 实现实际的访问次数统计
        return getattr(user, 'visit_count', 1)
    
    def _get_premium_visit_count(self, user: User) -> int:
        """获取Premium用户访问次数"""
        # TODO: 实现Premium访问次数统计
        return getattr(user, 'premium_visit_count', 1)
    
    def _get_guide_show_counts(self, user: User) -> Dict[str, int]:
        """获取各个引导的显示次数"""
        # TODO: 从数据库或缓存中获取实际的显示次数
        return getattr(user, 'guide_show_counts', {})
    
    def _update_guide_status(self, user: User, guide_id: str, action: str, context: Dict[str, Any]):
        """更新用户引导状态"""
        # TODO: 实现引导状态的持久化存储
        # 可以存储在User模型的JSON字段中或单独的UserGuideStatus模型中
        pass
    
    def _generate_quick_actions(self, context: Dict[str, Any]) -> list:
        """生成快速操作建议"""
        quick_actions = []
        
        if context['is_new_user']:
            quick_actions.extend([
                {
                    'title': '🚀 开始第一次分析',
                    'description': '选择「教育内容研究」场景，最容易上手',
                    'action': 'start_first_analysis',
                    'priority': 10
                },
                {
                    'title': '📖 查看使用教程',
                    'description': '2分钟快速了解所有功能',
                    'action': 'view_tutorial',
                    'priority': 8
                }
            ])
        
        if context['is_premium']:
            quick_actions.append({
                'title': '⚡ 体验Premium功能',
                'description': '尝试批量分析或高级模板',
                'action': 'explore_premium',
                'priority': 9
            })
        
        if context['analysis_count'] > 0:
            quick_actions.append({
                'title': '📊 查看历史分析',
                'description': '回顾之前的分析结果',
                'action': 'view_history',
                'priority': 6
            })
        
        # 按优先级排序
        quick_actions.sort(key=lambda x: x['priority'], reverse=True)
        
        return quick_actions[:3]  # 只返回前3个


class TooltipView(APIView):
    """工具提示API - 上下文相关的帮助信息"""
    
    permission_classes = [IsAuthenticated]
    
    def get(self, request, element_id):
        """获取特定UI元素的工具提示"""
        try:
            user = request.user
            context = self._build_context(user, request)
            
            # 获取元素相关的工具提示
            tooltips = user_guide_manager.get_tooltips_for_element(element_id, context)
            
            if not tooltips:
                return Response({
                    'success': True,
                    'message': '没有相关的工具提示',
                    'data': {'tooltips': []}
                }, status=status.HTTP_200_OK)
            
            formatted_tooltips = [
                user_guide_manager.format_guide_for_frontend(tooltip, context)
                for tooltip in tooltips
            ]
            
            return Response({
                'success': True,
                'message': '工具提示获取成功',
                'data': {'tooltips': formatted_tooltips}
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'获取工具提示失败: {str(e)}',
                'error_code': 'TOOLTIP_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _build_context(self, user: User, request) -> Dict[str, Any]:
        """构建上下文信息"""
        try:
            analysis_count = UserAnalysisHistory.objects.filter(user=user).count()
        except:
            analysis_count = 0
        
        return {
            'is_premium': user.tier == 'premium',
            'analysis_count': analysis_count,
            'is_new_user': analysis_count < 3,
            'guide_show_counts': self._get_guide_show_counts(user)
        }
    
    def _get_guide_show_counts(self, user: User) -> Dict[str, int]:
        """获取引导显示次数"""
        # TODO: 实现实际的显示次数获取
        return {}


class ErrorHelpView(APIView):
    """错误帮助API - 智能错误处理和用户帮助"""
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        """获取错误相关的帮助内容"""
        try:
            error_type = request.data.get('error_type')
            error_context = request.data.get('context', {})
            
            if not error_type:
                return Response({
                    'success': False,
                    'message': '缺少错误类型',
                    'error_code': 'MISSING_ERROR_TYPE'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 构建完整上下文
            context = self._build_error_context(request.user, error_context)
            
            # 获取错误帮助内容
            error_help = user_guide_manager.get_error_help(error_type, context)
            
            if not error_help:
                return Response({
                    'success': True,
                    'message': '暂无相关错误帮助',
                    'data': {'help_content': None}
                }, status=status.HTTP_200_OK)
            
            formatted_help = user_guide_manager.format_guide_for_frontend(error_help, context)
            
            return Response({
                'success': True,
                'message': '错误帮助内容获取成功',
                'data': {'help_content': formatted_help}
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'success': False,
                'message': f'获取错误帮助失败: {str(e)}',
                'error_code': 'ERROR_HELP_ERROR'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _build_error_context(self, user: User, error_context: Dict[str, Any]) -> Dict[str, Any]:
        """构建错误上下文"""
        base_context = {
            'is_premium': user.tier == 'premium',
            'user_tier': user.tier,
        }
        base_context.update(error_context)
        return base_context