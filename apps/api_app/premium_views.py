"""
Premium User Configuration API Views
Premium用户专享API端点
"""

from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.utils.translation import gettext as _
from utils.premium_config_service import premium_service
import logging

logger = logging.getLogger(__name__)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def premium_config_status(request):
    """
    获取Premium用户配置状态
    GET /api/premium/config/status/
    """
    try:
        user = request.user
        status_info = premium_service.get_premium_user_config_status(user)
        
        return Response({
            'success': True,
            'data': status_info,
            'message': '配置状态获取成功'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"❌ 获取Premium配置状态失败: {str(e)}")
        return Response({
            'success': False,
            'error': str(e),
            'message': '获取配置状态失败'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def premium_auto_configure(request):
    """
    Premium用户一键自动配置
    POST /api/premium/config/auto-setup/
    """
    try:
        user = request.user
        
        # 验证用户是否是Premium级别
        if not user.can_customize_templates():
            return Response({
                'success': False,
                'message': 'Premium功能需要升级账户等级',
                'current_tier': user.tier,
                'required_tier': 'premium',
                'upgrade_url': '/upgrade/'
            }, status=status.HTTP_403_FORBIDDEN)
        
        # 执行自动配置
        result = premium_service.trigger_premium_auto_setup(user)
        
        if result['success']:
            return Response({
                'success': True,
                'data': result,
                'message': result.get('message', 'Premium自动配置成功！'),
                'user_experience': result.get('user_experience', '享受零配置体验！')
            }, status=status.HTTP_200_OK)
        else:
            return Response({
                'success': False,
                'error': result.get('message', '自动配置失败'),
                'data': result
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"❌ Premium自动配置失败: {str(e)}")
        return Response({
            'success': False,
            'error': str(e),
            'message': '自动配置过程中发生错误'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def premium_benefits(request):
    """
    获取Premium用户权益信息
    GET /api/premium/benefits/
    """
    try:
        user = request.user
        
        benefits = {
            'is_premium': user.can_customize_templates(),
            'current_tier': user.tier,
            'tier_display': user.get_tier_display_name(),
            'premium_features': {
                'auto_api_key_injection': {
                    'available': user.can_customize_templates(),
                    'description': '管理员预配置API keys自动注入，无需手动设置',
                    'icon': '🔑'
                },
                'one_click_setup': {
                    'available': user.can_customize_templates(),
                    'description': '一键完成所有配置，零技术门槛',
                    'icon': '⚡'
                },
                'priority_support': {
                    'available': user.can_customize_templates(),
                    'description': '优先技术支持和故障解决',
                    'icon': '🌟'
                },
                'advanced_templates': {
                    'available': user.can_customize_templates(),
                    'description': '访问高级AI模板和自定义功能',
                    'icon': '🚀'
                },
                'no_config_complexity': {
                    'available': user.can_customize_templates(),
                    'description': '告别复杂配置，专注核心业务',
                    'icon': '🎯'
                }
            },
            'hci_optimizations': {
                'simplified_interface': '简化的用户界面，隐藏技术复杂度',
                'smart_defaults': '智能默认配置，适合大多数使用场景',
                'contextual_help': '上下文相关的帮助和提示',
                'accessibility': '遵循WCAG无障碍设计标准',
                'user_experience': '以用户为中心的交互设计'
            }
        }
        
        return Response({
            'success': True,
            'data': benefits,
            'message': 'Premium权益信息获取成功'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"❌ 获取Premium权益信息失败: {str(e)}")
        return Response({
            'success': False,
            'error': str(e),
            'message': '获取权益信息失败'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def user_api_keys(request):
    """
    获取用户的API keys列表（隐藏敏感信息）
    GET /api/premium/api-keys/
    """
    try:
        user = request.user
        api_keys = user.get_active_api_keys()
        
        keys_data = []
        for key in api_keys:
            keys_data.append({
                'id': key.id,
                'name': key.name,
                'service_type': key.service_type,
                'key_type': key.key_type,
                'prefix': key.prefix,
                'is_active': key.is_active,
                'created_at': key.created_at,
                'last_used': key.last_used,
                'usage_count': key.usage_count,
                # 不暴露完整的API key
                'key_preview': f"{key.prefix}***"
            })
        
        return Response({
            'success': True,
            'data': {
                'api_keys': keys_data,
                'total_count': len(keys_data),
                'premium_auto_configured': len([k for k in keys_data if k['key_type'] == 'admin_configured'])
            },
            'message': 'API keys信息获取成功'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"❌ 获取用户API keys失败: {str(e)}")
        return Response({
            'success': False,
            'error': str(e),
            'message': '获取API keys信息失败'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)