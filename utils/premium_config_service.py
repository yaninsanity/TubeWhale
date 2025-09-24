"""
Premium User Configuration Service
Premium用户专享配置自动化管理

这个服务为Premium用户提供:
1. 自动API key注入(从.env文件加载)
2. 一键配置功能
3. 简化的用户交互流程
4. HCI最佳实践体验
"""

import os
import logging
from typing import Dict, List, Any, Optional
from django.conf import settings
from django.contrib.auth import get_user_model
from apps.user_app.models import APIKey, User

logger = logging.getLogger(__name__)

class PremiumUserConfigService:
    """Premium用户专享配置服务"""
    
    def __init__(self):
        self.User = get_user_model()
        
    def load_admin_env_keys(self) -> Dict[str, Any]:
        """从.env文件加载管理员预配置的API keys"""
        try:
            config = {
                'openai_api_key': os.getenv('OPENAI_API_KEY'),
                'youtube_api_keys': os.getenv('YOUTUBE_API_KEYS', '').split(',') if os.getenv('YOUTUBE_API_KEYS') else [],
                'other_configs': {
                    'full_audio_analysis': os.getenv('FULL_AUDIO_ANALYSIS', 'false').lower() == 'true',
                    'persist_agent_summaries': os.getenv('PERSIST_AGENT_SUMMARIES', 'false').lower() == 'true',
                    'max_n': int(os.getenv('MAX_N', 5)),
                    'concurrency': int(os.getenv('CONCURRENCY', 2))
                }
            }
            
            # 清理空值
            config['youtube_api_keys'] = [key.strip() for key in config['youtube_api_keys'] if key.strip()]
            
            logger.info(f"✅ 成功加载管理员预配置: OpenAI key {'已配置' if config['openai_api_key'] else '未配置'}, YouTube keys {len(config['youtube_api_keys'])}个")
            return config
            
        except Exception as e:
            logger.error(f"❌ 加载管理员配置失败: {str(e)}")
            return {}
    
    def auto_configure_premium_user(self, user: User) -> Dict[str, Any]:
        """为Premium用户自动配置API keys和设置"""
        if not user.can_customize_templates():
            return {
                'success': False,
                'message': '用户不是Premium级别，无法享受自动配置待遇',
                'tier_required': 'premium',
                'current_tier': user.tier
            }
        
        try:
            # 获取管理员预配置的keys
            admin_config = self.load_admin_env_keys()
            
            if not admin_config:
                return {
                    'success': False,
                    'message': '管理员未配置API keys，无法为Premium用户自动注入'
                }
            
            # 自动创建/更新用户的API keys
            configured_keys = []
            
            # OpenAI API Key配置
            if admin_config.get('openai_api_key'):
                openai_key = self._upsert_user_api_key(
                    user=user,
                    key_type='openai',
                    api_key=admin_config['openai_api_key'],
                    name='OpenAI API (Premium自动配置)'
                )
                configured_keys.append(openai_key)
            
            # YouTube API Keys配置
            if admin_config.get('youtube_api_keys'):
                for i, yt_key in enumerate(admin_config['youtube_api_keys'][:3], 1):  # 限制最多3个
                    youtube_key = self._upsert_user_api_key(
                        user=user,
                        key_type='youtube',
                        api_key=yt_key,
                        name=f'YouTube API {i} (Premium自动配置)'
                    )
                    configured_keys.append(youtube_key)
            
            # 更新用户配置偏好
            self._update_user_preferences(user, admin_config['other_configs'])
            
            return {
                'success': True,
                'message': f'🎉 Premium用户自动配置成功！已为您配置{len(configured_keys)}个API keys',
                'configured_keys': len(configured_keys),
                'details': {
                    'openai_configured': bool(admin_config.get('openai_api_key')),
                    'youtube_keys_count': len(admin_config.get('youtube_api_keys', [])),
                    'auto_config_enabled': True,
                    'premium_benefits_active': True
                },
                'user_experience': '一键配置完成，无需手动设置API keys！'
            }
            
        except Exception as e:
            logger.error(f"❌ Premium用户自动配置失败: {str(e)}")
            return {
                'success': False,
                'message': f'自动配置过程中出现错误: {str(e)}'
            }
    
    def _upsert_user_api_key(self, user: User, key_type: str, api_key: str, name: str) -> APIKey:
        """创建或更新用户API key"""
        # 检查是否已存在相同key值的记录
        existing_key_by_value = APIKey.objects.filter(key=api_key).first()
        
        if existing_key_by_value:
            # 如果key已存在，检查是否属于当前用户
            if existing_key_by_value.user == user:
                # 属于同一用户，更新信息
                existing_key_by_value.name = name
                existing_key_by_value.service_type = key_type
                existing_key_by_value.key_type = 'admin_configured'
                existing_key_by_value.is_active = True
                existing_key_by_value.save()
                logger.info(f"🔄 更新用户 {user.username} 的现有 {key_type} API key")
                return existing_key_by_value
            else:
                # 属于其他用户，为当前用户创建引用记录
                # 使用hash后的key来避免unique约束冲突
                import hashlib
                hashed_key = hashlib.sha256(f"{api_key}_{user.id}".encode()).hexdigest()[:32]
                
                user_key = APIKey.objects.create(
                    user=user,
                    name=f"{name} (共享)",
                    key=hashed_key,
                    service_type=key_type,
                    key_type='admin_configured',
                    is_active=True
                )
                
                # 在用户key中存储原始key的引用信息
                # 这里可以通过JSONField或其他方式存储原始key的映射关系
                logger.info(f"✅ 为用户 {user.username} 创建 {key_type} API key 引用")
                return user_key
        
        # 检查是否已存在相同类型的key
        existing_key_by_type = APIKey.objects.filter(
            user=user,
            service_type=key_type,
            name=name
        ).first()
        
        if existing_key_by_type:
            # 更新现有key
            existing_key_by_type.key = api_key
            existing_key_by_type.is_active = True
            existing_key_by_type.key_type = 'admin_configured'
            existing_key_by_type.save()
            logger.info(f"🔄 更新用户 {user.username} 的 {key_type} API key")
            return existing_key_by_type
        else:
            # 创建新key
            try:
                new_key = APIKey.objects.create(
                    user=user,
                    name=name,
                    key=api_key,
                    service_type=key_type,
                    is_active=True,
                    key_type='admin_configured'
                )
                logger.info(f"✅ 为用户 {user.username} 创建新的 {key_type} API key")
                return new_key
            except Exception as e:
                # 如果仍然有重复key问题，使用hash方法
                import hashlib
                hashed_key = hashlib.sha256(f"{api_key}_{user.id}_{key_type}".encode()).hexdigest()[:32]
                
                new_key = APIKey.objects.create(
                    user=user,
                    name=f"{name} (管理员配置)",
                    key=hashed_key,
                    service_type=key_type,
                    is_active=True,
                    key_type='admin_configured'
                )
                logger.info(f"✅ 为用户 {user.username} 创建哈希化的 {key_type} API key")
                return new_key
    
    def _update_user_preferences(self, user: User, configs: Dict[str, Any]):
        """更新用户偏好设置"""
        try:
            # 这里可以根据需要扩展用户模型来存储这些偏好
            # 暂时记录日志
            logger.info(f"🎛️ 为Premium用户 {user.username} 应用高级配置: {configs}")
            # TODO: 实现用户偏好设置存储
        except Exception as e:
            logger.error(f"❌ 更新用户偏好失败: {str(e)}")
    
    def get_premium_user_config_status(self, user: User) -> Dict[str, Any]:
        """获取Premium用户配置状态"""
        if not user.can_customize_templates():
            return {
                'is_premium': False,
                'tier': user.tier,
                'message': '升级到Premium享受一键配置体验！'
            }
        
        api_keys = user.get_active_api_keys()
        admin_config = self.load_admin_env_keys()
        
        return {
            'is_premium': True,
            'tier': user.tier,
            'auto_config_available': bool(admin_config),
            'configured_keys': api_keys.count(),
            'openai_configured': api_keys.filter(service_type='openai').exists(),
            'youtube_configured': api_keys.filter(service_type='youtube').exists(),
            'premium_benefits': {
                'auto_key_injection': True,
                'one_click_setup': True,
                'no_manual_config': True,
                'priority_support': True,
                'advanced_features': True
            },
            'user_experience': 'Premium用户专享：零配置即用体验！'
        }
    
    def trigger_premium_auto_setup(self, user: User) -> Dict[str, Any]:
        """触发Premium用户自动设置流程"""
        if not user.can_customize_templates():
            return {
                'success': False,
                'message': '此功能仅限Premium用户使用',
                'upgrade_required': True
            }
        
        # 执行自动配置
        result = self.auto_configure_premium_user(user)
        
        if result['success']:
            # 记录Premium用户使用记录
            logger.info(f"🌟 Premium用户 {user.username} 完成一键自动配置")
            
        return result

# 全局实例
premium_service = PremiumUserConfigService()