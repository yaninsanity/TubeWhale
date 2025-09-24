"""
Create Premium Test User Management Command
创建Premium测试用户的Django管理命令
"""

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from utils.premium_config_service import premium_service

User = get_user_model()

class Command(BaseCommand):
    help = '创建Premium测试用户并自动配置API keys'

    def add_arguments(self, parser):
        parser.add_argument(
            '--username', 
            type=str, 
            default='premium_user',
            help='用户名 (默认: premium_user)'
        )
        parser.add_argument(
            '--email', 
            type=str, 
            default='premium@tubewhale.local',
            help='邮箱地址 (默认: premium@tubewhale.local)'
        )
        parser.add_argument(
            '--password', 
            type=str, 
            default='premium123!',
            help='密码 (默认: premium123!)'
        )

    def handle(self, *args, **options):
        username = options['username']
        email = options['email']
        password = options['password']
        
        try:
            # 检查用户是否已存在
            if User.objects.filter(username=username).exists():
                user = User.objects.get(username=username)
                self.stdout.write(
                    self.style.WARNING(f'用户 {username} 已存在，将更新为Premium级别')
                )
            else:
                # 创建新用户
                user = User.objects.create_user(
                    username=username,
                    email=email,
                    password=password
                )
                self.stdout.write(
                    self.style.SUCCESS(f'✅ 创建新用户: {username}')
                )
            
            # 设置为Premium级别
            user.tier = 'premium'
            user.is_verified = True
            user.save()
            
            self.stdout.write(
                self.style.SUCCESS(f'✅ 用户 {username} 已设置为Premium级别')
            )
            
            # 执行Premium自动配置
            self.stdout.write('🔧 开始Premium自动配置...')
            result = premium_service.auto_configure_premium_user(user)
            
            if result['success']:
                self.stdout.write(
                    self.style.SUCCESS(f"🎉 {result['message']}")
                )
                self.stdout.write(f"✅ 配置了 {result['configured_keys']} 个API keys")
                self.stdout.write(f"✅ OpenAI配置: {result['details']['openai_configured']}")
                self.stdout.write(f"✅ YouTube keys: {result['details']['youtube_keys_count']}个")
            else:
                self.stdout.write(
                    self.style.ERROR(f"❌ 自动配置失败: {result['message']}")
                )
            
            # 显示登录信息
            self.stdout.write('\n' + '='*50)
            self.stdout.write(self.style.SUCCESS('🌟 Premium用户创建完成！'))
            self.stdout.write('='*50)
            self.stdout.write(f'👤 用户名: {username}')
            self.stdout.write(f'📧 邮箱: {email}')
            self.stdout.write(f'🔑 密码: {password}')
            self.stdout.write(f'⭐ 等级: Premium')
            self.stdout.write(f'🔗 登录地址: http://localhost/admin/login/')
            self.stdout.write('='*50)
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ 创建Premium用户失败: {str(e)}')
            )