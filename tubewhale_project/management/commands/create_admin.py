"""
Django management command: Create super administrator
"""
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.db import transaction

class Command(BaseCommand):
    help = 'Create or update super administrator account admin/admin123'

    def handle(self, *args, **options):
        """处理命令"""
        try:
            with transaction.atomic():
                # 检查是否已存在admin用户
                if User.objects.filter(username='admin').exists():
                    self.stdout.write(
                        self.style.WARNING('✅ 超级用户 "admin" 已存在')
                    )
                    admin_user = User.objects.get(username='admin')
                    admin_user.set_password('admin123')
                    admin_user.is_superuser = True
                    admin_user.is_staff = True
                    admin_user.is_active = True
                    admin_user.email = 'admin@tubewhale.com'
                    admin_user.first_name = 'Admin'
                    admin_user.last_name = 'User'
                    admin_user.save()
                    self.stdout.write(
                        self.style.SUCCESS('🔄 已更新 admin 用户密码为 admin123，确保最高权限')
                    )
                else:
                    # 创建新的超级用户
                    admin_user = User.objects.create_superuser(
                        username='admin',
                        email='admin@tubewhale.com',
                        password='admin123',
                        first_name='Admin',
                        last_name='User'
                    )
                    admin_user.is_active = True
                    admin_user.save()
                    self.stdout.write(
                        self.style.SUCCESS('🎉 成功创建超级用户: admin/admin123')
                    )
                
                # 显示用户信息
                self.stdout.write('\n📋 超级管理员信息:')
                self.stdout.write(f'   用户名: admin')
                self.stdout.write(f'   密码: admin123')
                self.stdout.write(f'   邮箱: admin@tubewhale.com')
                self.stdout.write(f'   超级用户: {admin_user.is_superuser}')
                self.stdout.write(f'   管理员: {admin_user.is_staff}')
                self.stdout.write(f'   激活状态: {admin_user.is_active}')
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ 创建超级用户失败: {e}')
            )