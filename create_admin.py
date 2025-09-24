#!/usr/bin/env python
"""
Create super admin management command
"""
import os
import sys
import django
from django.conf import settings

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings.development')

# Setup Django
django.setup()

from django.contrib.auth.models import User

def create_superuser():
    """Create superuser admin/admin123"""
    try:
        # Check if admin user already exists
        if User.objects.filter(username='admin').exists():
            print("✅ Superuser 'admin' already exists")
            admin_user = User.objects.get(username='admin')
            admin_user.set_password('admin123')
            admin_user.is_superuser = True
            admin_user.is_staff = True
            admin_user.save()
            print("🔄 Updated admin user password to admin123, ensuring highest privileges")
        else:
            # Create new superuser
            User.objects.create_superuser(
                username='admin',
                email='admin@tubewhale.com',
                password='admin123',
                first_name='Admin',
                last_name='User'
            )
            print("🎉 Successfully created superuser: admin/admin123")
    except Exception as e:
        print(f"❌ Failed to create superuser: {e}")

if __name__ == "__main__":
    create_superuser()