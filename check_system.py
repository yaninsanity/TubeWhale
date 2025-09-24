#!/usr/bin/env python
"""
Quick system check script
Verify Docker deployment and Django configuration
"""

import os
import sys
import django
import requests
from pathlib import Path

def check_docker_services():
    """Check if Docker services are running"""
    print("🐳 Checking Docker Services...")
    try:
        response = requests.get('http://localhost:8000', timeout=5)
        print("✅ Django Backend: Running (Status: {})".format(response.status_code))
    except requests.exceptions.ConnectionError:
        print("❌ Django Backend: Not accessible")
        return False
    except requests.exceptions.Timeout:
        print("⚠️  Django Backend: Timeout")
        return False
    
    try:
        response = requests.get('http://localhost', timeout=5)
        print("✅ Nginx Proxy: Running (Status: {})".format(response.status_code))
    except requests.exceptions.ConnectionError:
        print("❌ Nginx Proxy: Not accessible")
    except requests.exceptions.Timeout:
        print("⚠️  Nginx Proxy: Timeout")
    
    return True

def check_django_config():
    """Check Django configuration and models"""
    print("\n🐍 Checking Django Configuration...")
    
    # Set up Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')
    try:
        django.setup()
        print("✅ Django Setup: Success")
    except Exception as e:
        print("❌ Django Setup: Failed - {}".format(e))
        return False
    
    # Check models
    try:
        from apps.dashboard_app.models import AnalysisTemplate, TemplateCategory
        from apps.user_app.models import User
        
        template_count = AnalysisTemplate.objects.count()
        category_count = TemplateCategory.objects.count()
        user_count = User.objects.count()
        
        print("✅ Database Models: Accessible")
        print("  - Templates: {}".format(template_count))
        print("  - Categories: {}".format(category_count))
        print("  - Users: {}".format(user_count))
    except Exception as e:
        print("❌ Database Models: Error - {}".format(e))
        return False
    
    return True

def check_api_keys():
    """Check API keys configuration"""
    print("\n🔑 Checking API Keys...")
    
    openai_key = os.environ.get('OPENAI_API_KEY')
    youtube_key = os.environ.get('YOUTUBE_API_KEY')
    
    if openai_key:
        key_preview = openai_key[:8] + "..." + openai_key[-4:] if len(openai_key) > 12 else "SET"
        print("✅ OpenAI API Key: {} (Length: {})".format(key_preview, len(openai_key)))
    else:
        print("⚠️  OpenAI API Key: Not set")
    
    if youtube_key:
        key_preview = youtube_key[:8] + "..." + youtube_key[-4:] if len(youtube_key) > 12 else "SET"
        print("✅ YouTube API Key: {} (Length: {})".format(key_preview, len(youtube_key)))
    else:
        print("⚠️  YouTube API Key: Not set")

def check_wizard_templates():
    """Check if wizard templates exist"""
    print("\n📄 Checking Wizard Templates...")
    
    templates_to_check = [
        'templates/dashboard/wizard.html',
        'templates/onboarding/welcome.html',
        'templates/dashboard/jobs.html',
        'templates/dashboard/job_runner.html'
    ]
    
    for template_path in templates_to_check:
        full_path = Path(template_path)
        if full_path.exists():
            print("✅ {}: Found ({} bytes)".format(template_path, full_path.stat().st_size))
        else:
            print("❌ {}: Missing".format(template_path))

def main():
    """Run all system checks"""
    print("🚀 TubeWhale System Check")
    print("=" * 50)
    
    all_passed = True
    
    # Run checks
    all_passed &= check_docker_services()
    all_passed &= check_django_config()
    check_api_keys()
    check_wizard_templates()
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 System Status: ALL CHECKS PASSED")
        print("🌐 Access your system at: http://localhost")
        print("🔧 Django Admin: http://localhost/admin/")
        print("📊 New Wizard: http://localhost/dashboard/wizard/")
    else:
        print("❌ System Status: SOME CHECKS FAILED")
        print("Please review the errors above")
    
    return 0 if all_passed else 1

if __name__ == '__main__':
    sys.exit(main())