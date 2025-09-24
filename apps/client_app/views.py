"""
Client Authentication Views
独立的客户端登录注册系统
"""

from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json

from apps.user_app.models import User


def client_login_view(request):
    """客户端登录页面"""
    if request.user.is_authenticated:
        return redirect('client:workflow_start')
    
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        
        if username and password:
            user = authenticate(request, username=username, password=password)
            if user is not None:
                login(request, user)
                messages.success(request, _('Welcome back, {}!').format(user.username))
                
                # 重定向到工作流程起始页
                next_url = request.GET.get('next', 'client:workflow_start')
                return redirect(next_url)
            else:
                messages.error(request, _('Invalid username or password.'))
        else:
            messages.error(request, _('Please enter both username and password.'))
    
    context = {
        'title': _('Sign In - TubeWhale'),
        'subtitle': _('Access your analysis workspace'),
    }
    return render(request, 'client/login.html', context)


def client_register_view(request):
    """客户端注册页面"""
    if request.user.is_authenticated:
        return redirect('client:workflow_start')
    
    if request.method == 'POST':
        username = request.POST.get('username')
        email = request.POST.get('email')
        password = request.POST.get('password')
        password_confirm = request.POST.get('password_confirm')
        
        # 验证表单
        if not all([username, email, password, password_confirm]):
            messages.error(request, _('Please fill in all required fields.'))
        elif password != password_confirm:
            messages.error(request, _('Passwords do not match.'))
        elif User.objects.filter(username=username).exists():
            messages.error(request, _('Username already exists.'))
        elif User.objects.filter(email=email).exists():
            messages.error(request, _('Email already registered.'))
        else:
            # 创建新用户（默认basic tier）
            try:
                user = User.objects.create_user(
                    username=username,
                    email=email,
                    password=password,
                    tier='basic'
                )
                
                # 自动登录
                login(request, user)
                messages.success(request, _('Account created successfully! Welcome to TubeWhale!'))
                return redirect('client:workflow_start')
                
            except Exception as e:
                messages.error(request, _('Registration failed. Please try again.'))
    
    context = {
        'title': _('Create Account - TubeWhale'),
        'subtitle': _('Join our research platform'),
    }
    return render(request, 'client/register.html', context)


@login_required(login_url='client:login')
def client_logout_view(request):
    """客户端登出"""
    username = request.user.username
    logout(request)
    messages.success(request, _('Goodbye, {}! You have been logged out.').format(username))
    return redirect('client:login')


@login_required(login_url='client:login')
def client_profile_view(request):
    """用户个人资料页面"""
    context = {
        'title': _('My Profile'),
        'user': request.user,
    }
    return render(request, 'client/profile.html', context)


@csrf_exempt
@require_http_methods(["POST"])
def upgrade_tier_view(request):
    """模拟付费升级tier"""
    if not request.user.is_authenticated:
        return JsonResponse({'success': False, 'message': 'Please login first'})
    
    try:
        data = json.loads(request.body)
        target_tier = data.get('tier')
        
        if target_tier not in ['standard', 'premium']:
            return JsonResponse({'success': False, 'message': 'Invalid tier'})
        
        # 模拟付费验证（这里简化为直接成功）
        user = request.user
        current_tier = user.tier
        
        # 检查是否是升级
        tier_hierarchy = {'basic': 0, 'standard': 1, 'premium': 2}
        if tier_hierarchy.get(target_tier, 0) <= tier_hierarchy.get(current_tier, 0):
            return JsonResponse({
                'success': False, 
                'message': f'You already have {current_tier} tier or higher'
            })
        
        # 执行升级
        user.tier = target_tier
        user.save()
        
        # 返回成功响应
        return JsonResponse({
            'success': True,
            'message': f'Successfully upgraded to {target_tier} tier!',
            'new_tier': target_tier,
            'tier_display': user.get_tier_display_name()
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'message': 'Invalid request data'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)})


# Client-specific wrapper views for template system
from apps.user_app.decorators import client_login_required
from apps.dashboard_app import views as dashboard_views


# Temporary simple test endpoint
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def simple_test_view(request):
    """Simple test to check basic functionality"""
    from django.http import HttpResponse
    user_info = f"User: {request.user} (authenticated: {request.user.is_authenticated})"
    if request.user.is_authenticated:
        user_info += f" (tier: {getattr(request.user, 'tier', 'unknown')})"
    return HttpResponse(f"<h1>Simple Test</h1><p>{user_info}</p>")


@client_login_required
def client_template_selection_view(request):
    """Client-specific template selection with client authentication"""
    from apps.dashboard_app.models import TemplateCategory, AnalysisTemplate
    
    user = request.user
    
    # Get available categories and templates
    categories = TemplateCategory.objects.filter(is_active=True).order_by('order', 'name')
    
    # Filter templates based on user tier
    available_templates = AnalysisTemplate.objects.filter(
        is_active=True,
        category__is_active=True
    ).select_related('category')
    
    # Apply tier filtering
    user_accessible_templates = [
        template for template in available_templates 
        if template.can_access(user)
    ]
    
    # Group templates by category
    templates_by_category = {}
    for template in user_accessible_templates:
        category = template.category
        if category not in templates_by_category:
            templates_by_category[category] = []
        templates_by_category[category].append(template)
    
    # Get featured templates
    featured_templates = [
        template for template in user_accessible_templates 
        if template.is_featured
    ][:6]  # Limit to 6 featured templates
    
    context = {
        'categories': categories,
        'categorized_templates': templates_by_category,
        'featured_templates': featured_templates,
        'user_tier': user.tier,
        'user_tier_display': user.get_tier_display_name(),
    }
    
    return render(request, 'dashboard/template_selection.html', context)


# Temporary debug endpoint without authentication
def debug_template_view(request):
    """Debug endpoint to test template selection without authentication"""
    try:
        from django.http import HttpResponse
        from apps.dashboard_app.models import TemplateCategory, AnalysisTemplate
        
        categories = TemplateCategory.objects.filter(is_active=True)
        templates = AnalysisTemplate.objects.filter(is_active=True)
        
        debug_info = f"""
        <h1>Debug Template Info</h1>
        <h2>Categories: {categories.count()}</h2>
        <ul>
        """
        for cat in categories:
            debug_info += f"<li>{cat.name} (order: {cat.order})</li>"
        
        debug_info += f"""
        </ul>
        <h2>Templates: {templates.count()}</h2>
        <ul>
        """
        for tmpl in templates:
            debug_info += f"<li>{tmpl.name} - Tier: {tmpl.required_tier}</li>"
        
        debug_info += "</ul>"
        
        return HttpResponse(debug_info)
        
    except Exception as e:
        import traceback
        error_details = f"Debug error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        return HttpResponse(f"<h1>Debug Error</h1><pre>{error_details}</pre>", status=500)


@client_login_required  
def client_template_configure_view(request, template_id):
    """Client wrapper for template configuration with client authentication"""
    return dashboard_views.template_configure_view(request, template_id)


@client_login_required
def client_job_runner_view(request):
    """Client wrapper for job runner with client authentication"""
    return dashboard_views.job_runner_view(request)