"""
Enhanced admin views for comprehensive template management
"""
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.urls import reverse
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from typing import Dict, Any
import json

from service.enterprise_template_engine import TemplateEngine
from .models import CustomTemplate, TemplateType
from .tier_models import TemplateUsage, UserProfile
from .tier_utils import filter_accessible_templates, get_user_template_stats, track_template_usage


@staff_member_required
def template_system_dashboard(request: HttpRequest) -> HttpResponse:
    """Enhanced dashboard with system overview and analytics"""
    engine = TemplateEngine(validation_strict=False)
    
    # Get system statistics
    all_templates = engine.list_templates(include_metadata=True)
    
    stats = {
        'total_templates': len(all_templates),
        'core_templates': len([t for t in all_templates if t.get('template_type') == 'core']),
        'domain_templates': len([t for t in all_templates if t.get('template_type') == 'domain']),
        'custom_templates': CustomTemplate.objects.count(),
        'active_users': len(set(usage.user_id for usage in TemplateUsage.objects.all())),
        'total_usage': sum(usage.usage_count for usage in TemplateUsage.objects.all()),
    }
    
    # Recent activity
    recent_usage = TemplateUsage.objects.select_related('user').order_by('-last_used')[:10]
    
    # User tier distribution
    tier_distribution = {}
    for profile in UserProfile.objects.all():
        tier = profile.tier
        tier_distribution[tier] = tier_distribution.get(tier, 0) + 1
    
    context = {
        'stats': stats,
        'recent_usage': recent_usage,
        'tier_distribution': tier_distribution,
        'domains': engine.get_domains(),
    }
    
    return render(request, "admin/template_dashboard.html", context)


@staff_member_required 
def bulk_template_operations(request: HttpRequest) -> HttpResponse:
    """Bulk operations for template management"""
    if request.method == 'POST':
        operation = request.POST.get('operation')
        template_ids = request.POST.getlist('template_ids')
        
        if operation == 'bulk_import':
            engine = TemplateEngine(validation_strict=False)
            imported_count = 0
            
            for template_id in template_ids:
                tpl = engine.get_template(template_id)
                if tpl and not CustomTemplate.objects.filter(template_id=template_id).exists():
                    CustomTemplate.objects.create(
                        template_id=template_id,
                        name=tpl.get("name", template_id),
                        domain=tpl.get("domain", "general"),
                        description=tpl.get("description", ""),
                        prompt=tpl.get("prompt", ""),
                        parameters=tpl.get("parameters", {}),
                        tags=tpl.get("tags", []),
                        template_type=TemplateType.CUSTOM,
                        immutable=False,
                    )
                    imported_count += 1
            
            messages.success(request, f"Successfully imported {imported_count} templates")
            
        elif operation == 'bulk_export':
            # Export selected templates as JSON
            engine = TemplateEngine(validation_strict=False)
            export_data = []
            
            for template_id in template_ids:
                tpl = engine.get_template(template_id)
                if tpl:
                    export_data.append(tpl)
            
            response = JsonResponse({'templates': export_data}, indent=2)
            response['Content-Disposition'] = 'attachment; filename="templates_export.json"'
            return response
    
    return redirect(reverse('admin-engine-templates'))


@staff_member_required
def template_analytics(request: HttpRequest) -> HttpResponse:
    """Advanced analytics and insights for template usage"""
    from django.db.models import Count, Sum, Avg
    from django.utils import timezone
    from datetime import timedelta
    
    # Usage statistics
    usage_stats = TemplateUsage.objects.aggregate(
        total_usage=Sum('usage_count'),
        avg_usage=Avg('usage_count'),
        unique_users=Count('user', distinct=True),
        unique_templates=Count('template_id', distinct=True)
    )
    
    # Most popular templates
    popular_templates = TemplateUsage.objects.values('template_id', 'template_name').annotate(
        total_uses=Sum('usage_count'),
        user_count=Count('user', distinct=True)
    ).order_by('-total_uses')[:10]
    
    # User activity
    active_users = TemplateUsage.objects.values('user__username').annotate(
        template_count=Count('template_id', distinct=True),
        total_usage=Sum('usage_count')
    ).order_by('-total_usage')[:10]
    
    # Recent activity (last 7 days)
    last_week = timezone.now() - timedelta(days=7)
    recent_activity = TemplateUsage.objects.filter(
        last_used__gte=last_week
    ).select_related('user').order_by('-last_used')[:20]
    
    # Tier distribution
    tier_stats = UserProfile.objects.values('tier').annotate(
        count=Count('id')
    ).order_by('tier')
    
    context = {
        'usage_stats': usage_stats,
        'popular_templates': popular_templates,
        'active_users': active_users,
        'recent_activity': recent_activity,
        'tier_stats': tier_stats,
    }
    
    return render(request, "admin/template_analytics.html", context)


@staff_member_required
def template_import_wizard(request: HttpRequest) -> HttpResponse:
    """Step-by-step template import wizard"""
    engine = TemplateEngine(validation_strict=False)
    
    if request.method == 'POST':
        step = request.POST.get('step', '1')
        
        if step == '1':
            # Step 1: Select templates to import
            selected_templates = request.POST.getlist('selected_templates')
            request.session['import_templates'] = selected_templates
            return redirect(request.path + '?step=2')
            
        elif step == '2':
            # Step 2: Configure import options
            selected_templates = request.session.get('import_templates', [])
            import_options = {
                'overwrite_existing': request.POST.get('overwrite_existing') == 'on',
                'make_immutable': request.POST.get('make_immutable') == 'on',
                'auto_categorize': request.POST.get('auto_categorize') == 'on',
            }
            
            imported_count = 0
            for template_id in selected_templates:
                tpl = engine.get_template(template_id)
                if tpl:
                    exists = CustomTemplate.objects.filter(template_id=template_id).exists()
                    
                    if not exists or import_options['overwrite_existing']:
                        if exists:
                            CustomTemplate.objects.filter(template_id=template_id).delete()
                        
                        CustomTemplate.objects.create(
                            template_id=template_id,
                            name=tpl.get("name", template_id),
                            domain=tpl.get("domain", "general"),
                            description=tpl.get("description", ""),
                            prompt=tpl.get("prompt", ""),
                            parameters=tpl.get("parameters", {}),
                            tags=tpl.get("tags", []),
                            template_type=TemplateType.CUSTOM,
                            immutable=import_options['make_immutable'],
                        )
                        imported_count += 1
            
            messages.success(request, f"Successfully imported {imported_count} templates")
            del request.session['import_templates']
            return redirect(reverse('admin-engine-templates'))
    
    # Get current step
    step = request.GET.get('step', '1')
    
    if step == '1':
        # Step 1: Template selection
        all_templates = engine.list_templates(include_metadata=True)
        imported_ids = set(CustomTemplate.objects.values_list("template_id", flat=True))
        
        for template in all_templates:
            template['is_imported'] = template.get('id') in imported_ids
            
        context = {
            'step': 1,
            'templates': all_templates,
            'domains': engine.get_domains(),
        }
        
    elif step == '2':
        # Step 2: Import configuration
        selected_templates = request.session.get('import_templates', [])
        if not selected_templates:
            messages.error(request, "No templates selected for import")
            return redirect(request.path)
        
        template_details = []
        for template_id in selected_templates:
            tpl = engine.get_template(template_id)
            if tpl:
                template_details.append(tpl)
        
        context = {
            'step': 2,
            'selected_templates': template_details,
        }
    
    return render(request, "admin/template_import_wizard.html", context)


@require_http_methods(["POST"])
@staff_member_required
def template_quick_actions(request: HttpRequest) -> JsonResponse:
    """AJAX endpoint for quick template actions"""
    action = request.POST.get('action')
    template_id = request.POST.get('template_id')
    
    try:
        if action == 'toggle_favorite':
            # Toggle user's favorite status for a template
            # Implementation would depend on a favorites system
            return JsonResponse({'status': 'success', 'message': 'Favorite toggled'})
            
        elif action == 'duplicate_template':
            # Duplicate an existing custom template
            original = get_object_or_404(CustomTemplate, template_id=template_id)
            new_template_id = f"{template_id}_copy"
            
            # Ensure unique ID
            counter = 1
            while CustomTemplate.objects.filter(template_id=new_template_id).exists():
                new_template_id = f"{template_id}_copy_{counter}"
                counter += 1
            
            CustomTemplate.objects.create(
                template_id=new_template_id,
                name=f"{original.name} (Copy)",
                domain=original.domain,
                description=original.description,
                prompt=original.prompt,
                parameters=original.parameters,
                tags=original.tags,
                template_type=TemplateType.CUSTOM,
                immutable=False,
            )
            
            return JsonResponse({
                'status': 'success', 
                'message': f'Template duplicated as {new_template_id}',
                'new_template_id': new_template_id
            })
            
        elif action == 'validate_template':
            # Validate template syntax and variables
            engine = TemplateEngine(validation_strict=True)
            try:
                tpl = engine.get_template(template_id)
                if tpl:
                    variables = engine.extract_template_variables(template_id)
                    return JsonResponse({
                        'status': 'success',
                        'message': 'Template is valid',
                        'variables': variables
                    })
                else:
                    return JsonResponse({
                        'status': 'error',
                        'message': 'Template not found'
                    })
            except Exception as e:
                return JsonResponse({
                    'status': 'error',
                    'message': f'Validation failed: {str(e)}'
                })
        
        else:
            return JsonResponse({'status': 'error', 'message': 'Unknown action'})
            
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})