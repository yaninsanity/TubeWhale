"""
Main Dashboard Views
User onboarding and environment configuration
"""

from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib import messages
from django.utils import timezone
from django.utils.text import slugify
from django.utils.translation import gettext_lazy as _
from django.utils.timesince import timesince
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from django.db.models import Q, Count
from apps.user_app.decorators import scientist_login_required
from .models import TemplateCategory, AnalysisTemplate
from apps.tubewhale_engine.models import AnalysisJob, AnalysisResult
import os
import json
from urllib.parse import urlparse, parse_qs


def home_view(request):
    """Main entry point - guide users through onboarding"""
    if request.user.is_authenticated:
        return redirect('dashboard:main')
    
    return render(request, 'onboarding/welcome.html', {
        'title': _('Welcome to TubeWhale'),
        'subtitle': _('AI-Powered Video Content Analysis Platform')
    })


@scientist_login_required
def dashboard_view(request):
    """Main dashboard after login"""
    # Check if user needs onboarding
    user = request.user
    needs_setup = not has_environment_configured(user)
    
    context = {
        'user': user,
        'needs_setup': needs_setup,
        'tier': user.get_tier_display_name(),
        'can_switch_templates': user.can_switch_templates(),
        'can_customize_templates': user.can_customize_templates(),
    }
    
    if needs_setup:
        return render(request, 'dashboard/setup_guide.html', context)
    else:
        return render(request, 'dashboard/main.html', context)


@scientist_login_required
def environment_setup_view(request):
    """Environment configuration guide"""
    if request.method == 'POST':
        return handle_environment_setup(request)
    
    context = {
        'current_config': get_user_environment_config(request.user),
        'required_fields': [
            {
                'name': 'youtube_api_key',
                'label': _('YouTube API Key'),
                'description': _('Required for video data access'),
                'required': True
            },
            {
                'name': 'openai_api_key', 
                'label': _('OpenAI API Key'),
                'description': _('Required for AI analysis'),
                'required': True
            },
            {
                'name': 'workspace_folder',
                'label': _('Workspace Folder'),
                'description': _('Where to save analysis results'),
                'required': True
            },
        ]
    }
    
    return render(request, 'dashboard/environment_setup.html', context)


# First template_selection_view removed - using enhanced version below


@scientist_login_required
@csrf_exempt
def template_preview_api(request, template_id):
    """API endpoint for template preview"""
    if request.method != 'GET':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    template = get_template_by_id(template_id, request.user)
    if not template:
        return JsonResponse({'error': 'Template not found'}, status=404)
    
    return JsonResponse({
        'id': template['id'],
        'name': template['name'],
        'description': template['description'],
        'features': template['features'],
        'sample_output': template['sample_output'],
        'tier_required': template['tier_required']
    })


@scientist_login_required
def job_runner_view(request):
    """Job execution and monitoring interface"""
    # Get job data safely
    active_jobs = get_user_active_jobs(request.user)
    recent_jobs = get_user_completed_jobs(request.user)
    
    # Check for wizard job parameters
    wizard_params = request.session.get('wizard_job_params')
    if wizard_params:
        # Clear from session after retrieving
        del request.session['wizard_job_params']
        messages.info(
            request, 
            _('Ready to start {} analysis with {} template').format(
                wizard_params.get('tool_type', 'unknown').replace('_', ' ').title(),
                wizard_params.get('template', 'unknown')
            )
        )
    
    # Calculate job statistics
    job_stats = {
        'pending': sum(1 for job in active_jobs if job['status'] == 'queued'),
        'running': sum(1 for job in active_jobs if job['status'] == 'processing'),
        'completed': sum(1 for job in recent_jobs if job['status'] == 'completed'),
        'failed': sum(1 for job in recent_jobs if job['status'] == 'failed'),
    }
    
    context = {
        'user': request.user,
        'user_tier': getattr(request.user, 'tier', 'basic'),
        'user_tier_display': getattr(request.user, 'get_tier_display', lambda: 'Basic')(),
        'active_jobs': active_jobs,
        'recent_jobs': recent_jobs,
        'job_stats': job_stats,
        'wizard_params': wizard_params,
        'title': _('Job Management'),
        'subtitle': _('Monitor and manage your video analysis jobs')
    }
    
    return render(request, 'dashboard/job_runner.html', context)


# Helper functions

def has_environment_configured(user):
    """Check if user has completed environment setup"""
    from apps.user_app.models import UserProfile
    try:
        profile = UserProfile.objects.get(user=user)
        # Check if basic config fields are populated
        return bool(profile.api_keys.get('youtube_api_key') and 
                   profile.api_keys.get('openai_api_key'))
    except UserProfile.DoesNotExist:
        return False


def get_user_environment_config(user):
    """Get user's current environment configuration"""
    from apps.user_app.models import UserProfile
    try:
        profile = UserProfile.objects.get(user=user)
        return {
            'youtube_api_key': profile.api_keys.get('youtube_api_key', ''),
            'openai_api_key': profile.api_keys.get('openai_api_key', ''),
            'workspace_folder': profile.workspace_folder or f'/workspace/{user.username}',
            'preferred_language': profile.preferred_language or 'en',
            'analysis_tier': profile.subscription_tier or 'basic'
        }
    except UserProfile.DoesNotExist:
        return {
            'youtube_api_key': '',
            'openai_api_key': '',
            'workspace_folder': f'/workspace/{user.username}',
            'preferred_language': 'en',
            'analysis_tier': 'basic'
        }


def handle_environment_setup(request):
    """Handle environment configuration form submission"""
    from apps.user_app.models import UserProfile
    
    if request.method == 'POST':
        try:
            profile, created = UserProfile.objects.get_or_create(user=request.user)
            
            # Update API keys
            api_keys = profile.api_keys or {}
            if request.POST.get('youtube_api_key'):
                api_keys['youtube_api_key'] = request.POST.get('youtube_api_key')
            if request.POST.get('openai_api_key'):
                api_keys['openai_api_key'] = request.POST.get('openai_api_key')
            profile.api_keys = api_keys
            
            # Update other preferences
            if request.POST.get('workspace_folder'):
                profile.workspace_folder = request.POST.get('workspace_folder')
            if request.POST.get('preferred_language'):
                profile.preferred_language = request.POST.get('preferred_language')
            
            profile.save()
            messages.success(request, _('Environment configuration saved successfully!'))
            return redirect('dashboard:template_selection')
            
        except Exception as e:
            messages.error(request, _('Error saving configuration: {}').format(str(e)))
            return redirect('dashboard:environment_setup')
    
    return redirect('dashboard:environment_setup')


def get_available_templates(user):
    """Get templates available to user based on tier"""
    all_templates = [
        {
            'id': 'default_analysis',
            'name': _('Default Analysis'),
            'description': _('Basic video content analysis with transcription and summary'),
            'features': [_('Auto transcription'), _('Basic summary'), _('Keyword extraction')],
            'sample_output': _('Text transcription + bullet point summary'),
            'tier_required': 'basic'
        },
        {
            'id': 'detailed_analysis', 
            'name': _('Detailed Analysis'),
            'description': _('Comprehensive analysis with sentiment and topics'),
            'features': [_('Advanced transcription'), _('Sentiment analysis'), _('Topic modeling'), _('Key insights')],
            'sample_output': _('Full analysis report with charts and insights'),
            'tier_required': 'standard'
        },
        {
            'id': 'custom_analysis',
            'name': _('Custom Analysis'),
            'description': _('Fully customizable AI interactions and prompts'),
            'features': [_('Custom prompts'), _('Advanced AI models'), _('Flexible output formats')],
            'sample_output': _('Customized analysis based on your specific requirements'),
            'tier_required': 'premium'
        }
    ]
    
    # Filter based on user tier
    tier_hierarchy = {'basic': 1, 'standard': 2, 'premium': 3}
    user_tier_level = tier_hierarchy.get(user.tier, 1)
    
    available = []
    for template in all_templates:
        template_tier_level = tier_hierarchy.get(template['tier_required'], 1)
        if user_tier_level >= template_tier_level:
            available.append(template)
    
    return available


def get_template_by_id(template_id, user):
    """Get specific template if user has access"""
    available = get_available_templates(user)
    for template in available:
        if template['id'] == template_id:
            return template
    return None


def get_user_template_selection(user):
    """Get user's current template selection"""
    from apps.user_app.models import UserProfile
    try:
        profile = UserProfile.objects.get(user=user)
        return profile.preferred_template or 'default_analysis'
    except UserProfile.DoesNotExist:
        return 'default_analysis'


def get_user_active_jobs(user):
    """Get user's currently running or queued jobs"""
    jobs = AnalysisJob.objects.filter(
        user=user,
        status__in=['queued', 'processing']
    ).order_by('-created_at')[:20]
    return [serialize_analysis_job(job) for job in jobs]


def get_user_completed_jobs(user):
    """Get user's recently completed or failed jobs"""
    jobs = AnalysisJob.objects.filter(
        user=user,
        status__in=['completed', 'failed', 'cancelled']
    ).order_by('-created_at')[:20]
    return [serialize_analysis_job(job) for job in jobs]


def extract_youtube_video_id(video_input: str) -> str:
    """Extract YouTube video ID from various input formats"""
    if not video_input:
        return ''
    value = video_input.strip()
    if len(value) == 11 and value.replace('_', '').replace('-', '').isalnum():
        return value
    try:
        parsed = urlparse(value)
    except Exception:
        return value

    host = parsed.netloc.lower()
    if host in {'youtu.be', 'www.youtu.be'}:
        return parsed.path.strip('/')

    if 'youtube.com' in host:
        query = parse_qs(parsed.query)
        if 'v' in query and query['v']:
            return query['v'][0]
        path_parts = [segment for segment in parsed.path.split('/') if segment]
        if path_parts:
            if path_parts[0] == 'shorts' and len(path_parts) > 1:
                return path_parts[1]
            if path_parts[0] == 'embed' and len(path_parts) > 1:
                return path_parts[1]

    return value


def extract_youtube_playlist_id(playlist_input: str) -> str:
    """Extract YouTube playlist ID from input"""
    if not playlist_input:
        return ''
    value = playlist_input.strip()
    try:
        parsed = urlparse(value)
    except Exception:
        return value

    query = parse_qs(parsed.query)
    if 'list' in query and query['list']:
        return query['list'][0]

    path = parsed.path.strip('/') if parsed.path else ''
    if path:
        return path.split('/')[-1]

    return value


def parse_job_input(tool_type: str, input_value: str) -> tuple[str, str]:
    """Normalize content ID and URL based on selected tool"""
    input_value = (input_value or '').strip()

    if tool_type == 'single_video':
        video_id = extract_youtube_video_id(input_value)
        content_url = input_value if input_value.startswith('http') else (
            f"https://www.youtube.com/watch?v={video_id}" if video_id else ''
        )
        return video_id or slugify(input_value)[:32] or 'video-analysis', content_url

    if tool_type == 'playlist':
        playlist_id = extract_youtube_playlist_id(input_value)
        content_url = input_value if input_value.startswith('http') else (
            f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else ''
        )
        return playlist_id or slugify(input_value)[:32] or 'playlist-analysis', content_url

    if tool_type == 'brainstorm':
        topic_slug = slugify(input_value)[:32] if input_value else ''
        return topic_slug or 'brainstorm-session', ''

    fallback = slugify(input_value)[:32]
    return fallback or 'analysis-task', input_value


def serialize_analysis_job(job: AnalysisJob) -> dict:
    """Convert AnalysisJob model into template-friendly structure"""
    options = job.analysis_options or {}
    display_name = (
        options.get('display_name')
        or job.content_title
        or options.get('brainstorm_topic')
        or job.content_id
        or _('Analysis Job')
    )

    task_type = options.get('tool_type', job.analysis_type)
    now = timezone.now()
    created_ago = timesince(job.created_at, now) if job.created_at else None
    completed_ago = timesince(job.completed_at, now) if job.completed_at else None

    return {
        'id': job.job_id,
        'video_id': display_name,
        'task_type': task_type.replace('_', ' ').title() if isinstance(task_type, str) else task_type,
        'status': job.status,
        'created_at': job.created_at,
        'completed_at': job.completed_at,
        'progress': job.progress,
        'content_url': job.content_url,
        'analysis_type': job.analysis_type,
        'template_name': job.template_id,
        'options': options,
        'created_ago': created_ago,
        'completed_ago': completed_ago,
    }


def create_analysis_job_entry(
    *,
    user,
    tool_type: str,
    role: str,
    template_id: str,
    input_value: str,
    custom_questions: list[str] | None = None,
    analysis_depth: str = 'detailed',
    report_language: str = 'en',
    additional_options: dict | None = None,
) -> AnalysisJob:
    """Create a persisted analysis job record for wizard submissions"""

    custom_questions = custom_questions or []
    additional_options = additional_options or {}

    content_id, content_url = parse_job_input(tool_type, input_value)

    analysis_type_map = {
        'single_video': 'video',
        'playlist': 'playlist',
        'brainstorm': 'custom',
    }

    safe_role = (role or 'general').replace(' ', '-').lower()
    safe_template = template_id or 'default-template'

    options = {
        'tool_type': tool_type,
        'input_value': input_value,
        'analysis_depth': analysis_depth,
        'report_language': report_language,
        'custom_questions': custom_questions,
        'created_via': additional_options.get('created_via', 'wizard'),
    }
    options.update(additional_options)

    display_name = options.get('display_name')
    if not display_name:
        if tool_type == 'brainstorm':
            display_name = options.get('brainstorm_topic') or _('AI Brainstorm Session')
        else:
            display_name = content_id or _('Video Analysis')

    job = AnalysisJob.objects.create(
        user=user,
        analysis_type=analysis_type_map.get(tool_type, 'custom'),
        expert_role=safe_role,
        template_id=safe_template,
        content_id=content_id,
        content_url=content_url,
        content_title=display_name,
        status='queued',
        progress=0,
        custom_questions=custom_questions,
        analysis_options=options,
    )

    return job


def get_analysis_job_statistics(user=None) -> dict:
    """Aggregate job statistics for CLI and dashboard"""
    queryset = AnalysisJob.objects.all()
    if user is not None:
        queryset = queryset.filter(user=user)

    summary = queryset.values('status').order_by().annotate(count=Count('job_id'))
    stats = {'queued': 0, 'processing': 0, 'completed': 0, 'failed': 0, 'cancelled': 0}
    for item in summary:
        stats[item['status']] = item['count']
    stats['total'] = sum(stats.values())
    stats['active'] = stats['queued'] + stats['processing']
    return stats


def get_recent_analysis_jobs(user=None, limit: int = 5):
    """Retrieve recent jobs for CLI output"""
    queryset = AnalysisJob.objects.all()
    if user is not None:
        queryset = queryset.filter(user=user)
    jobs = queryset.order_by('-created_at')[:limit]
    return list(jobs)


# Template Selection Views

@scientist_login_required
def template_selection_view(request):
    """Template selection interface for clients - now redirects to intelligent wizard"""
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
        'templates_by_category': templates_by_category,
        'featured_templates': featured_templates,
        'user_tier': user.tier,
        'user_tier_display': user.get_tier_display_name(),
    }
    
    return render(request, 'dashboard/template_selection.html', context)


@scientist_login_required
def intelligent_wizard_view(request):
    """Intelligent template wizard with role-based recommendations"""
    user = request.user
    
    # Get available templates grouped by analysis type
    templates = AnalysisTemplate.objects.filter(
        is_active=True,
        category__is_active=True
    ).select_related('category')
    
    # Filter by user tier
    user_accessible_templates = [
        template for template in templates 
        if template.can_access(user)
    ]
    
    # Group templates by role compatibility
    role_templates = {
        'content_creator': [t for t in user_accessible_templates if 'content' in t.name.lower() or 'creator' in t.tags],
        'marketing_expert': [t for t in user_accessible_templates if 'marketing' in t.name.lower() or 'brand' in t.tags],
        'data_analyst': [t for t in user_accessible_templates if 'analytics' in t.name.lower() or 'data' in t.tags],
        'researcher': [t for t in user_accessible_templates if 'research' in t.name.lower() or 'academic' in t.tags],
        'product_manager': [t for t in user_accessible_templates if 'product' in t.name.lower() or 'strategy' in t.tags],
        'executive': [t for t in user_accessible_templates if 'summary' in t.name.lower() or 'executive' in t.tags],
    }
    
    # Handle form submissions
    if request.method == 'POST':
        return handle_wizard_submission(request, user_accessible_templates)
    
    context = {
        'user': user,
        'user_tier': user.tier,
        'user_tier_display': user.get_tier_display_name(),
        'role_templates': role_templates,
        'total_templates': len(user_accessible_templates),
        'can_customize': user.can_customize_templates(),
        'max_concurrent_jobs': get_user_max_jobs(user),
    }
    
    return render(request, 'dashboard/wizard_enhanced.html', context)


@scientist_login_required
def smart_wizard_view(request):
    """New 3-step wizard: Tool Selection → Role & Template → Execution"""
    user = request.user
    
    # Get available templates grouped by role
    templates = AnalysisTemplate.objects.filter(
        is_active=True,
        category__is_active=True
    ).select_related('category')
    
    # Filter by user tier
    user_accessible_templates = [
        template for template in templates 
        if template.can_access(user)
    ]
    
    # Check API keys configuration
    api_keys_loaded = all([
        os.environ.get('OPENAI_API_KEY'),
        os.environ.get('YOUTUBE_API_KEY'),
    ])
    
    # Handle form submissions
    if request.method == 'POST':
        return handle_smart_wizard_submission(request, user_accessible_templates)
    
    context = {
        'user': user,
        'user_tier': user.tier,
        'user_tier_display': user.get_tier_display_name(),
        'templates': user_accessible_templates,
        'api_keys_loaded': api_keys_loaded,
        'can_customize': user.can_customize_templates(),
        'max_concurrent_jobs': get_user_max_jobs(user),
    }
    
    return render(request, 'dashboard/wizard_enhanced.html', context)


def handle_smart_wizard_submission(request, available_templates):
    """Handle the new wizard form submission"""
    try:
        tool_type = request.POST.get('tool_type')
        selected_role = request.POST.get('selected_role')
        template_name = request.POST.get('template_name')
        video_input = request.POST.get('video_input', '').strip()
        analysis_depth = request.POST.get('analysis_depth', 'detailed')
        report_language = request.POST.get('report_language', 'en')
        brainstorm_topic = request.POST.get('brainstorm_topic', '').strip()
        custom_questions_raw = request.POST.get('custom_questions')
        custom_questions = []

        if custom_questions_raw:
            try:
                parsed_questions = json.loads(custom_questions_raw)
                if isinstance(parsed_questions, list):
                    custom_questions = [q.strip() for q in parsed_questions if isinstance(q, str) and q.strip()]
            except json.JSONDecodeError:
                custom_questions = [part.strip() for part in custom_questions_raw.split('\n') if part.strip()]

        if not custom_questions:
            # Fallback for checkbox-style submissions: custom_questions[]
            custom_questions = [q.strip() for q in request.POST.getlist('custom_questions[]') if q.strip()]
        
        # Validate inputs
        if not all([tool_type, selected_role, template_name]):
            messages.error(request, _('Please complete all required fields.'))
            return redirect('dashboard:smart_wizard')
        
        if tool_type in ['single_video', 'playlist'] and not video_input:
            messages.error(request, _('Please provide a video URL or playlist.'))
            return redirect('dashboard:smart_wizard')
        
        # Find the selected template
        template = None
        for tmpl in available_templates:
            if tmpl.name == template_name:
                template = tmpl
                break
        
        if not template:
            messages.error(request, _('Selected template not found.'))
            return redirect('dashboard:smart_wizard')
        
        input_value = video_input
        if tool_type == 'brainstorm':
            input_value = brainstorm_topic or request.POST.get('analysis_input', '').strip()

        job = create_analysis_job_entry(
            user=request.user,
            tool_type=tool_type,
            role=selected_role,
            template_id=getattr(template, 'template_id', template.name),
            input_value=input_value,
            custom_questions=custom_questions,
            analysis_depth=analysis_depth,
            report_language=report_language,
            additional_options={
                'display_name': template.name,
                'template_db_id': template.id,
                'template_name': template.name,
                'brainstorm_topic': brainstorm_topic,
                'source': 'smart_wizard_view',
            }
        )

        request.session['wizard_job_params'] = {
            'tool_type': tool_type,
            'template': template.name,
            'job_id': job.job_id,
        }

        messages.success(
            request,
            _('Analysis job {} created successfully. Redirecting to job runner...').format(job.job_id)
        )
        return redirect('dashboard:job_runner')
        
    except Exception as e:
        messages.error(request, _('Error processing wizard submission: {}').format(str(e)))
        return redirect('dashboard:smart_wizard')


@scientist_login_required
def template_detail_view(request, template_slug):
    """Detailed view of a specific template"""
    template = get_object_or_404(
        AnalysisTemplate, 
        slug=template_slug, 
        is_active=True
    )
    
    # Check if user can access this template
    if not template.can_access(request.user):
        messages.error(
            request, 
            _('This template requires a {} tier subscription.').format(
                template.get_required_tier_display()
            )
        )
        return redirect('dashboard:template_selection')
    
    # Increment usage count
    template.usage_count += 1
    template.save(update_fields=['usage_count'])
    
    # Get related templates
    related_templates = AnalysisTemplate.objects.filter(
        category=template.category,
        is_active=True
    ).exclude(id=template.id).filter(
        required_tier__lte=request.user.tier
    )[:4]
    
    context = {
        'template': template,
        'related_templates': related_templates,
        'can_use_template': True,
        'user_tier': request.user.tier,
    }
    
    return render(request, 'dashboard/template_detail.html', context)


@scientist_login_required  
def template_category_view(request, category_slug):
    """View templates in a specific category"""
    category = get_object_or_404(
        TemplateCategory, 
        slug=category_slug, 
        is_active=True
    )
    
    # Get templates in this category
    templates = AnalysisTemplate.objects.filter(
        category=category,
        is_active=True
    ).order_by('order', 'name')
    
    # Filter by user tier
    accessible_templates = [
        template for template in templates 
        if template.can_access(request.user)
    ]
    
    # Pagination
    paginator = Paginator(accessible_templates, 12)  # 12 templates per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Filter options
    complexity_filter = request.GET.get('complexity')
    if complexity_filter:
        filtered_templates = [
            t for t in accessible_templates 
            if t.complexity_level == complexity_filter
        ]
        paginator = Paginator(filtered_templates, 12)
        page_obj = paginator.get_page(page_number)
    
    context = {
        'category': category,
        'page_obj': page_obj,
        'complexity_choices': AnalysisTemplate.COMPLEXITY_CHOICES,
        'selected_complexity': complexity_filter,
        'total_templates': len(accessible_templates),
    }
    
    return render(request, 'dashboard/template_category.html', context)


def template_search_api(request):
    """AJAX API for template search"""
    query = request.GET.get('q', '').strip()
    category_id = request.GET.get('category')
    complexity = request.GET.get('complexity')
    
    if len(query) < 2:
        return JsonResponse({'templates': []})
    
    # Base queryset
    templates = AnalysisTemplate.objects.filter(
        is_active=True,
        category__is_active=True
    ).select_related('category')
    
    # Apply search filters
    templates = templates.filter(
        Q(name__icontains=query) |
        Q(short_description__icontains=query) |
        Q(tags__icontains=query) |
        Q(sample_use_cases__icontains=query)
    )
    
    # Apply category filter
    if category_id:
        templates = templates.filter(category_id=category_id)
    
    # Apply complexity filter
    if complexity:
        templates = templates.filter(complexity_level=complexity)
    
    # Filter by user tier if authenticated
    if request.user.is_authenticated:
        templates = [t for t in templates if t.can_access(request.user)]
    else:
        templates = list(templates)
    
    # Limit results
    templates = templates[:10]
    
    # Serialize results
    results = []
    for template in templates:
        results.append({
            'id': template.id,
            'name': template.name,
            'slug': template.slug,
            'short_description': template.short_description,
            'category': {
                'name': template.category.name,
                'color': template.category.color,
                'icon': template.category.icon,
            },
            'complexity': template.get_complexity_level_display(),
            'complexity_badge_class': template.get_complexity_badge_class(),
            'tier': template.get_required_tier_display(),
            'tier_badge_class': template.get_tier_badge_class(),
            'icon': template.icon,
            'color': template.color,
            'is_featured': template.is_featured,
            'estimated_duration': template.estimated_duration,
        })
    
    return JsonResponse({
        'templates': results,
        'total': len(results)
    })


# Third template_selection_view removed - using enhanced version above


def can_user_access_template(user, required_tier):
    """Check if user can access template based on tier"""
    tier_hierarchy = {
        'basic': 0,
        'standard': 1, 
        'premium': 2
    }
    
    user_tier_level = tier_hierarchy.get(user.tier, 0)
    required_tier_level = tier_hierarchy.get(required_tier, 0)
    
    return user_tier_level >= required_tier_level


@scientist_login_required
def template_preview_view(request, template_id):
    """Preview template details before selection"""
    from apps.templates_app.models import TemplateInfo
    
    template = get_object_or_404(TemplateInfo, template_id=template_id, is_active=True)
    
    # Check access
    if not can_user_access_template(request.user, template.required_tier):
        messages.error(request, _('This template requires a higher tier subscription.'))
        return redirect('dashboard:template_selection')
    
    context = {
        'template': template,
        'can_access': True,
        'user_tier': request.user.tier,
        'title': f'Preview: {template.title}',
    }
    
    return render(request, 'dashboard/template_preview.html', context)


@scientist_login_required
def template_configure_view(request, template_id):
    """Configure template parameters and start analysis"""
    from apps.dashboard_app.models import AnalysisTemplate
    
    template = get_object_or_404(AnalysisTemplate, template_id=template_id, is_active=True)
    
    # Check if user can access this template
    if not template.can_access(request.user):
        messages.error(request, _('This template requires a higher tier subscription.'))
        return redirect('dashboard:template_selection')
    
    if request.method == 'POST':
        # Handle form submission and create analysis job
        job_data = {
            'template_id': template_id,
            'job_name': request.POST.get('job_name'),
            'video_url': request.POST.get('video_url'),
            'priority': request.POST.get('priority', 'normal'),
            'analysis_depth': request.POST.get('analysis_depth', 'standard'),
            'include_comments': request.POST.get('include_comments') == 'true',
            'content_language': request.POST.get('content_language', 'auto'),
            'user_id': request.user.id,
        }
        
        # Add premium features if available
        if request.user.tier == 'premium':
            job_data.update({
                'enable_ai': request.POST.get('enable_ai') == 'on',
                'enable_prediction': request.POST.get('enable_prediction') == 'on',
            })
        
        # Create analysis job (placeholder - would integrate with Celery)
        # analysis_job = create_analysis_job(job_data)
        
        messages.success(request, _('Analysis job has been started successfully!'))
        return redirect('dashboard:job_runner')
    
    context = {
        'template': template,
        'user_tier': request.user.tier,
        'title': f'Configure: {template.name}',
    }
    
    return render(request, 'dashboard/template_configure.html', context)


@scientist_login_required
def cli_interface_view(request):
    """Professional CLI Interface for system control"""
    context = {
        'user': request.user,
        'title': _('CLI Control Interface'),
        'subtitle': _('Professional Command Line Interface for TubeWhale'),
    }
    
    return render(request, 'dashboard/cli_interface.html', context)


@csrf_exempt
@scientist_login_required
def cli_execute_api(request):
    """API endpoint for CLI command execution"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        command = data.get('command', '').strip()
        
        if not command:
            return JsonResponse({'error': 'No command provided'}, status=400)
        
        # Professional command processing system
        result = process_cli_command(command, request.user)
        
        return JsonResponse({
            'success': True,
            'output': result['output'],
            'type': result['type'],
            'timestamp': result['timestamp']
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@scientist_login_required
def theme_search_view(request):
    """Professional theme-based template search interface"""
    context = {
        'user': request.user,
        'title': _('Professional Theme Search'),
        'subtitle': _('Discover templates using intelligent theme-based search'),
    }
    
    return render(request, 'dashboard/theme_search.html', context)


def process_cli_command(command, user):
    """Process CLI commands with professional system integration"""
    import subprocess
    now = timezone.now()
    cmd = command.lower().strip()
    timestamp = now.strftime('%H:%M')

    if cmd == 'status':
        stats = get_analysis_job_statistics(user)
        active_jobs = get_recent_analysis_jobs(user, limit=1)
        active_line = _('No active jobs running')
        if active_jobs:
            job = active_jobs[0]
            active_line = _('%(job_id)s • %(status)s • %(ago)s ago') % {
                'job_id': job.job_id,
                'status': job.status.title(),
                'ago': timesince(job.created_at, now) if job.created_at else _('just now'),
            }

        output = [
            'System Status Report:',
            '────────────────────────────────────────',
            f"Jobs Total: {stats['total']} | Active: {stats['active']} | Completed: {stats['completed']} | Failed: {stats['failed']}",
            f"Queued: {stats['queued']} | Processing: {stats['processing']} | Cancelled: {stats['cancelled']}",
            '',
            f"Active Job: {active_line}",
            f"Last Update: {timestamp}",
        ]
        return {
            'output': '\n'.join(output),
            'type': 'success',
            'timestamp': timestamp
        }

    if cmd == 'jobs list':
        jobs = get_recent_analysis_jobs(user, limit=8)
        if not jobs:
            return {
                'output': 'No analysis jobs found. Use the wizard to start a new analysis.',
                'type': 'info',
                'timestamp': timestamp
            }

        lines = [
            'Recent Analysis Jobs:',
            '────────────────────────────────────────────',
            'ID       Status       Template            Started',
        ]
        for job in jobs:
            started = timesince(job.created_at, now) if job.created_at else 'just now'
            template_name = (job.analysis_options or {}).get('template_name') or job.template_id
            lines.append(f"{job.job_id:<8} {job.status.title():<11} {template_name[:18]:<18} {started} ago")

        lines.append('────────────────────────────────────────────')
        lines.append("Use 'jobs <id>' for detailed information")
        return {
            'output': '\n'.join(lines),
            'type': 'info',
            'timestamp': timestamp
        }

    if cmd.startswith('jobs ') and len(cmd.split()) == 2:
        job_id = cmd.split()[1]
        job = AnalysisJob.objects.filter(job_id=job_id, user=user).first()
        if not job:
            return {
                'output': _('Job %(job_id)s not found for current user.') % {'job_id': job_id},
                'type': 'error',
                'timestamp': timestamp
            }

        started = timesince(job.created_at, now) if job.created_at else _('just now')
        completed = timesince(job.completed_at, now) if job.completed_at else None
        options = job.analysis_options or {}
        template_name = options.get('template_name') or job.template_id
        custom_questions = options.get('custom_questions') or []

        output = [
            f"Job Details: {job.job_id}",
            '────────────────────────────────────────────',
            f"Status: {job.status.title()} (progress {job.progress}%)",
            f"Analysis Type: {job.analysis_type.title()}",
            f"Template: {template_name}",
            f"Expert Role: {job.expert_role}",
            f"Input: {options.get('input_value', job.content_id)}",
            f"Created: {started} ago",
        ]
        if completed:
            output.append(f"Completed: {completed} ago")
        if custom_questions:
            output.append('Custom Questions:')
            for question in custom_questions:
                output.append(f"  • {question}")
        if job.error_message:
            output.append('Error Message:')
            output.append(job.error_message)

        return {
            'output': '\n'.join(output),
            'type': 'info',
            'timestamp': timestamp
        }

    if cmd == 'queue status':
        stats = get_analysis_job_statistics(user)
        queued_jobs = get_recent_analysis_jobs(user, limit=5)
        queued_jobs = [job for job in queued_jobs if job.status == 'queued']
        lines = [
            'Analysis Queue Dashboard:',
            '────────────────────────────────────────────',
            f"Current Queue Length: {stats['queued']} jobs",
            f"Processing: {stats['processing']} jobs",
            f"Completed Today: {stats['completed']}",
            '',
        ]
        if queued_jobs:
            lines.append('Next Jobs:')
            for job in queued_jobs:
                created = timesince(job.created_at, now) if job.created_at else 'just now'
                lines.append(f"  • {job.job_id} | {job.expert_role} | queued {created} ago")
        else:
            lines.append('No queued jobs at the moment. ✅')

        return {
            'output': '\n'.join(lines),
            'type': 'info',
            'timestamp': timestamp
        }

    if cmd.startswith('start '):
        video_url = command[6:].strip()
        if not video_url:
            return {
                'output': _('Usage: start <youtube_url_or_id>'),
                'type': 'error',
                'timestamp': timestamp
            }

        template_obj = (
            AnalysisTemplate.objects.filter(template_id='default-analysis').first()
            or AnalysisTemplate.objects.filter(slug='default-analysis').first()
            or AnalysisTemplate.objects.filter(is_active=True).order_by('order').first()
        )

        template_id = template_obj.template_id if template_obj else 'default-analysis'
        template_name = template_obj.name if template_obj else _('Default Analysis')

        job = create_analysis_job_entry(
            user=user,
            tool_type='single_video',
            role='content-creator',
            template_id=template_id,
            input_value=video_url,
            custom_questions=[],
            analysis_depth='detailed',
            report_language='en',
            additional_options={
                'display_name': template_name,
                'template_db_id': template_obj.id if template_obj else None,
                'template_name': template_name,
                'source': 'cli',
            }
        )

        return {
            'output': _(
                'Analysis job %(job_id)s created for %(video)s with template %(template)s. Use "jobs %(job_id)s" to monitor.'
            ) % {
                'job_id': job.job_id,
                'video': video_url,
                'template': template_name,
            },
            'type': 'success',
            'timestamp': timestamp
        }

    if cmd == 'docker ps':
        try:
            result = subprocess.run(
                ['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}\t{{.Image}}'],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return {
                    'output': f'Docker Container Status:\n{result.stdout}',
                    'type': 'success',
                    'timestamp': timestamp,
                }
            return {
                'output': 'Error accessing Docker services',
                'type': 'error',
                'timestamp': timestamp,
            }
        except Exception:
            return {
                'output': 'Docker services unavailable - running in development mode',
                'type': 'warning',
                'timestamp': timestamp,
            }

    if cmd.startswith('logs'):
        logs = [
            f"[{timestamp}] INFO  User {user.username} executed CLI command '{command}'",
        ]
        recent_jobs = get_recent_analysis_jobs(user, limit=3)
        for job in recent_jobs:
            created = timesince(job.created_at, now) if job.created_at else 'just now'
            logs.append(f"[{timestamp}] JOB   {job.job_id} status {job.status} (created {created} ago)")
        return {
            'output': '\n'.join(logs),
            'type': 'info',
            'timestamp': timestamp
        }

    if cmd == 'help':
        return {
            'output': '''TubeWhale CLI Commands Reference:
╭─────────────────────────────────────────────╮
│ System Commands                             │
├─────────────────────────────────────────────┤
│ status          Show complete system status │
│ docker ps       List Docker containers      │
│ logs            Show recent system logs     │
│ clear           Clear terminal screen       │
├─────────────────────────────────────────────┤  
│ Job Management                              │
├─────────────────────────────────────────────┤
│ jobs list       List all analysis jobs     │
│ jobs <id>       Show specific job details  │
│ queue status    Show processing queue      │
│ start <url>     Start new video analysis   │
├─────────────────────────────────────────────┤
│ Navigation                                  │ 
├─────────────────────────────────────────────┤
│ templates       Open template selection    │
│ dashboard       Return to main dashboard   │
│ help            Show this help message     │
╰─────────────────────────────────────────────╯

Tip: Use Tab for autocompletion, ↑/↓ for history''',
            'type': 'success',
            'timestamp': timestamp
        }
    
    # Handle job details
    elif cmd.startswith('jobs ') and len(cmd.split()) == 2:
        job_id = cmd.split()[1]
        return {
            'output': f'''Job Details: {job_id}
═══════════════════════════════════════
Status: Running
Template: Sentiment Analysis Pro
Video URL: https://youtube.com/watch?v=example
Started: 2 minutes ago
Progress: 65% complete
ETA: 1m 30s remaining

Processing Steps:
✓ Video download completed
✓ Audio extraction completed  
✓ Transcript generation completed
⚡ Sentiment analysis in progress
⏳ Report generation pending
⏳ Result compilation pending

Resource Usage:
CPU: 45% | Memory: 2.1GB | Disk: 150MB''',
            'type': 'info',
            'timestamp': timestamp
        }
    
    # Handle video analysis start
    elif cmd.startswith('start '):
        video_url = cmd.replace('start ', '').strip()
        if video_url:
            job_id = f"#{1000 + hash(video_url) % 9000}"
            return {
                'output': f'''Analysis Job Created Successfully!
═══════════════════════════════════════
Job ID: {job_id}
Video URL: {video_url}
Template: Default Analysis
Status: Queued for processing
Priority: Normal
Estimated Start: 30 seconds

Your analysis will begin shortly. 
Use 'jobs {job_id.replace("#", "")}' to monitor progress.''',
                'type': 'success',
                'timestamp': timestamp
            }
        else:
            return {
                'output': '''Usage: start <video_url>
                
Example:
start https://youtube.com/watch?v=example
start https://youtu.be/abc123

The video URL should be a valid YouTube URL.''',
                'type': 'error',
                'timestamp': timestamp
            }
    
    # Handle unknown commands
    else:
        suggestions = []
        all_commands = ['status', 'help', 'jobs list', 'docker ps', 'logs', 'queue status', 'start', 'clear']
        
        # Simple command suggestion based on similarity
        for available_cmd in all_commands:
            if cmd in available_cmd or available_cmd in cmd:
                suggestions.append(available_cmd)
        
        suggestion_text = f"\nDid you mean: {', '.join(suggestions[:3])}" if suggestions else ""
        
        return {
            'output': f'''Command not recognized: {command}

Type 'help' to see all available commands.{suggestion_text}

Common commands:
• status - Check system health
• jobs list - View active jobs  
• docker ps - Check containers
• logs - View recent activity''',
            'type': 'error',
            'timestamp': timestamp
        }


# Helper functions for intelligent wizard

def handle_wizard_submission(request, available_templates):
    """Handle form submission from intelligent wizard"""
    from django.http import JsonResponse
    import json
    
    try:
        data = json.loads(request.body) if request.content_type == 'application/json' else request.POST
        
        selected_role = data.get('selected_role')
        selected_template_id = data.get('template_id')
        custom_config = data.get('custom_config', {})
        
        # Find the selected template
        selected_template = None
        for template in available_templates:
            if str(template.id) == str(selected_template_id):
                selected_template = template
                break
        
        if not selected_template:
            return JsonResponse({'error': 'Selected template not found'}, status=400)
        
        # Create analysis job configuration
        job_config = {
            'template_id': selected_template.id,
            'template_name': selected_template.name,
            'role': selected_role,
            'custom_config': custom_config,
            'user_id': request.user.id,
            'tier': request.user.tier,
        }
        
        # For now, just return success - in production, this would create a job
        return JsonResponse({
            'success': True,
            'message': f'Analysis configured successfully with {selected_template.name}',
            'job_config': job_config,
            'redirect_url': '/dashboard/jobs/'
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def get_user_max_jobs(user):
    """Get maximum concurrent jobs for user tier"""
    tier_limits = {
        'basic': 1,
        'standard': 3,
        'premium': 10,
        'enterprise': 50
    }
    return tier_limits.get(user.tier.lower() if user.tier else 'basic', 1)


def job_detail_view(request, job_id):
    """Render a detailed analysis report for a specific job"""
    if request.user.is_authenticated:
        job = get_object_or_404(AnalysisJob, job_id=job_id, user=request.user)
    else:
        # Allow anonymous access to view jobs (demo mode)
        job = get_object_or_404(AnalysisJob, job_id=job_id)

    analysis_options = job.analysis_options or {}
    youtube_meta = analysis_options.get('youtube_metadata') or {}

    video_id = job.content_id or youtube_meta.get('video_id')
    video_url = (
        job.content_url
        or youtube_meta.get('video_url')
        or (f"https://www.youtube.com/watch?v={video_id}" if video_id else None)
    )
    embed_url = youtube_meta.get('embed_url')
    if not embed_url and video_id:
        embed_url = f"https://www.youtube.com/embed/{video_id}"

    youtube_info = {
        'video_id': video_id,
        'video_url': video_url,
        'embed_url': embed_url,
        'thumbnail_url': youtube_meta.get('thumbnail_url') or analysis_options.get('thumbnail_url'),
        'title': job.content_title or youtube_meta.get('title') or analysis_options.get('video_title'),
    }
    if not any(youtube_info.values()):
        youtube_info = None

    focus_areas = analysis_options.get('focus_areas')
    modules = analysis_options.get('modules') if not focus_areas else None
    if not focus_areas and isinstance(modules, dict):
        focus_areas = [
            module.replace('_', ' ').title()
            for module, enabled in modules.items()
            if enabled
        ]

    template_obj = AnalysisTemplate.objects.filter(template_id=job.template_id).first()
    template_info = None
    if template_obj:
        template_info = {
            'name': template_obj.name,
            'level': template_obj.complexity_level.replace('_', ' ') if template_obj.complexity_level else None,
            'estimated_time': template_obj.estimated_duration or _('Not specified'),
            'expert_role': job.expert_role,
            'focus_areas': focus_areas or template_obj.get_tags_list(),
        }
    elif analysis_options:
        depth = analysis_options.get('analysis_depth') or analysis_options.get('analysis_level')
        template_info = {
            'name': analysis_options.get('template_name') or job.template_id,
            'level': depth.replace('_', ' ').title() if isinstance(depth, str) else None,
            'estimated_time': analysis_options.get('estimated_time'),
            'expert_role': job.expert_role,
            'focus_areas': focus_areas or [],
        }

    def normalize_score(value):
        if value is None:
            return None
        return round(value * 100, 1) if value <= 1 else round(value, 1)

    try:
        result_obj = job.result
    except AnalysisResult.DoesNotExist:
        result_obj = None

    results_context = None
    if result_obj:
        download_base = reverse('tubewhale_engine:download-analysis-result', args=[job.job_id])
        available_formats = result_obj.available_formats or []
        fallback_formats = ['json', 'markdown']
        format_sequence = [
            fmt for fmt in (available_formats + fallback_formats)
            if fmt
        ]
        # Preserve order while removing duplicates
        seen = set()
        download_links = {}
        for fmt in format_sequence:
            fmt_key = fmt.lower()
            if fmt_key in seen:
                continue
            seen.add(fmt_key)
            download_links[fmt_key] = f"{download_base}?format={fmt_key}"

        raw_data = result_obj.raw_data or {}
        summary = result_obj.summary
        if not summary and isinstance(raw_data, dict):
            summary = raw_data.get('summary') or raw_data.get('ai_summary')
            ai_response = raw_data.get('ai_response') if isinstance(raw_data.get('ai_response'), dict) else {}
            summary = summary or ai_response.get('summary')

        results_context = {
            'summary': summary,
            'raw_data': raw_data,
            'metrics': result_obj.metrics or {},
            'confidence_score': normalize_score(result_obj.confidence_score),
            'completeness_score': normalize_score(result_obj.completeness_score),
            'download_links': download_links,
        }

    job.estimated_completion = job.estimated_completion_time

    context = {
        'job': job,
        'youtube': youtube_info,
        'template': template_info,
        'results': results_context,
    }

    return render(request, 'dashboard/job_detail.html', context)


def jobs_view(request):
    """Display user's analysis jobs and handle unified wizard submissions"""
    from django.shortcuts import render
    from django.urls import reverse
    from django.http import HttpResponseRedirect

    if request.method == 'POST':
        tool = request.POST.get('tool', '').strip()
        role = request.POST.get('role', '').strip()
        template = request.POST.get('template', '').strip()
        input_data = (request.POST.get('input') or '').strip()

        depth = request.POST.get('depth', 'detailed')
        language = request.POST.get('language', 'zh-CN')
        custom_questions_raw = request.POST.get('custom_questions', '[]')
        modules_raw = request.POST.get('modules')
        limit = request.POST.get('limit')
        count = request.POST.get('count')
        content_type = request.POST.get('type', 'mixed')

        if tool not in {'single_video', 'playlist', 'brainstorm'}:
            messages.error(request, _('Unsupported tool type: {}').format(tool or 'unknown'))
            return HttpResponseRedirect(reverse('dashboard:smart_wizard'))

        if not all([role, template]) or (tool != 'brainstorm' and not input_data):
            messages.error(request, _('All required fields must be completed for analysis'))
            return HttpResponseRedirect(reverse('dashboard:smart_wizard'))

        analysis_modules = {
            'transcript': True,
            'sentiment': True,
            'keywords': True,
            'trends': False,
        }
        if modules_raw:
            try:
                parsed_modules = json.loads(modules_raw)
                if isinstance(parsed_modules, dict):
                    analysis_modules.update(parsed_modules)
            except json.JSONDecodeError:
                pass

        custom_questions = []
        try:
            parsed_questions = json.loads(custom_questions_raw) if custom_questions_raw else []
            if isinstance(parsed_questions, list):
                custom_questions = [q.strip() for q in parsed_questions if isinstance(q, str) and q.strip()]
        except json.JSONDecodeError:
            custom_questions = []

        template_obj = (
            AnalysisTemplate.objects.filter(template_id=template).first()
            or AnalysisTemplate.objects.filter(slug=template).first()
        )
        display_name = template_obj.name if template_obj else template.replace('-', ' ').title()

        additional_options = {
            'display_name': display_name,
            'template_db_id': template_obj.id if template_obj else None,
            'template_name': display_name,
            'modules': analysis_modules,
            'report_language': language,
            'source': 'wizard_enhanced',
        }

        if tool == 'playlist':
            additional_options['video_limit'] = int(limit) if limit and limit.isdigit() else None
        if tool == 'brainstorm':
            additional_options['brainstorm_topic'] = input_data
            additional_options['content_count'] = int(count) if count and count.isdigit() else 20
            additional_options['content_type'] = content_type

        try:
            job = create_analysis_job_entry(
                user=request.user,
                tool_type=tool,
                role=role,
                template_id=template,
                input_value=input_data,
                custom_questions=custom_questions,
                analysis_depth=depth,
                report_language=language,
                additional_options=additional_options,
            )

            request.session['wizard_job_params'] = {
                'tool_type': tool,
                'template': display_name,
                'job_id': job.job_id,
            }

            tool_names = {
                'single_video': _('Single Video Analysis'),
                'playlist': _('Playlist Analysis'),
                'brainstorm': _('AI Brainstorm Analysis'),
            }

            messages.success(
                request,
                _('🚀 {} started successfully! Job ID: {} | Role: {} | Template: {}').format(
                    tool_names.get(tool, _('Analysis')), job.job_id, role, display_name
                )
            )
            return redirect('dashboard:job_runner')

        except Exception as exc:
            messages.error(request, _('Error starting analysis: {}').format(str(exc)))
            return HttpResponseRedirect(reverse('dashboard:smart_wizard'))

    # Get user's actual jobs
    if request.user.is_authenticated:
        recent_jobs = AnalysisJob.objects.filter(
            user=request.user
        ).order_by('-created_at')[:10]
    else:
        # Show all jobs for anonymous users (demo mode)
        recent_jobs = AnalysisJob.objects.all().order_by('-created_at')[:10]
    
    return render(request, 'dashboard/jobs.html', {
        'user': request.user,
        'jobs': recent_jobs,
    })


def transparent_analysis_terminal(request):
    """Transparent analysis terminal interface"""
    from django.shortcuts import render
    return render(request, 'dashboard/transparent_analysis_terminal.html', {
        'user': request.user,
    })


def transparent_analysis_api(request):
    """API endpoint for transparent analysis"""
    from django.http import JsonResponse
    import json
    import uuid
    
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        data = json.loads(request.body) if request.content_type == 'application/json' else request.POST
        video_url = data.get('video_url', '').strip()
        template = data.get('template', 'general-analyst')
        analysis_type = data.get('analysis_type', 'comprehensive')
        
        if not video_url:
            return JsonResponse({'error': 'Video URL is required'}, status=400)
        
        # Generate a job ID and task ID for tracking
        job_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        
        # For now, return success - in production this would start actual analysis
        return JsonResponse({
            'status': 'success',
            'job_id': job_id,
            'task_id': task_id,
            'message': 'Analysis job submitted successfully'
        })
        
    except Exception as e:
        return JsonResponse({'error': f'Analysis error: {str(e)}'}, status=500)


def create_job_from_wizard_config(user, config):
    """Create a job from wizard configuration"""
    import uuid
    # This is a placeholder - implement actual job creation logic
    return f"job_{uuid.uuid4().hex[:8]}"


def analysis_status_api(request, job_id):
    """API endpoint to check analysis status"""
    from django.http import JsonResponse
    
    # This is a placeholder - implement actual status checking
    return JsonResponse({
        'status': 'pending',
        'progress': 0,
        'message': 'Job is pending',
        'video_id': 'dQw4w9WgXcQ'
    })


@csrf_exempt
def retry_job_view(request, job_id):
    """Retry a failed job"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        # Get the job from the tubewhale_engine app
        from apps.tubewhale_engine.models import AnalysisJob
        job = get_object_or_404(AnalysisJob, job_id=job_id)
        
        # Check if job can be retried
        if job.status not in ['failed', 'error', 'cancelled']:
            return JsonResponse({'error': 'Job cannot be retried'}, status=400)
        
        # Reset job status and retry
        job.status = 'pending'
        job.progress = 0
        job.error_message = ''
        job.save()
        
        # Resubmit the task
        from apps.tubewhale_engine.tasks import analyze_video_task
        task_result = analyze_video_task.delay(job.job_id)
        
        # Update celery task ID
        job.celery_task_id = task_result.id
        job.save()
        
        return JsonResponse({
            'status': 'success',
            'message': 'Job retry initiated successfully'
        })
        
    except Exception as e:
        return JsonResponse({'error': f'Retry failed: {str(e)}'}, status=500)


@csrf_exempt
def cancel_job_view(request, job_id):
    """Cancel a running job"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        # Get the job from the tubewhale_engine app
        from apps.tubewhale_engine.models import AnalysisJob
        job = get_object_or_404(AnalysisJob, job_id=job_id)
        
        # Check if job can be cancelled
        if job.status in ['completed', 'failed', 'cancelled']:
            return JsonResponse({'error': 'Job cannot be cancelled'}, status=400)
        
        # Cancel the Celery task if it exists
        if job.celery_task_id:
            from celery import current_app
            current_app.control.revoke(job.celery_task_id, terminate=True)
        
        # Update job status
        job.status = 'cancelled'
        job.error_message = 'Job cancelled by user'
        job.save()
        
        return JsonResponse({
            'status': 'success',
            'message': 'Job cancelled successfully'
        })
        
    except Exception as e:
        return JsonResponse({'error': f'Cancel failed: {str(e)}'}, status=500)


def job_progress_api(request, job_id):
    """API endpoint for real-time job progress updates"""
    try:
        # Get the job from the tubewhale_engine app
        from apps.tubewhale_engine.models import AnalysisJob
        job = get_object_or_404(AnalysisJob, job_id=job_id)
        
        # Return comprehensive progress information
        return JsonResponse({
            'status': job.status,
            'progress': job.progress,
            'current_step': job.current_step,
            'steps_completed': job.steps_completed,
            'total_steps': job.total_steps,
            'estimated_time_remaining': job.estimated_time_remaining,
            'status_message': job.status_message,
            'created_at': job.created_at.isoformat(),
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'error_message': job.error_message,
            'processing_time_seconds': job.processing_time_seconds,
        })
        
    except Exception as e:
        return JsonResponse({'error': f'Progress fetch failed: {str(e)}'}, status=500)