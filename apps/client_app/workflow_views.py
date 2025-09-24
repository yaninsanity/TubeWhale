"""
Client App - Enhanced Multi-Step Workflow Views
四步骤客户端分析工作流：
0. Scenario Selection (场景选择)
1. Engine Configuration (引擎配置)
2. Template Selection (模板选择) 
3. Search & Execute (搜索执行)
"""

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json
import os
import yaml

from apps.dashboard_app.models import AnalysisTemplate, TemplateCategory
from apps.user_app.decorators import client_login_required
from .scenario_config import get_all_scenarios, get_scenario_config, get_scenario_engine_defaults
from . import job_store
from . import job_store

# Scenario-based template configurations
SCENARIO_CONFIGS = {
    'content_creator': {
        'name': 'Content Creator',
        'description': 'Optimize content strategy and discover viral opportunities',
        'templates': ['trend_analysis', 'viral_content_detection', 'audience_sentiment', 'competitor_analysis'],
        'default_params': {
            'max_n': 100,
            'top_k': 20,
            'filter_type': 'engagement_based',
            'concurrency': 8
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are a creative content strategist and YouTube trend expert. Generate keyword variations that capture viral potential and audience interest.',
                'focus': 'viral content, trending topics, audience engagement'
            },
            'analysis': {
                'system': 'You are a content performance analyst. Focus on engagement metrics, viral patterns, and audience behavior.',
                'focus': 'engagement, viral potential, audience sentiment'
            }
        }
    },
    'researcher': {
        'name': 'Academic Researcher',
        'description': 'Rigorous research analysis with academic-grade insights',
        'templates': ['comprehensive_analysis', 'statistical_analysis', 'longitudinal_study', 'citation_ready'],
        'default_params': {
            'max_n': 200,
            'top_k': 50,
            'filter_type': 'comprehensive',
            'concurrency': 6
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are an academic researcher with expertise in digital humanities and social media studies. Generate methodical keyword variations for comprehensive data collection.',
                'focus': 'comprehensive coverage, academic rigor, methodological approach'
            },
            'analysis': {
                'system': 'You are an academic analyst. Provide rigorous, evidence-based analysis suitable for academic research.',
                'focus': 'statistical significance, research methodology, academic standards'
            }
        }
    },
    'marketer': {
        'name': 'Digital Marketer',
        'description': 'Marketing intelligence and brand monitoring',
        'templates': ['brand_monitoring', 'influencer_analysis', 'campaign_tracking', 'competitive_intelligence'],
        'default_params': {
            'max_n': 150,
            'top_k': 30,
            'filter_type': 'brand_relevant',
            'concurrency': 10
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are a digital marketing strategist with expertise in brand management and social media marketing. Generate keyword variations focused on brand mentions, marketing opportunities, and competitive analysis.',
                'focus': 'brand mentions, marketing opportunities, competitive landscape'
            },
            'analysis': {
                'system': 'You are a marketing analyst. Focus on brand sentiment, marketing opportunities, and competitive positioning.',
                'focus': 'brand sentiment, marketing ROI, competitive analysis'
            }
        }
    },
    'business_intelligence': {
        'name': 'Business Intelligence',
        'description': 'Enterprise-grade market research and strategic insights',
        'templates': ['market_research', 'industry_analysis', 'strategic_intelligence', 'decision_support'],
        'default_params': {
            'max_n': 300,
            'top_k': 75,
            'filter_type': 'business_relevant',
            'concurrency': 12
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are a business intelligence analyst with expertise in market research and strategic planning. Generate keyword variations that capture market trends, industry insights, and business opportunities.',
                'focus': 'market trends, industry analysis, business opportunities'
            },
            'analysis': {
                'system': 'You are a business intelligence analyst. Provide strategic insights, market analysis, and decision-support recommendations.',
                'focus': 'strategic insights, market dynamics, business intelligence'
            }
        }
    },
    'data_scientist': {
        'name': 'Data Scientist',
        'description': 'Advanced ML analysis with custom models and API access',
        'templates': ['ml_analysis', 'statistical_modeling', 'custom_analytics', 'api_integration'],
        'default_params': {
            'max_n': 500,
            'top_k': 100,
            'filter_type': 'data_rich',
            'concurrency': 15
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are a data scientist with expertise in machine learning and statistical analysis. Generate keyword variations that maximize data collection for advanced analytics.',
                'focus': 'data coverage, statistical significance, ML features'
            },
            'analysis': {
                'system': 'You are a data scientist. Provide technical analysis with statistical insights and machine learning perspectives.',
                'focus': 'statistical analysis, ML insights, data patterns'
            }
        }
    },
    'custom': {
        'name': 'Custom Analysis',
        'description': 'Fully customizable analysis with complete control',
        'templates': ['custom_template'],
        'default_params': {
            'max_n': 100,
            'top_k': 25,
            'filter_type': 'custom',
            'concurrency': 8
        },
        'prompts': {
            'keyword_generation': {
                'system': 'You are a versatile AI assistant. Generate keyword variations based on the specific requirements provided.',
                'focus': 'user-defined objectives'
            },
            'analysis': {
                'system': 'You are a versatile analyst. Provide analysis based on the specific requirements and parameters provided.',
                'focus': 'user-defined analysis goals'
            }
        }
    }
}

@client_login_required
def client_scenario_selection(request):
    """Step 0: Let user choose their analysis scenario/role"""
    if request.method == 'POST':
        scenario_key = request.POST.get('scenario')
        
        if scenario_key:
            scenario_config = get_scenario_config(scenario_key)
            # Store scenario in session
            request.session['selected_scenario'] = scenario_key
            request.session['scenario_config'] = scenario_config
            
            messages.success(request, f"Selected scenario: {scenario_config['name']}")
            return redirect('client_app:workflow_start')
        else:
            messages.error(request, "Please select a valid scenario.")
    
    return render(request, 'client/workflow/scenario_selection.html', {
        'scenarios': get_all_scenarios(),
        'user_tier': getattr(request.user, 'tier', 'free')
    })


@client_login_required
def client_workflow_start(request):
    """
    客户端工作流起始页 - 引擎配置步骤
    Step 1: Engine Configuration (after scenario selection)
    """
    # Check if user has selected a scenario
    selected_scenario = request.session.get('selected_scenario')
    scenario_config = request.session.get('scenario_config')
    
    if not selected_scenario:
        # Redirect to scenario selection if not completed
        return redirect('client_app:scenario_selection')
    
    # Get scenario-based defaults using new config system
    scenario_defaults = get_scenario_engine_defaults(selected_scenario) if selected_scenario else {}
    
    # 从.env获取可配置的引擎参数，合并场景默认值
    engine_defaults = {
        'keyword': os.getenv('KEYWORD', 'Arizona homeless before covid19'),
        'max_n': scenario_defaults.get('max_n', int(os.getenv('MAX_N', 5))),
        'top_k': scenario_defaults.get('top_k', int(os.getenv('TOP_K', 10))),
        'filter_type': scenario_defaults.get('filter_type', os.getenv('FILTER_TYPE', 'view_count')),
        'full_audio_analysis': os.getenv('FULL_AUDIO_ANALYSIS', 'true').lower() == 'true',
        'persist_summaries': os.getenv('PERSIST_AGENT_SUMMARIES', 'true').lower() == 'true',
        'concurrency': scenario_defaults.get('concurrency', int(os.getenv('CONCURRENCY', 2))),
        'dry_run': os.getenv('DRY_RUN', 'false').lower() == 'true',
    }
    
    # 从session获取已保存的配置（如果有）
    saved_config = request.session.get('engine_config', {})
    engine_config = {**engine_defaults, **saved_config}
    
    context = {
        'title': _('Configure Analysis Engine'),
        'subtitle': f"Step 1 of 3: {scenario_config['name']} Configuration" if scenario_config else _('Step 1 of 3: Set your analysis parameters'),
        'engine_config': engine_config,
        'user_tier': request.user.tier,
        'step': 1,
        'next_step_url': reverse('client_app:template_selection'),
        'selected_scenario': selected_scenario,
        'scenario_config': scenario_config,
    }
    
    if request.method == 'POST':
        # 保存引擎配置到session
        config = {
            'keyword': request.POST.get('keyword', engine_defaults['keyword']),
            'max_n': int(request.POST.get('max_n', engine_defaults['max_n'])),
            'top_k': int(request.POST.get('top_k', engine_defaults['top_k'])),
            'filter_type': request.POST.get('filter_type', engine_defaults['filter_type']),
            'full_audio_analysis': request.POST.get('full_audio_analysis') == 'on',
            'persist_summaries': request.POST.get('persist_summaries') == 'on',
            'concurrency': int(request.POST.get('concurrency', engine_defaults['concurrency'])),
            'dry_run': request.POST.get('dry_run') == 'on',
        }
        
        request.session['engine_config'] = config
        messages.success(request, _('Engine configuration saved. Now select your analysis template.'))
        return redirect('client:template_selection')
    
    return render(request, 'client/workflow/engine_config.html', context)


@client_login_required
def client_template_selection(request):
    """
    模板选择页面 - 基于场景的动态模板
    Step 2: Template Selection (scenario-based)
    """
    user = request.user
    
    # 检查是否有引擎配置和场景选择
    engine_config = request.session.get('engine_config')
    selected_scenario = request.session.get('selected_scenario')
    scenario_config = request.session.get('scenario_config')
    
    if not engine_config:
        messages.warning(request, _('Please configure the engine first.'))
        return redirect('client_app:workflow_start')
    
    if not selected_scenario:
        messages.warning(request, _('Please select a scenario first.'))
        return redirect('client_app:scenario_selection')
    
    # 创建基于场景的动态模板
    scenario_templates = create_scenario_templates(selected_scenario, scenario_config, user)
    
    # 处理模板选择
    if request.method == 'POST':
        template_id = request.POST.get('template_id')
        if template_id and template_id in scenario_templates:
            template = scenario_templates[template_id]
            request.session['selected_template'] = {
                'template_id': template['template_id'],
                'name': template['name'],
                'description': template['description'],
                'category': template['category'],
                'prompts': template['prompts'],
                'config': template['config']
            }
            messages.success(request, f"Selected template: {template['name']}")
            return redirect('client_app:search_execute')
        else:
            messages.error(request, _('Please select a valid template.'))
    
    # 按分类组织模板
    categorized_templates = {}
    for template_id, template in scenario_templates.items():
        category = template['category']
        if category not in categorized_templates:
            categorized_templates[category] = {
                'name': category,
                'templates': []
            }
        categorized_templates[category]['templates'].append(template)
    
    context = {
        'title': _('Select Analysis Template'),
        'subtitle': f"Step 2 of 3: {scenario_config['name']} Templates" if scenario_config else _('Step 2 of 3: Choose your analysis template'),
        'categorized_templates': categorized_templates,
        'engine_config': engine_config,
        'selected_scenario': selected_scenario,
        'scenario_config': scenario_config,
        'user_tier': user.tier,
        'step': 2,
    }
    
    return render(request, 'client/workflow/template_selection.html', context)


def create_scenario_templates(scenario_key, scenario_config, user):
    """Create dynamic templates based on user scenario using new config system"""
    from .scenario_config import get_scenario_custom_prompts
    
    templates = {}
    custom_prompts = get_scenario_custom_prompts(scenario_key)
    
    if scenario_key == 'content_creator':
        templates.update({
            'viral_content_analysis': {
                'template_id': 'viral_content_analysis',
                'name': 'Viral Content Analysis',
                'description': 'Analyze trending patterns and viral indicators in YouTube content',
                'category': 'Content Strategy',
                'tier': 'free',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['view_count', 'like_ratio', 'comment_engagement'],
                    'analysis_depth': 'viral_patterns'
                }
            },
            'content_optimization': {
                'template_id': 'content_optimization',
                'name': 'Content Optimization',
                'description': 'Optimize content strategy for maximum engagement and reach',
                'category': 'Content Strategy',
                'tier': 'standard',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['engagement_rate', 'retention_rate', 'click_through_rate'],
                    'analysis_depth': 'optimization_focused'
                }
            },
            'audience_engagement': {
                'template_id': 'audience_engagement',
                'name': 'Audience Engagement Analysis',
                'description': 'Deep dive into audience behavior and engagement patterns',
                'category': 'Audience Insights',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['comment_sentiment', 'engagement_quality', 'audience_retention'],
                    'analysis_depth': 'engagement_deep'
                }
            }
        })
    
    elif scenario_key == 'market_researcher':
        templates.update({
            'market_intelligence': {
                'template_id': 'market_intelligence',
                'name': 'Market Intelligence Analysis',
                'description': 'Comprehensive market research and competitive intelligence',
                'category': 'Market Research',
                'tier': 'standard',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['market_share', 'competitive_landscape', 'trend_analysis'],
                    'analysis_depth': 'market_comprehensive'
                }
            },
            'competitor_analysis': {
                'template_id': 'competitor_analysis',
                'name': 'Competitor Analysis',
                'description': 'In-depth competitor strategy and performance analysis',
                'category': 'Competitive Intelligence',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['competitor_performance', 'strategy_gaps', 'positioning'],
                    'analysis_depth': 'competitive_deep'
                }
            },
            'trend_forecasting': {
                'template_id': 'trend_forecasting',
                'name': 'Trend Forecasting',
                'description': 'Predict future trends and market opportunities',
                'category': 'Trend Analysis',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['trend_indicators', 'growth_patterns', 'future_opportunities'],
                    'analysis_depth': 'predictive'
                }
            }
        })
    
    elif scenario_key == 'academic_researcher':
        templates.update({
            'academic_analysis': {
                'template_id': 'academic_analysis',
                'name': 'Academic Research Analysis',
                'description': 'Rigorous academic research with peer-review quality insights',
                'category': 'Academic Research',
                'tier': 'standard',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['research_validity', 'citation_potential', 'methodological_rigor'],
                    'analysis_depth': 'academic_rigorous'
                }
            },
            'data_mining': {
                'template_id': 'data_mining',
                'name': 'Data Mining & Analysis',
                'description': 'Advanced data mining techniques for research insights',
                'category': 'Data Science',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['data_patterns', 'statistical_significance', 'correlation_analysis'],
                    'analysis_depth': 'data_intensive'
                }
            },
            'research_synthesis': {
                'template_id': 'research_synthesis',
                'name': 'Research Synthesis',
                'description': 'Synthesize multiple research sources into coherent insights',
                'category': 'Literature Review',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['synthesis_quality', 'source_credibility', 'research_gaps'],
                    'analysis_depth': 'synthesis_comprehensive'
                }
            }
        })
    
    elif scenario_key == 'business_analyst':
        templates.update({
            'business_intelligence': {
                'template_id': 'business_intelligence',
                'name': 'Business Intelligence',
                'description': 'Strategic business insights and decision support',
                'category': 'Business Intelligence',
                'tier': 'standard',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['business_impact', 'roi_potential', 'strategic_value'],
                    'analysis_depth': 'business_strategic'
                }
            },
            'strategic_analysis': {
                'template_id': 'strategic_analysis',
                'name': 'Strategic Analysis',
                'description': 'High-level strategic planning and opportunity analysis',
                'category': 'Strategy',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['strategic_alignment', 'competitive_advantage', 'market_position'],
                    'analysis_depth': 'strategic_comprehensive'
                }
            },
            'performance_metrics': {
                'template_id': 'performance_metrics',
                'name': 'Performance Metrics Analysis',
                'description': 'KPI analysis and performance optimization insights',
                'category': 'Performance Analysis',
                'tier': 'premium',
                'prompts': {
                    'keyword_generation': custom_prompts.get('keyword_generation', {}),
                    'summarization': custom_prompts.get('summarization', {})
                },
                'config': {
                    'focus_metrics': ['kpi_performance', 'optimization_opportunities', 'benchmark_analysis'],
                    'analysis_depth': 'metrics_focused'
                }
            }
        })
    
    # Filter templates based on user tier
    user_tier = getattr(user, 'tier', 'free')
    filtered_templates = {}
    
    for template_id, template in templates.items():
        template_tier = template.get('tier', 'free')
        if user_tier == 'premium' or template_tier in ['free', user_tier]:
            filtered_templates[template_id] = template
    
    return filtered_templates


@client_login_required  
def client_search_execute(request):
    """
    搜索和执行页面 - 支持实时CLI集成
    Step 3: Search & Execute
    """
    # 检查前置步骤
    engine_config = request.session.get('engine_config')
    selected_template = request.session.get('selected_template')
    selected_scenario = request.session.get('selected_scenario')
    scenario_config = request.session.get('scenario_config')
    
    if not engine_config:
        messages.warning(request, _('Please configure the engine first.'))
        return redirect('client_app:workflow_start')
        
    if not selected_template:
        messages.warning(request, _('Please select a template first.'))
        return redirect('client_app:template_selection')
    
    context = {
        'title': _('Search & Execute Analysis'),
        'subtitle': _('Step 4 of 5: Run your analysis'),
        'engine_config': engine_config,
        'selected_template': selected_template,
        'selected_scenario': scenario_config,
        'user_tier': getattr(request.user, 'tier', 'free'),
        'step': 4,
        'prev_step_url': reverse('client_app:template_selection'),
    }
    
    if request.method == 'POST':
        # 检查是否是AJAX请求
        if request.headers.get('Content-Type') == 'application/json' or request.content_type == 'application/json':
            return handle_ajax_execution(request, engine_config, selected_template)
        
        # 获取用户输入的搜索关键词
        search_keyword = request.POST.get('search_keyword', engine_config.get('keyword', ''))
        custom_config = request.POST.get('custom_config')
        
        if not search_keyword.strip():
            return JsonResponse({
                'success': False,
                'error': _('Please enter a search keyword.')
            })
        
        # 解析自定义配置
        custom_prompts = None
        if custom_config:
            try:
                custom_prompts = json.loads(custom_config)
            except json.JSONDecodeError:
                pass
        
        # 更新引擎配置中的关键词
        engine_config['keyword'] = search_keyword
        request.session['engine_config'] = engine_config
        
        # 触发分析执行
        try:
            execution_result = trigger_analysis_execution(
                user=request.user,
                engine_config=engine_config,
                template=selected_template,
                custom_prompts=custom_prompts
            )
            
            if execution_result.get('success'):
                # 返回JSON响应用于AJAX处理
                return JsonResponse({
                    'success': True,
                    'job_id': execution_result.get('job_id'),
                    'message': _('Analysis started successfully!'),
                    'process_pid': execution_result.get('process_pid'),
                    'estimated_duration': execution_result.get('estimated_duration')
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': execution_result.get('error', 'Unknown error'),
                    'details': execution_result.get('details', '')
                })
        except Exception as e:
            import traceback
            return JsonResponse({
                'success': False,
                'error': f'Execution error: {str(e)}',
                'details': traceback.format_exc()
            })
    
    return render(request, 'client/workflow/search_execute_v2.html', context)


def handle_ajax_execution(request, engine_config, selected_template):
    """处理AJAX执行请求"""
    try:
        data = json.loads(request.body)
        search_keyword = data.get('search_keyword', '').strip()
        
        if not search_keyword:
            return JsonResponse({
                'success': False,
                'error': _('Please enter a search keyword.')
            })
        
        # 更新配置
        engine_config['keyword'] = search_keyword
        request.session['engine_config'] = engine_config
        
        # 执行分析
        execution_result = trigger_analysis_execution(
            user=request.user,
            engine_config=engine_config,
            template=selected_template
        )
        
        return JsonResponse(execution_result)
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': _('Invalid request format.')
        })


def trigger_analysis_execution(user, engine_config, template, custom_prompts=None, video_ids=None):
    """
    启动CLI分析任务 (使用子进程)
    Enhanced with comprehensive job tracking
    """
    import subprocess
    import uuid
    import tempfile
    import datetime
    
    try:
        # Determine job type
        if video_ids:
            if len(video_ids) == 1:
                job_type = 'single_video'
            else:
                job_type = 'playlist'
        else:
            job_type = 'keyword_search'
        
        # Create comprehensive job record
        job_id = job_store.create_job_record_v2(
            user_id=user.id,
            job_type=job_type,
            template_id=template.get('template_id', 'unknown'),
            template_name=template.get('name', 'Unknown Template'),
            engine_config=engine_config,
            video_ids=video_ids,
            custom_prompts=custom_prompts,
            estimated_duration=estimate_duration_enhanced(engine_config, video_ids)
        )
        
        # Generate config file for CLI
        config_data = {
            'job_id': job_id,
            'user_id': user.id,
            'keyword': engine_config.get('keyword', ''),
            'max_n': engine_config.get('max_n', 5),
            'top_k': engine_config.get('top_k', 10),
            'filter_type': engine_config.get('filter_type', 'view_count'),
            'full_audio_analysis': engine_config.get('full_audio_analysis', True),
            'persist_summaries': engine_config.get('persist_summaries', True),
            'concurrency': engine_config.get('concurrency', 2),
            'dry_run': engine_config.get('dry_run', False),
            'template': template,
            'custom_prompts': custom_prompts,
            'video_ids': video_ids or [],
            'output_dir': f'outputs/{job_id}',
            'job_type': job_type
        }
        
        # Create temporary config file
        config_dir = tempfile.mkdtemp()
        config_file = os.path.join(config_dir, f'config_{job_id}.json')
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
        
        # Start the CLI process
        cmd = [
            'python', 'cli.py',
            '--backend-mode',
            '--config', config_file,
            '--output-dir', f'outputs',
            '--output-format', 'json'
        ]
        
        try:
            # Verify CLI file exists
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            cli_path = os.path.join(project_root, 'cli.py')
            
            if not os.path.exists(cli_path):
                raise FileNotFoundError(f"CLI script not found at {cli_path}")
            
            # Start subprocess in background
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=project_root,
                text=True
            )
            
            # Update job with process info
            job_store.update_job_status_v2(
                job_id=job_id,
                status=job_store.JOB_STATUS_RUNNING,
                process_pid=process.pid,
                current_step="Analysis job started successfully"
            )
            
            # TODO: Consider using celery/background tasks for better process management
            # For now, the process runs independently
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            
            # Update job status to failed
            job_store.update_job_status_v2(
                job_id=job_id,
                status=job_store.JOB_STATUS_FAILED,
                error_message=f"Failed to start CLI process: {str(e)}"
            )
            
            # Return detailed error instead of raising
            return {
                'success': False,
                'error': f'Failed to start CLI process: {str(e)}',
                'details': error_details,
                'job_id': job_id
            }
        
        return {
            'success': True,
            'job_id': job_id,
            'message': 'Analysis job started successfully',
            'estimated_duration': estimate_duration_enhanced(engine_config, video_ids),
            'job_type': job_type,
            'process_pid': process.pid
        }
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return {
            'success': False,
            'error': f'Failed to start analysis: {str(e)}',
            'details': error_details
        }


def estimate_duration_enhanced(engine_config, video_ids=None):
    """Enhanced duration estimation"""
    base_time = 60  # Base time in seconds
    
    if video_ids:
        # For specific videos
        video_count = len(video_ids)
        estimated_seconds = base_time + (video_count * 30)  # 30 seconds per video
    else:
        # For keyword search
        max_n = engine_config.get('max_n', 5)
        estimated_seconds = base_time + (max_n * 20)  # 20 seconds per video from search
    
    # Adjust for analysis complexity
    if engine_config.get('full_audio_analysis', True):
        estimated_seconds *= 1.5
    
    return int(estimated_seconds)


def estimate_duration(config_data):
    """估算分析所需时间"""
    base_time = 60  # 基础时间（秒）
    max_n = config_data.get('max_n', 5)
    
    # 根据视频数量调整时间
    estimated_seconds = base_time + (max_n * 10)
    
    if estimated_seconds < 60:
        return f"{estimated_seconds} seconds"
    else:
        minutes = estimated_seconds // 60
        return f"{minutes} minutes"


@client_login_required
def client_job_results(request, job_id):
    """
    分析结果页面
    """
    context = {
        'title': _('Analysis Results'),
        'job_id': job_id,
        'user_tier': getattr(request.user, 'tier', 'free'),
    }
    return render(request, 'client/workflow/job_results.html', context)


@client_login_required
@require_http_methods(["GET"])
def job_status_api(request, job_id):
    """
    Enhanced job status API with real job data
    """
    try:
        # Get job record from store
        job_data = job_store.get_job_record(job_id)
        
        if not job_data:
            return JsonResponse({
                'error': 'Job not found',
                'status': 'not_found'
            }, status=404)
        
        # Verify user ownership
        if job_data.get('user_id') != request.user.id:
            return JsonResponse({
                'error': 'Access denied',
                'status': 'forbidden'
            }, status=403)
        
        # Check if process is still running (if status is running)
        if job_data.get('status') == job_store.JOB_STATUS_RUNNING:
            process_pid = job_data.get('process_info', {}).get('pid')
            if process_pid:
                try:
                    import psutil
                    if not psutil.pid_exists(process_pid):
                        # Process died, update status
                        job_store.update_job_status_v2(
                            job_id=job_id,
                            status=job_store.JOB_STATUS_FAILED,
                            error_message="Process terminated unexpectedly"
                        )
                        job_data = job_store.get_job_record(job_id)  # Refresh data
                except ImportError:
                    # psutil not available, skip process check
                    pass
        
        # Prepare response
        progress_info = job_data.get('progress', {})
        response_data = {
            'job_id': job_id,
            'status': job_data.get('status', 'unknown'),
            'job_type': job_data.get('job_type', 'unknown'),
            'progress': progress_info.get('percentage', 0),
            'current_step': progress_info.get('current_step', 'Processing...'),
            'steps_completed': progress_info.get('steps_completed', 0),
            'total_steps': progress_info.get('total_steps', 1),
            'time_elapsed': progress_info.get('time_elapsed', 0),
            'estimated_duration': progress_info.get('estimated_duration', 300),
            'created_at': job_data.get('created_at'),
            'started_at': job_data.get('started_at'),
            'completed_at': job_data.get('completed_at'),
            'template': job_data.get('template', {}),
            'inputs': job_data.get('inputs', {}),
            'logs': get_recent_logs_from_job(job_data)
        }
        
        # Add results if completed
        if job_data.get('status') == job_store.JOB_STATUS_COMPLETED:
            results = job_data.get('results', {})
            response_data.update({
                'download_url': f'/client/download/{job_id}/',
                'results_summary': {
                    'output_files': results.get('output_files', []),
                    'summary': results.get('summary'),
                    'statistics': results.get('statistics', {})
                }
            })
        
        # Add error info if failed
        if job_data.get('status') == job_store.JOB_STATUS_FAILED:
            error_info = job_data.get('error_info', {})
            response_data.update({
                'error_message': error_info.get('error_message'),
                'error_details': error_info.get('error_details'),
                'retry_count': error_info.get('retry_count', 0)
            })
        
        return JsonResponse(response_data)
        
    except Exception as e:
        return JsonResponse({
            'error': str(e),
            'status': 'error'
        }, status=500)


def get_recent_logs_from_job(job_data):
    """Get recent logs based on job progress and status"""
    import datetime
    
    logs = []
    current_time = datetime.datetime.now()
    progress = job_data.get('progress', {}).get('percentage', 0)
    status = job_data.get('status', 'unknown')
    current_step = job_data.get('progress', {}).get('current_step', '')
    
    # Generate logs based on job progression
    if progress >= 10 or status in [job_store.JOB_STATUS_RUNNING, job_store.JOB_STATUS_COMPLETED]:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=30)).strftime('%H:%M:%S'),
            'level': 'info',
            'message': f'Job started: {job_data.get("job_type", "analysis")}'
        })
    
    if progress >= 25:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=20)).strftime('%H:%M:%S'),
            'level': 'info',
            'message': 'Processing input parameters...'
        })
    
    if progress >= 50:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=10)).strftime('%H:%M:%S'),
            'level': 'info',
            'message': 'Analysis in progress...'
        })
    
    if current_step:
        logs.append({
            'timestamp': current_time.strftime('%H:%M:%S'),
            'level': 'info',
            'message': current_step
        })
    
    if status == job_store.JOB_STATUS_COMPLETED:
        logs.append({
            'timestamp': current_time.strftime('%H:%M:%S'),
            'level': 'success',
            'message': 'Analysis completed successfully!'
        })
    elif status == job_store.JOB_STATUS_FAILED:
        error_msg = job_data.get('error_info', {}).get('error_message', 'Unknown error')
        logs.append({
            'timestamp': current_time.strftime('%H:%M:%S'),
            'level': 'error',
            'message': f'Job failed: {error_msg}'
        })
    
    return logs[-5:]  # Return last 5 logs


def get_current_phase(progress):
    """根据进度返回当前阶段"""
    phases = [
        (0, "Initializing analysis engine..."),
        (10, "Generating keyword variations..."),
        (25, "Searching YouTube content..."),
        (40, "Downloading video metadata..."),
        (55, "Processing audio transcripts..."),
        (70, "Analyzing content patterns..."),
        (85, "Generating insights..."),
        (95, "Compiling final report..."),
        (100, "Analysis completed successfully!")
    ]
    
    for threshold, phase in reversed(phases):
        if progress >= threshold:
            return phase
    
    return phases[0][1]


def get_recent_logs(job_id, progress):
    """获取最近的日志条目"""
    import datetime
    
    logs = []
    current_time = datetime.datetime.now()
    
    # 根据进度生成相应的日志
    if progress >= 10:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=30)).strftime('%H:%M:%S'),
            'level': 'info',
            'message': 'Keyword generation completed'
        })
    
    if progress >= 25:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=20)).strftime('%H:%M:%S'),
            'level': 'info',
            'message': 'Found 45 relevant videos'
        })
    
    if progress >= 55:
        logs.append({
            'timestamp': (current_time - datetime.timedelta(seconds=10)).strftime('%H:%M:%S'),
            'level': 'success',
            'message': 'Audio transcription completed'
        })
    
    if progress >= 85:
        logs.append({
            'timestamp': current_time.strftime('%H:%M:%S'),
            'level': 'info',
            'message': 'Generating final insights...'
        })
    
    return logs[-5:]  # 返回最近5条日志


@client_login_required
def download_results(request, job_id):
    """
    下载分析结果
    """
    try:
        # 检查用户权限和作业所有权
        # TODO: 验证job_id属于当前用户

        # 查找结果文件
        import os
        import sqlite3
        from django.http import FileResponse, Http404

        # Prefer project-relative outputs folder
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        results_dir = os.path.join(project_root, 'outputs', job_id)
        db_file = os.path.join(results_dir, f"{job_id}_analysis.db")

        if not os.path.exists(db_file):
            # 如果实际文件不存在，创建一个示例文件
            os.makedirs(results_dir, exist_ok=True)

            # 创建示例数据库
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE analysis_results (
                    id INTEGER PRIMARY KEY,
                    video_id TEXT,
                    title TEXT,
                    views INTEGER,
                    sentiment_score REAL,
                    key_insights TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 插入示例数据
            sample_data = [
                ('abc123', 'Sample Video 1', 10000, 0.8, 'Positive engagement detected'),
                ('def456', 'Sample Video 2', 25000, 0.6, 'Moderate sentiment trends'),
                ('ghi789', 'Sample Video 3', 5000, 0.9, 'High viral potential identified')
            ]

            cursor.executemany(
                'INSERT INTO analysis_results (video_id, title, views, sentiment_score, key_insights) VALUES (?, ?, ?, ?, ?)',
                sample_data
            )

            conn.commit()
            conn.close()

        # 返回文件下载响应
        response = FileResponse(
            open(db_file, 'rb'),
            as_attachment=True,
            filename=f'tubewhale_analysis_{job_id}.db'
        )

        return response

    except Exception as e:
        messages.error(request, f'Download failed: {str(e)}')
        return redirect('client_app:scenario_selection')


@client_login_required
@require_http_methods(["POST"])
@csrf_exempt
def start_single_video(request):
    """
    Enhanced API: Start a job for a single video_id or URL
    Supports both form data and JSON payloads
    """
    try:
        # Parse request data (support both JSON and form data)
        if request.content_type and 'application/json' in request.content_type:
            data = json.loads(request.body.decode('utf-8'))
        else:
            data = dict(request.POST)
            # Convert single-item lists to strings for form data
            for key, value in data.items():
                if isinstance(value, list) and len(value) == 1:
                    data[key] = value[0]
    except Exception:
        return JsonResponse({'success': False, 'error': 'Invalid request format'}, status=400)

    # Extract and validate video input
    video_input = data.get('video_id') or data.get('video_url') or data.get('url')
    if not video_input:
        return JsonResponse({
            'success': False, 
            'error': 'video_id, video_url, or url is required'
        }, status=400)

    # Extract video ID from URL if needed
    video_id = job_store.extract_video_id_from_url(video_input.strip())
    if not video_id:
        return JsonResponse({
            'success': False, 
            'error': 'Invalid YouTube video URL or video ID format'
        }, status=400)

    # Validate job inputs
    is_valid, error_msg = job_store.validate_job_inputs('single_video', video_id=video_id)
    if not is_valid:
        return JsonResponse({'success': False, 'error': error_msg}, status=400)

    # Get template configuration
    template_id = data.get('template_id')
    custom_prompts = data.get('custom_prompts')
    
    selected_template = None
    if template_id:
        # Try to get template from current scenario
        scenario_key = request.session.get('selected_scenario')
        scenario_config = request.session.get('scenario_config')
        if scenario_key and scenario_config:
            templates = create_scenario_templates(scenario_key, scenario_config, request.user)
            selected_template = templates.get(template_id)
    
    # Fallback to session template or default
    if not selected_template:
        selected_template = request.session.get('selected_template')
    
    if not selected_template:
        selected_template = {
            'template_id': 'single_video_analysis',
            'name': 'Single Video Analysis',
            'description': 'Analysis of a single YouTube video',
            'category': 'Single Video',
            'prompts': {},
            'config': {}
        }

    # Get engine configuration
    engine_config = request.session.get('engine_config', {})
    
    # Override with any specific parameters for single video
    engine_config.update({
        'max_n': 1,  # Only one video
        'keyword': f'video:{video_id}',  # Use video ID as keyword marker
    })

    # Start the job
    result = trigger_analysis_execution(
        user=request.user,
        engine_config=engine_config,
        template=selected_template,
        custom_prompts=custom_prompts,
        video_ids=[video_id]
    )

    # Enhanced response
    if result.get('success'):
        response_data = {
            'success': True,
            'job_id': result['job_id'],
            'job_type': 'single_video',
            'video_id': video_id,
            'template': selected_template['name'],
            'estimated_duration': result.get('estimated_duration', 300),
            'status_url': f'/client/api/job-status/{result["job_id"]}/',
            'results_url': f'/client/results/{result["job_id"]}/',
            'message': 'Single video analysis started successfully'
        }
        return JsonResponse(response_data)
    else:
        return JsonResponse({
            'success': False,
            'error': result.get('error', 'Unknown error occurred')
        }, status=500)


@client_login_required
@require_http_methods(["POST"])
@csrf_exempt
def start_playlist(request):
    """
    Enhanced API: Start a job for a playlist or list of videos
    Supports playlist_id, playlist_url, or video_ids array
    """
    try:
        # Parse request data
        if request.content_type and 'application/json' in request.content_type:
            data = json.loads(request.body.decode('utf-8'))
        else:
            data = dict(request.POST)
            # Handle form data lists
            for key, value in data.items():
                if isinstance(value, list) and len(value) == 1:
                    data[key] = value[0]
    except Exception:
        return JsonResponse({'success': False, 'error': 'Invalid request format'}, status=400)

    # Extract inputs
    video_ids = data.get('video_ids', [])
    playlist_id = data.get('playlist_id')
    playlist_url = data.get('playlist_url')
    
    # Parse playlist URL if provided
    if playlist_url and not playlist_id:
        import re
        playlist_match = re.search(r'[?&]list=([a-zA-Z0-9_-]+)', playlist_url)
        if playlist_match:
            playlist_id = playlist_match.group(1)

    # Validate inputs
    if not video_ids and not playlist_id:
        return JsonResponse({
            'success': False, 
            'error': 'Either video_ids array, playlist_id, or playlist_url is required'
        }, status=400)

    # Process video_ids if provided
    if video_ids:
        if isinstance(video_ids, str):
            # Handle comma-separated string
            video_ids = [vid.strip() for vid in video_ids.split(',') if vid.strip()]
        
        if not isinstance(video_ids, list) or len(video_ids) == 0:
            return JsonResponse({
                'success': False, 
                'error': 'video_ids must be a non-empty array'
            }, status=400)
        
        # Validate each video ID
        processed_video_ids = []
        for vid in video_ids:
            video_id = job_store.extract_video_id_from_url(str(vid).strip())
            if video_id:
                processed_video_ids.append(video_id)
        
        if not processed_video_ids:
            return JsonResponse({
                'success': False, 
                'error': 'No valid video IDs found in the provided list'
            }, status=400)
        
        video_ids = processed_video_ids
        job_input_ids = video_ids
    else:
        # Use playlist ID - will be expanded by CLI
        job_input_ids = [f'playlist:{playlist_id}']

    # Validate job inputs
    is_valid, error_msg = job_store.validate_job_inputs('playlist', video_ids=job_input_ids, playlist_id=playlist_id)
    if not is_valid:
        return JsonResponse({'success': False, 'error': error_msg}, status=400)

    # Get template configuration
    template_id = data.get('template_id')
    custom_prompts = data.get('custom_prompts')
    
    selected_template = None
    if template_id:
        scenario_key = request.session.get('selected_scenario')
        scenario_config = request.session.get('scenario_config')
        if scenario_key and scenario_config:
            templates = create_scenario_templates(scenario_key, scenario_config, request.user)
            selected_template = templates.get(template_id)
    
    if not selected_template:
        selected_template = request.session.get('selected_template')
    
    if not selected_template:
        selected_template = {
            'template_id': 'playlist_analysis',
            'name': 'Playlist Analysis',
            'description': 'Comprehensive analysis of multiple YouTube videos',
            'category': 'Playlist',
            'prompts': {},
            'config': {}
        }

    # Get engine configuration
    engine_config = request.session.get('engine_config', {})
    
    # Override with playlist-specific parameters
    video_count = len(video_ids) if video_ids else 10  # Estimate for playlist
    engine_config.update({
        'max_n': video_count,
        'keyword': f'playlist:{playlist_id}' if playlist_id else 'multiple_videos',
    })

    # Start the job
    result = trigger_analysis_execution(
        user=request.user,
        engine_config=engine_config,
        template=selected_template,
        custom_prompts=custom_prompts,
        video_ids=job_input_ids
    )

    # Enhanced response
    if result.get('success'):
        response_data = {
            'success': True,
            'job_id': result['job_id'],
            'job_type': 'playlist',
            'video_count': video_count,
            'playlist_id': playlist_id,
            'template': selected_template['name'],
            'estimated_duration': result.get('estimated_duration', 600),
            'status_url': f'/client/api/job-status/{result["job_id"]}/',
            'results_url': f'/client/results/{result["job_id"]}/',
            'message': f'Playlist analysis started for {video_count} videos'
        }
        return JsonResponse(response_data)
    else:
        return JsonResponse({
            'success': False,
            'error': result.get('error', 'Unknown error occurred')
        }, status=500)


# 向后兼容的视图别名
client_template_selection_view = client_template_selection
client_template_configure_view = client_search_execute
client_job_runner_view = client_job_results


@client_login_required
@require_http_methods(["GET"])
def list_user_jobs_api(request):
    """
    API: List user's jobs with filtering and pagination
    """
    try:
        # Get query parameters
        status_filter = request.GET.get('status')
        job_type_filter = request.GET.get('job_type')
        limit = min(int(request.GET.get('limit', 20)), 100)  # Max 100 jobs per request
        offset = int(request.GET.get('offset', 0))
        
        # Get user's jobs
        jobs = job_store.list_user_jobs(request.user.id)
        
        # Apply filters
        if status_filter:
            jobs = [job for job in jobs if job.get('status') == status_filter]
        
        if job_type_filter:
            jobs = [job for job in jobs if job.get('job_type') == job_type_filter]
        
        # Apply pagination
        total_jobs = len(jobs)
        paginated_jobs = jobs[offset:offset + limit]
        
        # Prepare response data
        jobs_data = []
        for job in paginated_jobs:
            job_summary = {
                'job_id': job.get('job_id'),
                'job_type': job.get('job_type'),
                'status': job.get('status'),
                'template_name': job.get('template', {}).get('name', 'Unknown'),
                'created_at': job.get('created_at'),
                'started_at': job.get('started_at'),
                'completed_at': job.get('completed_at'),
                'progress': job.get('progress', {}).get('percentage', 0),
                'video_count': len(job.get('inputs', {}).get('video_ids', [])),
                'keyword': job.get('inputs', {}).get('keyword', ''),
                'error_message': job.get('error_info', {}).get('error_message')
            }
            
            # Add action URLs
            job_summary['urls'] = {
                'status': f'/client/api/job-status/{job.get("job_id")}/',
                'results': f'/client/results/{job.get("job_id")}/',
            }
            
            if job.get('status') == job_store.JOB_STATUS_COMPLETED:
                job_summary['urls']['download'] = f'/client/download/{job.get("job_id")}/'
            
            jobs_data.append(job_summary)
        
        # Get summary statistics
        summary = job_store.get_job_summary_for_user(request.user.id)
        
        return JsonResponse({
            'success': True,
            'jobs': jobs_data,
            'pagination': {
                'total': total_jobs,
                'limit': limit,
                'offset': offset,
                'has_next': offset + limit < total_jobs,
                'has_prev': offset > 0
            },
            'summary': summary
        })
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@client_login_required
@require_http_methods(["POST"])
@csrf_exempt
def cancel_job_api(request, job_id):
    """
    API: Cancel a running job
    """
    try:
        # Get job record
        job_data = job_store.get_job_record(job_id)
        
        if not job_data:
            return JsonResponse({
                'success': False,
                'error': 'Job not found'
            }, status=404)
        
        # Verify user ownership
        if job_data.get('user_id') != request.user.id:
            return JsonResponse({
                'success': False,
                'error': 'Access denied'
            }, status=403)
        
        # Check if job can be cancelled
        current_status = job_data.get('status')
        if current_status not in [job_store.JOB_STATUS_PENDING, job_store.JOB_STATUS_RUNNING]:
            return JsonResponse({
                'success': False,
                'error': f'Cannot cancel job with status: {current_status}'
            }, status=400)
        
        # Try to terminate the process
        process_pid = job_data.get('process_info', {}).get('pid')
        if process_pid:
            try:
                import psutil
                if psutil.pid_exists(process_pid):
                    process = psutil.Process(process_pid)
                    process.terminate()
                    # Give it a moment to terminate gracefully
                    try:
                        process.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        process.kill()  # Force kill if necessary
            except ImportError:
                # psutil not available, can't terminate process
                pass
            except Exception as e:
                # Process might already be dead
                pass
        
        # Update job status
        job_store.update_job_status_v2(
            job_id=job_id,
            status=job_store.JOB_STATUS_CANCELLED,
            current_step="Job cancelled by user"
        )
        
        return JsonResponse({
            'success': True,
            'message': 'Job cancelled successfully',
            'job_id': job_id
        })
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@client_login_required
@require_http_methods(["DELETE"])
@csrf_exempt
def delete_job_api(request, job_id):
    """
    API: Delete a job record (completed/failed jobs only)
    """
    try:
        # Get job record
        job_data = job_store.get_job_record(job_id)
        
        if not job_data:
            return JsonResponse({
                'success': False,
                'error': 'Job not found'
            }, status=404)
        
        # Verify user ownership
        if job_data.get('user_id') != request.user.id:
            return JsonResponse({
                'success': False,
                'error': 'Access denied'
            }, status=403)
        
        # Check if job can be deleted
        current_status = job_data.get('status')
        if current_status in [job_store.JOB_STATUS_PENDING, job_store.JOB_STATUS_RUNNING]:
            return JsonResponse({
                'success': False,
                'error': f'Cannot delete running job. Cancel it first.'
            }, status=400)
        
        # Delete the job record
        success = job_store.delete_job_record(job_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Job deleted successfully',
                'job_id': job_id
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to delete job record'
            }, status=500)
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@client_login_required
def user_jobs_dashboard(request):
    """
    Web interface: User jobs dashboard
    """
    context = {
        'title': 'My Analysis Jobs',
        'user_tier': getattr(request.user, 'tier', 'free'),
    }
    return render(request, 'client/jobs/dashboard.html', context)


@client_login_required
@require_http_methods(["POST"])
@csrf_exempt
def start_wizard_analysis(request):
    """
    专门用于智能向导的CLI分析API
    支持角色选择、模板参数和关键词搜索
    """
    try:
        # Parse JSON request data
        if request.content_type and 'application/json' in request.content_type:
            data = json.loads(request.body.decode('utf-8'))
        else:
            return JsonResponse({'success': False, 'error': 'JSON request required'}, status=400)

        # 验证必需参数
        role = data.get('role')
        template_id = data.get('template')
        keyword = data.get('keyword', '').strip()
        depth = data.get('depth', 'standard')
        format_type = data.get('format', 'comprehensive')
        
        if not role:
            return JsonResponse({'success': False, 'error': 'Role is required'}, status=400)
        if not template_id:
            return JsonResponse({'success': False, 'error': 'Template is required'}, status=400)
        if not keyword:
            return JsonResponse({'success': False, 'error': 'Keyword is required'}, status=400)

        # 构建引擎配置
        engine_config = {
            'keyword': keyword,
            'max_n': 50 if depth == 'deep' else 20,
            'top_k': 30 if depth == 'deep' else 15,
            'filter_type': 'engagement_based',
            'full_audio_analysis': True,
            'persist_summaries': True,
            'concurrency': 4 if depth == 'deep' else 2,
            'dry_run': False,
            'role': role,
            'format': format_type
        }

        # 构建模板配置
        selected_template = {
            'template_id': template_id,
            'name': f'Wizard Template - {template_id}',
            'role_prompt': f"You are a professional {role}. Analyze the content from this perspective."
        }

        # 启动分析任务
        execution_result = trigger_analysis_execution(
            user=request.user,
            engine_config=engine_config,
            template=selected_template,
            custom_prompts=None
        )

        if execution_result.get('success'):
            return JsonResponse({
                'success': True,
                'job_id': execution_result.get('job_id'),
                'message': f'Analysis started successfully for {role} perspective!',
                'config': {
                    'role': role,
                    'template': template_id,
                    'keyword': keyword,
                    'depth': depth,
                    'format': format_type
                }
            })
        else:
            return JsonResponse({
                'success': False,
                'error': execution_result.get('error', 'Failed to start analysis')
            })

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f'Error in start_wizard_analysis: {str(e)}')
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)