"""
Management command to populate TemplateInfo data with sample templates
"""

from django.core.management.base import BaseCommand
from apps.templates_app.models import TemplateInfo


class Command(BaseCommand):
    help = 'Populate TemplateInfo with sample template data'

    def handle(self, *args, **options):
        """Create sample template information entries"""
        
        sample_templates = [
            {
                'template_id': 'youtube_content_analysis',
                'title': 'YouTube Content Analysis',
                'description': 'Comprehensive analysis of YouTube videos including transcription, sentiment analysis, and key insights extraction.',
                'theme': 'Social Media Research',
                'category': 'research',
                'difficulty_level': 'beginner',
                'estimated_time': '15-30 minutes',
                'features': [
                    'Automatic video transcription',
                    'Sentiment analysis',
                    'Key topics extraction',
                    'Engagement metrics analysis',
                    'Export to multiple formats'
                ],
                'use_cases': [
                    'Market research on competitor content',
                    'Academic research on social trends',
                    'Content strategy development',
                    'Brand monitoring and reputation management'
                ],
                'required_tier': 'basic',
                'sort_order': 1
            },
            {
                'template_id': 'advanced_nlp_pipeline',
                'title': 'Advanced NLP Processing Pipeline',
                'description': 'Advanced natural language processing with custom models, entity recognition, and deep semantic analysis.',
                'theme': 'AI & Machine Learning',
                'category': 'research',
                'difficulty_level': 'advanced',
                'estimated_time': '45-90 minutes',
                'features': [
                    'Custom NLP model training',
                    'Named entity recognition',
                    'Topic modeling with LDA',
                    'Semantic similarity analysis',
                    'Custom preprocessing pipelines'
                ],
                'use_cases': [
                    'Academic research papers analysis',
                    'Legal document processing',
                    'Medical literature review',
                    'Scientific publication analysis'
                ],
                'required_tier': 'premium',
                'sort_order': 2
            },
            {
                'template_id': 'content_generation_assistant',
                'title': 'AI Content Generation Assistant',
                'description': 'Generate high-quality content using AI models with customizable styles, tones, and formats.',
                'theme': 'Content Creation',
                'category': 'content',
                'difficulty_level': 'intermediate',
                'estimated_time': '20-45 minutes',
                'features': [
                    'Multiple content formats (blog, social, email)',
                    'Customizable writing styles',
                    'SEO optimization suggestions',
                    'Plagiarism detection',
                    'Multi-language support'
                ],
                'use_cases': [
                    'Blog content creation',
                    'Social media campaigns',
                    'Email marketing content',
                    'Product descriptions'
                ],
                'required_tier': 'standard',
                'sort_order': 3
            },
            {
                'template_id': 'data_visualization_dashboard',
                'title': 'Interactive Data Visualization Dashboard',
                'description': 'Create stunning interactive dashboards and visualizations from your data analysis results.',
                'theme': 'Data Visualization',
                'category': 'business',
                'difficulty_level': 'intermediate',
                'estimated_time': '30-60 minutes',
                'features': [
                    'Interactive charts and graphs',
                    'Real-time data updates',
                    'Custom color themes',
                    'Export to web and PDF',
                    'Responsive design'
                ],
                'use_cases': [
                    'Business intelligence reporting',
                    'Research presentation',
                    'Academic paper visuals',
                    'Client reporting dashboards'
                ],
                'required_tier': 'standard',
                'sort_order': 4
            },
            {
                'template_id': 'automated_report_generator',
                'title': 'Automated Report Generator',
                'description': 'Generate comprehensive reports automatically from your data with customizable templates and scheduling.',
                'theme': 'Business Intelligence',
                'category': 'business',
                'difficulty_level': 'beginner',
                'estimated_time': '10-25 minutes',
                'features': [
                    'Automated data collection',
                    'Customizable report templates',
                    'Scheduled report generation',
                    'Multi-format export (PDF, Excel, Word)',
                    'Email delivery integration'
                ],
                'use_cases': [
                    'Weekly business reports',
                    'Research progress updates',
                    'Performance monitoring',
                    'Compliance reporting'
                ],
                'required_tier': 'basic',
                'sort_order': 5
            },
            {
                'template_id': 'api_integration_workflow',
                'title': 'API Integration Workflow',
                'description': 'Seamlessly integrate with external APIs and automate data workflows across multiple platforms.',
                'theme': 'System Integration',
                'category': 'technical',
                'difficulty_level': 'advanced',
                'estimated_time': '60-120 minutes',
                'features': [
                    'Multiple API connectors',
                    'Data transformation pipelines',
                    'Error handling and retry logic',
                    'Real-time monitoring',
                    'Custom webhook support'
                ],
                'use_cases': [
                    'CRM data synchronization',
                    'Social media data aggregation',
                    'E-commerce analytics',
                    'IoT data processing'
                ],
                'required_tier': 'premium',
                'sort_order': 6
            },
            {
                'template_id': 'custom_ai_chatbot',
                'title': 'Custom AI Chatbot Builder',
                'description': 'Build and deploy custom AI chatbots with advanced conversational capabilities and domain expertise.',
                'theme': 'Artificial Intelligence',
                'category': 'technical',
                'difficulty_level': 'expert',
                'estimated_time': '90-180 minutes',
                'features': [
                    'Custom knowledge base integration',
                    'Multi-turn conversation handling',
                    'Sentiment-aware responses',
                    'Integration with messaging platforms',
                    'Analytics and conversation insights'
                ],
                'use_cases': [
                    'Customer service automation',
                    'Educational tutoring bots',
                    'Research assistant bots',
                    'Domain-specific expertise bots'
                ],
                'required_tier': 'premium',
                'sort_order': 7
            },
            {
                'template_id': 'social_media_monitor',
                'title': 'Social Media Monitoring & Analysis',
                'description': 'Monitor and analyze social media mentions, trends, and sentiment across multiple platforms.',
                'theme': 'Social Media Analytics',
                'category': 'social',
                'difficulty_level': 'intermediate',
                'estimated_time': '25-50 minutes',
                'features': [
                    'Multi-platform monitoring (Twitter, Instagram, Facebook)',
                    'Real-time sentiment analysis',
                    'Trend detection and alerts',
                    'Influencer identification',
                    'Competitive analysis'
                ],
                'use_cases': [
                    'Brand reputation monitoring',
                    'Campaign performance tracking',
                    'Crisis management',
                    'Market trend analysis'
                ],
                'required_tier': 'standard',
                'sort_order': 8
            }
        ]

        created_count = 0
        updated_count = 0

        for template_data in sample_templates:
            template_info, created = TemplateInfo.objects.get_or_create(
                template_id=template_data['template_id'],
                defaults=template_data
            )
            
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f'✅ Created: {template_info.title}')
                )
            else:
                # Update existing template with new data
                for key, value in template_data.items():
                    if key != 'template_id':  # Don't update the ID
                        setattr(template_info, key, value)
                template_info.save()
                updated_count += 1
                self.stdout.write(
                    self.style.WARNING(f'🔄 Updated: {template_info.title}')
                )

        self.stdout.write(
            self.style.SUCCESS(
                f'\n🎉 Successfully processed {len(sample_templates)} templates:'
                f'\n   📝 Created: {created_count}'
                f'\n   🔄 Updated: {updated_count}'
            )
        )