#!/usr/bin/env python3
"""
Enhanced TubeWhale CLI with Industrial-Grade Template Integration
Perfect interaction with dynamic analysis levels and expert roles
"""

import argparse
import requests
import json
import time
import sys
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TubeWhaleCLI:
    """Industrial-grade CLI for TubeWhale video analysis"""
    
    def __init__(self, api_base: str):
        self.api_base = api_base.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'TubeWhale-CLI/2.0'
        })
    
    def get_available_templates(self, role: Optional[str] = None, level: Optional[str] = None) -> Dict:
        """Get available analysis templates with filtering"""
        
        params = {}
        if role:
            params['role'] = role
        if level:
            params['level'] = level
        
        try:
            response = self.session.get(f"{self.api_base}/api/v1/templates/available/", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get templates: {e}")
            return {"error": str(e)}
    
    def get_expert_roles(self) -> Dict:
        """Get available expert roles with details"""
        
        try:
            response = self.session.get(f"{self.api_base}/api/v1/roles/available/")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get roles: {e}")
            return {"error": str(e)}
    
    def validate_configuration(self, template_id: str, expert_role: str) -> Dict:
        """Validate template and role combination"""
        
        try:
            response = self.session.post(
                f"{self.api_base}/api/v1/analysis/validate/",
                json={
                    'template_id': template_id,
                    'expert_role': expert_role
                }
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to validate configuration: {e}")
            return {"error": str(e)}
    
    def start_analysis(self, video_id: str, expert_role: str, template_id: str) -> Dict:
        """Start video analysis with enhanced error handling"""
        
        # First validate configuration
        validation = self.validate_configuration(template_id, expert_role)
        if not validation.get('valid', False):
            return {"error": f"Invalid configuration: {validation.get('error', 'Unknown error')}"}
        
        # Display processing estimate
        estimate = validation.get('processing_estimate', {})
        if estimate:
            logger.info(f"🕐 Estimated processing time: {estimate.get('estimated_time_human', 'Unknown')}")
            logger.info(f"📊 Analysis complexity: {estimate.get('complexity_level', 'Unknown')}")
        
        try:
            response = self.session.post(
                f"{self.api_base}/api/v1/analysis/video/",
                json={
                    'video_id': video_id,
                    'expert_role': expert_role,
                    'template': template_id
                }
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to start analysis: {e}")
            return {"error": str(e)}
    
    def get_job_details(self, job_id: str) -> Dict:
        """Get comprehensive job details with visualization data"""
        
        try:
            response = self.session.get(f"{self.api_base}/api/v1/jobs/{job_id}/")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get job details: {e}")
            return {"error": str(e)}
    
    def get_analysis_dashboard(self, job_id: str) -> Dict:
        """Get dashboard data for completed analysis"""
        
        try:
            response = self.session.get(f"{self.api_base}/api/v1/jobs/{job_id}/dashboard/")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return {"error": str(e)}
    
    def list_jobs(self, status: Optional[str] = None, page: int = 1, per_page: int = 10) -> Dict:
        """List analysis jobs with filtering and pagination"""
        
        params = {'page': page, 'per_page': per_page}
        if status:
            params['status'] = status
        
        try:
            response = self.session.get(f"{self.api_base}/api/v1/jobs/", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to list jobs: {e}")
            return {"error": str(e)}
    
    def wait_for_completion(self, job_id: str, timeout: int = 600) -> Dict:
        """Wait for analysis completion with progress updates"""
        
        start_time = time.time()
        last_progress = -1
        
        while time.time() - start_time < timeout:
            job_details = self.get_job_details(job_id)
            
            if "error" in job_details:
                return job_details
            
            job_info = job_details.get('job', {})
            status = job_info.get('status')
            progress = job_info.get('progress', 0)
            status_message = job_info.get('status_message', '')
            
            # Show progress updates
            if progress != last_progress:
                logger.info(f"⏳ Progress: {progress}% - {status_message}")
                last_progress = progress
            
            if status == 'completed':
                logger.info("✅ Analysis completed successfully!")
                return job_details
            elif status == 'failed':
                error_msg = job_info.get('error_message', 'Unknown error')
                logger.error(f"❌ Analysis failed: {error_msg}")
                return {"error": f"Analysis failed: {error_msg}"}
            
            time.sleep(2)
        
        return {"error": "Analysis timed out"}
    
    def display_results(self, job_details: Dict, format_type: str = 'summary'):
        """Display analysis results in various formats"""
        
        if "error" in job_details:
            print(f"Error: {job_details['error']}")
            return
        
        job_info = job_details.get('job', {})
        youtube_info = job_details.get('youtube', {})
        template_info = job_details.get('template', {})
        results = job_details.get('results', {})
        
        print("\n" + "="*80)
        print("🎬 TUBEWHALE ANALYSIS RESULTS")
        print("="*80)
        
        # Job Information
        print(f"📊 Job ID: {job_info.get('job_id')}")
        print(f"🎥 Video: {youtube_info.get('video_url')}")
        print(f"👤 Expert Role: {job_info.get('expert_role', '').replace('_', ' ').title()}")
        print(f"📋 Template: {template_info.get('name', 'Unknown')} ({template_info.get('level', 'unknown')} level)")
        print(f"⏱️  Completed: {job_info.get('completed_at', 'Unknown')}")
        
        if results and 'error' not in results:
            print(f"🎯 Confidence Score: {results.get('confidence_score', 0):.2f}")
            print(f"📈 Completeness: {results.get('completeness_score', 0):.2f}")
            
            # Summary
            if format_type in ['summary', 'full']:
                print("\n📝 EXECUTIVE SUMMARY:")
                print("-" * 40)
                print(results.get('summary', 'No summary available'))
            
            # Metrics
            if format_type in ['metrics', 'full']:
                metrics = results.get('metrics', {})
                if metrics:
                    print("\n📊 PERFORMANCE METRICS:")
                    print("-" * 40)
                    for key, value in metrics.items():
                        print(f"  {key.replace('_', ' ').title()}: {value}")
            
            # Recommendations
            if format_type in ['recommendations', 'full']:
                recommendations = results.get('raw_data', {}).get('ai_response', {}).get('recommendations', [])
                if recommendations:
                    print("\n💡 RECOMMENDATIONS:")
                    print("-" * 40)
                    for i, rec in enumerate(recommendations, 1):
                        print(f"  {i}. {rec}")
            
            # Download Links
            download_links = results.get('download_links', {})
            if download_links:
                print("\n📥 DOWNLOAD OPTIONS:")
                print("-" * 40)
                for format_name, link in download_links.items():
                    print(f"  {format_name.upper()}: {self.api_base}{link}")
        
        print("="*80)


def create_parser():
    """Create enhanced argument parser"""
    
    parser = argparse.ArgumentParser(
        description="TubeWhale CLI - Industrial-grade video analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s templates --role content_creator
  %(prog)s analyze dQw4w9WgXcQ --role marketing_expert --template detailed_analysis
  %(prog)s job 12345abc --dashboard
  %(prog)s jobs --status completed --page 1
        """
    )
    
    parser.add_argument('--api-base', required=True, help='API base URL')
    parser.add_argument('--timeout', type=int, default=600, help='Analysis timeout in seconds')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Templates command
    templates_parser = subparsers.add_parser('templates', help='List available templates')
    templates_parser.add_argument('--role', help='Filter by expert role')
    templates_parser.add_argument('--level', choices=['basic', 'detailed', 'expert', 'comprehensive'], help='Filter by analysis level')
    
    # Roles command
    roles_parser = subparsers.add_parser('roles', help='List available expert roles')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze a video')
    analyze_parser.add_argument('video_id', help='YouTube video ID')
    analyze_parser.add_argument('--role', required=True, help='Expert role for analysis')
    analyze_parser.add_argument('--template', required=True, help='Analysis template ID')
    analyze_parser.add_argument('--format', choices=['summary', 'full', 'metrics', 'recommendations'], default='summary', help='Output format')
    analyze_parser.add_argument('--no-wait', action='store_true', help='Don\'t wait for completion')
    
    # Job command
    job_parser = subparsers.add_parser('job', help='Get job details')
    job_parser.add_argument('job_id', help='Analysis job ID')
    job_parser.add_argument('--dashboard', action='store_true', help='Show dashboard data')
    job_parser.add_argument('--format', choices=['summary', 'full', 'metrics', 'recommendations'], default='summary', help='Output format')
    
    # Jobs command
    jobs_parser = subparsers.add_parser('jobs', help='List analysis jobs')
    jobs_parser.add_argument('--status', choices=['pending', 'processing', 'completed', 'failed'], help='Filter by status')
    jobs_parser.add_argument('--page', type=int, default=1, help='Page number')
    jobs_parser.add_argument('--per-page', type=int, default=10, help='Jobs per page')
    
    return parser


def main():
    """Main CLI entry point"""
    
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli = TubeWhaleCLI(args.api_base)
    
    if args.command == 'templates':
        result = cli.get_available_templates(args.role, args.level)
        if "error" in result:
            print(f"Error: {result['error']}")
            return
        
        templates = result.get('templates', [])
        print(f"\n📋 Available Templates ({len(templates)} found):")
        print("="*60)
        
        for template in templates:
            print(f"ID: {template['id']}")
            print(f"Name: {template['name']}")
            print(f"Level: {template['level'].upper()}")
            print(f"Time: {template['estimated_time']}")
            print(f"Compatible Roles: {', '.join(template['compatible_roles'])}")
            print("-" * 40)
    
    elif args.command == 'roles':
        result = cli.get_expert_roles()
        if "error" in result:
            print(f"Error: {result['error']}")
            return
        
        roles = result.get('roles', [])
        print(f"\n👤 Available Expert Roles ({len(roles)} found):")
        print("="*60)
        
        for role in roles:
            print(f"ID: {role['id']}")
            print(f"Name: {role['name']}") 
            print(f"Specialization: {role['specialization']}")
            print(f"Focus Areas: {', '.join(role['focus_areas'])}")
            print("-" * 40)
    
    elif args.command == 'analyze':
        print(f"🎬 Starting analysis for video: {args.video_id}")
        print(f"👤 Expert Role: {args.role}")
        print(f"📋 Template: {args.template}")
        
        result = cli.start_analysis(args.video_id, args.role, args.template)
        if "error" in result:
            print(f"Error: {result['error']}")
            return
        
        job_id = result.get('job_id')
        print(f"🚀 Analysis started with job ID: {job_id}")
        
        if not args.no_wait:
            job_details = cli.wait_for_completion(job_id, args.timeout)
            cli.display_results(job_details, args.format)
        else:
            print(f"Use 'job {job_id}' to check status later")
    
    elif args.command == 'job':
        if args.dashboard:
            result = cli.get_analysis_dashboard(args.job_id)
            if "error" in result:
                print(f"Error: {result['error']}")
                return
            
            print(f"\n📊 Dashboard Data for Job: {args.job_id}")
            print("="*60)
            print(json.dumps(result, indent=2))
        else:
            result = cli.get_job_details(args.job_id)
            cli.display_results(result, args.format)
    
    elif args.command == 'jobs':
        result = cli.list_jobs(args.status, args.page, args.per_page)
        if "error" in result:
            print(f"Error: {result['error']}")
            return
        
        jobs = result.get('jobs', [])
        pagination = result.get('pagination', {})
        
        print(f"\n📋 Analysis Jobs (Page {pagination.get('current_page', 1)} of {pagination.get('total_pages', 1)}):")
        print("="*80)
        
        for job in jobs:
            status_emoji = {
                'completed': '✅',
                'processing': '⏳', 
                'pending': '⏸️',
                'failed': '❌'
            }.get(job['status'], '❓')
            
            print(f"{status_emoji} {job['job_id']} - {job['video_id']} ({job['status']})")
            print(f"   Role: {job['expert_role']} | Template: {job['template_id']}")
            print(f"   Created: {job['created_at']} | YouTube: {job['video_url']}")
            if job.get('summary'):
                print(f"   Summary: {job['summary']}")
            print("-" * 60)


if __name__ == '__main__':
    main()