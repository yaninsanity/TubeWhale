#!/usr/bin/env python3
"""
TubeWhale CLI - Lean Industrial Framework
Professional YouTube content analysis with minimal intrusion design
"""

import sys
import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Import lean framework components
from utils.cli_framework import create_cli_framework, ErrorCategory, ErrorSeverity
from utils.database_manager import create_database_manager
from utils.performance_monitor import create_performance_monitor, PerformanceTimer
from utils.youtube_prompt_manager import YouTubePromptManager

# Import existing agents for integration
try:
    from agents.search_agent import SearchAgent
    from agents.transcript_agent import run_transcript
    from agents.summarizer_agent import gpt_summarizer_agent
    from agents.audio_agent import AudioProcessingAgent
    from agents.standardizer_agent import StandardizerAgent
except ImportError as e:
    print(f"Warning: Some agents not available: {e}")

class TubeWhaleCLI:
    """
    Lean CLI interface for TubeWhale with industrial-grade reliability
    Supports both async and sync database operations
    Minimal intrusion design with comprehensive error handling
    """
    
    def __init__(self):
        # Initialize framework components
        self.logger, self.error_handler, self.exception_handler = create_cli_framework()
        self.db_manager, self.db_interface = create_database_manager()
        self.metrics_collector, self.performance_optimizer = create_performance_monitor()
        
        # Initialize async database support
        try:
            from utils.database import Database
            from utils.async_database import AsyncDatabase
            self.sync_db = Database()
            self.async_db = AsyncDatabase(self.sync_db)
            self.async_support = True
            self.logger.logger.info("✅ Async database support enabled")
        except ImportError as e:
            self.async_support = False
            self.logger.logger.warning(f"⚠️ Async database support disabled: {e}")
        
        # Initialize prompt manager
        self.prompt_manager = YouTubePromptManager()
        
        # CLI state
        self.config = self._load_config()
        
        # Log startup
        self.logger.logger.info("🚀 TubeWhale CLI initialized with lean framework")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load CLI configuration with sensible defaults"""
        
        default_config = {
            "output_format": "table",
            "analysis_type": "comprehensive",
            "max_concurrent": 3,
            "cache_enabled": True,
            "verbose": False
        }
        
        config_path = Path("config.json")
        if config_path.exists():
            try:
                with open(config_path) as f:
                    user_config = json.load(f)
                default_config.update(user_config)
            except Exception as e:
                self.logger.logger.warning(f"Failed to load config: {e}")
        
        return default_config
    
    def run_analysis(self, video_id: str, analysis_type: str = "comprehensive") -> Dict[str, Any]:
        """
        Run YouTube video analysis with performance monitoring
        
        Args:
            video_id: YouTube video ID
            analysis_type: Type of analysis (tech, educational, comprehensive)
            
        Returns:
            Analysis results
        """
        
        with PerformanceTimer(self.metrics_collector, f"analyze_{analysis_type}", {"video_id": video_id}):
            try:
                # Get appropriate prompt template
                if analysis_type == "tech":
                    prompt = self.prompt_manager.get_tech_analysis_prompt()
                elif analysis_type == "educational":
                    prompt = self.prompt_manager.get_educational_assessment_prompt()
                else:
                    prompt = self.prompt_manager.get_comprehensive_quality_prompt()
                
                # TODO: Integrate with actual analysis agents
                # For now, return a placeholder with real prompt
                result = {
                    "video_id": video_id,
                    "analysis_type": analysis_type,
                    "prompt_template": prompt[:200] + "..." if len(prompt) > 200 else prompt,
                    "status": "ready_for_agent_integration",
                    "timestamp": str(self.metrics_collector.system_snapshots[-1].timestamp) if self.metrics_collector.system_snapshots else "unknown"
                }
                
                self.logger.logger.info(f"✅ Analysis setup completed for {video_id}")
                return result
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"video_id": video_id, "analysis_type": analysis_type})
                raise Exception(f"Analysis failed: {cli_error.message}")
    
    def show_dashboard(self):
        """Display CLI dashboard with system status and recent activity"""
        
        with PerformanceTimer(self.metrics_collector, "show_dashboard"):
            try:
                # Get system health
                system_health = self.metrics_collector.get_system_health()
                
                # Get database stats
                db_stats = self.db_interface.get_analysis_stats()
                
                # Get performance stats
                perf_stats = self.metrics_collector.get_operation_stats()
                
                # Display dashboard
                print("\n" + "="*60)
                print("🐋 TUBEWHALE DASHBOARD")
                print("="*60)
                
                # System Health
                status_emoji = {"healthy": "✅", "warning": "⚠️", "critical": "❌"}
                print(f"\n📊 SYSTEM STATUS: {status_emoji.get(system_health['status'], '❓')} {system_health['status'].upper()}")
                
                if system_health.get('warnings'):
                    for warning in system_health['warnings']:
                        print(f"   ⚠️ {warning}")
                
                # Database Stats
                print(f"\n📈 DATABASE STATISTICS:")
                print(f"   Total Videos: {db_stats.get('total_videos', 0)}")
                print(f"   Total Analyses: {db_stats.get('total_analyses', 0)}")
                print(f"   Average Score: {db_stats.get('average_score', 0)}")
                
                # Performance Stats
                if perf_stats.get('total_operations', 0) > 0:
                    print(f"\n⚡ PERFORMANCE METRICS:")
                    print(f"   Total Operations: {perf_stats['total_operations']}")
                    print(f"   Success Rate: {perf_stats['success_rate']:.1f}%")
                    print(f"   Average Duration: {perf_stats['duration_stats']['average_ms']:.1f}ms")
                
                print("\n" + "="*60)
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"operation": "dashboard"})
                print(f"❌ Dashboard error: {cli_error.message}")
    
    def show_performance_report(self):
        """Show detailed performance analysis and recommendations"""
        
        with PerformanceTimer(self.metrics_collector, "performance_report"):
            try:
                analysis = self.performance_optimizer.analyze_performance()
                
                print("\n" + "="*60)
                print("📊 PERFORMANCE ANALYSIS REPORT")
                print("="*60)
                
                # Top Operations
                print("\n🔥 TOP OPERATIONS BY DURATION:")
                for i, op in enumerate(analysis['top_operations'][:5], 1):
                    print(f"   {i}. {op['operation']}")
                    print(f"      Average: {op['avg_duration']}ms | Count: {op['count']} | Success: {op['success_rate']}%")
                
                # Recommendations
                if analysis['recommendations']:
                    print("\n💡 OPTIMIZATION RECOMMENDATIONS:")
                    for i, rec in enumerate(analysis['recommendations'], 1):
                        print(f"   {i}. {rec}")
                
                print(f"\n📅 Report generated: {analysis['analysis_timestamp']}")
                print("="*60)
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"operation": "performance_report"})
                print(f"❌ Performance report error: {cli_error.message}")
    
    def list_recent_videos(self, limit: int = 10):
        """List recent video analyses"""
        
        with PerformanceTimer(self.metrics_collector, "list_videos", {"limit": limit}):
            try:
                videos = self.db_interface.get_video_analysis_summary(limit)
                
                if not videos:
                    print("📝 No video analyses found")
                    return
                
                print(f"\n📹 RECENT VIDEO ANALYSES (Last {limit})")
                print("-" * 80)
                
                for video in videos:
                    title = str(video.get('title', 'Unknown'))[:50]
                    score = video.get('score', 'N/A')
                    analysis_type = video.get('analysis_type', 'Unknown')
                    
                    print(f"🎬 {title}")
                    print(f"   Score: {score} | Type: {analysis_type} | Channel: {video.get('channel_name', 'Unknown')}")
                    print(f"   Video ID: {video.get('video_id', 'Unknown')}")
                    print()
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"operation": "list_videos", "limit": limit})
                print(f"❌ Video listing error: {cli_error.message}")
    
    def show_prompts(self):
        """Show available prompt templates"""
        
        with PerformanceTimer(self.metrics_collector, "show_prompts"):
            try:
                print("\n" + "="*60)
                print("📝 AVAILABLE PROMPT TEMPLATES")
                print("="*60)
                
                # Tech Analysis Prompt
                tech_prompt = self.prompt_manager.get_tech_analysis_prompt()
                print("\n🔧 TECHNICAL ANALYSIS PROMPT:")
                print(f"   Length: {len(tech_prompt)} characters")
                print(f"   Preview: {tech_prompt[:150]}...")
                
                # Educational Assessment Prompt
                edu_prompt = self.prompt_manager.get_educational_assessment_prompt()
                print("\n🎓 EDUCATIONAL ASSESSMENT PROMPT:")
                print(f"   Length: {len(edu_prompt)} characters")
                print(f"   Preview: {edu_prompt[:150]}...")
                
                # Comprehensive Quality Prompt
                quality_prompt = self.prompt_manager.get_comprehensive_quality_prompt()
                print("\n⭐ COMPREHENSIVE QUALITY PROMPT:")
                print(f"   Length: {len(quality_prompt)} characters")
                print(f"   Preview: {quality_prompt[:150]}...")
                
                print("\n" + "="*60)
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"operation": "show_prompts"})
                print(f"❌ Prompts display error: {cli_error.message}")
    
    def show_database_info(self):
        """Show database capabilities and dual interface support"""
        
        with PerformanceTimer(self.metrics_collector, "database_info"):
            try:
                print("\n" + "="*60)
                print("🗄️ DATABASE INTERFACE INFORMATION")
                print("="*60)
                
                # Show sync interface info
                print("\n🔄 SYNC INTERFACE (Lean Framework):")
                db_perf = self.db_manager.get_performance_stats()
                print(f"   Connection Pool: Active")
                print(f"   Query Cache: {db_perf.get('cache_entries', 0)} entries")
                print(f"   Total Queries: {db_perf.get('total_queries', 0)}")
                print(f"   Success Rate: {db_perf.get('success_rate', 0):.1f}%")
                
                # Show async interface info
                print(f"\n⚡ ASYNC INTERFACE:")
                if self.async_support:
                    print("   Status: ✅ Available")
                    print("   ThreadPool: Active for async operations")
                    print("   Lean Integration: ✅ Enabled")
                    print("   Backward Compatibility: ✅ Full support")
                else:
                    print("   Status: ❌ Disabled (missing dependencies)")
                
                # Show dual interface capabilities
                print(f"\n🏭 INDUSTRIAL CAPABILITIES:")
                print("   ✅ Connection Pooling")
                print("   ✅ Query Caching with TTL")
                print("   ✅ Performance Monitoring")
                print("   ✅ Error Recovery")
                print("   ✅ Thread-safe Operations")
                print("   ✅ Async/Sync Dual Interface")
                
                print("\n" + "="*60)
                
            except Exception as e:
                cli_error = self.error_handler.handle_error(e, {"operation": "database_info"})
                print(f"❌ Database info error: {cli_error.message}")
    
    def cleanup_and_exit(self):
        """Clean shutdown with resource cleanup"""
        
        try:
            # Show session summary
            session_summary = self.logger.get_session_summary()
            if session_summary["total_errors"] > 0:
                print(f"\n⚠️ Session completed with {session_summary['total_errors']} errors")
            else:
                print("\n✅ Session completed successfully")
            
            # Show final performance stats
            db_perf = self.db_manager.get_performance_stats()
            if db_perf.get("total_queries", 0) > 0:
                print(f"📊 Database: {db_perf['total_queries']} queries, {db_perf['success_rate']:.1f}% success rate")
            
            # Cleanup resources
            self.metrics_collector.stop_monitoring()
            self.db_manager.close()
            
            self.logger.logger.info("🏁 TubeWhale CLI shutdown complete")
            
        except Exception as e:
            print(f"❌ Cleanup error: {e}")

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    
    parser = argparse.ArgumentParser(
        description="TubeWhale - Professional YouTube Content Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py analyze dQw4w9WgXcQ --type tech
  python cli.py dashboard
  python cli.py performance
  python cli.py list --limit 20
  python cli.py prompts
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze YouTube video')
    analyze_parser.add_argument('video_id', help='YouTube video ID')
    analyze_parser.add_argument('--type', choices=['tech', 'educational', 'comprehensive'], 
                               default='comprehensive', help='Analysis type')
    
    # Dashboard command
    subparsers.add_parser('dashboard', help='Show system dashboard')
    
    # Performance command
    subparsers.add_parser('performance', help='Show performance report')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List recent analyses')
    list_parser.add_argument('--limit', type=int, default=10, help='Number of results')
    
    # Prompts command
    subparsers.add_parser('prompts', help='Show available prompt templates')
    
    # Database info command
    subparsers.add_parser('database', help='Show database interface information')
    
    # Global options
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--config', help='Configuration file path')
    
    return parser

def main():
    """Main CLI entry point with global exception handling"""
    
    cli = TubeWhaleCLI()
    
    try:
        with cli.exception_handler:
            parser = create_parser()
            args = parser.parse_args()
            
            if not args.command:
                parser.print_help()
                return
            
            # Enable verbose logging if requested
            if args.verbose:
                import logging
                cli.logger.logger.setLevel(logging.DEBUG)
            
            # Execute commands
            if args.command == 'analyze':
                result = cli.run_analysis(args.video_id, args.type)
                print(json.dumps(result, indent=2))
                
            elif args.command == 'dashboard':
                cli.show_dashboard()
                
            elif args.command == 'performance':
                cli.show_performance_report()
                
            elif args.command == 'list':
                cli.list_recent_videos(args.limit)
            
            elif args.command == 'prompts':
                cli.show_prompts()
            
            elif args.command == 'database':
                cli.show_database_info()
            
    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        cli.cleanup_and_exit()

if __name__ == "__main__":
    main()