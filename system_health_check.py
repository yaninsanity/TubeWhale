#!/usr/bin/env python3
"""
TubeWhale System Health Check & Status Monitor
==============================================

Professional system health monitoring and status validation tool.
Provides comprehensive system diagnostics and performance monitoring.

Usage:
    python system_health_check.py [options]
    
Options:
    --verbose    Enable detailed logging
    --fix        Attempt to fix detected issues
    --export     Export health report
    --monitor    Continuous monitoring mode

Author: TubeWhale Development Team
Version: 1.0.0
"""

import os
import sys
import json
import time
import logging
import datetime
import subprocess
import argparse
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('tubewhale-health')

@dataclass
class HealthCheckResult:
    """Health check result data structure"""
    component: str
    status: str  # 'healthy', 'warning', 'error'
    message: str
    details: Dict[str, Any] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
        if self.details is None:
            self.details = {}

class TubeWhaleHealthMonitor:
    """
    Professional system health monitoring and diagnostics
    """
    
    def __init__(self, verbose: bool = False, auto_fix: bool = False):
        self.verbose = verbose
        self.auto_fix = auto_fix
        self.project_root = Path(__file__).parent
        self.health_results: List[HealthCheckResult] = []
        
        if verbose:
            logger.setLevel(logging.DEBUG)
            
        logger.info("🏥 TubeWhale Health Monitor initialized")
    
    def run_comprehensive_health_check(self) -> Dict[str, Any]:
        """
        Execute comprehensive system health check
        """
        logger.info("🔍 Starting comprehensive health check...")
        
        # Clear previous results
        self.health_results.clear()
        
        # Execute health checks
        checks = [
            ("Docker Services", self._check_docker_services),
            ("Database Connectivity", self._check_database),
            ("CLI Integration", self._check_cli_system),
            ("Template System", self._check_template_system),
            ("Job Management", self._check_job_management),
            ("File System", self._check_file_system),
            ("API Endpoints", self._check_api_endpoints),
            ("System Resources", self._check_system_resources),
            ("Security Configuration", self._check_security),
            ("Performance Metrics", self._check_performance)
        ]
        
        for check_name, check_function in checks:
            logger.info(f"🔍 Checking {check_name}...")
            try:
                check_function()
                logger.info(f"✅ {check_name} - OK")
            except Exception as e:
                logger.error(f"❌ {check_name} - Error: {str(e)}")
                self.health_results.append(HealthCheckResult(
                    component=check_name,
                    status='error',
                    message=f"Health check failed: {str(e)}"
                ))
        
        # Generate summary
        summary = self._generate_health_summary()
        logger.info(f"🏥 Health check completed. Overall status: {summary['overall_status']}")
        
        return summary
    
    def _check_docker_services(self):
        """Check Docker services status"""
        try:
            # Check if Docker is running
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise Exception("Docker not available")
            
            # Check docker-compose services
            result = subprocess.run(['docker-compose', 'ps'], 
                                  capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                services_output = result.stdout
                if 'tubewhale-backend' in services_output and 'Up' in services_output:
                    self.health_results.append(HealthCheckResult(
                        component="Docker Services",
                        status='healthy',
                        message="Docker services running normally",
                        details={"services": services_output.strip()}
                    ))
                else:
                    self.health_results.append(HealthCheckResult(
                        component="Docker Services",
                        status='warning',
                        message="Some services may not be running",
                        details={"services": services_output.strip()}
                    ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="Docker Services", 
                    status='warning',
                    message="Could not check docker-compose services"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Docker Services",
                status='error', 
                message=f"Docker check failed: {str(e)}"
            ))
    
    def _check_database(self):
        """Check database connectivity and health"""
        try:
            # Check if database files exist
            db_files = list(self.project_root.glob("*.db"))
            sqlite_files = list(self.project_root.glob("*.sqlite*"))
            
            if db_files or sqlite_files:
                self.health_results.append(HealthCheckResult(
                    component="Database Connectivity",
                    status='healthy',
                    message="Database files found and accessible",
                    details={
                        "db_files": [str(f) for f in db_files],
                        "sqlite_files": [str(f) for f in sqlite_files]
                    }
                ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="Database Connectivity",
                    status='warning',
                    message="No database files found"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Database Connectivity",
                status='error',
                message=f"Database check failed: {str(e)}"
            ))
    
    def _check_cli_system(self):
        """Check CLI system integration"""
        try:
            main_py = self.project_root / 'main.py'
            
            if main_py.exists():
                # Test CLI execution
                result = subprocess.run([sys.executable, str(main_py), '--help'],
                                      capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    self.health_results.append(HealthCheckResult(
                        component="CLI Integration",
                        status='healthy',
                        message="CLI system operational",
                        details={"help_output": result.stdout[:200] + "..."}
                    ))
                else:
                    self.health_results.append(HealthCheckResult(
                        component="CLI Integration",
                        status='error',
                        message="CLI execution failed",
                        details={"error": result.stderr}
                    ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="CLI Integration",
                    status='error',
                    message="main.py not found"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="CLI Integration",
                status='error',
                message=f"CLI check failed: {str(e)}"
            ))
    
    def _check_template_system(self):
        """Check template system availability"""
        try:
            template_file = self.project_root / 'utils' / 'template_system.py'
            
            if template_file.exists():
                # Test template import
                try:
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("template_system", template_file)
                    template_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(template_module)
                    
                    # Check if TemplateManager exists
                    if hasattr(template_module, 'TemplateManager'):
                        self.health_results.append(HealthCheckResult(
                            component="Template System",
                            status='healthy',
                            message="Template system available and functional"
                        ))
                    else:
                        self.health_results.append(HealthCheckResult(
                            component="Template System",
                            status='warning',
                            message="Template system file exists but TemplateManager not found"
                        ))
                        
                except Exception as import_error:
                    self.health_results.append(HealthCheckResult(
                        component="Template System",
                        status='error',
                        message=f"Template system import failed: {str(import_error)}"
                    ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="Template System",
                    status='error',
                    message="Template system file not found"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Template System",
                status='error',
                message=f"Template check failed: {str(e)}"
            ))
    
    def _check_job_management(self):
        """Check job management system"""
        try:
            job_manager_file = self.project_root / 'utils' / 'enhanced_job_manager.py'
            outputs_dir = self.project_root / 'outputs'
            jobs_dir = outputs_dir / 'jobs'
            
            status_messages = []
            overall_status = 'healthy'
            
            # Check job manager file
            if job_manager_file.exists():
                status_messages.append("Job manager module available")
            else:
                status_messages.append("Job manager module missing")
                overall_status = 'error'
            
            # Check outputs directory
            if outputs_dir.exists() and outputs_dir.is_dir():
                status_messages.append("Outputs directory exists")
                
                # Check jobs directory
                if jobs_dir.exists() and jobs_dir.is_dir():
                    job_files = list(jobs_dir.glob('*.json'))
                    status_messages.append(f"Jobs directory exists with {len(job_files)} job records")
                else:
                    status_messages.append("Jobs directory missing")
                    overall_status = 'warning' if overall_status == 'healthy' else overall_status
            else:
                status_messages.append("Outputs directory missing")
                overall_status = 'warning' if overall_status == 'healthy' else overall_status
            
            self.health_results.append(HealthCheckResult(
                component="Job Management",
                status=overall_status,
                message="; ".join(status_messages)
            ))
            
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Job Management",
                status='error',
                message=f"Job management check failed: {str(e)}"
            ))
    
    def _check_file_system(self):
        """Check file system permissions and structure"""
        try:
            issues = []
            critical_paths = [
                self.project_root / 'outputs',
                self.project_root / 'templates',
                self.project_root / 'apps',
                self.project_root / 'utils'
            ]
            
            for path in critical_paths:
                if not path.exists():
                    issues.append(f"Missing: {path}")
                elif not os.access(path, os.R_OK):
                    issues.append(f"No read access: {path}")
                elif path.name == 'outputs' and not os.access(path, os.W_OK):
                    issues.append(f"No write access: {path}")
            
            if not issues:
                self.health_results.append(HealthCheckResult(
                    component="File System",
                    status='healthy',
                    message="File system permissions and structure OK"
                ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="File System", 
                    status='warning' if len(issues) < 3 else 'error',
                    message=f"File system issues: {'; '.join(issues)}"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="File System",
                status='error',
                message=f"File system check failed: {str(e)}"
            ))
    
    def _check_api_endpoints(self):
        """Check API endpoint availability"""
        try:
            import requests
            
            endpoints_to_check = [
                ('http://localhost:8001/health/', 'Health endpoint'),
                ('http://localhost:8001/admin/', 'Admin interface'),
                ('http://localhost:8001/client/', 'Client interface')
            ]
            
            healthy_endpoints = 0
            total_endpoints = len(endpoints_to_check)
            endpoint_details = {}
            
            for url, description in endpoints_to_check:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code < 500:  # Accept any non-server-error status
                        healthy_endpoints += 1
                        endpoint_details[description] = f"OK ({response.status_code})"
                    else:
                        endpoint_details[description] = f"Server error ({response.status_code})"
                except requests.RequestException as e:
                    endpoint_details[description] = f"Connection failed: {str(e)}"
            
            if healthy_endpoints == total_endpoints:
                status = 'healthy'
                message = "All API endpoints accessible"
            elif healthy_endpoints > 0:
                status = 'warning'
                message = f"{healthy_endpoints}/{total_endpoints} endpoints accessible"
            else:
                status = 'error'
                message = "No API endpoints accessible"
            
            self.health_results.append(HealthCheckResult(
                component="API Endpoints",
                status=status,
                message=message,
                details=endpoint_details
            ))
            
        except ImportError:
            self.health_results.append(HealthCheckResult(
                component="API Endpoints",
                status='warning',
                message="Cannot check endpoints - requests library not available"
            ))
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="API Endpoints",
                status='error',
                message=f"API check failed: {str(e)}"
            ))
    
    def _check_system_resources(self):
        """Check system resource availability"""
        try:
            # Check disk space
            disk_usage = os.statvfs(self.project_root)
            free_space_gb = (disk_usage.f_bavail * disk_usage.f_frsize) / (1024**3)
            
            resource_status = []
            overall_status = 'healthy'
            
            if free_space_gb < 1:
                resource_status.append(f"Low disk space: {free_space_gb:.1f}GB")
                overall_status = 'error'
            elif free_space_gb < 5:
                resource_status.append(f"Disk space warning: {free_space_gb:.1f}GB")
                overall_status = 'warning' if overall_status == 'healthy' else overall_status
            else:
                resource_status.append(f"Disk space OK: {free_space_gb:.1f}GB")
            
            # Try to get memory info
            try:
                import psutil
                memory = psutil.virtual_memory()
                if memory.available < 1024**3:  # Less than 1GB available
                    resource_status.append(f"Low memory: {memory.available/(1024**3):.1f}GB")
                    overall_status = 'warning' if overall_status == 'healthy' else overall_status
                else:
                    resource_status.append(f"Memory OK: {memory.available/(1024**3):.1f}GB available")
            except ImportError:
                resource_status.append("Memory check unavailable (psutil not installed)")
            
            self.health_results.append(HealthCheckResult(
                component="System Resources",
                status=overall_status,
                message="; ".join(resource_status)
            ))
            
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="System Resources",
                status='error',
                message=f"Resource check failed: {str(e)}"
            ))
    
    def _check_security(self):
        """Check security configuration"""
        try:
            security_issues = []
            
            # Check for .env files
            env_files = list(self.project_root.glob('**/.env*'))
            if env_files:
                for env_file in env_files:
                    if oct(env_file.stat().st_mode)[-3:] != '600':
                        security_issues.append(f"Insecure permissions on {env_file}")
            
            # Check for secret key configuration
            if 'SECRET_KEY' not in os.environ:
                security_issues.append("SECRET_KEY environment variable not set")
            
            # Check for debug mode in production
            if os.environ.get('DEBUG', '').lower() in ['true', '1', 'on']:
                security_issues.append("DEBUG mode enabled - not recommended for production")
            
            if not security_issues:
                self.health_results.append(HealthCheckResult(
                    component="Security Configuration",
                    status='healthy',
                    message="Security configuration appears secure"
                ))
            else:
                self.health_results.append(HealthCheckResult(
                    component="Security Configuration",
                    status='warning',
                    message=f"Security issues: {'; '.join(security_issues)}"
                ))
                
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Security Configuration",
                status='error',
                message=f"Security check failed: {str(e)}"
            ))
    
    def _check_performance(self):
        """Check system performance metrics"""
        try:
            # Simple performance test - file I/O
            test_file = self.project_root / 'outputs' / '.health_check_test'
            start_time = time.time()
            
            # Write test
            test_data = "Performance test data" * 1000
            test_file.write_text(test_data)
            
            # Read test
            read_data = test_file.read_text()
            
            # Cleanup
            test_file.unlink()
            
            io_time = time.time() - start_time
            
            if io_time < 0.1:
                status = 'healthy'
                message = f"Performance OK - I/O test: {io_time:.3f}s"
            elif io_time < 0.5:
                status = 'warning'
                message = f"Performance warning - I/O test: {io_time:.3f}s"
            else:
                status = 'error'
                message = f"Performance issue - I/O test: {io_time:.3f}s"
            
            self.health_results.append(HealthCheckResult(
                component="Performance Metrics",
                status=status,
                message=message
            ))
            
        except Exception as e:
            self.health_results.append(HealthCheckResult(
                component="Performance Metrics",
                status='error',
                message=f"Performance check failed: {str(e)}"
            ))
    
    def _generate_health_summary(self) -> Dict[str, Any]:
        """Generate comprehensive health summary"""
        healthy_count = sum(1 for r in self.health_results if r.status == 'healthy')
        warning_count = sum(1 for r in self.health_results if r.status == 'warning')
        error_count = sum(1 for r in self.health_results if r.status == 'error')
        total_checks = len(self.health_results)
        
        if error_count > 0:
            overall_status = 'critical'
        elif warning_count > total_checks // 2:
            overall_status = 'degraded'
        elif warning_count > 0:
            overall_status = 'warning'
        else:
            overall_status = 'healthy'
        
        return {
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'overall_status': overall_status,
            'summary': {
                'total_checks': total_checks,
                'healthy': healthy_count,
                'warnings': warning_count,
                'errors': error_count,
                'health_score': (healthy_count / total_checks * 100) if total_checks > 0 else 0
            },
            'detailed_results': [
                {
                    'component': r.component,
                    'status': r.status,
                    'message': r.message,
                    'details': r.details,
                    'timestamp': r.timestamp
                }
                for r in self.health_results
            ],
            'recommendations': self._generate_recommendations()
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on health check results"""
        recommendations = []
        
        for result in self.health_results:
            if result.status == 'error':
                if result.component == "Docker Services":
                    recommendations.append("Start Docker services with: docker-compose up -d")
                elif result.component == "CLI Integration":
                    recommendations.append("Check CLI dependencies and permissions")
                elif result.component == "Template System":
                    recommendations.append("Verify template system installation and Django configuration")
                elif result.component == "File System":
                    recommendations.append("Check file permissions and create missing directories")
                elif result.component == "API Endpoints":
                    recommendations.append("Start Django development server or check service status")
                elif result.component == "System Resources":
                    recommendations.append("Free up disk space or memory resources")
                    
            elif result.status == 'warning':
                if result.component == "Security Configuration":
                    recommendations.append("Review security settings and environment variables")
                elif result.component == "Performance Metrics":
                    recommendations.append("Consider system optimization or resource upgrade")
        
        if not recommendations:
            recommendations.append("System appears healthy - no immediate action required")
        
        return recommendations
    
    def export_health_report(self, filename: str = None) -> str:
        """Export health report to JSON file"""
        if filename is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"tubewhale_health_report_{timestamp}.json"
        
        summary = self._generate_health_summary()
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 Health report exported to: {filename}")
        return filename
    
    def continuous_monitoring(self, interval_minutes: int = 5):
        """Run continuous health monitoring"""
        logger.info(f"🔄 Starting continuous monitoring (interval: {interval_minutes} minutes)")
        
        try:
            while True:
                self.run_comprehensive_health_check()
                time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            logger.info("⏹️ Monitoring stopped by user")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="TubeWhale System Health Monitor")
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--fix', action='store_true', help='Attempt to fix issues')
    parser.add_argument('--export', help='Export report to file')
    parser.add_argument('--monitor', type=int, metavar='MINUTES', 
                       help='Continuous monitoring interval in minutes')
    
    args = parser.parse_args()
    
    # Initialize health monitor
    monitor = TubeWhaleHealthMonitor(verbose=args.verbose, auto_fix=args.fix)
    
    if args.monitor:
        monitor.continuous_monitoring(args.monitor)
    else:
        # Run health check
        summary = monitor.run_comprehensive_health_check()
        
        # Print summary
        print(f"\n🏥 TubeWhale System Health Report")
        print(f"{'='*50}")
        print(f"Overall Status: {summary['overall_status'].upper()}")
        print(f"Health Score: {summary['summary']['health_score']:.1f}%")
        print(f"Checks: {summary['summary']['healthy']}✅ {summary['summary']['warnings']}⚠️ {summary['summary']['errors']}❌")
        
        # Print detailed results
        print(f"\nDetailed Results:")
        print(f"{'-'*50}")
        for result in summary['detailed_results']:
            status_icon = {'healthy': '✅', 'warning': '⚠️', 'error': '❌'}.get(result['status'], '❓')
            print(f"{status_icon} {result['component']}: {result['message']}")
        
        # Print recommendations
        if summary['recommendations']:
            print(f"\nRecommendations:")
            print(f"{'-'*50}")
            for rec in summary['recommendations']:
                print(f"💡 {rec}")
        
        # Export if requested
        if args.export:
            monitor.export_health_report(args.export)
        
        # Exit with appropriate code
        if summary['overall_status'] in ['critical', 'error']:
            sys.exit(1)
        elif summary['overall_status'] in ['degraded', 'warning']:
            sys.exit(2)
        else:
            sys.exit(0)

if __name__ == "__main__":
    main()