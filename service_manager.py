#!/usr/bin/env python3
"""
TubeWhale 服务启动和健康监控系统
提供独立的启动状态记录和错误追踪
"""

import os
import sys
import time
import json
import logging
import datetime
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
import docker
from dataclasses import dataclass, asdict

@dataclass
class ServiceStatus:
    """服务状态数据结构"""
    name: str
    status: str  # starting, running, failed, stopped
    start_time: Optional[datetime.datetime] = None
    last_check: Optional[datetime.datetime] = None
    error_message: Optional[str] = None
    health_check_url: Optional[str] = None
    restart_count: int = 0
    pid: Optional[int] = None

class TubeWhaleServiceManager:
    """TubeWhale服务管理器"""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.status_file = self.project_root / "logs" / "service_status.json"
        self.log_file = self.project_root / "logs" / "service_manager.log"
        
        # 确保日志目录存在
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # 服务配置
        self.services = {
            'postgres': {
                'container_name': 'tubewhale_postgres',
                'health_check': self._check_postgres,
                'critical': True
            },
            'redis': {
                'container_name': 'tubewhale_redis',
                'health_check': self._check_redis,
                'critical': True
            },
            'backend': {
                'container_name': 'tubewhale_backend',
                'health_check': self._check_backend,
                'critical': True,
                'health_url': 'http://localhost:8000/admin/'
            },
            'nginx': {
                'container_name': 'tubewhale_nginx',
                'health_check': self._check_nginx,
                'critical': True,
                'health_url': 'http://localhost/'
            },
            'celery_worker': {
                'container_name': 'tubewhale_celery_worker',
                'health_check': self._check_celery_worker,
                'critical': False
            },
            'celery_beat': {
                'container_name': 'tubewhale_celery_beat',
                'health_check': self._check_celery_beat,
                'critical': False
            },
            'websocket': {
                'container_name': 'tubewhale_websocket_full',
                'health_check': self._check_websocket,
                'critical': False,
                'health_url': 'http://localhost:8004/'
            }
        }
        
        # Docker client
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.logger.error(f"Failed to connect to Docker: {e}")
            self.docker_client = None
    
    def save_status(self, statuses: Dict[str, ServiceStatus]):
        """保存服务状态到文件"""
        try:
            status_data = {
                name: {
                    **asdict(status),
                    'start_time': status.start_time.isoformat() if status.start_time else None,
                    'last_check': status.last_check.isoformat() if status.last_check else None
                }
                for name, status in statuses.items()
            }
            
            with open(self.status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save status: {e}")
    
    def load_status(self) -> Dict[str, ServiceStatus]:
        """从文件载入服务状态"""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    data = json.load(f)
                
                statuses = {}
                for name, status_data in data.items():
                    # 转换时间字符串回datetime对象
                    if status_data.get('start_time'):
                        status_data['start_time'] = datetime.datetime.fromisoformat(status_data['start_time'])
                    if status_data.get('last_check'):
                        status_data['last_check'] = datetime.datetime.fromisoformat(status_data['last_check'])
                    
                    statuses[name] = ServiceStatus(**status_data)
                
                return statuses
                
        except Exception as e:
            self.logger.error(f"Failed to load status: {e}")
        
        return {}
    
    def start_services(self) -> Dict[str, ServiceStatus]:
        """启动所有服务并返回状态"""
        self.logger.info("🚀 Starting TubeWhale services...")
        
        statuses = {}
        
        # 初始化所有服务状态
        for service_name in self.services.keys():
            statuses[service_name] = ServiceStatus(
                name=service_name,
                status='starting',
                start_time=datetime.datetime.now()
            )
        
        # 保存初始状态
        self.save_status(statuses)
        
        try:
            # 启动Docker Compose服务
            self.logger.info("Starting Docker Compose services...")
            result = subprocess.run(
                ['docker', 'compose', 'up', '-d'],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5分钟超时
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Docker Compose started successfully")
                
                # 等待服务启动
                time.sleep(10)
                
                # 检查每个服务的健康状态
                statuses = self.check_all_services_health(statuses)
                
            else:
                self.logger.error(f"❌ Docker Compose failed: {result.stderr}")
                for service_name in statuses.keys():
                    statuses[service_name].status = 'failed'
                    statuses[service_name].error_message = result.stderr
                    
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Docker Compose startup timeout")
            for service_name in statuses.keys():
                statuses[service_name].status = 'failed'
                statuses[service_name].error_message = "Startup timeout"
                
        except Exception as e:
            self.logger.error(f"❌ Failed to start services: {e}")
            for service_name in statuses.keys():
                statuses[service_name].status = 'failed'
                statuses[service_name].error_message = str(e)
        
        # 保存最终状态
        self.save_status(statuses)
        return statuses
    
    def check_all_services_health(self, statuses: Dict[str, ServiceStatus]) -> Dict[str, ServiceStatus]:
        """检查所有服务的健康状态"""
        self.logger.info("🔍 Checking service health...")
        
        for service_name, service_config in self.services.items():
            try:
                # 获取Docker容器状态
                if self.docker_client:
                    try:
                        container = self.docker_client.containers.get(service_config['container_name'])
                        container_status = container.status
                        
                        if container_status == 'running':
                            # 运行特定的健康检查
                            health_ok = service_config['health_check']()
                            
                            if health_ok:
                                statuses[service_name].status = 'running'
                                statuses[service_name].error_message = None
                                self.logger.info(f"✅ {service_name}: Healthy")
                            else:
                                statuses[service_name].status = 'failed'
                                statuses[service_name].error_message = "Health check failed"
                                self.logger.warning(f"⚠️ {service_name}: Health check failed")
                        else:
                            statuses[service_name].status = 'failed'
                            statuses[service_name].error_message = f"Container status: {container_status}"
                            self.logger.error(f"❌ {service_name}: Container not running ({container_status})")
                            
                    except docker.errors.NotFound:
                        statuses[service_name].status = 'failed'
                        statuses[service_name].error_message = "Container not found"
                        self.logger.error(f"❌ {service_name}: Container not found")
                        
            except Exception as e:
                statuses[service_name].status = 'failed'
                statuses[service_name].error_message = str(e)
                self.logger.error(f"❌ {service_name}: Health check error - {e}")
            
            # 更新检查时间
            statuses[service_name].last_check = datetime.datetime.now()
        
        return statuses
    
    def _check_postgres(self) -> bool:
        """检查PostgreSQL健康状态"""
        try:
            result = subprocess.run(
                ['docker', 'exec', 'tubewhale_postgres', 'pg_isready', '-U', 'tubewhale'],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except:
            return False
    
    def _check_redis(self) -> bool:
        """检查Redis健康状态"""
        try:
            result = subprocess.run(
                ['docker', 'exec', 'tubewhale_redis', 'redis-cli', '-a', 'redis123', 'ping'],
                capture_output=True,
                timeout=10
            )
            return b'PONG' in result.stdout
        except:
            return False
    
    def _check_backend(self) -> bool:
        """检查Django后端健康状态"""
        try:
            response = requests.get('http://localhost:8000/admin/', timeout=10)
            return response.status_code in [200, 302]  # 302 for login redirect
        except:
            return False
    
    def _check_nginx(self) -> bool:
        """检查Nginx健康状态"""
        try:
            response = requests.get('http://localhost/', timeout=10)
            return response.status_code in [200, 302]
        except:
            return False
    
    def _check_celery_worker(self) -> bool:
        """检查Celery Worker健康状态"""
        try:
            result = subprocess.run(
                ['docker', 'exec', 'tubewhale_celery_worker', 'celery', '-A', 'tubewhale_project', 'inspect', 'ping'],
                capture_output=True,
                timeout=15
            )
            return result.returncode == 0
        except:
            return False
    
    def _check_celery_beat(self) -> bool:
        """检查Celery Beat健康状态"""
        try:
            # 检查进程是否在运行
            result = subprocess.run(
                ['docker', 'exec', 'tubewhale_celery_beat', 'pgrep', '-f', 'celery beat'],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except:
            return False
    
    def _check_websocket(self) -> bool:
        """检查WebSocket服务健康状态"""
        try:
            response = requests.get('http://localhost:8004/', timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def generate_status_report(self) -> str:
        """生成服务状态报告"""
        statuses = self.load_status()
        
        if not statuses:
            return "❌ No service status available"
        
        report = ["🔍 TubeWhale Service Status Report", "=" * 50]
        
        critical_services = []
        optional_services = []
        
        for service_name, status in statuses.items():
            service_config = self.services.get(service_name, {})
            is_critical = service_config.get('critical', False)
            
            status_icon = {
                'running': '✅',
                'starting': '🔄',
                'failed': '❌',
                'stopped': '⏹️'
            }.get(status.status, '❓')
            
            service_info = f"{status_icon} {service_name.upper()}: {status.status}"
            
            if status.error_message:
                service_info += f" ({status.error_message})"
            
            if status.last_check:
                service_info += f" [Last check: {status.last_check.strftime('%H:%M:%S')}]"
            
            if is_critical:
                critical_services.append(service_info)
            else:
                optional_services.append(service_info)
        
        if critical_services:
            report.append("\n🔥 Critical Services:")
            report.extend(critical_services)
        
        if optional_services:
            report.append("\n🔧 Optional Services:")
            report.extend(optional_services)
        
        # 整体健康状态
        all_critical_healthy = all(
            statuses.get(name, ServiceStatus(name, 'failed')).status == 'running'
            for name, config in self.services.items()
            if config.get('critical', False)
        )
        
        report.append(f"\n🏥 Overall Health: {'HEALTHY' if all_critical_healthy else 'UNHEALTHY'}")
        
        return "\n".join(report)
    
    def restart_failed_services(self) -> Dict[str, ServiceStatus]:
        """重启失败的服务"""
        statuses = self.load_status()
        failed_services = [name for name, status in statuses.items() if status.status == 'failed']
        
        if failed_services:
            self.logger.info(f"🔄 Restarting failed services: {failed_services}")
            
            for service_name in failed_services:
                try:
                    service_config = self.services[service_name]
                    container_name = service_config['container_name']
                    
                    # 重启容器
                    subprocess.run(['docker', 'restart', container_name], timeout=60)
                    
                    # 更新状态
                    statuses[service_name].status = 'starting'
                    statuses[service_name].restart_count += 1
                    statuses[service_name].start_time = datetime.datetime.now()
                    
                    time.sleep(5)  # 等待启动
                    
                except Exception as e:
                    self.logger.error(f"Failed to restart {service_name}: {e}")
                    statuses[service_name].error_message = f"Restart failed: {e}"
            
            # 重新检查健康状态
            time.sleep(10)
            statuses = self.check_all_services_health(statuses)
            self.save_status(statuses)
        
        return statuses

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("Usage: python service_manager.py <command> [project_root]")
        print("Commands: start, status, restart, health")
        sys.exit(1)
    
    command = sys.argv[1]
    project_root = sys.argv[2] if len(sys.argv) > 2 else os.getcwd()
    
    manager = TubeWhaleServiceManager(project_root)
    
    if command == 'start':
        print("🚀 Starting TubeWhale services...")
        statuses = manager.start_services()
        print(manager.generate_status_report())
        
    elif command == 'status':
        print(manager.generate_status_report())
        
    elif command == 'restart':
        print("🔄 Restarting failed services...")
        statuses = manager.restart_failed_services()
        print(manager.generate_status_report())
        
    elif command == 'health':
        statuses = manager.load_status()
        if statuses:
            statuses = manager.check_all_services_health(statuses)
            manager.save_status(statuses)
        print(manager.generate_status_report())
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()