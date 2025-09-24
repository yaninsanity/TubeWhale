"""
🎯 数据库事件监控 Django 管理命令
实时追踪数据库变化并通过WebSocket广播
"""
from django.core.management.base import BaseCommand
from apps.tubewhale_engine.database_event_monitor import DatabaseEventMonitor
import logging
import signal
import sys

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = '启动数据库事件监控服务'
    
    def __init__(self):
        super().__init__()
        self.monitor = None
    
    def handle_signal(self, signum, frame):
        """处理关闭信号"""
        logger.info(f"🛑 收到信号 {signum}，正在关闭数据库监控...")
        if self.monitor:
            self.monitor.stop_monitoring()
        sys.exit(0)
    
    def handle(self, *args, **options):
        # 注册信号处理器
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.stdout.write(
            self.style.SUCCESS('🎯 启动数据库事件监控服务...')
        )
        
        try:
            self.monitor = DatabaseEventMonitor()
            self.monitor.start_monitoring()
            
            # 保持主进程运行
            self.stdout.write(
                self.style.SUCCESS('✅ 数据库监控服务已启动，按 Ctrl+C 退出')
            )
            
            while True:
                import time
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING('⏹️ 监控服务被用户中断')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ 监控服务出错: {e}')
            )
            logger.error(f"数据库监控出错: {e}", exc_info=True)
        finally:
            if self.monitor:
                self.monitor.stop_monitoring()
            self.stdout.write(
                self.style.SUCCESS('✅ 数据库监控服务已停止')
            )