from django.core.management.base import BaseCommand
from django.utils import timezone

class Command(BaseCommand):
    help = "Check runtime system metrics availability (psutil) and print a sample snapshot."

    def handle(self, *args, **options):
        self.stdout.write(f"[check_system_metrics] {timezone.now().isoformat()}")
        try:
            import psutil  # type: ignore
            cpu = psutil.cpu_percent(interval=0.1)
            vm = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            self.stdout.write("psutil: AVAILABLE")
            self.stdout.write(f"CPU%={cpu}")
            self.stdout.write(f"MEM%={vm.percent} USED_MB={round(vm.used/1024/1024,1)} TOTAL_MB={round(vm.total/1024/1024,1)}")
            self.stdout.write(f"DISK%={disk.percent} USED_GB={round(disk.used/1024/1024/1024,2)} TOTAL_GB={round(disk.total/1024/1024/1024,2)}")
        except Exception as e:
            self.stdout.write("psutil: MISSING")
            self.stdout.write(f"Reason: {e}")
            try:
                import os
                load1, load5, load15 = os.getloadavg()
                self.stdout.write(f"LoadAvg: {load1:.2f} {load5:.2f} {load15:.2f}")
            except Exception:
                self.stdout.write("LoadAvg: unavailable")
            self.stdout.write("Rebuild Docker image ensuring psutil is installed (present in requirements.txt and django_requirements.txt).")
