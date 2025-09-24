"""
CLI Logger for TubeWhale
Sends CLI command logs to Django backend via HTTP API
"""

import json
import os
import time
import requests
from typing import List, Dict, Any, Optional
import logging


class CLILogger:
    """
    Main CLI logger that sends commands to Django backend via API
    Works with both local and containerized Django deployments
    """
    
    def __init__(self, api_base_url: str = None):
        # Auto-detect backend URL with fallback priority
        if api_base_url is None:
            api_base_url = os.environ.get('BACKEND_URL', 'http://localhost:8001')
        
        self.api_base_url = api_base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'TubeWhale-CLI/1.0'
        })
        
        # Test connection
        self.available = self._test_connection()
        
    def _test_connection(self) -> bool:
        """Test if Django backend is available"""
        try:
            response = self.session.get(f"{self.api_base_url}/api/v1/tubewhale/health/", timeout=3)
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"Django backend not available: {e}")
            return False
    
    def log_command(self, 
                   command: str,
                   args: List[str],
                   return_code: int,
                   stdout: str = "",
                   stderr: str = "",
                   duration_ms: Optional[int] = None,
                   meta: Optional[Dict[str, Any]] = None) -> bool:
        """Send CLI command log to Django API"""
        
        if not self.available:
            return False
            
        try:
            payload = {
                'command': command,
                'args': args or [],
                'return_code': return_code,
                'success': (return_code == 0),
                'stdout_truncated': self._truncate(stdout),
                'stderr_truncated': self._truncate(stderr),
                'duration_ms': duration_ms,
                'meta': meta or {},
                'timestamp': time.time()
            }
            
            # Send to command logs endpoint
            response = self.session.post(
                f"{self.api_base_url}/api/v1/tubewhale/command-logs/",
                data=json.dumps(payload),
                timeout=5
            )
            
            if response.status_code in [200, 201]:
                self.logger.debug(f"CLI command logged via API: {command}")
                return True
            else:
                self.logger.warning(f"API log failed: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to log CLI command via API: {e}")
            return False
    
    def _truncate(self, text: str, max_chars: int = 4000) -> str:
        """Truncate text to fit database limits"""
        if not text:
            return ''
        if len(text) > max_chars:
            return text[:max_chars] + f"\n...[truncated {len(text)-max_chars} chars]"
        return text
    
    def get_recent_commands(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent CLI commands from Django API"""
        
        if not self.available:
            return []
            
        try:
            response = self.session.get(
                f"{self.api_base_url}/api/v1/tubewhale/command-logs/?limit={limit}",
                timeout=5
            )
            
            if response.status_code == 200:
                return response.json().get('results', [])
            else:
                self.logger.warning(f"Failed to get recent commands: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to get recent commands: {e}")
            return []


class CommandTracker:
    """Context manager for tracking CLI command execution via API"""
    
    def __init__(self, logger: CLILogger, command: str, args: List[str]):
        self.logger = logger
        self.command = command
        self.args = args
        self.start_time = None
        self.stdout_parts = []
        self.stderr_parts = []
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration_ms = int((time.time() - self.start_time) * 1000)
            
            # Only consider it an error if there's an unhandled exception
            return_code = 1 if (exc_type and exc_type != SystemExit) else 0
            
            stdout = "\n".join(self.stdout_parts)
            stderr = "\n".join(self.stderr_parts)
            
            # Only add exception info for real exceptions (not SystemExit)
            if exc_val and exc_type != SystemExit:
                stderr += f"\nException: {str(exc_val)}"
            
            self.logger.log_command(
                command=self.command,
                args=self.args,
                return_code=return_code,
                stdout=stdout,
                stderr=stderr,
                duration_ms=duration_ms
            )
    
    def add_stdout(self, text: str):
        """Add stdout text to be logged"""
        if text:
            self.stdout_parts.append(text)
        
    def add_stderr(self, text: str):
        """Add stderr text to be logged"""
        if text:
            self.stderr_parts.append(text)


# Global logger instance
logger = CLILogger()


def log_command(command: str, args: List[str], **kwargs) -> bool:
    """Convenience function to log CLI command via API"""
    return logger.log_command(command, args, **kwargs)


def get_cli_tracker(command: str, args: List[str]) -> CommandTracker:
    """Get CLI command tracker context manager for API logging"""
    return CommandTracker(logger, command, args)


class FileLogger:
    """Fallback logger that writes to local JSON file"""
    
    def __init__(self, log_file: str = "cli_commands.log"):
        self.log_file = log_file
        self.logger = logging.getLogger(__name__)
    
    def log_command(self, command: str, args: List[str], **kwargs) -> bool:
        """Log command to file"""
        try:
            log_entry = {
                'timestamp': time.time(),
                'command': command,
                'args': args,
                **kwargs
            }
            
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to log to file: {e}")
            return False


if __name__ == "__main__":
    # Test the API integration
    print("Testing CLI Logger...")
    
    if logger.available:
        print("✅ Django API available")
        
        # Test logging
        success = log_command(
            command="test",
            args=["--api-test"],
            return_code=0,
            stdout="API test successful",
            duration_ms=150
        )
        
        if success:
            print("✅ CLI command logged via API")
        else:
            print("❌ Failed to log CLI command via API")
    else:
        print("❌ Django API not available")
        print("Starting fallback file logging test...")
        
        file_logger = FileLogger()
        success = file_logger.log_command(
            command="test",
            args=["--file-test"],
            return_code=0,
            stdout="File test successful"
        )
        
        if success:
            print("✅ CLI command logged to file")
        else:
            print("❌ Failed to log CLI command to file")