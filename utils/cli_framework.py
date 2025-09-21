#!/usr/bin/env python3
"""
Industrial CLI Framework
Lean best practices for error handling, logging, and user experience
"""

import logging
import sys
import traceback
import json
import time
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from enum import Enum

class ErrorSeverity(Enum):
    """Error severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Error categories for precise classification"""
    NETWORK = "network"
    API = "api"
    VALIDATION = "validation"
    FILESYSTEM = "filesystem"
    DATABASE = "database"
    CONFIGURATION = "configuration"
    USER_INPUT = "user_input"
    SYSTEM = "system"

@dataclass
class CLIError:
    """Structured error representation"""
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    details: str
    user_action: str
    timestamp: str
    context: Dict[str, Any]

class CLILogger:
    """Professional CLI logging system"""
    
    def __init__(self, log_dir: str = "logs", app_name: str = "tubewhale"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.app_name = app_name
        
        # Setup loggers
        self._setup_logger()
        
        # Error tracking
        self.session_errors: List[CLIError] = []
        
    def _setup_logger(self):
        """Setup application logger with proper formatting"""
        
        self.logger = logging.getLogger(self.app_name)
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # File handler
        file_handler = logging.FileHandler(self.log_dir / f"{self.app_name}.log")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)8s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def log_error(self, cli_error: CLIError):
        """Log structured error"""
        self.session_errors.append(cli_error)
        
        # Log based on severity
        if cli_error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"💥 CRITICAL: {cli_error.message}")
        elif cli_error.severity == ErrorSeverity.ERROR:
            self.logger.error(f"❌ ERROR: {cli_error.message}")
        elif cli_error.severity == ErrorSeverity.WARNING:
            self.logger.warning(f"⚠️ WARNING: {cli_error.message}")
        else:
            self.logger.info(f"ℹ️ INFO: {cli_error.message}")
    
    def log_performance(self, operation: str, duration_ms: float, metadata: Dict[str, Any] = None):
        """Log performance metrics"""
        self.logger.info(f"⏱️ {operation}: {duration_ms:.2f}ms", extra={
            "operation": operation,
            "duration_ms": duration_ms,
            "metadata": metadata or {}
        })
    
    def get_session_summary(self) -> Dict[str, Any]:
        """Get session error summary"""
        return {
            "total_errors": len(self.session_errors),
            "by_severity": {
                severity.value: len([e for e in self.session_errors if e.severity == severity])
                for severity in ErrorSeverity
            },
            "by_category": {
                category.value: len([e for e in self.session_errors if e.category == category])
                for category in ErrorCategory
            }
        }

class ErrorHandler:
    """Intelligent error handling with recovery strategies"""
    
    def __init__(self, logger: CLILogger):
        self.logger = logger
        
    def handle_error(self, exception: Exception, context: Dict[str, Any] = None) -> CLIError:
        """Handle error with intelligent classification"""
        
        # Classify error
        category, severity = self._classify_error(exception)
        
        # Create structured error
        cli_error = CLIError(
            category=category,
            severity=severity,
            message=self._get_user_message(exception, category),
            details=str(exception),
            user_action=self._get_user_action(category),
            timestamp=datetime.now().isoformat(),
            context=context or {}
        )
        
        # Log the error
        self.logger.log_error(cli_error)
        
        return cli_error
    
    def _classify_error(self, exception: Exception) -> tuple[ErrorCategory, ErrorSeverity]:
        """Classify error by type"""
        
        error_type = type(exception).__name__
        
        # Network errors
        if any(net_error in error_type for net_error in ['ConnectionError', 'Timeout', 'URLError', 'HTTPError']):
            return ErrorCategory.NETWORK, ErrorSeverity.WARNING
        
        # API errors
        if any(api_error in error_type for api_error in ['APIError', 'RateLimitError', 'AuthenticationError']):
            return ErrorCategory.API, ErrorSeverity.ERROR
        
        # Validation errors
        if any(val_error in error_type for val_error in ['ValueError', 'ValidationError', 'TypeError']):
            return ErrorCategory.VALIDATION, ErrorSeverity.WARNING
        
        # Filesystem errors
        if any(fs_error in error_type for fs_error in ['FileNotFoundError', 'PermissionError', 'OSError']):
            return ErrorCategory.FILESYSTEM, ErrorSeverity.WARNING
        
        # Database errors
        if any(db_error in error_type for db_error in ['DatabaseError', 'IntegrityError', 'OperationalError']):
            return ErrorCategory.DATABASE, ErrorSeverity.ERROR
        
        # Default to system error
        return ErrorCategory.SYSTEM, ErrorSeverity.ERROR
    
    def _get_user_message(self, exception: Exception, category: ErrorCategory) -> str:
        """Generate user-friendly error message"""
        
        messages = {
            ErrorCategory.NETWORK: "Network connection issue. Please check your internet connection.",
            ErrorCategory.API: "API service temporarily unavailable. Please try again later.",
            ErrorCategory.VALIDATION: "Invalid input provided. Please check your parameters.",
            ErrorCategory.FILESYSTEM: "File system error. Please check file permissions.",
            ErrorCategory.DATABASE: "Database operation failed. Please try again.",
            ErrorCategory.CONFIGURATION: "Configuration error. Please check your settings.",
            ErrorCategory.USER_INPUT: "Invalid user input. Please review your command.",
            ErrorCategory.SYSTEM: "System error occurred. Please contact support if this persists."
        }
        
        return messages.get(category, "An unexpected error occurred.")
    
    def _get_user_action(self, category: ErrorCategory) -> str:
        """Generate recommended user action"""
        
        actions = {
            ErrorCategory.NETWORK: "Check your internet connection and try again.",
            ErrorCategory.API: "Wait a few minutes and retry. Check API key if issue persists.",
            ErrorCategory.VALIDATION: "Review your input parameters and correct any errors.",
            ErrorCategory.FILESYSTEM: "Check file paths and permissions.",
            ErrorCategory.DATABASE: "Ensure database is accessible and retry operation.",
            ErrorCategory.CONFIGURATION: "Review configuration file and environment variables.",
            ErrorCategory.USER_INPUT: "Check command syntax and parameter values.",
            ErrorCategory.SYSTEM: "Restart the application. Contact support if error persists."
        }
        
        return actions.get(category, "Try again or contact support.")

class ExceptionHandler:
    """Global exception handler for CLI application"""
    
    def __init__(self, logger: CLILogger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        
    def __enter__(self):
        """Setup exception handling context"""
        sys.excepthook = self.handle_exception
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup exception handling"""
        sys.excepthook = sys.__excepthook__
        
        # Log session summary
        summary = self.logger.get_session_summary()
        if summary["total_errors"] > 0:
            self.logger.logger.info(f"📊 Session Summary: {summary['total_errors']} errors occurred")
    
    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """Handle uncaught exceptions"""
        if issubclass(exc_type, KeyboardInterrupt):
            self.logger.logger.info("🛑 User interrupted execution")
            sys.exit(0)
        
        # Handle the error
        cli_error = self.error_handler.handle_error(exc_value, {
            "exception_type": exc_type.__name__,
            "traceback": traceback.format_exception(exc_type, exc_value, exc_traceback)
        })
        
        # Display user-friendly error message
        print(f"\n❌ {cli_error.message}")
        print(f"💡 {cli_error.user_action}")
        
        if cli_error.severity in [ErrorSeverity.ERROR, ErrorSeverity.CRITICAL]:
            print(f"🔍 Check logs for details: logs/{self.logger.app_name}.log")
        
        sys.exit(1)

def create_cli_framework(log_dir: str = "logs") -> tuple[CLILogger, ErrorHandler, ExceptionHandler]:
    """Factory function to create CLI framework components"""
    
    logger = CLILogger(log_dir)
    error_handler = ErrorHandler(logger)
    exception_handler = ExceptionHandler(logger, error_handler)
    
    return logger, error_handler, exception_handler