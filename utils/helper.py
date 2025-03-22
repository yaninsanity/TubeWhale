import os
import asyncio
import yaml
import logging
from datetime import datetime

def retry(max_retries=3, delay=2, backoff_factor=2):
    """
    重试装饰器，支持指数退避。
    如果被装饰的异步函数连续失败，则等待一段时间后重试，最多重试 max_retries 次。
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries} in {_delay} seconds...")
                    await asyncio.sleep(_delay)
                    _delay *= backoff_factor
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

def setup_logging(log_level=logging.INFO):
    """
    设置日志格式。
    为确保每次调用都重新配置日志（便于单元测试捕获），先清空 root logger 的所有 handlers，
    然后添加一个新的 StreamHandler，并立即刷新所有 handler。
    """
    root_logger = logging.getLogger()
    # 清除已有的所有 handler，但保留 caplog 的处理器
    for handler in root_logger.handlers[:]:
        if not isinstance(handler, type(logging.StreamHandler())):
            root_logger.removeHandler(handler)
    # 创建一个新的 StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
    root_logger.setLevel(log_level)
    root_logger.info("Logging is set up with level: %s", log_level)
    for handler in root_logger.handlers:
        handler.flush()
        
def load_yaml(path: str) -> dict:
    """
    加载 YAML 文件并返回字典，如果文件不存在或加载错误则返回空字典。
    """
    if not os.path.exists(path):
        logging.error(f"YAML file not found at: {path}")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            logging.info(f"Loaded YAML file from {path}")
            return data
    except Exception as e:
        logging.error(f"Error loading YAML file: {e}")
        return {}

def print_startup_banner():
    """
    打印启动横幅，将横幅内容写入日志（INFO 级别）。
    横幅中包含关键字 "Yaninsanity" 以便测试检测。
    """
    banner = r"""
Author Github: @Yaninsanity 
Follow me: https://github.com/yaninsanity/
About me: https://www.jl-blog.com/about/
Produced by Eclipzion Tech Squad 2025®
88888888888       888               888       888 888    888        d8888 888      8888888888 
    888           888               888   o   888 888    888       d88888 888      888        
    888           888               888  d8b  888 888    888      d88P888 888      888        
    888  888  888 88888b.   .d88b.  888 d888b 888 8888888888     d88P 888 888      8888888    
    888  888  888 888 "88b d8P  Y8b 888d88888b888 888    888    d88P  888 888      888        
    888  888  888 888  888 88888888 88888P Y88888 888    888   d88P   888 888      888        
    888  Y88b 888 888 d88P Y8b.     8888P   Y8888 888    888  d8888888888 888      888        
    888   "Y88888 88888P"   "Y8888  888P     Y888 888    888 d88P     888 88888888 8888888888             
"""
    logging.info(banner)
    for handler in logging.getLogger().handlers:
        handler.flush()
