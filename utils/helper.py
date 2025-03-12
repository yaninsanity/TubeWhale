import logging
import asyncio
import os
import yaml

def retry(max_retries=3, delay=2, backoff_factor=2):
    """
    重试装饰器，支持指数退避
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
    设置日志格式
    """
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging is set up with level: %s", log_level)

def load_yaml(path: str) -> dict:
    """
    加载 YAML 文件并返回字典
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
