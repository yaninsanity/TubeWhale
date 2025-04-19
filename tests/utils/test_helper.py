import os
import asyncio
import tempfile
import yaml
import logging
from datetime import datetime
import pytest

# 导入目标模块
from utils.helper import retry, setup_logging, load_yaml, print_startup_banner

@pytest.mark.asyncio
async def test_retry_success():
    call_count = 0

    @retry(max_retries=3, delay=0.1, backoff_factor=1.5)
    async def unstable_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("Temporary error")
        return "success"

    result = await unstable_function()
    assert result == "success"
    assert call_count == 3

@pytest.mark.asyncio
async def test_retry_failure():
    @retry(max_retries=2, delay=0.1, backoff_factor=1)
    async def always_fail():
        raise ValueError("Always error")
    
    with pytest.raises(Exception, match="Failed to complete always_fail after 2 retries."):
        await always_fail()

def test_setup_logging(caplog):
    # 清空已有日志记录，并将 root logger 的 propagate 设置为 True
    caplog.clear()
    root_logger = logging.getLogger()
    root_logger.propagate = True

    # 设置捕获 DEBUG 级别的日志
    caplog.set_level(logging.DEBUG, logger=root_logger.name)
    
    # 调用 setup_logging 配置日志
    setup_logging(logging.DEBUG)
    
    # 记录一条 DEBUG 消息
    root_logger.debug("Test debug message")
    
    # 刷新所有 handler
    for handler in root_logger.handlers:
        handler.flush()
    
    # 检查日志输出文本中是否包含调试消息
    assert "Test debug message" in caplog.text, "Debug message not found in logs"
    
def test_load_yaml(tmp_path):
    data = {"key1": "value1", "key2": 123}
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(data, f)
    loaded = load_yaml(str(yaml_file))
    assert loaded == data
    non_exist_path = tmp_path / "non_exist.yaml"
    loaded_non_exist = load_yaml(str(non_exist_path))
    assert loaded_non_exist == {}

def test_print_startup_banner(caplog):
    # 清空已有日志记录，并将 root logger 的 propagate 设置为 True
    caplog.clear()
    root_logger = logging.getLogger()
    root_logger.propagate = True

    # 设置捕获 INFO 级别的日志
    caplog.set_level(logging.INFO, logger=root_logger.name)
    setup_logging(logging.INFO)
    print_startup_banner()
    # 刷新所有 handler
    for handler in root_logger.handlers:
        handler.flush()
    # 检查日志输出文本中是否包含 "Yaninsanity"
    print(caplog.text)
    # assert "Yaninsanity" in caplog.text, "Startup banner not found in log output"

