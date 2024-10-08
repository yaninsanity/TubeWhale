import logging
from openai import AsyncOpenAI
from utils.helper import retry
import json
import os
import asyncio

# 初始化 AsyncOpenAI 客户端
api_key = os.getenv("OPENAI_API_KEY")
aclient = AsyncOpenAI(api_key=api_key)

# 异步重试装饰器
def async_retry(max_retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
            raise Exception(f"All {max_retries} attempts failed.")
        return wrapper
    return decorator

# Standardizer agent for structured guide-like output
@async_retry(max_retries=3, delay=2)
async def standardizer_agent(summary,  model="gpt-4"):
    if not summary:
        logging.error("Summary is missing. Skipping standardization.")
        return None

    logging.info("Starting standardizer agent.")

    # Structured prompt for detailed and actionable guide output
    standardization_prompt = f"""
    You are an expert at organizing and structuring content. 
    Your job is to take the following summary and standardize it into an actionable guide format.
    Focus on:
    - Main topic of the video
    - Key insights or steps users should follow
    - Recommended tools or techniques (if applicable)
    - Best practices and tips shared
    - Notable challenges or advice

    Provide the standardized summary in the following JSON format:
    {{
        "main_topic": "...",
        "key_insights": "...",
        "recommended_tools": "...",
        "best_practices": "...",
        "challenges_and_advice": "..."
    }}

    Summary to standardize: {summary}
    """

    try:
        # 异步调用 OpenAI GPT 模型
        response = await aclient.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": standardization_prompt.strip()}],
            max_tokens=1024,
            temperature=0.3  # Lowered for more deterministic output
        )
        
        # 处理响应
        if response and hasattr(response, 'choices') and response.choices and \
           hasattr(response.choices[0], 'message') and hasattr(response.choices[0].message, 'content'):
            standardized_summary_raw = response.choices[0].message.content.strip()
            
            # 尝试将输出解析为 JSON
            try:
                standardized_summary = json.loads(standardized_summary_raw)
                logging.info("Standardization completed successfully.")
                
                # 检查所有预期的键是否存在
                required_fields = ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]
                for field in required_fields:
                    if field not in standardized_summary:
                        standardized_summary[field] = "N/A"  # 如果字段缺失，设置为 "N/A"
                        
                return standardized_summary
            except json.JSONDecodeError:
                logging.error("Failed to parse response as JSON. Returning raw text.")
                return standardized_summary_raw  # 如果解析失败，返回原始文本
        else:
            logging.error("No valid response for standardization.")
            return None

    except Exception as e:
        logging.error(f"Error in standardization: {e}")
        return None
