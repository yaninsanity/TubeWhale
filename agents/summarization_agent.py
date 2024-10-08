import logging
from openai import AsyncOpenAI
from utils.helper import retry
from dotenv import load_dotenv
import os
import tiktoken
import asyncio
import json

# 加载环境变量
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

# 初始化 AsyncOpenAI 客户端
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

# 按令牌数量对文本进行分块的函数
def chunk_text_by_tokens(text, max_tokens=3000, overlap=200):
    """
    将文本按令牌数量分块，以适应模型的上下文窗口。
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")
    tokens = tokenizer.encode(text)
    chunks = []

    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)
    return chunks

# 带有上下文维护的摘要代理
@async_retry(max_retries=3, delay=2)
async def gpt_summarizer_agent(long_text, *, model="gpt-4"):
    logging.info("Starting summarization agent.")

    # 将文本分割为可管理的块
    chunks = chunk_text_by_tokens(long_text)
    summaries = []
    previous_summary = ""

    # 遍历每个块，保持上下文地进行摘要
    for i, chunk in enumerate(chunks):
        logging.info(f"Summarizing chunk {i + 1}/{len(chunks)}.")

        # 带有上下文的增强提示
        prompt = f"""
        You are an expert content creator whose goal is to produce actionable summaries for guide production.
        Each chunk of text must be summarized with the following in mind:
        - What are the key takeaways and steps that users should know?
        - What insights, tools, or best practices are mentioned?
        - What are the notable challenges and how are they addressed?

        Use the previous summary to maintain context and ensure no important details are missed.

        Previous Summary: {previous_summary}

        Text: {chunk}
        """

        try:
            # 异步调用 OpenAI GPT 模型
            response = await aclient.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt.strip()}],
                max_tokens=1024,
                temperature=0.3
            )

            # 访问响应内容
            if response and response.choices and response.choices[0].message.content:
                chunk_summary = response.choices[0].message.content.strip()
                logging.info(f"Chunk {i + 1} summary: {chunk_summary}")
                summaries.append(chunk_summary)
                previous_summary = chunk_summary  # 为下一个块保存上下文
            else:
                logging.warning(f"Failed to summarize chunk {i + 1}.")
                continue
        except Exception as e:
            logging.error(f"Error summarizing chunk {i + 1}: {e}")
            continue

    # 将所有块的摘要合并为最终摘要
    final_summary = " ".join(summaries)
    logging.info("Summarization completed.")
    return final_summary

# 用于结构化指南输出的标准化代理
@async_retry(max_retries=3, delay=2)
async def standardizer_agent(summary, *, model="gpt-4"):
    if not summary:
        logging.error("Summary is missing. Skipping standardization.")
        return None

    logging.info("Starting standardizer agent.")

    # 用于详细且可操作的指南输出的结构化提示
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
            temperature=0.3
        )

        # 访问响应内容
        if response and response.choices and response.choices[0].message.content:
            standardized_summary_raw = response.choices[0].message.content.strip()

            # 尝试将输出解析为 JSON
            try:
                standardized_summary = json.loads(standardized_summary_raw)
                logging.info("Standardization completed successfully.")

                # 检查所有预期的键是否存在
                required_fields = ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]
                for field in required_fields:
                    if field not in standardized_summary:
                        standardized_summary[field] = "N/A"

                return standardized_summary
            except json.JSONDecodeError:
                logging.error("Failed to parse response as JSON. Returning raw text.")
                return standardized_summary_raw  # 如果解析失败，返回原始文本
        else:
            logging.error("No valid response for standardization.")
            return None

    except Exception as e:
        logging.error(f"Error during standardization: {e}")
        return None

# 转录文本连接函数
def concatenate_transcript(transcript_data):
    concatenated_text = " ".join([segment["text"] for segment in transcript_data])
    total_duration = sum([segment["duration"] for segment in transcript_data])
    return concatenated_text, total_duration
