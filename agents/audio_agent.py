import os
import logging
import asyncio
import random  # 添加缺失的导入
from openai import AsyncOpenAI  # 请确保您使用的库支持异步操作

from yt_dlp import YoutubeDL
from pydub import AudioSegment
from io import BytesIO
import json
from dotenv import load_dotenv
import sys
import aiohttp
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 加载环境变量
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

# 验证 OpenAI API 密钥
if not openai_api_key:
    logging.error("OpenAI API key not found. Please set it in your environment variables.")
    sys.exit(1)

# 初始化 OpenAI 客户端
aclient = AsyncOpenAI(api_key=openai_api_key)

# 重试装饰器
def retry(max_retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logging.error(f"Error in {func.__name__}: {e}. Exceeded maximum retries.")
                        raise
                    else:
                        logging.warning(f"Error in {func.__name__}: {e}. Retrying {attempt}/{max_retries} after {delay} seconds...")
                        await asyncio.sleep(delay)
        return wrapper
    return decorator

# 下载 YouTube 视频音频
@retry(max_retries=3, delay=5)
async def download_audio(video_id):
    try:
        os.makedirs('downloads', exist_ok=True)
        audio_path = f'downloads/{video_id}.mp3'
        if os.path.exists(audio_path):
            logging.info(f"Audio file {audio_path} already exists. Skipping download.")
            return audio_path

        logging.info(f"Downloading audio for video ID: {video_id}")

        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'downloads/{video_id}.%(ext)s',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'quiet': True,
            'no_warnings': True,
        }

        def download():
            with YoutubeDL(ydl_opts) as ydl:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                ydl.download([video_url])

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, download)

        if os.path.exists(audio_path):
            logging.info(f"Audio downloaded successfully for video ID {video_id}.")
            return audio_path
        else:
            logging.error(f"Audio file {audio_path} not found after download.")
            return None

    except Exception as e:
        logging.error(f"Failed to download audio for video ID {video_id}: {e}")
        return None

# 分割音频
def split_audio(audio_path, max_duration_ms=60000):
    try:
        logging.info(f"Splitting audio {audio_path} into chunks of {max_duration_ms} ms.")
        audio = AudioSegment.from_file(audio_path)
        chunks = [audio[i:i + max_duration_ms] for i in range(0, len(audio), max_duration_ms)]
        logging.info(f"Audio split into {len(chunks)} chunks.")
        return chunks
    except Exception as e:
        logging.error(f"Failed to split audio {audio_path}: {e}")
        return []

# 转录音频块
@retry(max_retries=3, delay=5)
async def transcribe_audio_chunk(audio_chunk):
    try:
        # 将 AudioSegment 转换为字节
        audio_file = BytesIO()
        audio_chunk.export(audio_file, format="mp3")
        audio_file.seek(0)  # 重置文件指针

        # 使用 OpenAI Whisper API 进行转录
        logging.info("Transcribing audio chunk using OpenAI Whisper API.")

        url = "https://api.openai.com/v1/audio/transcriptions"

        headers = {
            "Authorization": f"Bearer {openai_api_key}",
        }

        # 添加随机延迟以模拟人类互动
        await asyncio.sleep(random.uniform(0.5, 2))

        form_data = aiohttp.FormData()
        form_data.add_field('file',
                            audio_file,
                            filename='audio_chunk.mp3',
                            content_type='audio/mpeg')
        form_data.add_field('model', 'whisper-1')
        form_data.add_field('response_format', 'text')

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=form_data) as resp:
                if resp.status == 200:
                    transcript_text = await resp.text()
                    logging.info("Transcription completed for audio chunk.")
                    return transcript_text
                else:
                    error_text = await resp.text()
                    logging.error(f"Failed to transcribe audio chunk with OpenAI: {error_text}")
                    return None
    except Exception as e:
        logging.error(f"Failed to transcribe audio chunk with OpenAI: {e}")
        return None

# 使用 OpenAI GPT-4 进行文本总结
@retry(max_retries=3, delay=5)
async def summarize_text(transcript_text, previous_summary, topic, metadata):
    try:
        # 定义系统提示和用户消息
        messages = [
            {"role": "system", "content": (
                f"You are an expert content creator whose goal is to produce actionable summaries for guide production.\n"
                f"Each chunk of text must be summarized with the following in mind:\n"
                f"- What are the key takeaways and steps that users should know?\n"
                f"- What insights, tools, or best practices are mentioned?\n"
                f"- What are the notable challenges and how are they addressed?\n"
                f"Now analyze this YouTube video content with this metadata: {json.dumps(metadata)}.\n"
                f"Focus on the topic: {topic}\n"
                f"Use the previous summary to maintain context and ensure no important details are missed."
            )},
            {"role": "user", "content": f"Previous Summary:\n{previous_summary}\n\nNew Transcript:\n{transcript_text}"}
        ]

        logging.info("Generating summary using OpenAI ChatCompletion.")
        response = await aclient.chat.completions.create(
            model="gpt-4",  # 修正后的模型名称
            messages=messages,
            max_tokens=1024,
            temperature=0.5
        )

        summary = response.choices[0].message.content.strip()
        logging.info("Summary generated for transcript chunk.")
        return summary

    except Exception as e:
        logging.error(f"Failed to summarize text with OpenAI: {e}")
        return None

# 递归总结多个摘要
async def recursive_summarize(summaries, topic, metadata):
    try:
        while len(summaries) > 1:
            new_summaries = []
            for i in range(0, len(summaries), 2):
                summaries_to_summarize = summaries[i:i+2]
                combined_summary = "\n\n".join(summaries_to_summarize)
                summary = await summarize_text(combined_summary, "", topic, metadata)
                if summary:
                    new_summaries.append(summary)
                else:
                    logging.error("Failed to generate recursive summary.")
            summaries = new_summaries
        if summaries:
            return summaries[0]
        else:
            logging.error("No summaries to combine.")
            return None
    except Exception as e:
        logging.error(f"Failed during recursive summarization: {e}")
        return None

# 标准化最终摘要
@retry(max_retries=3, delay=2)
async def standardize_summary(summary):
    if not summary:
        logging.error("Summary is missing. Skipping standardization.")
        return None

    logging.info("Starting standardizer agent.")

    # 标准化提示，确保 JSON 格式
    standardization_prompt = f"""
    You are an expert at organizing and structuring content.
    Your job is to take the following summary and standardize it into an actionable guide format.
    Ensure that the response is in valid JSON format.
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
        logging.info("Standardizing summary using OpenAI ChatCompletion.")
        response = await aclient.chat.completions.create(
            model="gpt-4",  # 修正后的模型名称
            messages=[{"role": "user", "content": standardization_prompt.strip()}],
            max_tokens=1024,
            temperature=0.3
        )

        standardized_summary_raw = response.choices[0].message.content.strip()

        # 尝试从响应中提取 JSON
        try:
            # 使用正则表达式查找 JSON 块
            json_match = re.search(r'\{.*\}', standardized_summary_raw, re.DOTALL)
            if json_match:
                standardized_summary_json = json_match.group(0)
                standardized_summary = json.loads(standardized_summary_json)
                logging.info("Standardization completed successfully.")

                # 确保所有预期的键都存在
                required_fields = ["main_topic", "key_insights", "recommended_tools", "best_practices", "challenges_and_advice"]
                for field in required_fields:
                    if field not in standardized_summary:
                        standardized_summary[field] = "N/A"

                return standardized_summary
            else:
                logging.error("No JSON found in the response. Returning raw text.")
                return standardized_summary_raw  # 如果未找到 JSON，返回原始文本

        except json.JSONDecodeError as json_err:
            logging.error(f"JSON decoding failed: {json_err}. Returning raw text.")
            return standardized_summary_raw  # 如果解析失败，返回原始文本

    except Exception as e:
        logging.error(f"Error during standardization: {e}")
        return None

# 处理音频并生成标准化摘要
async def transcribe_audio_to_summary(video_id, topic, metadata=None):
    try:
        # 步骤 0：检查元数据是否存在
        if metadata is None:
            logging.error(f"Metadata is missing for video ID: {video_id}. Skipping processing.")
            return None

        # 步骤 1：下载音频文件
        audio_path = await download_audio(video_id)
        if not audio_path or not os.path.exists(audio_path):
            logging.error(f"Audio download failed for video ID: {video_id}")
            return None

        # 步骤 2：分割音频
        audio_chunks = split_audio(audio_path, max_duration_ms=60000)  # 可以根据需要调整 max_duration_ms
        if not audio_chunks:
            logging.error(f"Failed to split audio for video ID: {video_id}")
            return None

        # 步骤 3：转录每个音频块并总结
        chunk_summaries = []
        previous_summary = ""
        for idx, chunk in enumerate(audio_chunks):
            logging.info(f"Processing audio chunk {idx + 1}/{len(audio_chunks)}")

            # 转录音频块
            transcript = await transcribe_audio_chunk(chunk)
            if not transcript:
                logging.error(f"Failed to transcribe audio chunk {idx + 1}")
                continue

            # 使用前一个摘要作为上下文进行总结
            summary = await summarize_text(transcript, previous_summary, topic, metadata)
            if summary:
                chunk_summaries.append(summary)
                previous_summary = summary  # 更新前一个摘要以保持上下文
            else:
                logging.error(f"Failed to summarize audio chunk {idx + 1}")

            # 添加随机延迟以模拟人类互动
            await asyncio.sleep(random.uniform(0.5, 2))

        if not chunk_summaries:
            logging.error(f"No summaries generated for video ID: {video_id}")
            return None

        # 步骤 4：递归总结所有摘要以获得最终摘要
        logging.info("Combining chunk summaries into final summary.")
        final_summary = await recursive_summarize(chunk_summaries, topic, metadata)
        if not final_summary:
            logging.error(f"Failed to generate final summary for video ID: {video_id}.")
            return None

        # 步骤 5：标准化最终摘要
        standardized_summary = await standardize_summary(final_summary)
        if not standardized_summary:
            logging.error(f"Failed to standardize summary for video ID: {video_id}.")
            return None

        # 可选：清理下载的音频文件
        # Uncomment the following lines if you want to remove the audio file after processing
        # if audio_path and os.path.exists(audio_path):
        #     os.remove(audio_path)
        #     logging.info(f"Removed audio file {audio_path} after processing.")

        return standardized_summary

    except Exception as e:
        logging.error(f"Failed to process video {video_id}: {e}")
        return None

# 主函数，用于处理单个视频
if __name__ == "__main__":
    # 从命令行参数获取视频 ID、主题和元数据
    if len(sys.argv) < 4:
        print("Usage: python audio_agent.py <video_id> <topic> <metadata_json>")
        print("Example: python audio_agent.py s1JZ5zCl1A0 'Virginia fishing tips' '{\"title\": \"How to Fish\", \"description\": \"...\"}'")
        sys.exit(1)

    video_id = sys.argv[1]
    topic = sys.argv[2]
    metadata_json = sys.argv[3]

    # 解析元数据 JSON
    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError:
        logging.error("Invalid metadata JSON provided.")
        sys.exit(1)  # 添加退出，以避免后续错误

    # 运行主函数
    async def main():
        logging.info("Starting the video processing script...")
        result = await transcribe_audio_to_summary(video_id, topic, metadata)
        if result:
            print("Standardized Summary:")
            print(json.dumps(result, indent=4, ensure_ascii=False))
        else:
            print("Failed to process the video.")

    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script terminated due to an unexpected error: {e}")
