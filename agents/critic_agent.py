import logging
import openai  # 确保已正确导入 openai
from datetime import datetime
from utils.database import store_ai_interaction
import json

# 主函数：对视频列表进行评价并返回排序后的视频列表
async def critic_agent(videos, api_key, conn=None):
    logging.info("Starting critic agent to rank videos.")
    openai.api_key = api_key  # 设置 OpenAI API 密钥

    if not videos:
        logging.error("No videos provided to critic agent.")
        return videos  # 返回原始视频列表

    # 构建用于评价的视频信息列表
    video_descriptions = []
    for idx, video in enumerate(videos):
        description = (
            f"Video {idx+1}:\n"
            f"Video ID: {video.get('video_id', 'N/A')}\n"
            f"Title: {video.get('title', 'N/A')}\n"
            f"Description: {video.get('description', 'N/A')}\n"
            f"Views: {video.get('view_count', 0)}\n"
            f"Likes: {video.get('like_count', 0)}\n"
            f"Comments: {video.get('comment_count', 0)}\n"
        )
        video_descriptions.append(description)

    # 优化后的 Prompt
    prompt = (
        "You are an expert video content analyst. Based on the following video information, rank all the videos from most to least relevant and high-quality for the topic of 'Virginia fishing'. "
        "Please provide the ranking as a numbered list, including each video's unique ID. "
        "For example:\n"
        "1. Video ID: ABC123\n"
        "2. Video ID: DEF456\n"
        "...\n\n"
        + "\n".join(video_descriptions)
    )

    try:
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Sending prompt to OpenAI API for critic agent.")

        # 使用正确的模型名称
        response = openai.ChatCompletion.create(
            model="gpt-4",  # 修正模型名称
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,  # 增加 max_tokens 以确保响应完整
            temperature=0.5
        )

        content = response.choices[0].message.content.strip()
        logging.info(f"Received response from OpenAI API: {content}")

        # 解析排名结果
        ranked_video_ids = []
        for line in content.splitlines():
            line = line.strip()
            if line and "Video ID:" in line:
                parts = line.split("Video ID:")
                if len(parts) == 2:
                    video_id = parts[1].strip()
                    ranked_video_ids.append(video_id)

        logging.info(f"Parsed ranked_video_ids: {ranked_video_ids}")

        # 创建视频ID到视频对象的映射
        video_dict = {video['video_id']: video for video in videos}

        # 根据排名结果排序
        ranked_videos = []
        for vid in ranked_video_ids:
            if vid in video_dict:
                ranked_videos.append(video_dict[vid])

        # 记录 AI 交互到数据库（如果需要）
        if conn:
            store_ai_interaction(
                conn,
                prompt,    # 输入
                content,   # 输出
                "critic_agent_ranking",  # 交互类型
                start_time  # 时间戳
            )

        # 如果解析结果为空，则按视图数排序作为备用
        if not ranked_videos:
            logging.warning("Failed to parse ranking from OpenAI response. Falling back to sorting by view count.")
            ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)

        return ranked_videos

    except Exception as e:
        logging.error(f"Error in critic agent: {e}")
        logging.exception(e)  # 记录完整的堆栈信息
        # 在发生错误时，按视图数排序作为备用
        ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
        return ranked_videos
