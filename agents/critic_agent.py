# critic_agent.py

import logging
from openai import OpenAI

from datetime import datetime
from utils.database import store_ai_interaction
client = None 
# 主函数：对视频列表进行评价并返回排序后的视频列表
async def critic_agent(videos, api_key, conn=None):
    logging.info("Starting critic agent to rank videos.")
      # 设置 OpenAI API 密钥

    client = OpenAI(api_key=api_key)  # 确保导入 openai
    if not videos:
        logging.error("No videos provided to critic agent.")
        return []

    # 构建用于评价的视频信息列表
    video_descriptions = []
    for idx, video in enumerate(videos):
        description = (
            f"Video {idx+1}:\n"
            f"Title: {video.get('title', 'N/A')}\n"
            f"Description: {video.get('description', 'N/A')}\n"
            f"Views: {video.get('view_count', 0)}\n"
            f"Likes: {video.get('like_count', 0)}\n"
            f"Comments: {video.get('comment_count', 0)}\n"
        )
        video_descriptions.append(description)

    # 准备给助手的提示
    prompt = (
        "You are an expert video content analyst. Based on the following video information, rank the videos from most to least relevant and high-quality for the topic:\n\n"
        + "\n".join(video_descriptions)
        + "\n\nProvide the ranking as a list of video numbers in order."
    )

    try:
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Sending prompt to OpenAI API for critic agent.")

        # 使用同步的 OpenAI API 调用
        response = client.chat.completions.create(model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=150,
        temperature=0.5)

        content = response.choices[0].message.content.strip()
        logging.info(f"Received response from OpenAI API: {content}")

        # 解析排名结果
        ranked_indices = []
        for line in content.splitlines():
            line = line.strip()
            if line.isdigit():
                idx = int(line) - 1
                if 0 <= idx < len(videos):
                    ranked_indices.append(idx)
            elif line.lower().startswith('video') and ':' in line:
                idx_part = line.split(':')[0]
                idx_str = idx_part.strip()[5:].strip()
                if idx_str.isdigit():
                    idx = int(idx_str) - 1
                    if 0 <= idx < len(videos):
                        ranked_indices.append(idx)
            elif '.' in line:
                idx_str = line.split('.')[0]
                if idx_str.isdigit():
                    idx = int(idx_str) - 1
                    if 0 <= idx < len(videos):
                        ranked_indices.append(idx)

        # 移除重复项，保持顺序
        ranked_indices = list(dict.fromkeys(ranked_indices))

        # 根据排名结果重新排序视频列表
        ranked_videos = [videos[idx] for idx in ranked_indices]

        # 如果解析结果为空，则按视图数排序作为备用
        if not ranked_videos:
            logging.warning("Failed to parse ranking from OpenAI response. Falling back to sorting by view count.")
            ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)

        # 记录 AI 交互到数据库（如果需要）
        if conn:
            store_ai_interaction(
                conn,
                prompt,    # 输入
                content,   # 输出
                "critic_agent_ranking",  # 交互类型
                start_time  # 时间戳
            )

        return ranked_videos

    except Exception as e:
        logging.error(f"Error in critic agent: {e}")
        # 在发生错误时，按视图数排序作为备用
        ranked_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
        return ranked_videos
