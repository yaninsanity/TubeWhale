import logging
from langchain_openai import OpenAI

# Ensure metrics like view_count and like_count are integers, or default to 0 if None
def ensure_metric_is_int(metric):
    return int(metric) if metric is not None else 0

# Evaluate keyword's performance based on video metrics
def evaluate_keyword(keyword, videos):
    logging.info(f"Evaluating keyword: {keyword}, found {len(videos)} videos.")
    total_views = sum(video.get('view_count', 0) for video in videos)
    total_likes = sum(video.get('like_count', 0) for video in videos)

    # 如果 critique 需要由模型生成，确保在此处添加
    critique = f"Critique for {keyword}: {total_views} views, {total_likes} likes."

    logging.info(f"Total views for keyword '{keyword}': {total_views}")
    logging.info(f"Total likes for keyword '{keyword}': {total_likes}")

    return {
        'keyword': keyword,
        'total_views': total_views,
        'total_likes': total_likes,
        'critique': critique  # 确保返回了 critique 字段
    }

async def critic_agent(search_results, api_key):
    logging.info("Starting critic agent to rank topics.")
    llm = OpenAI(api_key=api_key)

    rankings = []

    for keyword, videos in search_results.items():
        evaluation = evaluate_keyword(keyword, videos)
        rankings.append(evaluation)

    rankings.sort(key=lambda x: (x['total_views'], x['total_likes']), reverse=True)

    best_keyword = rankings[0]['keyword']
    logging.info(f"Best keyword based on views/likes: {best_keyword}")

    return best_keyword, rankings