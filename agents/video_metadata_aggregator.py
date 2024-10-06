import logging
from utils.youtube_api import get_youtube_service, fetch_video_metadata  # 从 utils 中获取统一的 YouTube 服务和元数据抓取函数

# 聚合多个视频的元数据，并计算加权分数
def aggregate_video_metadata(videos, youtube_api_key):
    logging.info("Aggregating video metadata.")

    video_metadata_list = []
    total_views = 0
    total_likes = 0
    total_comments = 0

    for video in videos:
        video_id = video['video_id']
        metadata = fetch_video_metadata(video_id, youtube_api_key)  # 调用 utils 中的统一元数据抓取函数

        if metadata:
            video_metadata_list.append(metadata)
            total_views += metadata['view_count']
            total_likes += metadata['like_count']
            total_comments += metadata['comment_count']

    # 计算每个视频的加权分数（可以根据需求调整权重）
    aggregated_metadata = {
        'total_views': total_views,
        'total_likes': total_likes,
        'total_comments': total_comments,
        'average_views': total_views // len(videos) if len(videos) > 0 else 0,
        'average_likes': total_likes // len(videos) if len(videos) > 0 else 0,
        'average_comments': total_comments // len(videos) if len(videos) > 0 else 0,
        'videos': video_metadata_list  # 存储每个视频的元数据，以便进一步分析
    }

    logging.info(f"Aggregated metadata: {aggregated_metadata}")
    return aggregated_metadata

# 多智能体搜索和元数据聚合
def multiagent_search(base_keyword, agent_count, max_n, top_k, youtube_api_key, openai_api_key, dry_run=False):
    logging.info(f"Starting multi-agent search with {agent_count} agents for keyword: {base_keyword}")

    if dry_run:
        logging.info("Dry run mode - skipping API calls.")
        return {}

    # Step 1: 使用 OpenAI 生成关键词变体
    generated_keywords = keyword_generator_agent(base_keyword, max_n, openai_api_key)

    # 如果生成的关键词少于 agent_count，调整 agent_count
    agent_count = min(agent_count, len(generated_keywords))

    search_results = {}

    # Step 2: 每个智能体搜索一个关键词，并聚合视频元数据
    for idx in range(agent_count):
        keyword = generated_keywords[idx]
        logging.info(f"Agent searching with keyword: {keyword}")

        # 调用 YouTube API 搜索视频
        videos = search_youtube_videos(keyword, youtube_api_key, top_k)
        if videos:
            # 聚合每个关键词相关的视频元数据
            aggregated_metadata = aggregate_video_metadata(videos, youtube_api_key)
            search_results[keyword] = aggregated_metadata

    logging.info(f"Search completed for {len(search_results)} keywords.")
    return search_results

# 关键词生成智能体（此处可复用）
def keyword_generator_agent(base_keyword, max_n, openai_api_key):
    llm = OpenAI(api_key=openai_api_key)
    prompt = f"Generate {max_n} relevant variations of the keyword '{base_keyword}' for a YouTube video search."

    try:
        response = llm.agenerate([prompt])
        generated_keywords = response.generations[0][0].text.split("\n")
        generated_keywords = list(set(filter(None, [kw.strip() for kw in generated_keywords])))
        logging.info(f"Generated {len(generated_keywords)} keyword variations.")
        return generated_keywords
    except Exception as e:
        logging.error(f"Error generating keywords with OpenAI: {e}")
        return [base_keyword]  # 如果失败，返回基础关键字以确保不中断流程

# YouTube 视频搜索函数（从 utils 中调用）
def search_youtube_videos(keyword, youtube_api_key, top_k):
    youtube = get_youtube_service(youtube_api_key)  # 使用 utils 中的 YouTube API 服务初始化
    logging.info(f"Fetching videos for keyword: {keyword}")

    try:
        request = youtube.search().list(part="snippet", q=keyword, maxResults=top_k)
        response = request.execute()

        videos = []
        for item in response['items']:
            video_data = {
                'video_id': item['id'].get('videoId', ''),
                'title': item['snippet'].get('title', 'N/A'),
                'description': item['snippet'].get('description', 'N/A'),
                'publish_time': item['snippet'].get('publishedAt', 'N/A'),
                'channel_title': item['snippet'].get('channelTitle', 'N/A')
            }
            videos.append(video_data)

        logging.info(f"Retrieved {len(videos)} videos for keyword: {keyword}")
        return videos
    except HttpError as e:
        logging.error(f"Error fetching videos for keyword '{keyword}': {e}")
        return []
