import logging

def filter_videos(videos, filter_type):
    logging.info(f"Filtering videos with filter type: {filter_type}")
    
    if not videos:
        logging.warning("No videos to filter.")
        return []

    if filter_type == 'view_count':
        # 按观看次数过滤
        logging.info(f"Sorting videos by view count.")
        return sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
    
    elif filter_type == 'like_count':
        # 按点赞数过滤
        logging.info(f"Sorting videos by like count.")
        return sorted(videos, key=lambda x: x.get('like_count', 0), reverse=True)
    
    elif filter_type == 'comment_count':
        # 按评论数过滤
        logging.info(f"Sorting videos by comment count.")
        return sorted(videos, key=lambda x: x.get('comment_count', 0), reverse=True)
    
    elif filter_type == 'date':
        # 按发布日期过滤
        logging.info(f"Sorting videos by publish time.")
        return sorted(videos, key=lambda x: x.get('publish_time', 'N/A'), reverse=True)
    
    elif filter_type == 'duration':
        # 按视频时长过滤
        logging.info(f"Sorting videos by duration.")
        return sorted(videos, key=lambda x: x.get('duration', 0), reverse=True)
    
    elif filter_type == 'combined':
        # 多重过滤示例：优先按观看次数，再按点赞数排序
        logging.info(f"Applying combined filtering: view count and like count.")
        return sorted(videos, key=lambda x: (x.get('view_count', 0), x.get('like_count', 0)), reverse=True)
    
    else:
        logging.warning("Unknown filter type. Returning unfiltered videos.")
        return videos
