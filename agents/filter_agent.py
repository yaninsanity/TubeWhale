import logging

def filter_videos(videos, filter_type):
    logging.info(f"Filtering videos with filter type: {filter_type}")
    
    if not videos:
        logging.warning("No videos to filter.")
        return []

    if filter_type == 'view_count':
        # Sort by view count
        logging.info(f"Sorting videos by view count.")
        return sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
    
    elif filter_type == 'like_count':
        # Sort by like count
        logging.info(f"Sorting videos by like count.")
        return sorted(videos, key=lambda x: x.get('like_count', 0), reverse=True)
    
    elif filter_type == 'comment_count':
        # Sort by comment count
        logging.info(f"Sorting videos by comment count.")
        return sorted(videos, key=lambda x: x.get('comment_count', 0), reverse=True)
    
    elif filter_type == 'date':
        # Sort by publish date
        logging.info(f"Sorting videos by publish time.")
        return sorted(videos, key=lambda x: x.get('publish_time', 'N/A'), reverse=True)
    
    elif filter_type == 'duration':
        # Sort by duration
        logging.info(f"Sorting videos by duration.")
        return sorted(videos, key=lambda x: x.get('duration', 0), reverse=True)
    
    elif filter_type == 'combined':
        # Combined filtering: view count and like count
        logging.info(f"Applying combined filtering: view count and like count.")
        return sorted(videos, key=lambda x: (x.get('view_count', 0), x.get('like_count', 0)), reverse=True)
    
    elif filter_type == 'relevance':
        # Sort by relevance
        logging.info(f"Sorting videos by relevance.")
        # Assuming 'relevance' is a key in your video data
        return sorted(videos, key=lambda x: x.get('relevance', 0), reverse=True)
    
    else:
        logging.warning("Unknown filter type. Returning unfiltered videos.")
        return videos
