import logging

def filter_videos(videos, filter_type):
    logging.info(f"Filtering videos with filter type: {filter_type}")
    if filter_type == 'view_count':
        return sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)
    elif filter_type == 'date':
        return sorted(videos, key=lambda x: x['publish_time'], reverse=True)
    else:
        logging.warning("Unknown filter type. Returning unfiltered videos.")
        return videos
