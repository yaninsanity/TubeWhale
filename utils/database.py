import sqlite3
import logging
from datetime import datetime

# 初始化数据库
def init_db(db_path):
    logging.info("Initializing database.")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建视频信息表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            publish_time TEXT,
            channel_title TEXT,
            hashtags TEXT,
            transcript TEXT,
            summary TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            timestamp TEXT
        )
    ''')

    # 创建评论信息表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            author TEXT NOT NULL,
            comment_text TEXT NOT NULL,
            like_count INTEGER,
            publish_time TEXT,
            FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
        )
    ''')

    # 创建脑暴结果表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS brainstormed_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            topics TEXT NOT NULL,
            critique TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')

    # 创建关键词分析表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keyword_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            critique TEXT,
            video_count INTEGER,
            timestamp TEXT
        )
    ''')

    conn.commit()
    logging.info("Database initialized.")
    return conn

# 存储视频信息到数据库
def store_video_summary(conn, video):
    logging.info(f"Storing video summary for video ID: {video['video_id']}")
    cursor = conn.cursor()

    try:
        # 使用事务确保数据持久化的可靠性
        cursor.execute('BEGIN')
        cursor.execute('''
            INSERT OR REPLACE INTO videos 
            (video_id, title, description, publish_time, channel_title, hashtags, transcript, summary, 
             view_count, like_count, comment_count, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (video['video_id'], video.get('title', 'N/A'), video.get('description', 'N/A'), video.get('publish_time', 'N/A'),
              video.get('channel_title', 'N/A'), ','.join(video.get('hashtags', [])), video.get('transcript', 'N/A'),
              video.get('summary', 'N/A'), video.get('view_count', 0), video.get('like_count', 0),
              video.get('comment_count', 0), video.get('timestamp', 'N/A')))
        conn.commit()
        logging.info(f"Video summary stored for video ID: {video['video_id']}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store video summary for video ID {video['video_id']}: {e}")
        raise

# 批量存储评论信息到数据库
def store_comments(conn, video_id, comments):
    logging.info(f"Storing comments for video ID: {video_id}")
    cursor = conn.cursor()

    try:
        cursor.execute('BEGIN')
        cursor.executemany('''
            INSERT INTO comments (video_id, author, comment_text, like_count, publish_time)
            VALUES (?, ?, ?, ?, ?)
        ''', [(video_id, comment['author'], comment['text'], comment['like_count'], comment['publish_time']) for comment in comments])
        conn.commit()
        logging.info(f"Comments stored for video ID: {video_id}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store comments for video ID {video_id}: {e}")
        raise

# 存储脑暴结果
def store_brainstormed_topics(conn, topics, critique):
    logging.info("Storing brainstormed topics.")
    cursor = conn.cursor()

    try:
        cursor.execute('BEGIN')
        cursor.execute('''
            INSERT INTO brainstormed_topics (keyword, topics, critique, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (topics[0], ', '.join(topics), critique, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        logging.info("Brainstormed topics stored.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store brainstormed topics: {e}")
        raise

def store_keyword_analysis(conn, keyword_analysis):
    logging.info("Storing keyword analysis in the database.")
    cursor = conn.cursor()

    try:
        cursor.execute('BEGIN')
        for analysis in keyword_analysis:
            # 确保 'critique' 和 'total_views'、'total_likes' 字段存在
            cursor.execute('''
                INSERT INTO keyword_analysis (keyword, critique, video_count, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (
                analysis['keyword'], 
                analysis['critique'],  # Critique 在这里正确插入
                analysis['total_views'],  # Video count 可以是 total_views 或者 total_likes
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        conn.commit()
        logging.info("Keyword analysis stored successfully.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store keyword analysis: {e}")
        raise
