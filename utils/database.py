import sqlite3
import logging
from datetime import datetime

# 初始化数据库
def init_db(db_path):
    logging.info("Initializing database.")
    try:
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
                summary_source TEXT,  
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                weighted_score REAL DEFAULT 0,  
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
                like_count INTEGER DEFAULT 0,
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
                topic_score REAL DEFAULT 0,
                timestamp TEXT NOT NULL
            )
        ''')

        # 创建关键词分析表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keyword_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                critique TEXT,
                total_views INTEGER DEFAULT 0,
                total_likes INTEGER DEFAULT 0,
                weighted_score REAL DEFAULT 0,
                timestamp TEXT
            )
        ''')

        # 创建转录和总结表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transcripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,
                transcript TEXT NOT NULL,
                summary TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
            )
        ''')

        conn.commit()
        logging.info("Database initialized.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize the database: {e}")
        raise

# 存储视频信息到数据库
def store_video_summary(conn, video):
    if not conn:
        logging.error("Connection is None. Cannot store video summary.")
        return
    
    # 设置默认值，避免 None 造成的错误
    video['view_count'] = video.get('view_count', 0) or 0
    video['like_count'] = video.get('like_count', 0) or 0
    video['comment_count'] = video.get('comment_count', 0) or 0
    video['weighted_score'] = video.get('weighted_score', 0) or 0

    logging.info(f"Storing video summary for video ID: {video['video_id']}")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO videos 
            (video_id, title, description, publish_time, channel_title, hashtags, transcript, summary, 
             summary_source, view_count, like_count, comment_count, weighted_score, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (video['video_id'], 
              video.get('title', 'N/A'), 
              video.get('description', 'N/A'), 
              video.get('publish_time', 'N/A'),
              video.get('channel_title', 'N/A'), 
              ','.join(video.get('hashtags', [])), 
              video.get('transcript', 'N/A'),
              video.get('summary', 'N/A'), 
              video.get('summary_source', 'N/A'),  
              video['view_count'], 
              video['like_count'],
              video['comment_count'], 
              video['weighted_score'],  
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        logging.info(f"Video summary stored for video ID: {video['video_id']}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store video summary for video ID {video['video_id']}: {e}")
        raise


# 批量存储评论信息到数据库
def store_comments(conn, video_id, comments):
    if not conn:
        logging.error("Connection is None. Cannot store comments.")
        return
    
    logging.info(f"Storing comments for video ID: {video_id}")
    cursor = conn.cursor()

    if not comments:
        logging.warning(f"No comments found for video ID: {video_id}")
        return

    try:
        cursor.execute('BEGIN')
        cursor.executemany('''
            INSERT INTO comments (video_id, author, comment_text, like_count, publish_time)
            VALUES (?, ?, ?, ?, ?)
        ''', [
            (video_id, comment['author'], comment['text'], comment.get('like_count', 0) or 0, comment['publish_time'])
            for comment in comments
            if all(key in comment for key in ['author', 'text', 'like_count', 'publish_time'])  
        ])
        conn.commit()
        logging.info(f"Comments stored for video ID: {video_id}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store comments for video ID {video_id}: {e}")
        raise


# 存储脑暴结果
def store_brainstormed_topics(conn, topics, critique, topic_score):
    if not conn:
        logging.error("Connection is None. Cannot store brainstormed topics.")
        return
    
    logging.info("Storing brainstormed topics.")
    cursor = conn.cursor()

    if not topics or not isinstance(topics, list) or len(topics) == 0:
        logging.error("Topics list is empty or invalid.")
        return

    try:
        topics_str = ', '.join(str(topic) for topic in topics)

        cursor.execute('BEGIN')
        cursor.execute('''
            INSERT INTO brainstormed_topics (keyword, topics, critique, topic_score, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            topics[0],  
            topics_str,  
            critique,  
            topic_score or 0,  # 确保 topic_score 有值
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        conn.commit()
        logging.info("Brainstormed topics stored.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store brainstormed topics: {e}")
        raise

# 存储关键词分析结果
def store_keyword_analysis(conn, keyword_analysis):
    if not conn:
        logging.error("Connection is None. Cannot store keyword analysis.")
        return
    
    logging.info("Storing keyword analysis in the database.")
    cursor = conn.cursor()

    try:
        cursor.execute('BEGIN')
        for analysis in keyword_analysis:
            cursor.execute('''
                INSERT INTO keyword_analysis (keyword, critique, total_views, total_likes, weighted_score, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                analysis['keyword'], 
                analysis['critique'], 
                analysis.get('total_views', 0) or 0,  
                analysis.get('total_likes', 0) or 0,
                analysis.get('weighted_score', 0) or 0,  
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        conn.commit()
        logging.info("Keyword analysis stored successfully.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store keyword analysis: {e}")
        raise

# 存储音频转录和总结
def store_transcript_summary(conn, video_id, transcript, summary):
    if not conn:
        logging.error("Connection is None. Cannot store transcript and summary.")
        return
    
    logging.info(f"Storing transcript and summary for video ID: {video_id}")
    cursor = conn.cursor()

    if not isinstance(video_id, str) or not video_id:
        logging.error(f"Invalid video_id: {video_id}")
        return
    
    if not isinstance(transcript, str) or not transcript.strip():
        logging.error(f"Invalid or empty transcript for video ID: {video_id}")
        return
    
    if not isinstance(summary, str) or not summary.strip():
        logging.error(f"Invalid or empty summary for video ID: {video_id}")
        return

    try:
        cursor.execute('BEGIN')
        cursor.execute('''
            INSERT INTO transcripts (video_id, transcript, summary, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (
            video_id,  
            transcript.strip(),  
            summary.strip(),  
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
        ))
        conn.commit()
        logging.info(f"Transcript and summary successfully stored for video ID: {video_id}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store transcript and summary for video ID {video_id}: {e}")
        raise

# 通用存储数据函数
def store_data(conn, table_name, data_dict):
    if not conn:
        logging.error("Connection is None. Cannot store data.")
        return
    
    logging.info(f"Storing data in table: {table_name}")
    cursor = conn.cursor()

    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['?' for _ in data_dict])
    values = list(data_dict.values())

    try:
        cursor.execute('BEGIN')
        cursor.execute(f'''
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        ''', values)
        conn.commit()
        logging.info(f"Data successfully stored in {table_name}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store data in {table_name}: {e}")
        raise
