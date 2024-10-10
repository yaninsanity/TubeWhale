import sqlite3
import logging
from datetime import datetime
import json  # Add this import for JSON serialization

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
                tags TEXT,
                category_id TEXT,
                duration TEXT,
                dimension TEXT,
                definition TEXT,
                caption TEXT,
                licensed_content BOOLEAN,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                weighted_score REAL DEFAULT 0,
                default_audio_language TEXT,
                country_code TEXT,
                timestamp TEXT,
                llm_summary TEXT,  -- AI-generated summary
                transcript TEXT,  -- Raw transcript from video (if exists)
                is_transcript INTEGER DEFAULT 0,  -- Boolean flag, 0 for False, 1 for True
                audio_summary TEXT  -- Audio-based summary (if exists)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_interactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_data TEXT NOT NULL,      -- JSON serialized input data
                output_data TEXT NOT NULL,     -- JSON serialized output data
                interaction_type TEXT NOT NULL, -- Type of interaction (e.g., 'keyword_search', 'summarization')
                timestamp TEXT NOT NULL         -- Timestamp of the interaction
            );
        ''')
        # 创建评论信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                author TEXT NOT NULL,
                comment_text TEXT NOT NULL,
                like_count INTEGER DEFAULT 0,
                publish_time TEXT,
                viewer_rating TEXT,
                moderation_status TEXT,
                parent_id TEXT,
                FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES comments(comment_id) ON DELETE CASCADE
            )
        ''')

        conn.commit()
        logging.info("Database initialized.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize the database: {e}")
        raise

# 存储视频信息到数据库
def store_video_metadata(conn, video_metadata):
    if not conn:
        logging.error("Connection is None. Cannot store video metadata.")
        return
    
    # 设置默认值，避免 None 造成的错误
    video_metadata['view_count'] = int(video_metadata.get('view_count', 0)) or 0
    video_metadata['like_count'] = int(video_metadata.get('like_count', 0)) or 0
    video_metadata['comment_count'] = int(video_metadata.get('comment_count', 0)) or 0

    # 计算 weighted_score：这里的公式可以自定义
    video_metadata['weighted_score'] = round((video_metadata['view_count'] * 0.1) + (video_metadata['like_count'] * 0.5) + (video_metadata['comment_count'] * 0.4), 2)

    logging.info(f"Storing metadata for video ID: {video_metadata['id']}")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO videos 
            (video_id, title, description, publish_time, channel_title, tags, category_id, duration, dimension, 
             definition, caption, licensed_content, view_count, like_count, comment_count, weighted_score, 
             default_audio_language, country_code, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_metadata['id'],  # 视频ID
            video_metadata['snippet'].get('title', 'N/A'),  # 视频标题
            video_metadata['snippet'].get('description', 'N/A'),  # 视频描述
            video_metadata['snippet'].get('publishedAt', 'N/A'),  # 发布日期
            video_metadata['snippet'].get('channelTitle', 'N/A'),  # 频道名称
            ','.join(video_metadata['snippet'].get('tags', [])),  # 视频标签
            video_metadata['snippet'].get('categoryId', 'N/A'),  # 视频分类
            video_metadata['contentDetails'].get('duration', 'N/A'),  # 视频时长
            video_metadata['contentDetails'].get('dimension', 'N/A'),  # 视频维度
            video_metadata['contentDetails'].get('definition', 'N/A'),  # 清晰度
            video_metadata['contentDetails'].get('caption', 'false'),  # 是否有字幕
            video_metadata['contentDetails'].get('licensedContent', False),  # 是否为授权内容
            video_metadata['view_count'],  # 观看次数
            video_metadata['like_count'],  # 点赞次数
            video_metadata['comment_count'],  # 评论次数
            video_metadata['weighted_score'],  # 自定义加权评分
            video_metadata['snippet'].get('defaultAudioLanguage', 'N/A'),  # 默认音频语言
            video_metadata['snippet'].get('defaultLanguage', 'N/A'),  # 国家代码
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 插入时间戳
        ))
        conn.commit()
        logging.info(f"Metadata stored for video ID: {video_metadata['id']}")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store video metadata for video ID {video_metadata['id']}: {e}")
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
        for comment in comments:
            cursor.execute('''
                INSERT INTO comments (video_id, comment_id, author, comment_text, like_count, publish_time, viewer_rating, moderation_status, parent_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video_id, 
                comment['comment_id'],  # 唯一的 comment_id
                comment['author'], 
                comment['text'], 
                comment.get('like_count', 0) or 0, 
                comment['publish_time'],
                comment.get('viewer_rating', 'none'),  # 存储观看者的评分
                comment.get('moderation_status', 'published'),  # 存储审核状态
                comment['parent_id']  # 直接从 comment 字典中获取 parent_id
            ))

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
            topic_score or 0,  
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        conn.commit()
        logging.info("Brainstormed topics stored.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Failed to store brainstormed topics: {e}")
        raise

def store_ai_interaction(conn, input_data, output_data, interaction_type, timestamp):
    if not conn:
        logging.error("Connection is None. Cannot store AI interaction.")
        return
    
    logging.info(f"Storing AI interaction of type: {interaction_type}")
    
    try:
        cursor = conn.cursor()
        
        # Serialize input and output data to JSON format, ensuring proper serialization
        input_json = json.dumps(input_data, default=str)  # Convert input_data to JSON
        output_json = json.dumps(output_data, default=str)  # Convert output_data to JSON

        # Inserting into the database
        cursor.execute('''
            INSERT INTO ai_interactions (input_data, output_data, interaction_type, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (input_json, output_json, interaction_type, timestamp))
        
        conn.commit()
        logging.info(f"AI interaction of type {interaction_type} stored successfully.")
    
    except sqlite3.Error as e:
        conn.rollback()  # Roll back in case of error
        logging.error(f"Failed to store AI interaction: {e}")
        raise  # Reraise exception to handle it properly elsewhere

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


def update_video_metadata(conn, video_id, llm_summary, transcript, audio_summary=None):
    is_transcript = 1 if transcript else 0  # 1 if transcript exists, else 0
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE videos
            SET llm_summary = ?, transcript = ?, is_transcript = ?, audio_summary = ?
            WHERE video_id = ?
        ''', (llm_summary, transcript, is_transcript, audio_summary, video_id))
        
        conn.commit()
        logging.info(f"Video {video_id} metadata updated with AI summary and transcript.")
    
    except Exception as e:
        logging.error(f"Failed to update video metadata for {video_id}: {e}")
        conn.rollback()
        raise e