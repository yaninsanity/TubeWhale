import sqlite3
import logging

def init_db(db_path):
    logging.info("Initializing database.")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
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
            summary TEXT
        )
    ''')
    conn.commit()
    logging.info("Database initialized.")
    return conn

def store_video_summary(conn, video):
    logging.info(f"Storing video summary for video ID: {video['video_id']}")
    cursor = conn.cursor()
    cursor.execute('INSERT INTO videos (video_id, title, description, publish_time, channel_title, hashtags, transcript, summary) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                   (video['video_id'], video['title'], video['description'], video['publish_time'], video['channel_title'], ','.join(video['hashtags']), video['transcript'], video['summary']))
    conn.commit()
    logging.info(f"Video summary stored for video ID: {video['video_id']}")
