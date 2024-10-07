import os
import pytest
import sqlite3
from datetime import datetime
import logging

# Correctly import your database functions from utils.database
from utils.database import init_db, store_video_summary, store_comments, store_brainstormed_topics, store_keyword_analysis, store_transcript_summary

# Set up logging for better debugging during tests
logging.basicConfig(level=logging.INFO)

# Create temporary database path for testing
@pytest.fixture(scope="function")
def db_connection():
    db_path = 'test_database.db'
    conn = init_db(db_path)
    yield conn  # Provide the connection for testing
    conn.close()
    if os.path.exists(db_path):
        os.remove(db_path)  # Clean up the database file after testing

# Test storing video summary
def test_store_video_summary(db_connection):
    """Test storing video information."""
    video = {
        'video_id': 'test_video_001',
        'title': 'Test Video',
        'description': 'This is a test video.',
        'publish_time': '2024-01-01 12:00:00',
        'channel_title': 'Test Channel',
        'hashtags': ['test', 'video'],
        'transcript': 'Test transcript.',
        'summary': 'Test summary.',
        'summary_source': 'Test source',
        'view_count': 100,
        'like_count': 10,
        'comment_count': 5,
        'weighted_score': 4.5  # Normal case
    }
    store_video_summary(db_connection, video)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM videos WHERE video_id = 'test_video_001'")
    result = cursor.fetchone()
    
    assert result is not None, "Video summary was not stored."
    assert result[1] == 'test_video_001', "Incorrect video_id stored."
    assert result[2] == 'Test Video', "Incorrect title stored."

# Test storing comments
def test_store_comments(db_connection):
    """Test storing comments."""
    comments = [
        {'author': 'User1', 'text': 'Great video!', 'like_count': 5, 'publish_time': '2024-01-01 13:00:00'},
        {'author': 'User2', 'text': 'Very informative.', 'like_count': 3, 'publish_time': '2024-01-01 14:00:00'}
    ]
    store_comments(db_connection, 'test_video_001', comments)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM comments WHERE video_id = 'test_video_001'")
    results = cursor.fetchall()

    assert len(results) == 2, "Not all comments were stored."
    assert results[0][2] == 'User1', "Incorrect author for first comment."

# Test storing brainstormed topics
def test_store_brainstormed_topics(db_connection):
    """Test storing brainstormed topics."""
    topics = ['fishing', 'outdoors']
    critique = 'Good topics for outdoor enthusiasts.'
    topic_score = 4.7

    store_brainstormed_topics(db_connection, topics, critique, topic_score)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM brainstormed_topics WHERE keyword = 'fishing'")
    result = cursor.fetchone()

    assert result is not None, "Brainstormed topics were not stored."
    assert result[1] == 'fishing', "Incorrect keyword stored."
    assert result[2] == 'fishing, outdoors', "Incorrect topics stored."

# Test storing keyword analysis
def test_store_keyword_analysis(db_connection):
    """Test storing keyword analysis."""
    keyword_analysis = [
        {'keyword': 'fishing', 'critique': 'Very popular keyword.', 'total_views': 50000, 'total_likes': 1000, 'weighted_score': 4.8}
    ]
    store_keyword_analysis(db_connection, keyword_analysis)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM keyword_analysis WHERE keyword = 'fishing'")
    result = cursor.fetchone()

    assert result is not None, "Keyword analysis was not stored."
    assert result[1] == 'fishing', "Incorrect keyword stored."
    assert result[2] == 'Very popular keyword.', "Incorrect critique stored."

# Test storing transcript and summary
def test_store_transcript_summary(db_connection):
    """Test storing transcript and summary."""
    video_id = 'test_video_001'
    transcript = 'This is a test transcript.'
    summary = 'This is a test summary.'

    store_transcript_summary(db_connection, video_id, transcript, summary)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM transcripts WHERE video_id = ?", (video_id,))
    result = cursor.fetchone()

    assert result is not None, "Transcript and summary were not stored."
    assert result[1] == video_id, "Incorrect video_id stored."
    assert result[2] == 'This is a test transcript.', "Incorrect transcript stored."
    assert result[3] == 'This is a test summary.', "Incorrect summary stored."
