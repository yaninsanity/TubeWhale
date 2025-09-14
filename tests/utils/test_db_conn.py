#!/usr/bin/env python3
import os
import tempfile
import json
import pytest
import logging
from datetime import datetime, timedelta
from utils.database import Database, Video, AIInteraction, Comment, KeywordAnalysis, Transcript, BrainstormedTopic

# 配置日志器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="function")
def db(temp_db_path):
    # 传递配置好的 logger 实例
    database = Database(temp_db_path, logger)
    yield database
    database.close()

@pytest.fixture(scope="function")
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="test_db_")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)

def sample_video_metadata() -> dict:
    return {
        "id": "test_video_001",
        "snippet": {
            "title": "Test Video",
            "description": "This is a test video.",
            "publishedAt": "2024-01-01T12:00:00Z",
            "channelTitle": "Test Channel",
            "tags": ["test", "video"],
            "categoryId": "22",
            "defaultAudioLanguage": "en",
            "defaultLanguage": "US"
        },
        "contentDetails": {
            "duration": "PT5M",
            "dimension": "2d",
            "definition": "hd",
            "caption": "true",
            "licensedContent": True
        },
        "view_count": "1000",
        "like_count": "100",
        "comment_count": "50"
    }

def test_schema_creation(db: Database):
    from sqlalchemy import inspect
    inspector = inspect(db.engine)
    expected_tables = {"videos", "ai_interactions", "comments", "keyword_analysis", "transcripts", "brainstormed_topics"}
    existing_tables = set(inspector.get_table_names())
    for table in expected_tables:
        assert table in existing_tables, f"Expected table '{table}' not found. Found: {existing_tables}"

def test_store_video_metadata(db: Database):
    meta = sample_video_metadata()
    db.store_video_metadata(meta)
    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == meta["id"]).first()
    session.close()
    assert video is not None, "Video metadata was not stored."
    assert video.title == "Test Video", "Video title stored incorrectly."
    assert video.view_count == 1000, "View count stored incorrectly."

def test_update_video_metadata(db: Database):
    meta = sample_video_metadata()
    db.store_video_metadata(meta)
    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == meta["id"]).first()
    eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d %H:%M:%S')
    video.timestamp = eight_days_ago
    session.commit()
    session.close()
    db.update_video_metadata("test_video_001", "Updated LLM Summary", "Updated Transcript", "Updated Audio Summary", ai_cost=0.123)
    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == "test_video_001").first()
    session.close()
    assert video is not None, "Video not found after update."
    assert video.llm_summary == "Updated LLM Summary", "LLM summary not updated."
    assert video.transcript == "Updated Transcript", "Transcript not updated."
    assert video.audio_summary == "Updated Audio Summary", "Audio summary not updated."
    updated_time = datetime.strptime(video.timestamp, '%Y-%m-%d %H:%M:%S')
    assert (datetime.now() - updated_time) < timedelta(minutes=1), "Timestamp not updated correctly."

def test_store_comments(db: Database):
    video_id = "test_video_002"
    comments = [
        {
            "comment_id": "c1",
            "author": "Alice",
            "text": "Great video!",
            "like_count": 5,
            "publish_time": "2024-01-02T12:00:00Z",
            "viewer_rating": "none",
            "moderation_status": "published",
            "parent_id": None
        },
        {
            "comment_id": "c2",
            "author": "Bob",
            "text": "Very informative.",
            "like_count": 3,
            "publish_time": "2024-01-02T13:00:00Z",
            "viewer_rating": "none",
            "moderation_status": "published",
            "parent_id": "c1"
        }
    ]
    db.store_comments(video_id, comments)
    session = db.get_session()
    stored_comments = session.query(Comment).filter(Comment.video_id == video_id).all()
    session.close()
    assert len(stored_comments) == 2, "Not all comments were stored."

def test_store_brainstormed_topics(db: Database):
    topics = ["fishing", "outdoors"]
    critique = "Good for outdoor enthusiasts."
    topic_score = 4.5
    db.store_brainstormed_topics(topics, critique, topic_score)
    session = db.get_session()
    topic_entry = session.query(BrainstormedTopic).filter(BrainstormedTopic.keyword == topics[0]).first()
    session.close()
    assert topic_entry is not None, "Brainstormed topics not stored."
    assert topic_entry.critique == critique, "Critique stored incorrectly."
    assert abs(topic_entry.topic_score - topic_score) < 1e-6, "Topic score stored incorrectly."

def test_store_transcript_summary(db: Database):
    video_id = "test_video_003"
    transcript = "This is a test transcript."
    summary = "This is a test summary."
    db.store_transcript_summary(video_id, transcript, summary)
    session = db.get_session()
    record = session.query(Transcript).filter(Transcript.video_id == video_id).first()
    session.close()
    assert record is not None, "Transcript summary record not stored."
    assert record.transcript.strip() == transcript, "Transcript stored incorrectly."
    assert record.summary.strip() == summary, "Summary stored incorrectly."

def test_store_ai_interaction(db: Database):
    input_data = {"input": "Test input"}
    output_data = {"output": "Test output"}
    interaction_type = "summarization"
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db.store_ai_interaction(input_data, output_data, interaction_type, tokens_used=150, cost=0.045, timestamp=ts)
    session = db.get_session()
    interaction = session.query(AIInteraction).filter(AIInteraction.interaction_type == interaction_type).first()
    session.close()
    assert interaction is not None, "AI interaction not stored."
    stored_input = json.loads(interaction.input_data)
    stored_output = json.loads(interaction.output_data)
    assert stored_input == input_data, "Stored input data mismatch."
    assert stored_output == output_data, "Stored output data mismatch."
    assert interaction.tokens_used == 150, "Tokens used stored incorrectly."
    assert abs(interaction.cost - 0.045) < 1e-6, "Cost stored incorrectly."
    ts_attr = getattr(interaction, "timestamp", None) or getattr(interaction, "created_at", None)
    if ts_attr is not None:
        assert ts_attr == ts, "Timestamp mismatch in AI interaction."
    else:
        pytest.skip("AIInteraction model has no timestamp or created_at attribute.")

def test_store_keyword_analysis(db: Database):
    analysis_data = [
        {
            "keyword": "fishing",
            "critique": "Popular keyword.",
            "total_views": 50000,
            "total_likes": 1000,
            "weighted_score": 4.8
        }
    ]
    db.store_keyword_analysis(analysis_data)
    session = db.get_session()
    analysis = session.query(KeywordAnalysis).filter(KeywordAnalysis.keyword == "fishing").first()
    session.close()
    assert analysis is not None, "Keyword analysis not stored."
    assert analysis.total_views == 50000, "Total views stored incorrectly."
    assert analysis.total_likes == 1000, "Total likes stored incorrectly."
    assert abs(analysis.weighted_score - 4.8) < 1e-6, "Weighted score stored incorrectly."

def test_store_data(db: Database):
    data = {
        "video_id": "test_video_004",
        "title": "Generic Test Video",
        "description": "Testing store_data function.",
        "publish_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "channel_title": "Generic Channel",
        "tags": "test,data",
        "category_id": "22",
        "duration": "PT10M",
        "dimension": "2d",
        "definition": "hd",
        "caption": "false",
        "licensed_content": False,
        "view_count": 500,
        "like_count": 50,
        "comment_count": 20,
        "weighted_score": 0.0,
        "default_audio_language": "en",
        "country_code": "US",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    db.store_data("videos", data)

    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == data["video_id"]).first()
    session.close()
    
    assert video is not None, "Generic data not stored in videos table."
    assert video.title == data["title"], "Title stored incorrectly in generic data."

def test_video_thumbnail_url_storage(db: Database):
    """测试视频 thumbnail_url 字段的存储功能"""
    meta = sample_video_metadata()
    # 添加 thumbnail_url 到测试数据
    meta["thumbnail_url"] = "https://i.ytimg.com/vi/test_video_001/maxresdefault.jpg"
    
    db.store_video_metadata(meta)
    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == meta["id"]).first()
    session.close()
    
    assert video is not None, "Video with thumbnail_url was not stored."
    assert video.thumbnail_url == meta["thumbnail_url"], f"Expected {meta['thumbnail_url']}, got {video.thumbnail_url}"

def test_video_schema_includes_thumbnail_url(db: Database):
    """测试 Video 表模式包含 thumbnail_url 字段"""
    from sqlalchemy import inspect
    inspector = inspect(db.engine)
    columns = [col['name'] for col in inspector.get_columns('videos')]
    assert 'thumbnail_url' in columns, f"thumbnail_url column not found in videos table. Columns: {columns}"

def test_video_thumbnail_url_null_handling(db: Database):
    """测试 thumbnail_url 字段的默认值处理"""
    meta = sample_video_metadata()
    # 不设置 thumbnail_url，应该存储为空字符串
    
    db.store_video_metadata(meta)
    session = db.get_session()
    video = session.query(Video).filter(Video.video_id == meta["id"]).first()
    session.close()
    
    assert video is not None, "Video without thumbnail_url was not stored."
    assert video.thumbnail_url == '', f"Expected empty string for thumbnail_url, got {repr(video.thumbnail_url)}"

if __name__ == "__main__":
    pytest.main(["-v", "--maxfail=1"])
