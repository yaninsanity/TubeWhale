import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Float, Boolean, DateTime, ForeignKey, Table, MetaData
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

Base = declarative_base()

# -------------------------------
# ORM 模型定义
# -------------------------------

class Video(Base):
    __tablename__ = "videos"
    id = Column(Integer, primary_key=True)
    video_id = Column(String, unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    description = Column(Text)
    publish_time = Column(String)
    channel_title = Column(String)
    tags = Column(Text)
    category_id = Column(String)
    duration = Column(String)
    dimension = Column(String)
    definition = Column(String)
    caption = Column(String)
    licensed_content = Column(Boolean, default=False)
    view_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    weighted_score = Column(Float, default=0.0)
    default_audio_language = Column(String)
    country_code = Column(String)
    timestamp = Column(String)  # 更新时间，格式 '%Y-%m-%d %H:%M:%S'
    llm_summary = Column(Text)
    transcript = Column(Text)
    is_transcript = Column(Integer, default=0)  # 0: False, 1: True
    audio_summary = Column(Text)
    ai_cost = Column(Float, default=0.0)  # 确保模型中定义了该列

    # 关联评论与转录记录
    comments = relationship("Comment", back_populates="video", cascade="all, delete")
    transcripts = relationship("Transcript", back_populates="video", cascade="all, delete")


class AIInteraction(Base):
    __tablename__ = "ai_interactions"
    id = Column(Integer, primary_key=True)
    input_data = Column(Text, nullable=False)
    output_data = Column(Text, nullable=False)
    interaction_type = Column(String, nullable=False)
    tokens_used = Column(Integer, default=0)
    cost = Column(Float, default=0.0)
    timestamp = Column(String, nullable=False)


class Comment(Base):
    __tablename__ = "comments"
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey("videos.video_id", ondelete="CASCADE"), nullable=False, index=True)
    comment_id = Column(String, nullable=False)
    author = Column(String, nullable=False)
    comment_text = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)
    publish_time = Column(String)
    viewer_rating = Column(String, default="none")
    moderation_status = Column(String, default="published")
    parent_id = Column(String, nullable=True)

    video = relationship("Video", back_populates="comments")


class KeywordAnalysis(Base):
    __tablename__ = "keyword_analysis"
    id = Column(Integer, primary_key=True)
    keyword = Column(String, nullable=False)
    critique = Column(Text)
    total_views = Column(Integer, default=0)
    total_likes = Column(Integer, default=0)
    weighted_score = Column(Float, default=0.0)
    timestamp = Column(String, nullable=False)


class Transcript(Base):
    __tablename__ = "transcripts"
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey("videos.video_id", ondelete="CASCADE"), nullable=False, index=True)
    transcript = Column(Text, nullable=False)
    summary = Column(Text, nullable=False)
    timestamp = Column(String, nullable=False)

    video = relationship("Video", back_populates="transcripts")


class BrainstormedTopic(Base):
    __tablename__ = "brainstormed_topics"
    id = Column(Integer, primary_key=True)
    keyword = Column(String, nullable=False)
    topics = Column(Text)
    critique = Column(Text)
    topic_score = Column(Float, default=0.0)
    timestamp = Column(String, nullable=False)


# -------------------------------
# Database 类封装
# -------------------------------
class Database:
    """
    数据库封装类，负责初始化数据库及所有数据的存储和更新操作。

    表设计说明：
      - videos 表：存储视频相关元数据，包括 AI 生成的摘要、转录、音频摘要及累计的 OpenAI 调用费用。
      - ai_interactions 表：记录所有 AI 接口调用交互的详细信息。
      - comments 表：存储视频评论数据，与 videos 表通过 video_id 关联。
      - keyword_analysis 表：存储关键词分析结果。
      - transcripts 表：存储转录和摘要历史记录。
      - brainstormed_topics 表：存储头脑风暴产生的话题和评分。

    更新控制：
      如果某视频在指定周期（默认为 7 天）内已更新，则更新操作将被跳过。
    
    注意：如果你遇到 "no such column: videos.ai_cost" 的错误，
          请确保删除旧的数据库文件或在开发阶段设置 recreate=True，
          以便重新创建数据库模式。生产环境建议使用 Alembic 进行迁移管理。
    """
    def __init__(self, db_path: str, recreate: bool = False):
        logger.info("Initializing database with path: %s", db_path)
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False, future=True)
        if recreate:
            logger.info("Recreating database: dropping all tables.")
            Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, future=True)
        logger.info("Database tables created.")

    def get_session(self) -> Session:
        return self.Session()

    def close(self):
        # SQLAlchemy 的连接池将在程序退出时自动释放
        logger.info("Database closed (engine will be disposed on program exit).")

    def should_update_video_metadata(self, video_id: str, period_days: int = 7) -> bool:
        session = self.get_session()
        try:
            video = session.query(Video).filter(Video.video_id == video_id).first()
            if video and video.timestamp:
                last_update = datetime.strptime(video.timestamp, '%Y-%m-%d %H:%M:%S')
                if datetime.now() - last_update < timedelta(days=period_days):
                    logger.info("Video %s updated recently on %s; skipping update.", video_id, video.timestamp)
                    return False
            return True
        finally:
            session.close()

    def store_video_metadata(self, video_metadata: dict):
        session = self.get_session()
        try:
            video_metadata['view_count'] = int(video_metadata.get('view_count', 0)) or 0
            video_metadata['like_count'] = int(video_metadata.get('like_count', 0)) or 0
            video_metadata['comment_count'] = int(video_metadata.get('comment_count', 0)) or 0
            video_metadata['weighted_score'] = round(
                (video_metadata['view_count'] * 0.1) +
                (video_metadata['like_count'] * 0.5) +
                (video_metadata['comment_count'] * 0.4), 2
            )
            current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            video = session.query(Video).filter(Video.video_id == video_metadata['id']).first()
            if not video:
                video = Video(video_id=video_metadata['id'])
            video.title = video_metadata['snippet'].get('title', 'N/A')
            video.description = video_metadata['snippet'].get('description', 'N/A')
            video.publish_time = video_metadata['snippet'].get('publishedAt', 'N/A')
            video.channel_title = video_metadata['snippet'].get('channelTitle', 'N/A')
            video.tags = ','.join(video_metadata['snippet'].get('tags', []))
            video.category_id = video_metadata['snippet'].get('categoryId', 'N/A')
            video.duration = video_metadata['contentDetails'].get('duration', 'N/A')
            video.dimension = video_metadata['contentDetails'].get('dimension', 'N/A')
            video.definition = video_metadata['contentDetails'].get('definition', 'N/A')
            video.caption = video_metadata['contentDetails'].get('caption', 'false')
            video.licensed_content = video_metadata['contentDetails'].get('licensedContent', False)
            video.view_count = video_metadata['view_count']
            video.like_count = video_metadata['like_count']
            video.comment_count = video_metadata['comment_count']
            video.weighted_score = video_metadata['weighted_score']
            video.default_audio_language = video_metadata['snippet'].get('defaultAudioLanguage', 'N/A')
            video.country_code = video_metadata['snippet'].get('defaultLanguage', 'N/A')
            video.timestamp = current_ts
            video.llm_summary = video_metadata.get('llm_summary')
            video.transcript = video_metadata.get('transcript')
            video.is_transcript = video_metadata.get('is_transcript', 0)
            video.audio_summary = video_metadata.get('audio_summary')
            video.ai_cost = video_metadata.get('ai_cost', 0.0)
            session.merge(video)
            session.commit()
            logger.info("Metadata stored for video ID: %s", video_metadata['id'])
        except Exception as e:
            session.rollback()
            logger.error("Failed to store video metadata for %s: %s", video_metadata.get('id', 'UNKNOWN'), e)
            raise
        finally:
            session.close()

    def update_video_metadata(self, video_id: str, llm_summary: str, transcript: str,
                              audio_summary: str = None, ai_cost: float = 0.0):
        if not self.should_update_video_metadata(video_id):
            logger.info("Skipping update for video %s (updated recently).", video_id)
            return
        session = self.get_session()
        try:
            video = session.query(Video).filter(Video.video_id == video_id).first()
            if video:
                video.llm_summary = llm_summary
                video.transcript = transcript
                video.is_transcript = 1 if transcript else 0
                video.audio_summary = audio_summary
                video.ai_cost = ai_cost
                video.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                session.commit()
                logger.info("Video %s metadata updated.", video_id)
            else:
                logger.error("Video %s not found for update.", video_id)
        except Exception as e:
            session.rollback()
            logger.error("Failed to update metadata for video %s: %s", video_id, e)
            raise
        finally:
            session.close()

    def store_comments(self, video_id: str, comments: List[Dict[str, Any]]):
        session = self.get_session()
        try:
            for comment in comments:
                new_comment = Comment(
                    video_id=video_id,
                    comment_id=comment['comment_id'],
                    author=comment['author'],
                    comment_text=comment['text'],
                    like_count=int(comment.get('like_count', 0)) or 0,
                    publish_time=comment['publish_time'],
                    viewer_rating=comment.get('viewer_rating', 'none'),
                    moderation_status=comment.get('moderation_status', 'published'),
                    parent_id=comment['parent_id']
                )
                session.add(new_comment)
            session.commit()
            logger.info("Stored %d comments for video %s.", len(comments), video_id)
        except Exception as e:
            session.rollback()
            logger.error("Failed to store comments for video %s: %s", video_id, e)
            raise
        finally:
            session.close()

    def store_brainstormed_topics(self, topics: List[str], critique: str, topic_score: float):
        if not topics:
            logger.error("Topics list is empty or invalid.")
            return
        session = self.get_session()
        try:
            topics_str = ', '.join(topics)
            new_topic = BrainstormedTopic(
                keyword=topics[0],
                topics=topics_str,
                critique=critique,
                topic_score=topic_score or 0.0,
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            session.add(new_topic)
            session.commit()
            logger.info("Brainstormed topics stored.")
        except Exception as e:
            session.rollback()
            logger.error("Failed to store brainstormed topics: %s", e)
            raise
        finally:
            session.close()

    def store_transcript_summary(self, video_id: str, transcript: str, summary: str):
        if not video_id or not transcript.strip() or not summary.strip():
            logger.error("Invalid input for transcript and summary storage.")
            return
        session = self.get_session()
        try:
            new_record = Transcript(
                video_id=video_id,
                transcript=transcript.strip(),
                summary=summary.strip(),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            session.add(new_record)
            session.commit()
            logger.info("Transcript and summary stored for video %s.", video_id)
        except Exception as e:
            session.rollback()
            logger.error("Failed to store transcript and summary for video %s: %s", video_id, e)
            raise
        finally:
            session.close()

    def store_ai_interaction(self, input_data: Dict[str, Any], output_data: Dict[str, Any],
                             interaction_type: str, tokens_used: int = 0, cost: float = 0.0,
                             timestamp: Optional[str] = None):
        session = self.get_session()
        try:
            new_interaction = AIInteraction(
                input_data=json.dumps(input_data, default=str),
                output_data=json.dumps(output_data, default=str),
                interaction_type=interaction_type,
                tokens_used=tokens_used,
                cost=cost,
                timestamp=timestamp if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            session.add(new_interaction)
            session.commit()
            logger.info("AI interaction stored successfully for type %s.", interaction_type)
        except Exception as e:
            session.rollback()
            logger.error("Failed to store AI interaction: %s", e)
            raise
        finally:
            session.close()

    def store_keyword_analysis(self, keyword_analysis: List[Dict[str, Any]]):
        session = self.get_session()
        try:
            for analysis in keyword_analysis:
                new_entry = KeywordAnalysis(
                    keyword=analysis['keyword'],
                    critique=analysis['critique'],
                    total_views=int(analysis.get('total_views', 0)) or 0,
                    total_likes=int(analysis.get('total_likes', 0)) or 0,
                    weighted_score=float(analysis.get('weighted_score', 0)) or 0.0,
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
                session.add(new_entry)
            session.commit()
            logger.info("Keyword analysis stored successfully.")
        except Exception as e:
            session.rollback()
            logger.error("Failed to store keyword analysis: %s", e)
            raise
        finally:
            session.close()

    def store_data(self, table_name: str, data_dict: Dict[str, Any]):
        session = self.get_session()
        try:
            from sqlalchemy import MetaData  # 延迟导入
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            if table_name not in metadata.tables:
                raise Exception(f"Table '{table_name}' not found in database.")
            table = metadata.tables[table_name]
            ins = table.insert().values(**data_dict)
            session.execute(ins)
            session.commit()
            logger.info("Data stored successfully in %s.", table_name)
        except Exception as e:
            session.rollback()
            logger.error("Failed to store data in %s: %s", table_name, e)
            raise
        finally:
            session.close()


__all__ = ["Database"]
