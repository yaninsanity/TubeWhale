import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, Column, Integer, String, Text, Float, Boolean, ForeignKey, MetaData
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers: 
    logger.addHandler(logging.StreamHandler())
    logger.info("Logger initialized for database module.")


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
    timestamp = Column(String)  # 格式 '%Y-%m-%d %H:%M:%S'
    llm_summary = Column(Text)
    transcript = Column(Text)
    is_transcript = Column(Integer, default=0)
    audio_summary = Column(Text)
    ai_cost = Column(Float, default=0.0)

    comments = relationship("Comment", back_populates="video", cascade="all, delete")
    transcripts = relationship("Transcript", back_populates="video", cascade="all, delete")
    process_logs = relationship("VideoProcessLog", back_populates="video", cascade="all, delete")

class AIInteraction(Base):
    __tablename__ = "ai_interactions"
    id = Column(Integer, primary_key=True)
    input_data = Column(Text, nullable=False)
    output_data = Column(Text, nullable=False)
    interaction_type = Column(String, nullable=False)
    tokens_used = Column(Integer, default=0)
    cost = Column(Float, default=0.0)
    request_time = Column(String, nullable=False)
    response_time = Column(String, nullable=False)
    duration_ms = Column(Integer, default=0)

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

class VideoProcessLog(Base):
    __tablename__ = "video_process_logs"
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey("videos.video_id", ondelete="CASCADE"), nullable=False, index=True)
    step = Column(String, nullable=False)
    status = Column(String, nullable=False)
    details = Column(Text)
    timestamp = Column(String, nullable=False)

    video = relationship("Video", back_populates="process_logs")

# -------------------------------
# Database 封装类
# -------------------------------
class Database:
    """
    数据库封装类，负责初始化数据库及所有数据的存储与更新操作，
    包括视频元数据、AI 交互、评论、关键词分析、转录摘要、头脑风暴话题及处理日志。
    """
    def __init__(self, db_path: str, logger: logging.Logger, recreate: bool = False):
        self.logger = logger or logging.getLogger(__name__)  # 默认使用当前模块的日志器
        self.logger.info("Initializing database with path: %s", db_path)
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False, future=True)
        if recreate:
            self.logger.info("Recreating database: dropping all tables.")
            Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, future=True)
        self.logger.info("Database tables created.")

    def get_session(self):
        return self.Session()

    def close(self):
        self.logger.info("Database closed (engine will be disposed on program exit).")

    def _commit_session(self, session, action_desc: str):
        try:
            session.commit()
            self.logger.info("%s succeeded.", action_desc)
        except Exception as e:
            session.rollback()
            self.logger.error("%s failed: %s", action_desc, e)
            raise
        finally:
            session.close()

    def should_update_video_metadata(self, video_id: str, period_days: int = 7) -> bool:
        with self.get_session() as session:
            video = session.query(Video).filter(Video.video_id == video_id).first()
            if video and video.timestamp:
                last_update = datetime.strptime(video.timestamp, '%Y-%m-%d %H:%M:%S')
                if datetime.now() - last_update < timedelta(days=period_days):
                    self.logger.info("Video %s updated on %s recently; skipping update.", video_id, video.timestamp)
                    return False
            return True

    def store_video_metadata(self, video_metadata: dict):
        session = self.get_session()
        try:
            # Sanitizing data (ensuring default values for missing fields)
            video_metadata['view_count'] = int(video_metadata.get('view_count', 0)) or 0
            video_metadata['like_count'] = int(video_metadata.get('like_count', 0)) or 0
            video_metadata['comment_count'] = int(video_metadata.get('comment_count', 0)) or 0
            video_metadata['weighted_score'] = round(
                (video_metadata['view_count'] * 0.1) +
                (video_metadata['like_count'] * 0.5) +
                (video_metadata['comment_count'] * 0.4), 2
            )
            current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            session.expire_on_commit = False
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
            self._commit_session(session, f"Storing metadata for video ID: {video_metadata['id']}")
        except Exception:
            session.rollback()
            raise


    def update_video_metadata(self, video_id: str, llm_summary: str, transcript: str,
                              audio_summary: str = None, ai_cost: float = 0.0):
        if not self.should_update_video_metadata(video_id):
            logger.info("Skipping update for video %s (recent update).", video_id)
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
                self._commit_session(session, f"Updating metadata for video {video_id}")
            else:
                logger.error("Video %s not found for update.", video_id)
        except Exception:
            session.rollback()
            raise

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
                    parent_id=comment.get('parent_id')
                )
                session.add(new_comment)
            self._commit_session(session, f"Storing comments for video {video_id}")
        except Exception:
            session.rollback()
            raise

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
            self._commit_session(session, "Storing brainstormed topics")
        except Exception:
            session.rollback()
            raise

    def store_transcript_summary(self, video_id: str, transcript: str, summary: str):
        if not video_id or not transcript.strip() or not summary.strip():
            logger.error("Invalid input for transcript summary storage.")
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
            self._commit_session(session, f"Storing transcript and summary for video {video_id}")
        except Exception:
            session.rollback()
            raise

    def store_ai_interaction(self, input_data: Dict[str, Any], output_data: Dict[str, Any],
                             interaction_type: str, tokens_used: int = 0, cost: float = 0.0,
                             timestamp: Optional[str] = None, duration_ms: int = 0):
        session = self.get_session()
        try:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_interaction = AIInteraction(
                input_data=json.dumps(input_data, default=str),
                output_data=json.dumps(output_data, default=str),
                interaction_type=interaction_type,
                tokens_used=tokens_used,
                cost=cost,
                request_time=timestamp if timestamp else now_str,
                response_time=now_str,
                duration_ms=duration_ms
            )
            session.add(new_interaction)
            self._commit_session(session, f"Storing AI interaction for type {interaction_type}")
        except Exception:
            session.rollback()
            raise

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
            self._commit_session(session, "Storing keyword analysis")
        except Exception:
            session.rollback()
            raise

    def store_video_process_log(self, video_id: str, step: str, status: str, details: str = ""):
        session = self.get_session()
        try:
            new_log = VideoProcessLog(
                video_id=video_id,
                step=step,
                status=status,
                details=details,
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            session.add(new_log)
            self._commit_session(session, f"Storing process log for video {video_id}, step '{step}'")
        except Exception:
            session.rollback()
            raise

    def store_data(self, table_name: str, data_dict: Dict[str, Any]):
        session = self.get_session()
        try:
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            
            if table_name not in metadata.tables:
                self.logger.error(f"Table '{table_name}' not found in database.")
                raise Exception(f"Table '{table_name}' not found in database.")
            
            table = metadata.tables[table_name]
            
            # Sanitize the data_dict to match the expected column names
            for key in list(data_dict.keys()):
                if key not in table.columns:
                    self.logger.warning(f"Column '{key}' does not exist in table '{table_name}', removing it.")
                    del data_dict[key]
            
            # Insert the data into the table
            ins = table.insert().values(**data_dict)
            session.execute(ins)
            
            # Commit changes to the database
            self._commit_session(session, f"Storing data in table {table_name}")
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to store data in table {table_name}: {e}")
            raise
        finally:
            session.close()
            self.logger.info(f"Data stored successfully in table {table_name}.")
