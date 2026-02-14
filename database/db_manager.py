import sqlite3
import logging
from typing import Dict, Any, List, Set
from datetime import date

from config import DB_CONFIG
from models.data_models import ChannelDetails, VideoData, CommentData

def adapt_date(d: date) -> str:
    """Adapter function to store Python date as ISO string in SQLite."""
    return d.isoformat()

sqlite3.register_adapter(date, adapt_date)

class DatabaseManager:
    def __init__(self, db_path: str = DB_CONFIG):
        self.db_path = db_path
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(self.db_path)
    
    def ensure_transcript_column_exists(self, cursor: sqlite3.Cursor) -> None:
        """Ensure the transcriptLanguage column exists in the Videos table."""
        try:
            cursor.execute("SELECT transcriptLanguage FROM Videos LIMIT 1")
        except sqlite3.OperationalError:
            logging.info("Adding transcriptLanguage column to Videos table")
            cursor.execute("ALTER TABLE Videos ADD COLUMN transcriptLanguage TEXT")
    
    def insert_channel_details(self, channel: ChannelDetails) -> None:
        """
        Insert channel details into the SQLite database.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO channels (channel_id, channel_name, subscriber_count) VALUES (?, ?, ?)",
                    (channel.channel_id, channel.channel_name, channel.subscriber_count)
                )
                conn.commit()
                logging.info("Inserted channel details for %s", channel.channel_id)
        except sqlite3.Error as e:
            logging.error("Database error inserting channel details: %s", e)
    
    def video_exists_in_database(self, cursor: sqlite3.Cursor, video_id: str) -> bool:
        """
        Check if a video exists in the database.
        """
        try:
            cursor.execute("SELECT COUNT(*) FROM Videos WHERE videoId = ?", (video_id,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logging.error("Error checking existence of video %s: %s", video_id, e)
            return False
    
    def get_existing_video_ids(self, cursor: sqlite3.Cursor) -> Set[str]:
        """
        Get all existing video IDs from the database as a set for fast lookups.
        """
        try:
            cursor.execute("SELECT videoId FROM Videos")
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logging.error("Error fetching existing video IDs: %s", e)
            return set()
    
    def save_video_and_comments(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor,
                               channel_data: Dict[str, Any], video_data: Dict[str, Any],
                               comments: List[Dict[str, Any]]) -> bool:
        """
        Save video and comment data to the SQLite database.
        """
        try:
            video_collected_date = video_data['collectedDate'].isoformat()

            # Check if transcriptLanguage column exists, add it if missing
            self.ensure_transcript_column_exists(cursor)
                
            # Update Videos table with transcript information
            cursor.execute("""
                INSERT INTO Videos (
                    videoId, channelId, videoTitle, videoAudio, videoTranscript,
                    viewCount, likeCount, commentCount, publishedAt, collectedDate,
                    transcriptLanguage
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(videoId) DO UPDATE SET
                    videoTitle = excluded.videoTitle,
                    viewCount = excluded.viewCount,
                    likeCount = excluded.likeCount,
                    commentCount = excluded.commentCount,
                    collectedDate = excluded.collectedDate,
                    videoTranscript = COALESCE(excluded.videoTranscript, videoTranscript),
                    transcriptLanguage = COALESCE(excluded.transcriptLanguage, transcriptLanguage)
            """, (
                video_data['videoId'],
                video_data['channelId'],
                video_data['videoTitle'],
                video_data['videoAudio'],
                video_data['videoTranscript'],
                video_data['viewCount'],
                video_data['likeCount'],
                video_data['commentCount'],
                video_data['publishedAt'],
                video_collected_date,
                video_data.get('transcriptLanguage')
            ))
            
            # Insert Comments and Replies
            for comment in comments:
                comment_collected_date = comment['collectedDate'].isoformat()
                cursor.execute("""
                    INSERT INTO Comments (
                        commentId, videoId, parentCommentId, userId, 
                        userName, content, likeCount, publishedAt, collectedDate
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(commentId) DO UPDATE SET
                        content = excluded.content,
                        likeCount = excluded.likeCount,
                        collectedDate = excluded.collectedDate
                """, (
                    comment['commentId'],
                    comment['videoId'],
                    comment['parentCommentId'],
                    comment['userId'],
                    comment['userName'],
                    comment['content'],
                    comment['likeCount'],
                    comment['publishedAt'],
                    comment_collected_date
                ))
            
            conn.commit()
            return True
        except Exception as e:
            logging.error("Database error while saving video %s: %s", video_data.get('videoId'), e)
            conn.rollback()
            return False
