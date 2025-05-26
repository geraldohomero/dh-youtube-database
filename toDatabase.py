import sqlite3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, date
import time
from sqlite3 import register_adapter
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from config import get_api_key, CHANNEL_IDS, DB_CONFIG, rotate_api_key
import sys
from pathlib import Path

# Add the project root to path to enable imports
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Import transcript functionality
try:
    from data.transcriptions.transcript import get_transcript, format_transcript_text
except ImportError:
    logging.error("Failed to import transcript module. Transcript functionality will be disabled.")
    
    # Create placeholder functions if import fails
    def get_transcript(video_id):
        return False, "Transcript functionality not available", None
    
    def format_transcript_text(transcript_data):
        return "Transcript formatting not available"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Initialize YouTube API using the rotated API key
youtube = build('youtube', 'v3', developerKey=get_api_key())

def adapt_date(d: date) -> str:
    """Adapter function to store Python date as ISO string in SQLite."""
    return d.isoformat()

register_adapter(date, adapt_date)

@dataclass
class ChannelDetails:
    channel_id: str
    channel_name: str
    subscriber_count: int

def safe_execute(request) -> Dict[str, Any]:
    """
    Executes a YouTube API request.
    Rotates API key and reattempts the request if quota is exceeded.
    """
    global youtube
    try:
        return request.execute()
    except HttpError as e:
        error_content = e.content.decode('utf-8')
        if e.resp.status == 403 and "quotaExceeded" in error_content:
            logging.info("Quota exceeded. Rotating API key...")
            new_api_key = rotate_api_key()
            youtube = build('youtube', 'v3', developerKey=new_api_key)
            return request.execute()
        else:
            logging.error("YouTube API error: %s", e)
            raise

def get_channel_details(channel_id: str) -> Optional[ChannelDetails]:
    """
    Fetch channel details from YouTube API by channel ID.
    Returns ChannelDetails or None on error.
    """
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = safe_execute(request)
        items = response.get('items', [])
        if items:
            channel = items[0]
            return ChannelDetails(
                channel_id=channel_id,
                channel_name=channel['snippet']['title'],
                subscriber_count=int(channel['statistics']['subscriberCount'])
            )
        else:
            logging.warning("No channel found with id: %s", channel_id)
    except Exception as e:
        logging.error("Error fetching channel details for id %s: %s", channel_id, e)
    return None

def insert_channel_details(channel: ChannelDetails) -> None:
    """
    Insert channel details into the SQLite database.
    """
    try:
        with sqlite3.connect(DB_CONFIG) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO channels (channel_id, channel_name, subscriber_count) VALUES (?, ?, ?)",
                (channel.channel_id, channel.channel_name, channel.subscriber_count)
            )
            conn.commit()
            logging.info("Inserted channel details for %s", channel.channel_id)
    except sqlite3.Error as e:
        logging.error("Database error inserting channel details: %s", e)

def get_video_details(video_id: str, channel_id: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed video information from YouTube API.
    Returns a dictionary with video details or None on error.
    Adds a 'commentsEnabled' flag to indicate if comments are available.
    """
    try:
        request = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        )
        response = safe_execute(request)
        if response.get('items'):
            video = response['items'][0]
            published_at = datetime.strptime(video['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            # Determine if comments are enabled (the API returns 'commentCount' only if enabled)
            comments_enabled = 'commentCount' in video['statistics']
            
            # Try to get the transcript with one retry on failure
            try:
                success, transcript_text, transcript_lang = get_transcript(video_id)
            except Exception as e:
                logging.warning("First transcript attempt failed for video %s: %s. Trying again...", video_id, e)
                # Wait briefly before retry
                time.sleep(1)
                try:
                    success, transcript_text, transcript_lang = get_transcript(video_id)
                except Exception as e:
                    logging.error("Second transcript attempt also failed for video %s: %s", video_id, e)
                    success, transcript_text, transcript_lang = False, None, None
            
            return {
                'videoId': video_id,
                'channelId': channel_id,
                'videoTitle': video['snippet']['title'],
                'videoAudio': None,
                'videoTranscript': transcript_text if success else None,
                'transcriptLanguage': transcript_lang if success else None,
                'viewCount': int(video['statistics'].get('viewCount', 0)),
                'likeCount': int(video['statistics'].get('likeCount', 0)),
                'commentCount': int(video['statistics']['commentCount']) if comments_enabled else 0,
                'publishedAt': published_at.strftime('%Y-%m-%d %H:%M:%S'),
                'collectedDate': datetime.now().date(),
                'commentsEnabled': comments_enabled
            }
        else:
            logging.warning("No video details found for video id: %s", video_id)
        return None
    except Exception as e:
        logging.error("Error fetching video details for video id %s: %s", video_id, e)
        return None

def get_video_comments(video_id: str) -> List[Dict[str, Any]]:
    """
    Fetch video comments (and their replies) from YouTube API.
    Returns a list of comment dictionaries.
    If comments are disabled, logs a concise message and returns an empty list.
    """
    comments = []
    next_page_token = None
    current_date = datetime.now().date()

    try:
        while True:
            try:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = safe_execute(request)
            except HttpError as e:
                # Decode error content once rather than logging the full response.
                error_message = e.content.decode("utf-8") if e.content else ""
                if e.resp.status == 403 and "commentsDisabled" in error_message:
                    logging.info("Comments are disabled for video %s. Skipping.", video_id)
                    return []
                else:
                    logging.error("Error fetching comments for video %s. Skipping.", video_id)
                    return []

            for item in response.get("items", []):
                top_comment = item["snippet"]["topLevelComment"]
                comment_id = top_comment["id"]
                comment_snippet = top_comment["snippet"]
                comment_data = {
                    "commentId": comment_id,
                    "videoId": video_id,
                    "parentCommentId": None,
                    "userId": comment_snippet["authorChannelId"]["value"],
                    "userName": comment_snippet["authorDisplayName"],
                    "content": comment_snippet["textDisplay"],
                    "likeCount": comment_snippet["likeCount"],
                    "publishedAt": datetime.strptime(comment_snippet["publishedAt"], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
                    "collectedDate": current_date
                }
                comments.append(comment_data)
                
                # Process replies if any
                for reply in item.get("replies", {}).get("comments", []):
                    reply_snippet = reply["snippet"]
                    comments.append({
                        "commentId": reply["id"],
                        "videoId": video_id,
                        "parentCommentId": comment_id,
                        "userId": reply_snippet["authorChannelId"]["value"],
                        "userName": reply_snippet["authorDisplayName"],
                        "content": reply_snippet["textDisplay"],
                        "likeCount": reply_snippet["likeCount"],
                        "publishedAt": datetime.strptime(reply_snippet["publishedAt"], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
                        "collectedDate": current_date
                    })

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    except Exception:
        logging.error("Unexpected error fetching comments for video %s. Skipping.", video_id)
        return []

    return comments

def get_channel_videos(channel_id: str) -> List[str]:
    """
    Get list of all video IDs for a given channel using the uploads playlist.
    """
    video_ids = []
    next_page_token = None
    
    try:
        # uploads playlist ID for this channel
        channel_request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        channel_response = safe_execute(channel_request)
        
        if not channel_response.get('items'):
            logging.error("Could not find channel with ID: %s", channel_id)
            return []
        
        # Extract the uploads playlist ID
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        logging.info("Found uploads playlist ID: %s for channel: %s", uploads_playlist_id, channel_id)
        
        # fetch all videos from this playlist
        while True:
            playlist_request = youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response = safe_execute(playlist_request)
            
            for item in playlist_response.get('items', []):
                video_ids.append(item['snippet']['resourceId']['videoId'])
            
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break
            
            # Add a slight delay to respect API quota
            time.sleep(0.5)

        # Retrieve and log channel name along with video count
        channel_response = youtube.channels().list(
            part="snippet",
            id=channel_id
        ).execute()
        if channel_response.get("items"):
            channel_name = channel_response["items"][0]["snippet"]["title"]
            logging.info("Found %d videos for channel '%s' (ID: %s)", len(video_ids), channel_name, channel_id)
        else:
            logging.info("Found %d videos for channel (ID: %s)", len(video_ids), channel_id)
    except Exception as e:
        logging.error("Error fetching channel videos for channel %s: %s", channel_id, e)
    
    return video_ids

def save_to_database(conn: sqlite3.Connection, cursor: sqlite3.Cursor,
                     channel_data: Dict[str, Any], video_data: Dict[str, Any],
                     comments: List[Dict[str, Any]]) -> bool:
    """
    Save video and comment data to the SQLite database.
    """
    try:
        video_collected_date = video_data['collectedDate'].isoformat()

        # Check if transcriptLanguage column exists, add it if missing
        try:
            cursor.execute("SELECT transcriptLanguage FROM Videos WHERE videoId = ? LIMIT 1", 
                          (video_data['videoId'],))
        except sqlite3.OperationalError:
            logging.info("Adding transcriptLanguage column to Videos table")
            cursor.execute("ALTER TABLE Videos ADD COLUMN transcriptLanguage TEXT")
            
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

def video_exists_in_database(cursor: sqlite3.Cursor, video_id: str) -> bool:
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

def main():
    with sqlite3.connect(DB_CONFIG) as conn:
        cursor = conn.cursor()
        try:
            # Add transcriptLanguage column if it doesn't exist
            try:
                cursor.execute("SELECT transcriptLanguage FROM Videos LIMIT 1")
            except sqlite3.OperationalError:
                logging.info("Adding transcriptLanguage column to Videos table")
                cursor.execute("ALTER TABLE Videos ADD COLUMN transcriptLanguage TEXT")
                conn.commit()

            for channel_id in CHANNEL_IDS:
                details = get_channel_details(channel_id)
                if not details:
                    logging.warning("Skipping channel %s due to missing details.", channel_id)
                    continue

                video_ids = get_channel_videos(channel_id)
                channel_data = {
                    'channelId': details.channel_id,
                    'channelName': details.channel_name,
                    'dayCollected': datetime.now().date(),
                    'numberOfSubscribers': details.subscriber_count,
                    'numberOfVideos': len(video_ids)
                }

                total_videos = len(video_ids)
                for i, video_id in enumerate(video_ids, start=1):
                    video_data = get_video_details(video_id, channel_id)
                    if not video_data:
                        continue

                    if video_exists_in_database(cursor, video_data['videoId']):
                        logging.info("Video %s already exists in the database. Checking for transcript...", video_data['videoId'])
                        # Check if we need to update the transcript
                        cursor.execute("SELECT videoTranscript FROM Videos WHERE videoId = ?", (video_data['videoId'],))
                        result = cursor.fetchone()
                        if result and result[0] is None:
                            # Try to get transcript for existing video that's missing transcript
                            success, transcript_text, transcript_lang = get_transcript(video_id)
                            if success:
                                logging.info("Downloaded transcript for existing video %s", video_id)
                                cursor.execute(
                                    "UPDATE Videos SET videoTranscript = ?, transcriptLanguage = ? WHERE videoId = ?", 
                                    (transcript_text, transcript_lang, video_id)
                                )
                                conn.commit()
                            else:
                                logging.info("Couldn't download transcript for existing video %s: %s. Retrying once...", video_id, transcript_text)
                                # Wait briefly before retry
                                time.sleep(1)
                                try:
                                    success, transcript_text, transcript_lang = get_transcript(video_id)
                                except Exception as e:
                                    logging.error("Second transcript attempt also failed for existing video %s: %s", video_id, e)
                                    success = False
                                if success:
                                    logging.info("Downloaded transcript for existing video %s on retry", video_id)
                                    cursor.execute(
                                        "UPDATE Videos SET videoTranscript = ?, transcriptLanguage = ? WHERE videoId = ?", 
                                        (transcript_text, transcript_lang, video_id)
                                    )
                                    conn.commit()
                                else:
                                    logging.info("Couldn't download transcript for existing video %s after retry: %s", video_id, transcript_text)
                        continue

                    # Check if comments are enabled for the video.
                    if not video_data.get('commentsEnabled'):
                        logging.info("Comments are disabled for video %s. Skipping fetching comments.", video_id)
                        comments = []
                    else:
                        comments = get_video_comments(video_id)
                    
                    if save_to_database(conn, cursor, channel_data, video_data, comments):
                        transcript_status = "with transcript" if video_data.get('videoTranscript') else "without transcript"
                        logging.info("(%d/%d) Saved data for video %s (%d comments/replies, %s)",
                                     i, total_videos, video_id, len(comments), transcript_status)
                    else:
                        logging.error("Failed to save data for video %s", video_id)
                    
                    time.sleep(1)  # Respect API quota

                remaining_videos = total_videos - i
                logging.info("Processing complete for channel %s. %d videos remaining (if any further processing is needed).",
                             channel_id, remaining_videos)
        finally:
            cursor.close()

if __name__ == "__main__":
    main()
