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
            return {
                'videoId': video_id,
                'channelId': channel_id,
                'videoTitle': video['snippet']['title'],
                'videoAudio': None,
                'videoTranscript': None,
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
    """
    comments = []
    next_page_token = None
    current_date = datetime.now().date()

    try:
        while True:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = safe_execute(request)
            
            for item in response.get('items', []):
                top_comment = item['snippet']['topLevelComment']
                comment_id = top_comment['id']
                comment_snippet = top_comment['snippet']
                
                comment_data = {
                    'commentId': comment_id,
                    'videoId': video_id,
                    'parentCommentId': None,
                    'userId': comment_snippet['authorChannelId']['value'],
                    'userName': comment_snippet['authorDisplayName'],
                    'content': comment_snippet['textDisplay'],
                    'likeCount': comment_snippet['likeCount'],
                    'publishedAt': datetime.strptime(comment_snippet['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
                    'collectedDate': current_date
                }
                comments.append(comment_data)
                
                # Process replies, if any
                for reply in item.get('replies', {}).get('comments', []):
                    reply_snippet = reply['snippet']
                    comments.append({
                        'commentId': reply['id'],
                        'videoId': video_id,
                        'parentCommentId': comment_id,
                        'userId': reply_snippet['authorChannelId']['value'],
                        'userName': reply_snippet['authorDisplayName'],
                        'content': reply_snippet['textDisplay'],
                        'likeCount': reply_snippet['likeCount'],
                        'publishedAt': datetime.strptime(reply_snippet['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
                        'collectedDate': current_date
                    })
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

    except Exception as e:
        logging.error("Error fetching comments for video %s: %s", video_id, e)
    
    return comments

def get_channel_videos(channel_id: str) -> List[str]:
    """
    Get list of all video IDs for a given channel.
    """
    video_ids = []
    next_page_token = None
    
    try:
        while True:
            request = youtube.search().list(
                part="snippet",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token,
                type="video"
            )
            response = safe_execute(request)
            
            video_ids.extend([
                item['id']['videoId']
                for item in response.get('items', [])
                if item['id'].get('kind') == "youtube#video"
            ])
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

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

        # Update Videos table
        cursor.execute("""
            INSERT INTO Videos (
                videoId, channelId, videoTitle, videoAudio, videoTranscript,
                viewCount, likeCount, commentCount, publishedAt, collectedDate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(videoId) DO UPDATE SET
                videoTitle = excluded.videoTitle,
                viewCount = excluded.viewCount,
                likeCount = excluded.likeCount,
                commentCount = excluded.commentCount,
                collectedDate = excluded.collectedDate
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
            video_collected_date
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
                        logging.info("Video %s already exists in the database. Skipping...", video_data['videoId'])
                        continue

                    # Check if comments are enabled for the video.
                    if not video_data.get('commentsEnabled'):
                        logging.info("Comments are disabled for video %s. Skipping fetching comments.", video_id)
                        comments = []
                    else:
                        comments = get_video_comments(video_id)
                    
                    if save_to_database(conn, cursor, channel_data, video_data, comments):
                        logging.info("(%d/%d) Saved data for video %s (%d comments/replies)",
                                     i, total_videos, video_id, len(comments))
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
