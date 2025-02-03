import sqlite3
from googleapiclient.discovery import build
from datetime import datetime, date
import time
from sqlite3 import register_adapter
import logging
from dataclasses import dataclass
from typing import Optional
from config import get_api_key, CHANNEL_IDS, DB_CONFIG

# Initialize YouTube API using the rotated API key
youtube = build('youtube', 'v3', developerKey=get_api_key())

# Setup logging
logging.basicConfig(level=logging.INFO)

def adapt_date(d: date) -> str:
    """Adapter function to store Python date as ISO string in SQLite."""
    return d.isoformat()

register_adapter(date, adapt_date)

@dataclass
class ChannelDetails:
    channel_id: str
    channel_name: str
    subscriber_count: int

def get_channel_details(channel_id: str) -> Optional[ChannelDetails]:
    """
    Fetch channel details from YouTube API by channel ID.
    
    Returns ChannelDetails or None if not found or on error.
    """
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()
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
    Uses a context manager for safe connection closing.
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
        logging.error("Database error: %s", e)

def get_video_details(video_id, channel_id):
    """Get detailed video information"""
    try:
        request = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        )
        response = request.execute()
        
        if response['items']:
            video = response['items'][0]
            return {
                'videoId': video_id,
                'channelId': channel_id,
                'videoTitle': video['snippet']['title'],
                'videoAudio': None,
                'videoTranscript': None,
                'viewCount': int(video['statistics'].get('viewCount', 0)),
                'likeCount': int(video['statistics'].get('likeCount', 0)),
                'commentCount': int(video['statistics'].get('commentCount', 0)),
                'publishedAt': datetime.strptime(
                    video['snippet']['publishedAt'], 
                    '%Y-%m-%dT%H:%M:%SZ'
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'collectedDate': datetime.now().date()
            }
        return None
    except Exception as e:
        print(f"Error fetching video details: {str(e)}")
        return None

def get_video_comments(video_id):
    """Fetch video comments and replies with proper IDs"""
    try:
        comments = []
        next_page_token = None
        current_date = datetime.now().date()
        
        while True:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                # Process top-level comment
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
                    'publishedAt': datetime.strptime(
                        comment_snippet['publishedAt'], 
                        '%Y-%m-%dT%H:%M:%SZ'
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'collectedDate': current_date
                }
                
                # Process replies
                replies = []
                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        reply_snippet = reply['snippet']
                        replies.append({
                            'commentId': reply['id'],
                            'videoId': video_id,
                            'parentCommentId': comment_id,
                            'userId': reply_snippet['authorChannelId']['value'],
                            'userName': reply_snippet['authorDisplayName'],
                            'content': reply_snippet['textDisplay'],
                            'likeCount': reply_snippet['likeCount'],
                            'publishedAt': datetime.strptime(
                                reply_snippet['publishedAt'], 
                                '%Y-%m-%dT%H:%M:%SZ'
                            ).strftime('%Y-%m-%d %H:%M:%S'),
                            'collectedDate': current_date
                        })
                
                comments.append(comment_data)
                comments.extend(replies)  # Add replies directly to comments list
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        
        return comments
    except Exception as e:
        print(f"Error fetching comments for video {video_id}: {str(e)}")
        return []

def get_channel_videos(channel_id):
    """Get list of all video IDs for a channel"""
    try:
        video_ids = []
        next_page_token = None
        
        while True:
            request = youtube.search().list(
                part="snippet",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token,
                type="video"
            )
            response = request.execute()
            
            video_ids.extend([
                item['id']['videoId'] 
                for item in response['items'] 
                if item['id']['kind'] == "youtube#video"
            ])
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        
        print(f"Found {len(video_ids)} videos for channel {channel_id}")
        return video_ids
    except Exception as e:
        print(f"Error fetching channel videos: {str(e)}")
        return []

def save_to_database(conn, cursor, channel_data, video_data, comments):
    """Save all collected data to database"""
    try:
        # Convert dates to ISO format strings before saving
        channel_collected_date = channel_data['dayCollected'].isoformat()
        video_collected_date = video_data['collectedDate'].isoformat()
        
        # Save channel data
        cursor.execute("""
            INSERT INTO Channels (
                channelId, channelName, dayCollected, 
                numberOfSubscribers, numberOfVideos
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(channelId) DO UPDATE SET
                channelName = excluded.channelName,
                dayCollected = excluded.dayCollected,
                numberOfSubscribers = excluded.numberOfSubscribers,
                numberOfVideos = excluded.numberOfVideos
        """, (
            channel_data['channelId'],
            channel_data['channelName'],
            channel_collected_date,  # Use the converted date
            channel_data['numberOfSubscribers'],
            channel_data['numberOfVideos']
        ))

        # Insert/Update Video
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
            video_collected_date  # Use the converted date
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
                comment_collected_date  # Use the converted date
            ))

        conn.commit()
        return True
    except Exception as e:
        print(f"Database error: {str(e)}")
        conn.rollback()
        return False

def video_exists_in_database(cursor, video_id):
    """Check if a video exists in the database"""
    try:
        cursor.execute("SELECT COUNT(*) FROM Videos WHERE videoId = ?", (video_id,))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        print(f"Error checking video existence: {str(e)}")
        return False

def main():
    conn = sqlite3.connect(DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        for channel_id in CHANNEL_IDS:
            details = get_channel_details(channel_id)
            if not details:
                print(f"Skipping channel {channel_id}")
                continue

            video_ids = get_channel_videos(channel_id)
            
            # Build a dictionary from ChannelDetails with extra required fields.
            channel_data = {
                'channelId': details.channel_id,
                'channelName': details.channel_name,
                'dayCollected': datetime.now().date(),  # current collected date
                'numberOfSubscribers': details.subscriber_count,
                'numberOfVideos': len(video_ids)
            }

            for video_id in video_ids:
                video_data = get_video_details(video_id, channel_id)
                if not video_data:
                    continue
                
                if video_exists_in_database(cursor, video_data['videoId']):
                    print(f"Video {video_data['videoId']} already exists in the database. Skipping...")
                    continue
                
                comments = get_video_comments(video_id)
                if save_to_database(conn, cursor, channel_data, video_data, comments):
                    print(f"Saved data for video {video_id} ({len(comments)} comments/replies)")
                else:
                    print(f"Failed to save data for video {video_id}")
                
                time.sleep(1)  # Respect API quota
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
