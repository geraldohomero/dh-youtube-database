import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import get_api_key, rotate_api_key
from models.data_models import ChannelDetails

class YouTubeAPIClient:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=get_api_key())
    
    def safe_execute(self, request) -> Dict[str, Any]:
        """
        Executes a YouTube API request.
        Rotates API key and reattempts the request if quota is exceeded.
        """
        try:
            return request.execute()
        except HttpError as e:
            error_content = e.content.decode('utf-8')
            if e.resp.status == 403 and "quotaExceeded" in error_content:
                logging.info("Quota exceeded. Rotating API key...")
                new_api_key = rotate_api_key()
                self.youtube = build('youtube', 'v3', developerKey=new_api_key)
                return request.execute()
            else:
                logging.error("YouTube API error: %s", e)
                raise

    def get_channel_details(self, channel_id: str) -> Optional[ChannelDetails]:
        """
        Fetch channel details from YouTube API by channel ID.
        Returns ChannelDetails or None on error.
        """
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics",
                id=channel_id
            )
            response = self.safe_execute(request)
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

    def get_video_details(self, video_id: str, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed video information from YouTube API.
        Returns a dictionary with video details or None on error.
        Adds a 'commentsEnabled' flag to indicate if comments are available.
        """
        try:
            channel_id = channel_id.strip() if channel_id else channel_id
            
            request = self.youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            )
            response = self.safe_execute(request)
            if response.get('items'):
                video = response['items'][0]
                published_at = datetime.strptime(video['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                comments_enabled = 'commentCount' in video['statistics']
                
                return {
                    'videoId': video_id,
                    'channelId': channel_id,
                    'videoTitle': video['snippet']['title'],
                    'videoAudio': None,
                    'viewCount': int(video['statistics'].get('viewCount', 0)),
                    'likeCount': int(video['statistics'].get('likeCount', 0)),
                    'commentCount': int(video['statistics']['commentCount']) if comments_enabled else 0,
                    'publishedAt': published_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'commentsEnabled': comments_enabled
                }
            else:
                logging.warning("No video details found for video id: %s", video_id)
            return None
        except Exception as e:
            logging.error("Error fetching video details for video id %s: %s", video_id, e)
            return None

    def get_video_comments(self, video_id: str) -> List[Dict[str, Any]]:
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
                    request = self.youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=next_page_token
                    )
                    response = self.safe_execute(request)
                except HttpError as e:
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

        except Exception as e:
            logging.error("Error fetching comments for video %s: %s", video_id, e)
        return comments

    def get_channel_videos(
        self, 
        channel_id: str, 
        published_after: Optional[str] = None, 
        published_before: Optional[str] = None
    ) -> List[str]:
        """
        Get list of all video IDs for a given channel, with optional date filters.
        Uses search.list endpoint when date filters are provided, channels().list otherwise.
        """
        video_ids = []
        next_page_token = None
        
        try:
            # Use search endpoint only when date filters are provided
            if published_after is not None or published_before is not None:
                logging.info(f"Fetching videos for channel {channel_id} between {published_after} and {published_before}")
                
                while True:
                    search_request = self.youtube.search().list(
                        part="snippet",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token,
                        type='video',
                        order='date',
                        publishedAfter=published_after,
                        publishedBefore=published_before
                    )
                    search_response = self.safe_execute(search_request)
                    
                    for item in search_response.get('items', []):
                        if item['id']['kind'] == 'youtube#video':
                            video_ids.append(item['id']['videoId'])
                    
                    next_page_token = search_response.get('nextPageToken')
                    if not next_page_token:
                        break
                    
                    time.sleep(0.5)
            else:
                # Use channels endpoint to get uploads playlist for all videos
                logging.info(f"Fetching all videos for channel {channel_id}")
                
                channel_request = self.youtube.channels().list(
                    part="contentDetails",
                    id=channel_id
                )
                channel_response = self.safe_execute(channel_request)
                
                if channel_response.get('items'):
                    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                    
                    while True:
                        playlist_request = self.youtube.playlistItems().list(
                            part="snippet",
                            playlistId=uploads_playlist_id,
                            maxResults=50,
                            pageToken=next_page_token
                        )
                        playlist_response = self.safe_execute(playlist_request)
                        
                        for item in playlist_response.get('items', []):
                            video_ids.append(item['snippet']['resourceId']['videoId'])
                        
                        next_page_token = playlist_response.get('nextPageToken')
                        if not next_page_token:
                            break
                        
                        time.sleep(0.5)

            channel_response = self.youtube.channels().list(
                part="snippet",
                id=channel_id
            ).execute()
            if channel_response.get("items"):
                channel_name = channel_response["items"][0]["snippet"]["title"]
                logging.info("Found %d videos for channel '%s' (ID: %s) in the specified period.", len(video_ids), channel_name, channel_id)
            else:
                logging.info("Found %d videos for channel (ID: %s) in the specified period.", len(video_ids), channel_id)
        except Exception as e:
            logging.error("Error fetching channel videos for channel %s: %s", channel_id, e)
        
        return video_ids
