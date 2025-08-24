import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

from api.youtube_client import YouTubeAPIClient
from database.db_manager import DatabaseManager
from services.transcript_service import TranscriptService
from models.data_models import ChannelDetails, ChannelData

class VideoProcessingService:
    def __init__(self):
        self.youtube_client = YouTubeAPIClient()
        self.db_manager = DatabaseManager()
        self.transcript_service = TranscriptService()
    
    def process_video_details(self, video_id: str, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Process video details including transcript fetching.
        """
        video_data = self.youtube_client.get_video_details(video_id, channel_id)
        if not video_data:
            return None
        
        # Add transcript information
        success, transcript_text, transcript_lang = self.transcript_service.get_transcript_with_retry(video_id)
        video_data.update({
            'videoTranscript': transcript_text if success else None,
            'transcriptLanguage': transcript_lang if success else None,
            'collectedDate': datetime.now().date()
        })
        
        return video_data
    
    def process_channel_videos(
        self, 
        channel_id: str, 
        existing_video_ids: set,
        published_after: Optional[str] = None,
        published_before: Optional[str] = None
    ) -> None:
        """
        Process all videos for a given channel.
        """
        details = self.youtube_client.get_channel_details(channel_id)
        if not details:
            logging.warning("Skipping channel %s due to missing details.", channel_id)
            return

        # Only pass date parameters if they are provided
        kwargs = {}
        if published_after is not None:
            kwargs['published_after'] = published_after
        if published_before is not None:
            kwargs['published_before'] = published_before

        video_ids = self.youtube_client.get_channel_videos(channel_id, **kwargs)
        channel_data = {
            'channelId': details.channel_id,
            'channelName': details.channel_name,
            'dayCollected': datetime.now().date(),
            'numberOfSubscribers': details.subscriber_count,
            'numberOfVideos': len(video_ids)
        }

        total_videos = len(video_ids)
        new_videos = 0
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            for i, video_id in enumerate(video_ids, start=1):
                # Fast in-memory check instead of database query
                if video_id in existing_video_ids:
                    if i % 10 == 0:  # Log only every 10th video to reduce output
                        logging.info("(%d/%d) Video %s already exists in database. Skipping.", 
                                    i, total_videos, video_id)
                    continue
                    
                new_videos += 1
                video_data = self.process_video_details(video_id, channel_id)
                if not video_data:
                    continue
                
                # Check if comments are enabled for the video.
                if not video_data.get('commentsEnabled'):
                    logging.info("Comments are disabled for video %s. Skipping fetching comments.", video_id)
                    comments = []
                else:
                    comments = self.youtube_client.get_video_comments(video_id)
                
                if self.db_manager.save_video_and_comments(conn, cursor, channel_data, video_data, comments):
                    transcript_status = "with transcript" if video_data.get('videoTranscript') else "without transcript"
                    logging.info("(%d/%d) Saved data for video %s (%d comments/replies, %s)",
                                 i, total_videos, video_id, len(comments), transcript_status)
                else:
                    logging.error("Failed to save data for video %s", video_id)
                
                time.sleep(1)  # Respect API quota

            remaining_videos = total_videos - i if total_videos > 0 else 0
            logging.info("Processing complete for channel %s. %d videos remaining (if any further processing is needed).",
                         channel_id, remaining_videos)
