import logging
import time
from typing import Tuple, Optional

try:
    from data.transcriptions.transcript import get_transcript, format_transcript_text
    TRANSCRIPT_AVAILABLE = True
except ImportError:
    logging.error("Failed to import transcript module. Transcript functionality will be disabled.")
    TRANSCRIPT_AVAILABLE = False

class TranscriptService:
    def __init__(self):
        self.available = TRANSCRIPT_AVAILABLE
    
    def get_transcript_with_retry(self, video_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Get transcript for a video with retry logic.
        Returns (success, transcript_text, transcript_lang)
        """
        if not self.available:
            return False, "Transcript functionality not available", None
        
        try:
            success, transcript_text, transcript_lang = get_transcript(video_id)
            return success, transcript_text, transcript_lang
        except Exception as e:
            logging.warning("First transcript attempt failed for video %s: %s. Trying again...", video_id, e)
            # Wait briefly before retry
            time.sleep(1)
            try:
                success, transcript_text, transcript_lang = get_transcript(video_id)
                return success, transcript_text, transcript_lang
            except Exception as e:
                logging.error("Second transcript attempt also failed for video %s: %s", video_id, e)
                return False, None, None
    
    def format_transcript(self, transcript_data) -> str:
        """Format transcript data if available."""
        if not self.available:
            return "Transcript formatting not available"
        return format_transcript_text(transcript_data)
