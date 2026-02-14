import logging
import sys
from pathlib import Path

# Add the project root to path to enable imports
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from config import CHANNEL_IDS
from database.db_manager import DatabaseManager
from services.video_service import VideoProcessingService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    """
    Main orchestration function for processing YouTube channels.
    """
    db_manager = DatabaseManager()
    video_service = VideoProcessingService()
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Ensure database schema is up to date
            db_manager.ensure_transcript_column_exists(cursor)
            conn.commit()
                
            # Fetch all existing video IDs at once for fast lookups
            existing_video_ids = db_manager.get_existing_video_ids(cursor)
            logging.info(f"Found {len(existing_video_ids)} existing videos in database")

            # Process each channel
            for channel_id in CHANNEL_IDS:
                video_service.process_channel_videos(channel_id, existing_video_ids)
                
        finally:
            cursor.close()

if __name__ == "__main__":
    main()
