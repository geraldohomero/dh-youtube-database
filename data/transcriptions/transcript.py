import os
import sqlite3
import time
import logging
import concurrent.futures
from pathlib import Path
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_CONFIG = PROJECT_ROOT / "db" / "YouTubeStats.sqlite3"

def format_duration(seconds: float) -> str:
    """Format seconds into a string in hh:mm:ss format."""
    mins, sec = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}"

def get_videos_needing_transcript() -> list:
    """Retrieve the list of videoIds that need transcription."""
    with sqlite3.connect(DB_CONFIG) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT videoId FROM Videos WHERE videoTranscript IS NULL")
        video_ids = [row[0] for row in cursor.fetchall()]
    return video_ids

def get_transcript(video_id: str) -> tuple:
    """
    Get transcript for a video with language preference.
    Prioritizes manually created transcripts in Portuguese first, then English,
    then falls back to auto-generated transcripts if necessary.
    
    Returns:
        (success, transcript_text, language)
    """
    try:
        # Get list of available transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        
        # First try to find manually created transcripts
        manual_transcript = None
        for transcript in transcript_list:
            if not transcript.is_generated:
                if transcript.language_code == 'pt-BR' or transcript.language_code == 'pt':
                    # Found manual Portuguese transcript - ideal case
                    transcript_data = transcript.fetch()
                    return True, format_transcript_text(transcript_data), transcript.language_code
                elif transcript.language_code.startswith('en'):
                    # Store English transcript but keep looking for Portuguese
                    manual_transcript = (transcript.fetch(), transcript.language_code)
        
        # Use the stored manual English transcript if found
        if manual_transcript:
            return True, format_transcript_text(manual_transcript[0]), manual_transcript[1]
        
        # Try auto-generated transcripts with language preference
        try:
            # Try Portuguese first
            transcript = transcript_list.find_transcript(['pt', 'pt-BR'])
            transcript_data = transcript.fetch()
            return True, format_transcript_text(transcript_data), transcript.language_code
        except:
            # Fall back to English
            try:
                transcript = transcript_list.find_transcript(['en', 'en-US', 'en-GB'])
                transcript_data = transcript.fetch()
                return True, format_transcript_text(transcript_data), transcript.language_code
            except:
                # Last resort: just get any available transcript
                transcript = transcript_list.find_transcript(['pt', 'en', 'es'])
                transcript_data = transcript.fetch()
                return True, format_transcript_text(transcript_data), transcript.language_code
    
    except NoTranscriptFound:
        return False, "No transcript available for this video", None
    except TranscriptsDisabled:
        return False, "Transcripts are disabled for this video", None
    except Exception as e:
        return False, f"Error retrieving transcript: {str(e)}", None

def format_transcript_text(transcript_data: list) -> str:
    """Format transcript data into readable text with timestamps."""
    formatted_text = ""
    try:
        # Handle different types of transcript data
        if not isinstance(transcript_data, list):
            # If it's not a list, try to convert it to a list
            try:
                # Some transcript objects might have a .get_transcript() method
                if hasattr(transcript_data, 'get_transcript'):
                    transcript_data = transcript_data.get_transcript()
                # Other transcript objects might be iterable but not a list
                elif hasattr(transcript_data, '__iter__') and not isinstance(transcript_data, str):
                    transcript_data = list(transcript_data)
                else:
                    # If we can't handle it, log and return empty
                    logging.error(f"Unhandled transcript data type: {type(transcript_data)}")
                    return "Transcript format not supported"
            except Exception as e:
                logging.error(f"Error processing transcript data: {str(e)}")
                return "Error processing transcript"
        
        # Now process each item in the transcript data
        for item in transcript_data:
            # Handle both dictionary access and attribute access
            if isinstance(item, dict):
                start_time = format_timestamp(item.get('start', 0))
                text = item.get('text', '').replace('\n', ' ')
            else:
                # Try attribute access for objects
                start_time = format_timestamp(getattr(item, 'start', 0))
                text = getattr(item, 'text', '').replace('\n', ' ')
                
            formatted_text += f"[{start_time}] {text}\n"
            
    except Exception as e:
        logging.error(f"Error formatting transcript: {str(e)}")
        return f"Error formatting transcript: {str(e)}"
        
    return formatted_text

def format_timestamp(seconds: float) -> str:
    """Convert seconds to MM:SS format."""
    mins, secs = divmod(int(seconds), 60)
    return f"{mins:02d}:{secs:02d}"

def update_video_transcript(conn, video_id: str, transcript_text: str, language_code: str) -> None:
    """Update the videoTranscript field in the database with the transcript text."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE Videos 
            SET videoTranscript = ?,
                transcriptLanguage = ?
            WHERE videoId = ?
            """, (transcript_text, language_code, video_id)
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        if "no such column" in str(e):
            # Handle missing column by adding it
            logging.warning("Missing column detected. Updating schema...")
            try:
                # Add transcriptLanguage column if it doesn't exist
                cursor.execute("ALTER TABLE Videos ADD COLUMN transcriptLanguage TEXT")
                conn.commit()
                
                # Try the update again
                cursor.execute(
                    """
                    UPDATE Videos 
                    SET videoTranscript = ?,
                        transcriptLanguage = ?
                    WHERE videoId = ?
                    """, (transcript_text, language_code, video_id)
                )
                conn.commit()
            except Exception as inner_e:
                logging.error(f"Failed to update schema: {str(inner_e)}")
                # Fallback to just updating videoTranscript
                cursor.execute(
                    """
                    UPDATE Videos 
                    SET videoTranscript = ?
                    WHERE videoId = ?
                    """, (transcript_text, video_id)
                )
                conn.commit()
        else:
            raise

def process_video_transcript(video_id: str) -> tuple:
    """
    Process a single video: download transcript and update database.
    Returns tuple of (success, message)
    """
    try:
        # Each thread needs its own DB connection
        conn = sqlite3.connect(DB_CONFIG)
        
        # Check if transcript already exists in DB
        cursor = conn.cursor()
        cursor.execute("SELECT videoTranscript FROM Videos WHERE videoId = ?", (video_id,))
        row = cursor.fetchone()
        if row and row[0]:
            return True, f"Transcript already exists for video {video_id}"
        
        # Download transcript
        success, transcript_text, language = get_transcript(video_id)
        if not success:
            return False, transcript_text  # transcript_text contains error message in this case
        
        # Update database with the full transcript text
        update_video_transcript(conn, video_id, transcript_text, language)
        
        return True, f"Downloaded and stored {language} transcript for video {video_id} directly in database"
    except Exception as e:
        return False, f"Error processing transcript for video {video_id}: {str(e)}"
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def main():
    logging.info("Starting parallel transcript download script.")
    video_ids = get_videos_needing_transcript()
    total_videos = len(video_ids)
    logging.info("Total videos to process: %d", total_videos)
    start_time = time.time()
    processed_count = 0
    
    # Use a thread pool with max 5 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Submit all tasks to the executor
        future_to_video = {
            executor.submit(process_video_transcript, video_id): video_id 
            for video_id in video_ids
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_video):
            video_id = future_to_video[future]
            processed_count += 1
            
            try:
                success, message = future.result()
                if success:
                    logging.info("[%d/%d] %s", processed_count, total_videos, message)
                else:
                    logging.warning("[%d/%d] %s", processed_count, total_videos, message)
            except Exception as e:
                logging.error("[%d/%d] Unexpected error with video %s: %s", 
                              processed_count, total_videos, video_id, str(e))
            
            # Calculate and show progress
            elapsed = time.time() - start_time
            if processed_count > 0:
                avg_time = elapsed / processed_count
                estimated_total = avg_time * total_videos
                estimated_remaining = estimated_total - elapsed
                logging.info(
                    "Progress: %d/%d (%.1f%%). Est. total time: %s, Est. remaining: %s",
                    processed_count,
                    total_videos,
                    (processed_count/total_videos)*100,
                    format_duration(estimated_total),
                    format_duration(estimated_remaining)
                )
    
    logging.info("Script completed. Processed %d videos in %s", 
                 total_videos, format_duration(time.time() - start_time))

if __name__ == "__main__":
    main()
