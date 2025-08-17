import os
import sqlite3
import time
import logging
import concurrent.futures
from pathlib import Path
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from youtube_transcript_api.proxies import WebshareProxyConfig

# ANSI color codes for terminal output
GREEN = '\033[92m'
RESET = '\033[0m'

# Load environment variables
load_dotenv()

# Configure logging with color support
class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages"""
    
    def format(self, record):
        # Format the message first using the parent formatter
        message = super().format(record)
        
        # Add color for success messages about transcripts
        if record.levelno == logging.INFO and ("Downloaded and stored" in record.getMessage() or 
                                              "Transcript already exists" in record.getMessage()):
            return f"{GREEN}{message}{RESET}"
        
        return message

# Configure logging with custom formatter
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create console handler with custom formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = ColoredFormatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_CONFIG = PROJECT_ROOT / "db" / "YouTubeStats.sqlite3"

def create_youtube_api():
    """Create YouTubeTranscriptApi instance with proxy configuration."""
    # Obtendo credenciais do proxy do arquivo .env
    proxy_username = os.getenv('WEBSHARE_PROXY_USERNAME')
    proxy_password = os.getenv('WEBSHARE_PROXY_PASSWORD')
    
    # Verificando se as credenciais existem
    if not proxy_username or not proxy_password:
        logging.warning("Credenciais de proxy não encontradas no arquivo .env")
        logging.warning("Configure WEBSHARE_PROXY_USERNAME e WEBSHARE_PROXY_PASSWORD no arquivo .env")
        # Tentando sem proxy como fallback
        return YouTubeTranscriptApi()
    else:
        # Configurando o proxy da Webshare
        proxy_config = WebshareProxyConfig(
            proxy_username=proxy_username,
            proxy_password=proxy_password,
            # Opcional: você pode filtrar por localização
            # filter_ip_locations=["br", "us"],
        )
        
        # Instanciando a API com a configuração de proxy
        logging.info("Usando proxy Webshare para evitar bloqueio de IP")
        return YouTubeTranscriptApi(proxy_config=proxy_config)

def format_duration(seconds: float) -> str:
    """Format seconds into a string in hh:mm:ss format."""
    mins, sec = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}"

def get_videos_needing_transcript() -> list:
    """Retrieve the list of videoIds that need transcription."""
    with sqlite3.connect(DB_CONFIG) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT videoId FROM Videos WHERE videoTranscript IS NULL ORDER BY rowid DESC")
        video_ids = [row[0] for row in cursor.fetchall()]
    return video_ids

def transcript_exists(video_id: str) -> bool:
    """
    Check if a transcript exists for the video using the API (much faster than fetching).
    Returns True if transcript exists, False otherwise.
    """
    try:
        # Create API instance without fetching the content
        api = YouTubeTranscriptApi()
        api.list(video_id)
        logging.info(f"Transcript available for {video_id} (API check)")
        return True
    except (NoTranscriptFound, TranscriptsDisabled):
        return False
    except Exception as e:
        error_msg = str(e).lower()
        # If it's about the video itself, it likely doesn't have transcripts
        if "video is no longer available" in error_msg or "video unavailable" in error_msg:
            return False
        if "age-restricted" in error_msg or "age restricted" in error_msg:
            return False
            
        # For IP blocks or any other error, we'll assume it might have transcripts
        # and try with the proxy in the actual fetch
        if "ip" in error_msg and "block" in error_msg:
            logging.info(f"IP block detected during API check for {video_id}, will try with proxy")
        else:
            logging.info(f"Error during API check for {video_id}, will try with proxy: {e}")
            
        return True

def get_transcript(video_id: str) -> tuple:
    """
    Get transcript for a video using YouTube Transcript API with Webshare proxy.
    
    Returns:
        (success, transcript_text, language)
    """
    # First quickly check if transcript exists using the API
    if not transcript_exists(video_id):
        logging.info(f"No transcript available for {video_id} (API check)")
        return False, "No transcript available for this video", None
    
    try:
        logging.info(f"Getting transcript for {video_id} using YouTube API with Webshare proxy")
        
        # Create API instance with proxy
        ytt_api = create_youtube_api()
        
        # Fetch transcript with preference for Portuguese then English
        transcript = ytt_api.fetch(video_id, languages=['pt', 'pt-BR', 'en'])
        
        if not transcript or not hasattr(transcript, 'snippets'):
            return False, "Failed to fetch transcript data", None
        
        # Format the transcript into text
        formatted_transcript = format_transcript_from_api(transcript)
        
        return True, formatted_transcript, transcript.language_code
        
    except (NoTranscriptFound, TranscriptsDisabled):
        return False, "No transcript available for this video", None
    except Exception as e:
        logging.error(f"Error getting transcript for {video_id}: {e}")
        return False, f"Error: {str(e)}", None

def format_transcript_from_api(transcript) -> str:
    """
    Format transcript data from the YouTube Transcript API into readable text.
    
    Args:
        transcript: A FetchedTranscript object from the YouTube Transcript API
        
    Returns:
        Formatted transcript text with timestamps
    """
    formatted_lines = []
    
    try:
        # Process each snippet in the transcript
        for snippet in transcript.snippets:
            timestamp = format_timestamp_from_seconds(snippet.start)
            text = snippet.text.replace('\n', ' ')
            formatted_lines.append(f"[{timestamp}] {text}")
        
        return "\n".join(formatted_lines)
    except Exception as e:
        logging.error(f"Error formatting transcript: {e}")
        
        # Fallback to raw data if available
        try:
            raw_data = transcript.to_raw_data()
            formatted_lines = []
            for item in raw_data:
                timestamp = format_timestamp_from_seconds(item.get('start', 0))
                text = item.get('text', '').replace('\n', ' ')
                formatted_lines.append(f"[{timestamp}] {text}")
            return "\n".join(formatted_lines)
        except:
            return f"Error formatting transcript: {str(e)}"

def format_timestamp_from_seconds(seconds: float) -> str:
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
        
        # Download transcript using YouTube API with Webshare proxy
        success, transcript_text, language = get_transcript(video_id)
        if not success:
            return False, transcript_text  # contains error message
        
        # Update database with the transcript
        update_video_transcript(conn, video_id, transcript_text, language)
        
        # Success message (will be colored green by the formatter)
        return True, f"Downloaded and stored {language} transcript for video {video_id} using YouTube API"
    except Exception as e:
        return False, f"Error processing transcript for video {video_id}: {str(e)}"
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def main():
    logging.info("Starting transcript download script using YouTube API with Webshare proxy.")
    video_ids = get_videos_needing_transcript()
    total_videos = len(video_ids)
    logging.info("Total videos to process: %d", total_videos)
    logging.info("Processing videos from newest to oldest")
    start_time = time.time()
    processed_count = 0
    
    # Use a thread pool with reduced workers to avoid overwhelming the system
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
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
                    # Success messages will be colored green by the formatter
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
