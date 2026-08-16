import os
import sqlite3
import time
import logging
import concurrent.futures
from pathlib import Path
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from youtube_transcript_api.proxies import WebshareProxyConfig
import random

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
        cursor.execute("SELECT videoId FROM Videos WHERE videoTranscript IS NULL OR videoTranscript = '' ORDER BY rowid ASC")
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
    Returns:
        (success, transcript_text, language)
    """
    ytt_api = create_youtube_api()

    max_attempts = 5
    base_delay = 2.0

    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"Getting transcript for {video_id} (attempt {attempt}/{max_attempts})")
            transcript = ytt_api.fetch(video_id, languages=['pt', 'pt-BR', 'en'])

            if not transcript or not hasattr(transcript, 'snippets'):
                return False, "Failed to fetch transcript data", None

            formatted_transcript = format_transcript_from_api(transcript)
            return True, formatted_transcript, transcript.language_code

        except (NoTranscriptFound, TranscriptsDisabled):
            return False, "No transcript available for this video", None
        except Exception as e:
            error_msg = str(e).lower()

            # bloqueios/rate-limit típicos
            retryable = (
                "429" in error_msg
                or "too many requests" in error_msg
                or "/sorry/" in error_msg
                or "temporarily blocked" in error_msg
            )

            if attempt == max_attempts or not retryable:
                logging.error(f"Error getting transcript for {video_id}: {e}")
                return False, f"Error: {str(e)}", None

            sleep_s = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            logging.warning(
                f"Rate-limit/block detected for {video_id}. Retrying in {sleep_s:.1f}s..."
            )
            time.sleep(sleep_s)

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

def fetch_transcript_worker(video_id: str) -> tuple:
    """
    Download transcript for a single video using the YouTube API with Webshare proxy.
    Returns a tuple of (video_id, success, transcript_text, language)
    """
    try:
        success, transcript_text, language = get_transcript(video_id)
        return video_id, success, transcript_text, language
    except Exception as e:
        return video_id, False, f"Error: {str(e)}", None

def main():
    logging.info("Starting transcript download script using YouTube API with Webshare proxy.")
    video_ids = get_videos_needing_transcript()
    total_videos = len(video_ids)
    logging.info("Total videos to process: %d", total_videos)
    logging.info("Processing videos from oldest to newest")
    start_time = time.time()
    processed_count = 0
    
    # Establish a single database connection for sequential writing
    conn = sqlite3.connect(DB_CONFIG)
    
    try:
        # Use a thread pool with 10 workers to fetch transcripts in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks to the executor
            future_to_video = {
                executor.submit(fetch_transcript_worker, video_id): video_id 
                for video_id in video_ids
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_video):
                processed_count += 1
                video_id = future_to_video[future]
                
                try:
                    _, success, transcript_text, language = future.result()
                    
                    if success:
                        # Write to database sequentially
                        update_video_transcript(conn, video_id, transcript_text, language)
                        logging.info("[%d/%d] Downloaded and stored %s transcript for video %s using YouTube API", 
                                     processed_count, total_videos, language, video_id)
                    else:
                        # Handle failures/unavailable videos by writing to DB sequentially
                        if transcript_text == "No transcript available for this video":
                            update_video_transcript(conn, video_id, "ND", "ND")
                            logging.info("[%d/%d] No transcript available for video %s. Marked as 'ND' in DB.", 
                                         processed_count, total_videos, video_id)
                        elif "the video is no longer available" in transcript_text.lower():
                            update_video_transcript(conn, video_id, "The video is no longer available", "unavailable")
                            logging.info("[%d/%d] Video %s is no longer available. Marked in DB.", 
                                         processed_count, total_videos, video_id)
                        elif "this video is private" in transcript_text.lower():
                            update_video_transcript(conn, video_id, "This video is private", "unavailable")
                            logging.info("[%d/%d] Video %s is private. Marked in DB.", 
                                         processed_count, total_videos, video_id)
                        elif "age-restricted" in transcript_text.lower():
                            update_video_transcript(conn, video_id, "This video is age-restricted", "unavailable")
                            logging.info("[%d/%d] Video %s is age-restricted. Marked in DB.", 
                                         processed_count, total_videos, video_id)
                        else:
                            logging.warning("[%d/%d] Error getting transcript for video %s: %s", 
                                            processed_count, total_videos, video_id, transcript_text)
                            
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
    finally:
        conn.close()
        logging.info("Script completed. Processed %d videos in %s", 
                     total_videos, format_duration(time.time() - start_time))

if __name__ == "__main__":
    main()
