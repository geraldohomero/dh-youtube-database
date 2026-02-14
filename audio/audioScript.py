import os
import sqlite3
import time
import logging
import concurrent.futures
from yt_dlp import YoutubeDL
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_CONFIG = PROJECT_ROOT / "db" / "YouTubeStats.sqlite3"
AUDIO_DIR = Path(__file__).resolve().parent / "audio_files"  # store audio files in a dedicated folder

# Ensure AUDIO_DIR exists
AUDIO_DIR.mkdir(parents=True, exist_ok=True)

def download_audio(video_url: str, output_path: str) -> None:
    """
    Download the smallest possible audio track from the given YouTube URL 
    and convert it to a low quality MP3.
    """
    ydl_opts = {
        'format': 'worstaudio',  # choose the smallest available audio format
        'outtmpl': output_path,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '64',  # low quality for a smaller file size
        }],
        'quiet': True,
        'no_warnings': True,
    }
    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
    except Exception as e:
        logging.error("Error downloading audio from %s: %s", video_url, e)
        raise

def update_video_audio_path(cursor, video_id: str, audio_path: str) -> None:
    """
    Update the videoAudio field for the given video in the database.
    """
    cursor.execute(
        """
        UPDATE Videos 
        SET videoAudio = ?
        WHERE videoId = ?
        """, (audio_path, video_id)
    )

def get_video_ids() -> list:
    """
    Retrieve the list of videoId values from the Videos table.
    """
    with sqlite3.connect(DB_CONFIG) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT videoId FROM Videos")
        video_ids = [row[0] for row in cursor.fetchall()]
    return video_ids

def format_duration(seconds: float) -> str:
    """
    Format seconds into a string in hh:mm:ss format.
    """
    mins, sec = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}"

def process_video(video_id: str, project_root: Path):
    """
    Process a single video: check if it exists and download if needed.
    Returns tuple of (success, message)
    """
    try:
        # Each thread needs its own DB connection
        conn = sqlite3.connect(DB_CONFIG)
        cursor = conn.cursor()
        
        # Check in the DB if audio already exists
        cursor.execute("SELECT videoAudio FROM Videos WHERE videoId = ?", (video_id,))
        row = cursor.fetchone()
        if row and row[0]:
            return (True, f"Audio already exists in DB for video {video_id}: {row[0]}")
            
        # Build paths and URL
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        audio_file = AUDIO_DIR / f"{video_id}.mp3"
        relative_audio_file = os.path.relpath(audio_file, project_root)
            
        # Double-check on the file system
        if audio_file.exists():
            return (True, f"Audio file already exists for video {video_id}: {relative_audio_file}")
            
        # Download audio
        download_audio(video_url, str(audio_file))
        update_video_audio_path(cursor, video_id, relative_audio_file)
        conn.commit()
        return (True, f"Downloaded and updated audio for video {video_id}")
    except Exception as e:
        return (False, f"Error processing video {video_id}: {str(e)}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

def main():
    logging.info("Starting parallel audio download script.")
    project_root = PROJECT_ROOT
    video_ids = get_video_ids()
    total_videos = len(video_ids)
    logging.info("Total videos to process: %d", total_videos)
    start_time = time.time()
    processed_count = 0
    
    # Use a thread pool with max 5 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks to the executor
        future_to_video = {
            executor.submit(process_video, video_id, project_root): video_id 
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
                    logging.error("[%d/%d] %s", processed_count, total_videos, message)
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