import os
import sqlite3
import time
import logging
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

def main():
    logging.info("Starting audio download script.")
    project_root = PROJECT_ROOT
    video_ids = get_video_ids()
    total_videos = len(video_ids)
    logging.info("Total videos to process: %d", total_videos)
    start_time = time.time()

    conn = sqlite3.connect(DB_CONFIG)
    cursor = conn.cursor()

    try:
        for index, video_id in enumerate(video_ids):
            processed = index + 1
            elapsed = time.time() - start_time

            # Calculate estimates only if at least one video is processed
            if processed > 0:
                avg_time = elapsed / processed
                estimated_total = avg_time * total_videos
                estimated_remaining = estimated_total - elapsed
            else:
                avg_time = estimated_total = estimated_remaining = 0

            logging.info(
                "Processing video %d/%d. Estimated total script time: %s, Time remaining: %s",
                processed,
                total_videos,
                format_duration(estimated_total),
                format_duration(estimated_remaining)
            )

            # Check in the DB if audio already exists
            cursor.execute("SELECT videoAudio FROM Videos WHERE videoId = ?", (video_id,))
            row = cursor.fetchone()
            if row and row[0]:
                logging.info("Audio already exists in DB for video %s: %s", video_id, row[0])
                continue

            # Build paths and URL
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            audio_file = AUDIO_DIR / f"{video_id}.mp3"
            relative_audio_file = os.path.relpath(audio_file, project_root)

            # Double-check on the file system
            if audio_file.exists():
                logging.info("Audio file already exists for video %s: %s", video_id, relative_audio_file)
                continue

            logging.info("Downloading audio for video %s ...", video_id)
            try:
                download_audio(video_url, str(audio_file))
                update_video_audio_path(cursor, video_id, relative_audio_file)
                conn.commit()
                logging.info("Downloaded and updated audio for video %s", video_id)
            except Exception as e:
                logging.error("Error processing video %s: %s", video_id, e)
    finally:
        cursor.close()
        conn.close()
        logging.info("Script completed.")

if __name__ == "__main__":
    main()