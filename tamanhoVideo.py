import logging
from typing import List

from api.youtube_client import YouTubeAPIClient
from database.db_manager import DatabaseManager


BATCH_SIZE = 50


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def get_video_ids_without_length(cursor) -> List[str]:
    cursor.execute("""
        SELECT videoId
        FROM Videos
        WHERE videoLength IS NULL OR videoLength <= 0
    """)
    return [row[0] for row in cursor.fetchall()]


def update_video_lengths() -> None:
    db_manager = DatabaseManager()
    youtube_client = YouTubeAPIClient()

    with db_manager.get_connection() as conn:
        cursor = conn.cursor()

        db_manager.ensure_video_length_column_exists(cursor)
        conn.commit()

        video_ids = get_video_ids_without_length(cursor)
        total_videos = len(video_ids)

        if total_videos == 0:
            logging.info("No videos pending duration update.")
            return

        logging.info("Found %d videos without duration. Processing in batches of %d.", total_videos, BATCH_SIZE)

        processed = 0
        updated = 0

        for batch_index, batch_ids in enumerate(chunked(video_ids, BATCH_SIZE), start=1):
            durations = youtube_client.get_video_lengths_batch(batch_ids)

            for video_id in batch_ids:
                duration_seconds = durations.get(video_id, 0)
                cursor.execute(
                    "UPDATE Videos SET videoLength = ? WHERE videoId = ?",
                    (duration_seconds, video_id)
                )
                if duration_seconds > 0:
                    updated += 1

            conn.commit()
            processed += len(batch_ids)
            logging.info(
                "Batch %d finished: %d/%d videos processed. Durations found so far: %d",
                batch_index,
                processed,
                total_videos,
                updated
            )

        logging.info("Done. Processed %d videos and stored duration for %d videos.", processed, updated)


if __name__ == "__main__":
    update_video_lengths()
