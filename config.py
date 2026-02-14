import os
import logging
from dotenv import load_dotenv
from filelock import FileLock
from typing import List

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Environment variables
API_KEYS: List[str] = os.getenv("YOUTUBE_API_KEYS", "").split(",")
CHANNEL_IDS: List[str] = os.getenv("CHANNEL_IDS", "").split(",")
DB_CONFIG: str = os.getenv("DB_CONFIG", "./db/YouTubeStats.sqlite3")

# File paths for API key rotation
BASE_DIR = os.path.dirname(__file__)
KEY_TRACK_FILE = os.path.join(BASE_DIR, "apikey_index.txt")
LOCK_FILE = KEY_TRACK_FILE + ".lock"

def get_api_key() -> str:
    """
    Retrieves the current API key from the KEY_TRACK_FILE in a thread-safe manner.
    Does not rotate the key.
    
    Returns:
        The current YouTube API key.
    """
    try:
        with FileLock(LOCK_FILE, timeout=5):
            if os.path.exists(KEY_TRACK_FILE):
                with open(KEY_TRACK_FILE, "r") as f:
                    index = int(f.read().strip())
            else:
                index = 0
    except Exception as e:
        logging.error("Error reading API key index: %s", e)
        index = 0

    current_key = API_KEYS[index % len(API_KEYS)] if API_KEYS else ""
    logging.info("Current API key (index %d): %s", index, current_key)
    return current_key

def rotate_api_key() -> str:
    """
    Rotates the API key when a quota error is met.
    Updates the KEY_TRACK_FILE to point to the next API key in sequence,
    and returns the new API key.
    
    Returns:
        The new YouTube API key after rotation.
    """
    try:
        with FileLock(LOCK_FILE, timeout=5):
            if os.path.exists(KEY_TRACK_FILE):
                with open(KEY_TRACK_FILE, "r") as f:
                    index = int(f.read().strip())
            else:
                index = 0

            new_index = (index + 1) % len(API_KEYS) if API_KEYS else 0
            try:
                with open(KEY_TRACK_FILE, "w") as f:
                    f.write(str(new_index))
                logging.info("Rotating API key: changed from index %d to %d", index, new_index)
            except Exception as e:
                logging.error("Failed to update API key index file: %s", e)
                new_index = index  # fallback to the current index if write fails

    except Exception as e:
        logging.error("Error during API key rotation: %s", e)
        new_index = 0

    new_key = API_KEYS[new_index] if API_KEYS else ""
    logging.info("New API key (index %d): %s", new_index, new_key)
    return new_key