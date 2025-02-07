# Python
import os
from dotenv import load_dotenv
from filelock import FileLock

load_dotenv()

API_KEYS = os.getenv("YOUTUBE_API_KEYS").split(",")
CHANNEL_IDS = os.getenv("CHANNEL_IDS").split(",")
DB_CONFIG = os.getenv("DB_CONFIG", "./db/YouTubeStats.sqlite3")

# File to track the current API key index
KEY_TRACK_FILE = os.path.join(os.path.dirname(__file__), "apikey_index.txt")
LOCK_FILE = KEY_TRACK_FILE + ".lock"

def get_api_key() -> str:
    """
    Retrieves the current API key from the KEY_TRACK_FILE in a thread-safe manner.
    Does not rotate the key.
    """
    with FileLock(LOCK_FILE, timeout=5):
        try:
            if os.path.exists(KEY_TRACK_FILE):
                with open(KEY_TRACK_FILE, "r") as f:
                    index = int(f.read().strip())
            else:
                index = 0
        except Exception:
            index = 0

    current_key = API_KEYS[index % len(API_KEYS)]
    print(f"[INFO] Current API key (index {index}): {current_key}")
    return current_key

def rotate_api_key() -> str:
    """
    Rotates the API key when a quota error is met:
    Updates the KEY_TRACK_FILE to point to the next API key in sequence,
    and returns the new API key.
    """
    with FileLock(LOCK_FILE, timeout=5):
        try:
            if os.path.exists(KEY_TRACK_FILE):
                with open(KEY_TRACK_FILE, "r") as f:
                    index = int(f.read().strip())
            else:
                index = 0
        except Exception:
            index = 0

        new_index = (index + 1) % len(API_KEYS)
        try:
            with open(KEY_TRACK_FILE, "w") as f:
                f.write(str(new_index))
            print(f"[INFO] Rotating API key: changed from index {index} to {new_index}")
        except Exception as e:
            print(f"[ERROR] Failed to update API key index file: {e}")

    new_key = API_KEYS[new_index]
    print(f"[INFO] New API key (index {new_index}): {new_key}")
    return new_key