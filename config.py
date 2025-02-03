import os

# Multiple API keys separated by commas in the environment variable YOUTUBE_API_KEYS.
# For example: "key1,key2,key3"
API_KEYS = os.environ.get("YOUTUBE_API_KEYS", "x,y").split(",")
CHANNEL_IDS = os.environ.get("CHANNEL_IDS", "z").split(",")
DB_CONFIG = os.environ.get("DB_CONFIG", "./db/YouTubeStats.sqlite3")

# File to track the current API key index
KEY_TRACK_FILE = os.path.join(os.path.dirname(__file__), "apikey_index.txt")

def get_api_key() -> str:
    """
    Retrieves the next API key in sequence.
    Tracks the current key index via a file, then rotates sequentially.
    Shows the selected key and index on the terminal.
    """
    try:
        if os.path.exists(KEY_TRACK_FILE):
            with open(KEY_TRACK_FILE, "r") as f:
                index = int(f.read())
        else:
            index = 0
    except Exception:
        index = 0

    current_key = API_KEYS[index % len(API_KEYS)]
    new_index = (index + 1) % len(API_KEYS)
    
    try:
        with open(KEY_TRACK_FILE, "w") as f:
            f.write(str(new_index))
    except Exception as e:
        pass

    print(f"Using API key (index: {index}): {current_key}")
    return current_key