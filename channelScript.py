import csv
import os
from datetime import datetime
from typing import Tuple, Optional

import requests
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

YOUTUBE_DATA_V3 = os.getenv('YOUTUBE_DATA_V3')

if not YOUTUBE_DATA_V3:
    raise ValueError("YOUTUBE_DATA_V3 não está definido no arquivo .env")

def get_channel_statistics(channel_id: str, api_key: str) -> Tuple[str, str]:
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'items' in data and data['items']:
            statistics = data['items'][0]['statistics']
            return statistics['videoCount'], statistics['subscriberCount']
        return 'N/A', 'N/A'
    except requests.RequestException as e:
        logger.error(f"Error fetching channel statistics: {e}")
        return 'N/A', 'N/A'

def main():
    csv_file_path = 'data/ytChannels.csv'
    api_key = YOUTUBE_DATA_V3

    logger.info(f"Reading CSV file: {csv_file_path}")

    with open(csv_file_path, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['numberOfVideos', 'numberOfSubscribers', 'dataCollected']
        rows = []

        for row in reader:
            channel_id = row.get('id')
            name = row.get('name')
            if channel_id:
                logger.info(f"Getting statistics for channel: {name}")
                video_count, subscriber_count = get_channel_statistics(channel_id, api_key)
                row['numberOfVideos'] = video_count
                row['numberOfSubscribers'] = subscriber_count
                row['dataCollected'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"Channel ID {channel_id}: {video_count} videos, {subscriber_count} subscribers")
            rows.append(row)

    # Write updated data back to CSV
    with open(csv_file_path, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
