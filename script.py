import requests
import json
import csv
from keys import YOUTUBE_DATA_V3

def get_all_video_in_channel(channel_name, channel_id):
    api_key = YOUTUBE_DATA_V3

    base_video_url = 'https://www.youtube.com/watch?v='
    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
    base_video_data_url = 'https://www.googleapis.com/youtube/v3/videos?'

    first_url = base_search_url+'key={}&channelId={}&part=snippet,id&order=date&maxResults=25'.format(api_key, channel_id)

    video_data = []
    url = first_url
    while True:
        response = requests.get(url)
        data = json.loads(response.content)
        
        if 'items' not in data:
            print(f"Erro na resposta da API para o canal {channel_name} ({channel_id}): {data}")
            break
        
        for video in data['items']:
            if 'videoId' in video['id']:
                video_id = video['id']['videoId']
                video_title = video['snippet']['title'].replace('"', '""')  # Escapar aspas duplas
                video_published_at = video['snippet']['publishedAt']
                
                video_data_url = base_video_data_url+'key={}&id={}&part=statistics'.format(api_key, video_id)
                video_data_response = requests.get(video_data_url)
                video_data_content = json.loads(video_data_response.content)
                
                video_views = video_data_content['items'][0]['statistics'].get('viewCount') or 0
                video_likes = video_data_content['items'][0]['statistics'].get('likeCount') or 0
                video_comments = video_data_content['items'][0]['statistics'].get('commentCount') or 0

                video_data.append([channel_name, channel_id, video_id, video_title, video_published_at, video_likes, video_comments, video_views])

        if 'nextPageToken' in data:
            next_page_url = first_url + '&pageToken={}'.format(data['nextPageToken'])
            url = next_page_url
        else:
            break
    return video_data

# Ler os canais do arquivo CSV
with open('./canais/canais.csv', mode='r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    youtube_channels = {row['name']: row['id'] for row in reader}

for channel_name, channel_id in youtube_channels.items():
    print(f'Getting videos for channel: {channel_name}')
    video_data = get_all_video_in_channel(channel_name, channel_id)
    
    # Criar um arquivo CSV para cada canal
    filename = f'{channel_name}_{channel_id}.csv'
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL)
        writer.writerow(["Channel Name", "Channel ID", "Video ID", "Video Title", "Published At", "Likes", "Comments", "Views"])
        writer.writerows(video_data)
