import pandas as pd
import sqlite3
from googleapiclient.discovery import build
import googleapiclient.errors

# Chave da API
API_KEY = 'SUA_CHAVE_API'

# Configurações da API
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Conexão ao banco de dados SQLite
conn = sqlite3.connect('youtube_data.db')
cursor = conn.cursor()

# Criação da tabela (se ela não existir)
cursor.execute('''CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    likes INTEGER,
                    views INTEGER
                  )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS comments (
                    comment_id TEXT PRIMARY KEY,
                    video_id TEXT,
                    text TEXT,
                    likes INTEGER,
                    FOREIGN KEY (video_id) REFERENCES videos(video_id)
                  )''')

# Função para buscar os vídeos de um canal
def get_videos_from_channel(channel_id):
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()

    # Obtém a playlist de uploads do canal
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Busca vídeos da playlist de uploads
    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=uploads_playlist_id,
        maxResults=50  # Pega até 50 vídeos por vez
    )
    response = request.execute()

    video_ids = []
    for item in response['items']:
        video_ids.append(item['snippet']['resourceId']['videoId'])

    return video_ids

# Função para buscar informações de vídeos
def get_video_details(video_ids):
    request = youtube.videos().list(
        part="snippet,statistics",
        id=','.join(video_ids)
    )
    response = request.execute()

    for video in response['items']:
        video_id = video['id']
        title = video['snippet']['title']
        description = video['snippet']['description']
        likes = int(video['statistics'].get('likeCount', 0))  # Nem todos os vídeos mostram likes
        views = int(video['statistics'].get('viewCount', 0))

        # Insere no banco de dados
        cursor.execute('''INSERT OR IGNORE INTO videos (video_id, title, description, likes, views)
                          VALUES (?, ?, ?, ?, ?)''', (video_id, title, description, likes, views))

        # Obtém e insere os comentários
        get_video_comments(video_id)

    conn.commit()

# Função para buscar comentários de vídeos
def get_video_comments(video_id):
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50  # Até 50 comentários por vez
        )
        response = request.execute()

        for item in response['items']:
            comment_id = item['id']
            text = item['snippet']['topLevelComment']['snippet']['textDisplay']
            likes = int(item['snippet']['topLevelComment']['snippet']['likeCount'])

            # Insere no banco de dados
            cursor.execute('''INSERT OR IGNORE INTO comments (comment_id, video_id, text, likes)
                              VALUES (?, ?, ?, ?)''', (comment_id, video_id, text, likes))
    except googleapiclient.errors.HttpError as e:
        print(f"Erro ao obter comentários para o vídeo {video_id}: {e}")

# Função principal para processar todos os canais
def process_channels(csv_file):
    # Carrega a lista de canais do arquivo CSV
    df = pd.read_csv(csv_file)

    # Itera sobre cada canal
    for channel_id in df['channel_id']:
        video_ids = get_videos_from_channel(channel_id)
        get_video_details(video_ids)

# Executa o script para processar o CSV
process_channels('canais.csv')

# Fecha a conexão com o banco de dados
conn.close()
