import csv
import requests
from datetime import datetime
from keys import YOUTUBE_DATA_V3

# Função para obter estatísticas de um canal do YouTube
def get_channel_statistics(channel_id, api_key):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    if 'items' in data and len(data['items']) > 0:
        statistics = data['items'][0]['statistics']
        return statistics['videoCount'], statistics['subscriberCount']
    else:
        return 'N/A', 'N/A'  # Retorna 'N/A' se a chave 'items' não estiver presente

# Caminho correto para o arquivo CSV
csv_file_path = './canais/canais.csv'
api_key = YOUTUBE_DATA_V3

print(f"Lendo o arquivo CSV: {csv_file_path}")

# Ler o arquivo CSV e adicionar as novas colunas
with open(csv_file_path, mode='r', newline='') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['numberOfVideos', 'numberOfSubscribers', 'dataCollected']
    rows = []

    for row in reader:
        name= row['name']
        channel_id = row['id']
        print(f"Obtendo estatísticas para o canal: {name}")
        number_of_videos, number_of_subscribers = get_channel_statistics(channel_id, api_key)
        row['numberOfVideos'] = number_of_videos
        row['numberOfSubscribers'] = number_of_subscribers
        row['dataCollected'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Canal ID {channel_id}: {number_of_videos} vídeos, {number_of_subscribers} inscritos")
        rows.append(row)

print("Escrevendo o novo arquivo CSV com as colunas adicionais")

# Escrever o novo CSV com as colunas adicionais
with open(csv_file_path, mode='w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Arquivo CSV atualizado salvo em: {csv_file_path}")
