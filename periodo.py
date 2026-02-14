import logging
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Adiciona o diretório raiz ao path para permitir importações dos módulos do projeto
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from config import CHANNEL_IDS
from database.db_manager import DatabaseManager
from api.youtube_client import YouTubeAPIClient
from services.transcript_service import TranscriptService

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

def _parse_rfc3339(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

def get_uploads_playlist_id(youtube, channel_id):
    """Obtém o ID da playlist de uploads de um canal."""
    try:
        response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if 'items' in response and response['items']:
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        logging.error(f"Erro ao obter playlist de uploads: {e}")
    return None

def _build_video_data(item: dict) -> dict:
    """
    Monta payload do vídeo + transcript.
    Executa em thread separada para paralelizar I/O.
    """
    video_id = item['id']

    # Instância por thread (evita compartilhamento de estado)
    transcript_service = TranscriptService()
    success, transcript_text, transcript_lang = transcript_service.get_transcript_with_retry(video_id)

    return {
        'videoId': video_id,
        'channelId': item['snippet']['channelId'],
        'videoTitle': item['snippet']['title'],
        'videoAudio': None,
        'videoTranscript': transcript_text if success else None,
        'viewCount': int(item['statistics'].get('viewCount', 0)),
        'likeCount': int(item['statistics'].get('likeCount', 0)),
        'commentCount': int(item['statistics'].get('commentCount', 0)),
        'publishedAt': item['snippet']['publishedAt'],
        'collectedDate': datetime.now().date(),
        'transcriptLanguage': transcript_lang if success else None
    }

INACTIVITY_TIMEOUT = 420 # 7 minutos
last_activity_time = time.time()

def log_activity():
    """Atualiza o tempo da última atividade."""
    global last_activity_time
    last_activity_time = time.time()

def check_timeout():
    """Verifica se passou o tempo de inatividade."""
    elapsed = time.time() - last_activity_time
    if elapsed > INACTIVITY_TIMEOUT:
        logging.warning(f"Inatividade detectada por {elapsed:.0f}s. Reiniciando script...")
        return True
    return False

def main():
    """
    Busca vídeos dos canais configurados dentro de um período específico
    usando a iteração da playlist de uploads para garantir precisão.
    """
    # Definição do período solicitado
    START_DATE = "2013-10-31T00:00:00Z"
    END_DATE = "2024-01-01T23:59:59Z"

    start_dt = _parse_rfc3339(START_DATE)
    end_dt = _parse_rfc3339(END_DATE)

    db_manager = DatabaseManager()
    
    # Correção: Instancia a classe e acessa o objeto de serviço 'youtube'
    client = YouTubeAPIClient()
    youtube = client.youtube
    
    logging.info(f"Iniciando busca de vídeos publicados entre {START_DATE} e {END_DATE}")

    transcript_service = TranscriptService()

    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Garante que a estrutura do banco está correta
            db_manager.ensure_transcript_column_exists(cursor)
            conn.commit()
            log_activity()
                
            # Carrega IDs de vídeos já existentes
            existing_video_ids = db_manager.get_existing_video_ids(cursor)
            logging.info(f"Vídeos já existentes no banco: {len(existing_video_ids)}")
            log_activity()

            for channel_id in CHANNEL_IDS:
                logging.info(f"Iniciando processamento do canal: {channel_id}")
                
                # Obtém dados do canal necessários para o método de salvamento
                channel_data = client.get_channel_details(channel_id)
                if not channel_data:
                    logging.error(f"Não foi possível obter detalhes do canal {channel_id}")
                    continue

                uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
                if not uploads_playlist_id:
                    logging.warning(f"Playlist de uploads não encontrada para o canal {channel_id}")
                    continue

                logging.info(f"Playlist de uploads: {uploads_playlist_id}")
                log_activity()
                
                next_page_token = None
                stop_search = False
                pending_video_ids = []

                while not stop_search:
                    if check_timeout():
                        raise TimeoutError("Inatividade detectada. Reiniciando...")
                    
                    try:
                        pl_request = youtube.playlistItems().list(
                            playlistId=uploads_playlist_id,
                            part='snippet,contentDetails',
                            maxResults=50,
                            pageToken=next_page_token
                        )
                        pl_response = pl_request.execute()
                        log_activity()
                    except Exception as e:
                        logging.error(f"Erro na requisição da API: {e}")
                        break

                    for item in pl_response.get('items', []):
                        published_at = item['snippet']['publishedAt']
                        video_id = item['contentDetails']['videoId']

                        published_dt = _parse_rfc3339(published_at)

                        if published_dt > end_dt:
                            continue

                        if published_dt < start_dt:
                            logging.info(f"Alcançada data limite inferior ({published_at}). Parando busca no canal.")
                            stop_search = True
                            break

                        if video_id not in existing_video_ids:
                            pending_video_ids.append(video_id)

                    next_page_token = pl_response.get('nextPageToken')
                    if not next_page_token:
                        break

                total_to_insert = len(pending_video_ids)
                inserted_count = 0

                logging.info(
                    f"Canal {channel_id}: {total_to_insert} vídeos novos serão inseridos no banco dentro do período."
                )

                if total_to_insert == 0:
                    logging.info(f"Total de vídeos processados para o canal {channel_id}: 0")
                    continue

                batch_size = 15
                with tqdm(total=total_to_insert, desc=f"Canal {channel_id}", unit="vídeo") as pbar:
                    for i in range(0, total_to_insert, batch_size):
                        if check_timeout():
                            raise TimeoutError("Inatividade detectada. Reiniciando...")
                        
                        video_ids_batch = pending_video_ids[i:i + batch_size]

                        try:
                            vid_request = youtube.videos().list(
                                id=','.join(video_ids_batch),
                                part='snippet,statistics,contentDetails'
                            )
                            vid_response = vid_request.execute()
                            log_activity()

                            items = vid_response.get('items', [])
                            if not items:
                                continue

                            # Paraleliza somente a etapa de montagem/transcript
                            max_workers = min(10, len(items))
                            prepared_videos = []

                            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                                futures = [executor.submit(_build_video_data, item) for item in items]
                                for future in as_completed(futures):
                                    try:
                                        prepared_videos.append(future.result())
                                        log_activity()
                                    except Exception as e:
                                        logging.error(f"Erro no processamento paralelo de vídeo: {e}")

                            # Escrita no banco permanece serial (mais seguro)
                            for video_data in prepared_videos:
                                try:
                                    db_manager.save_video_and_comments(conn, cursor, channel_data, video_data, [])
                                    inserted_count += 1
                                    existing_video_ids.add(video_data['videoId'])
                                    pbar.update(1)
                                    log_activity()
                                except Exception as e:
                                    logging.error(f"Erro ao salvar vídeo {video_data.get('videoId')}: {e}")

                            conn.commit()
                            remaining_count = total_to_insert - inserted_count
                            logging.info(
                                f"Canal {channel_id}: inseridos {inserted_count}/{total_to_insert} vídeos no banco; faltam {remaining_count}."
                            )
                        except Exception as e:
                            logging.error(f"Erro ao salvar vídeos: {e}")

                logging.info(f"Total de vídeos processados para o canal {channel_id}: {inserted_count}")

        except TimeoutError as e:
            logging.error(str(e))
            return False
        finally:
            cursor.close()
            
    logging.info("Busca por período concluída com sucesso.")
    return True

if __name__ == "__main__":
    while True:
        try:
            log_activity()
            success = main()
            if success:
                break
        except Exception as e:
            logging.error(f"Erro na execução do script: {e}")
        
        logging.info("Aguardando 30 segundos antes de reiniciar...")
        time.sleep(30)