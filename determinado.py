import logging
from datetime import datetime
from config import CHANNEL_IDS
from database.db_manager import DatabaseManager


from services.video_service import VideoProcessingService

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main_determinado(start_date: str, end_date: str):
    """
    Função principal para processar vídeos do YouTube em um período específico.

    Args:
        start_date (str): Data de início no formato 'YYYY-MM-DDTHH:MM:SSZ'.
        end_date (str): Data de fim no formato 'YYYY-MM-DDTHH:MM:SSZ'.
    """
    db_manager = DatabaseManager()
    video_service = VideoProcessingService()
    
    logging.info(f"Iniciando busca de vídeos publicados entre {start_date} e {end_date}.")

    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Garante que o esquema do banco de dados está atualizado
            db_manager.ensure_transcript_column_exists(cursor)
            conn.commit()
                
            # Busca todos os IDs de vídeo existentes para evitar duplicação
            existing_video_ids = db_manager.get_existing_video_ids(cursor)
            logging.info(f"Encontrados {len(existing_video_ids)} vídeos existentes no banco de dados.")

            # Processa cada canal
            for channel_id in CHANNEL_IDS:
                logging.info(f"Processando canal: {channel_id}")
                # A lógica de paginação e busca de vídeos por data deve ser implementada
                # dentro de `process_channel_videos` ou um método similar.
                # Assumindo que `process_channel_videos` pode receber datas.
                video_service.process_channel_videos(
                    channel_id, 
                    existing_video_ids,
                    published_after=start_date,
                    published_before=end_date
                )
                
        finally:
            cursor.close()
    
    logging.info("Processo de busca de vídeos por período determinado concluído.")

if __name__ == "__main__":
    # Período de análise: 31 de outubro de 2022 a 31 de março de 2023
    # Formato RFC 3339 exigido pela API do YouTube
    START_DATE = "2022-10-31T00:00:00Z"
    END_DATE = "2023-03-31T23:59:59Z"
    
    main_determinado(START_DATE, END_DATE)