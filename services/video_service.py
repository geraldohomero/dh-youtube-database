import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from api.youtube_client import YouTubeAPIClient
from database.db_manager import DatabaseManager
from services.transcript_service import TranscriptService
from models.data_models import ChannelDetails, ChannelData

def process_video_task(video_id: str, channel_id: str) -> Dict[str, Any]:
    """
    Função independente que processa um vídeo em uma thread separada.
    Cada thread cria suas próprias instâncias de cliente para evitar conflitos.
    """
    # Criar instâncias independentes para cada thread
    youtube_client = YouTubeAPIClient()
    transcript_service = TranscriptService()
    
    try:
        # Obter detalhes do vídeo
        video_data = youtube_client.get_video_details(video_id, channel_id)
        if not video_data:
            return {'success': False, 'video_id': video_id}
        
        # Obter transcrição
        success, transcript_text, transcript_lang = transcript_service.get_transcript_with_retry(video_id)
        video_data.update({
            'videoTranscript': transcript_text if success else None,
            'transcriptLanguage': transcript_lang if success else None,
            'collectedDate': datetime.now().date()
        })
        
        # Obter comentários se estiverem habilitados
        comments = []
        if video_data.get('commentsEnabled'):
            comments = youtube_client.get_video_comments(video_id)
        
        return {
            'success': True,
            'video_id': video_id,
            'video_data': video_data,
            'comments': comments
        }
    except Exception as e:
        logging.error(f"Error in thread processing video {video_id}: {str(e)}")
        return {'success': False, 'video_id': video_id, 'error': str(e)}

class VideoProcessingService:
    def __init__(self):
        self.youtube_client = YouTubeAPIClient()
        self.db_manager = DatabaseManager()
        self.transcript_service = TranscriptService()
        self.max_workers = 7

    def process_video_details(self, video_id: str, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Process video details including transcript fetching.
        """
        video_data = self.youtube_client.get_video_details(video_id, channel_id)
        if not video_data:
            return None
        
        # Add transcript information
        success, transcript_text, transcript_lang = self.transcript_service.get_transcript_with_retry(video_id)
        video_data.update({
            'videoTranscript': transcript_text if success else None,
            'transcriptLanguage': transcript_lang if success else None,
            'collectedDate': datetime.now().date()
        })
        
        return video_data
     
    def process_channel_videos(
        self, 
        channel_id: str, 
        existing_video_ids: set,
        published_after: Optional[str] = None,
        published_before: Optional[str] = None
    ) -> None:
        """
        Process all videos for a given channel with parallel processing.
        """
        details = self.youtube_client.get_channel_details(channel_id)
        if not details:
            logging.warning("Skipping channel %s due to missing details.", channel_id)
            return

        # Only pass date parameters if they are provided
        kwargs = {}
        if published_after is not None:
            kwargs['published_after'] = published_after
        if published_before is not None:
            kwargs['published_before'] = published_before

        video_ids = self.youtube_client.get_channel_videos(channel_id, **kwargs)
        
        # Filtrar vídeos que já existem no banco de dados
        videos_to_process = [video_id for video_id in video_ids if video_id not in existing_video_ids]
        
        channel_data = {
            'channelId': details.channel_id,
            'channelName': details.channel_name,
            'dayCollected': datetime.now().date(),
            'numberOfSubscribers': details.subscriber_count,
            'numberOfVideos': len(video_ids)
        }

        total_videos = len(video_ids)
        videos_to_process_count = len(videos_to_process)
        
        if videos_to_process_count == 0:
            logging.info(f"No new videos to process for channel {channel_id} out of {total_videos} total videos.")
            return
            
        logging.info(f"Processing {videos_to_process_count} new videos (out of {total_videos} total) for channel {channel_id}")
        logging.info(f"Using parallel processing with {self.max_workers} workers")
        
        processed_count = 0
        
        # Processar em lotes para controlar a memória
        batch_size = 7
        for i in range(0, len(videos_to_process), batch_size):
            batch = videos_to_process[i:i+batch_size]
            logging.info(f"Processing batch {i//batch_size + 1} with {len(batch)} videos")
            
            # Usar ThreadPoolExecutor para processar vídeos em paralelo
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Iniciar todas as tarefas
                future_to_video = {
                    executor.submit(process_video_task, vid, channel_id): vid 
                    for vid in batch
                }
                
                # Processar os resultados conforme eles são concluídos
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    for future in as_completed(future_to_video):
                        video_id = future_to_video[future]
                        try:
                            result = future.result()
                            
                            if result['success']:
                                processed_count += 1
                                video_data = result['video_data']
                                comments = result['comments']
                                
                                if self.db_manager.save_video_and_comments(conn, cursor, channel_data, video_data, comments):
                                    transcript_status = "with transcript" if video_data.get('videoTranscript') else "without transcript"
                                    logging.info(f"({processed_count}/{videos_to_process_count}) Saved data for video {video_id} ({len(comments)} comments, {transcript_status})")
                                else:
                                    logging.error(f"Failed to save data for video {video_id}")
                            else:
                                logging.warning(f"Failed to process video {video_id}: {result.get('error', 'Unknown error')}")
                        except Exception as e:
                            logging.error(f"Error handling result for video {video_id}: {str(e)}")
            
            # Pequena pausa entre lotes para não sobrecarregar a API
            if i + batch_size < len(videos_to_process):
                time.sleep(2)
        
        logging.info(f"Completed processing for channel {channel_id}. Successfully processed {processed_count} out of {videos_to_process_count} videos.")
