import logging
import time
import requests
from typing import Tuple, Optional
from requests.adapters import HTTPAdapter
from requests.exceptions import ProxyError, SSLError, ReadTimeout, ConnectTimeout, RequestException
from urllib3.util.retry import Retry

HTTP_TIMEOUT = (8, 20)  # connect, read
PROXY_RETRY_TOTAL = 2

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=PROXY_RETRY_TOTAL,
        connect=PROXY_RETRY_TOTAL,
        read=0,  # não ficar preso em read retry
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=None,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

try:
    from data.transcriptions.transcript import get_transcript, format_transcript_from_api as format_transcript_text
    TRANSCRIPT_AVAILABLE = True
except ImportError as exc:
    logging.exception("Failed to import transcript module. Transcript functionality will be disabled.")
    TRANSCRIPT_AVAILABLE = False
    
class TranscriptService:
    def __init__(self):
        self.available = TRANSCRIPT_AVAILABLE
        if self.available:
            logging.info("Transcript functionality is available and enabled")
        else:
            logging.warning("Transcript functionality is disabled due to import errors")
    
    def get_transcript_with_retry(self, video_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Get transcript for a video with retry logic.
        Returns (success, transcript_text, transcript_lang)
        """
        if not self.available:
            return False, "Transcript functionality not available", None
        
        try:
            # em toda chamada HTTP:
            # response = self.session.get(url, params=params, proxies=proxies, timeout=HTTP_TIMEOUT)
            # response.raise_for_status()
            pass
        except (ProxyError, SSLError, ReadTimeout, ConnectTimeout) as e:
            logging.warning(f"Falha de proxy/rede para {video_id}: {e}")
            return False, None, None
        except RequestException as e:
            logging.warning(f"Erro HTTP para {video_id}: {e}")
            return False, None, None
        
        try:
            success, transcript_text, transcript_lang = get_transcript(video_id)
            return success, transcript_text, transcript_lang
        except Exception as e:
            logging.warning("First transcript attempt failed for video %s: %s. Trying again...", video_id, e)
            # Wait briefly before retry
            time.sleep(1)
            try:
                success, transcript_text, transcript_lang = get_transcript(video_id)
                return success, transcript_text, transcript_lang
            except Exception as e:
                logging.error("Second transcript attempt also failed for video %s: %s", video_id, e)
                return False, None, None
    
    def format_transcript(self, transcript_data) -> str:
        """Format transcript data if available."""
        if not self.available:
            return "Transcript formatting not available"
        return format_transcript_text(transcript_data)