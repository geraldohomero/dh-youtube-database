import yt_dlp
import time
import random
import os

def download_transcription(video_url, output_path='transcript.txt', lang='en', max_retries=3, ignore_errors=False):
    """
    Download transcription from a YouTube video with retry mechanism for rate limiting.
    
    Args:
        video_url: URL of the YouTube video
        output_path: Path to save the transcript
        lang: Language code for the subtitles
        max_retries: Maximum number of retry attempts
        ignore_errors: Whether to continue even if subtitle download fails
    """
    ydl_opts = {
        'skip_download': True,
        'writesubtitles': True,
        'writeautomaticsub': True,
        'subtitleslangs': [lang],
        'outtmpl': '%(id)s.%(ext)s',
    }

    video_id = None
    retries = 0
    
    while retries <= max_retries:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                video_id = info.get('id')
                # If we get here, the download was successful
                break
        except yt_dlp.utils.DownloadError as e:
            if "HTTP Error 429" in str(e) and retries < max_retries:
                retries += 1
                wait_time = (2 ** retries) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Rate limit hit. Waiting {wait_time:.2f} seconds before retry {retries}/{max_retries}...")
                time.sleep(wait_time)
            elif ignore_errors:
                print(f"Warning: Failed to download subtitles - {str(e)}")
                if 'video_id' in locals() and video_id:
                    break
                else:
                    print("Could not extract video ID. Exiting.")
                    return
            else:
                print(f"Error downloading transcription: {str(e)}")
                return
    
    if video_id:
        # Find the subtitle file
        for ext in ['vtt', 'srt', 'srv3', 'srv2', 'srv1']:
            sub_file = f"{video_id}.{lang}.{ext}"  # Include language code in filename
            if os.path.exists(sub_file):
                try:
                    with open(sub_file, 'r', encoding='utf-8') as f:
                        transcript = f.read()
                    with open(output_path, 'w', encoding='utf-8') as out_f:
                        out_f.write(transcript)
                    print(f"Transcription saved to {output_path}")
                    # Clean up the subtitle file
                    os.remove(sub_file)
                    return
                except Exception as e:
                    print(f"Error processing subtitle file: {str(e)}")
                    continue
        
        print("No transcription file found after successful download. YouTube may not have subtitles for this video.")
    else:
        print("Failed to download transcription after all retries.")

if __name__ == "__main__":
    # Replace with your desired YouTube video URL
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    download_transcription(video_url, ignore_errors=True)