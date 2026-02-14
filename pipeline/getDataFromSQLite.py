import sqlite3
import csv
import re
from pathlib import Path
import spacy

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DB_PATH = BASE_DIR / "db" / "YouTubeStatsPipe2.sqlite3"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "transcripts_limpos5ComMetric.csv"

# Carrega o modelo spaCy para português
nlp = spacy.load("pt_core_news_sm")

def clean_transcript(text):
    if not text:
        return ""
    # Remove timestamps (format: [00:00], [12:34], [1:23:45], etc.)
    text = re.sub(r'\[\d{1,2}:\d{2}(?::\d{2})?\]', '', text)
    # Remove outros formatos de timestamp (ex: 00:00, 0:00:00, etc.)
    text = re.sub(r'\b\d{1,2}:\d{2}(?::\d{2})?\b', '', text)
    # Remove line breaks
    text = text.replace('\n', ' ').replace('\r', ' ')
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    # Limpeza e normalização com spaCy
    doc = nlp(text)
    # Mantém apenas tokens alfabéticos, lematizados e não stopwords
    cleaned = " ".join([token.lemma_ for token in doc if token.is_alpha and not token.is_stop])
    return cleaned

def export_videos_to_csv():
    conn = sqlite3.connect(RAW_DB_PATH)
    cursor = conn.cursor()
    query = """
        SELECT
            videoId,
            channelId,
            videoTitle,
            videoTranscript,
            publishedAt,
            transcriptLanguage,
            viewCount,
            likeCount,
            commentCount
        FROM Videos
        WHERE publishedAt >= '2022-10-31 00:00:00'
          AND publishedAt < '2023-04-01 00:00:00'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    # Clean transcripts
    cleaned_rows = []
    for row in rows:
        row = list(row)
        # videoTranscript is at index 3
        row[3] = clean_transcript(row[3])
        cleaned_rows.append(row)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(cleaned_rows)

    conn.close()

if __name__ == "__main__":
    export_videos_to_csv()
