import sqlite3
import csv
import os

DB_PATH = "./db/YouTubeStats.sqlite3"
OUTPUT_PATH = "./data/videos_sem_transcricao.csv"

def export_missing_transcripts():
    if not os.path.exists(DB_PATH):
        print(f"Erro: O banco de dados não foi encontrado em: {DB_PATH}")
        return

    print("Conectando ao banco de dados...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Query to fetch all videos missing transcripts
    query = """
        SELECT 
            v.videoId, 
            c.channelName, 
            v.videoTitle, 
            v.publishedAt 
        FROM Videos v
        LEFT JOIN Channels c ON v.channelId = c.channelId
        WHERE v.videoTranscript IS NULL OR v.videoTranscript = ''
        ORDER BY c.channelName, v.publishedAt DESC
    """
    
    print("Buscando vídeos sem transcrição...")
    cur.execute(query)
    rows = cur.fetchall()

    total_missing = len(rows)
    print(f"Encontrados {total_missing} vídeos sem transcrição.")

    if total_missing > 0:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['VideoID', 'Canal', 'Titulo', 'DataPublicacao'])
            writer.writerows(rows)
        print(f"Lista de vídeos exportada com sucesso para: {OUTPUT_PATH}")
    
    # Get total videos for context
    cur.execute("SELECT COUNT(*) FROM Videos")
    total_videos = cur.fetchone()[0]

    # Group by channel for summary
    group_query = """
        SELECT 
            COALESCE(c.channelName, 'Canal Desconhecido/Nulo') as nome_canal, 
            COUNT(v.videoId) as total_videos,
            SUM(CASE WHEN v.videoTranscript IS NULL OR v.videoTranscript = '' THEN 1 ELSE 0 END) as sem_transcricao
        FROM Videos v
        LEFT JOIN Channels c ON v.channelId = c.channelId
        GROUP BY v.channelId, c.channelName
        ORDER BY sem_transcricao DESC
    """
    cur.execute(group_query)
    channel_summary = cur.fetchall()

    print("\nResumo por canal (Ordenado por quantidade sem transcrição):")
    print("-" * 80)
    print(f"{'Canal':<45} | {'Total Vídeos':<12} | {'Sem Transcrição':<15} | {'% Sem Transcr.':<12}")
    print("-" * 80)
    for row in channel_summary:
        canal, total, sem_trans = row
        pct = (sem_trans / total * 100) if total > 0 else 0
        if sem_trans > 0:
            print(f"{canal[:45]:<45} | {total:<12} | {sem_trans:<15} | {pct:>11.1f}%")
    print("-" * 80)
    print(f"Geral: {total_missing} de {total_videos} vídeos não possuem transcrição ({total_missing/total_videos*100:.1f}% do banco).")

    conn.close()

if __name__ == "__main__":
    export_missing_transcripts()
