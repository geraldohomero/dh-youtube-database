import os
import sqlite3
import pandas as pd
import re
import spacy
from spacy.lang.pt.stop_words import STOP_WORDS
from pathlib import Path
import unicodedata
import gc
import psutil
import time
import sys
from typing import Generator, List, Optional

# ------------------------------------------------------------
# Configuração de diretórios
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DB_PATH = BASE_DIR / "db" / "YouTubeStatsPipe2.sqlite3"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "transcripts_limpos4ComMetric.csv"

# Configuração de memória - otimizada para 32GB RAM
INITIAL_CHUNK_SIZE = 2000  # Aumentado de 500 para 2000
MIN_CHUNK_SIZE = 200       # Aumentado de 50 para 200
MAX_CHUNK_SIZE = 5000      # Aumentado de 1000 para 5000
CHUNK_SIZE = INITIAL_CHUNK_SIZE
MAX_MEMORY_PERCENT = 85    # Aumentado de 75 para 85
MEMORY_THRESHOLD = 90      # Aumentado de 80 para 90
CRITICAL_MEMORY = 95       # Aumentado de 90 para 95
MEMORY_CHECK_FREQUENCY = 50  # Reduzimos a frequência de verificação

# ------------------------------------------------------------
# Função para monitorar uso de memória
# ------------------------------------------------------------
def get_memory_usage():
    """Retorna o uso atual de memória como porcentagem."""
    # Forçar coleta antes de verificar para obter valor mais preciso
    gc.collect()
    process = psutil.Process(os.getpid())
    return process.memory_percent()

def check_memory_usage(force_gc=False):
    """Verifica o uso de memória e força coleta de lixo se necessário."""
    mem_usage = get_memory_usage()
    
    # Saída de emergência se atingir memória crítica
    if mem_usage > CRITICAL_MEMORY:
        print(f"\n🚨 ALERTA CRÍTICO: Uso de memória em {mem_usage:.1f}% - Encerrando para evitar travamento!")
        sys.exit(1)
        
    # Verificar se estamos acima do limite
    if mem_usage > MAX_MEMORY_PERCENT or force_gc:
        print(f"\n⚠️ Uso de memória alto: {mem_usage:.1f}% - Executando coleta de lixo...")
        
        # Coleta de lixo mais agressiva
        for _ in range(3):  # Múltiplas passagens
            gc.collect()
            
        # Aguardar brevemente para o sistema liberar a memória
        time.sleep(0.5)
        
        mem_usage_after = get_memory_usage()
        print(f"Memória após coleta: {mem_usage_after:.1f}%")
        
        # Se ainda estiver acima do threshold, pausa para o sistema recuperar
        if mem_usage_after > MEMORY_THRESHOLD:
            pause_time = min(5, (mem_usage_after - MEMORY_THRESHOLD) / 5)  # Pausa proporcional
            print(f"Pausando por {pause_time:.1f} segundos para recuperar memória...")
            time.sleep(pause_time)
            
            # Verificar novamente após a pausa
            mem_usage_final = get_memory_usage()
            print(f"Memória após pausa: {mem_usage_final:.1f}%")
            
            # Ajustar tamanho do chunk com base no uso de memória
            global CHUNK_SIZE
            if mem_usage_final > MEMORY_THRESHOLD:
                # Reduzir o tamanho do chunk
                new_chunk_size = max(MIN_CHUNK_SIZE, int(CHUNK_SIZE * 0.7))
                if new_chunk_size != CHUNK_SIZE:
                    CHUNK_SIZE = new_chunk_size
                    print(f"Reduzindo tamanho do chunk para {CHUNK_SIZE}")
            elif mem_usage_final < 60:  # Memória está confortável
                # Aumentar o tamanho do chunk gradualmente
                new_chunk_size = min(MAX_CHUNK_SIZE, int(CHUNK_SIZE * 1.2))
                if new_chunk_size != CHUNK_SIZE:
                    CHUNK_SIZE = new_chunk_size
                    print(f"Aumentando tamanho do chunk para {CHUNK_SIZE}")
                    
        return True
    return False

# ------------------------------------------------------------
# Carregar modelo spaCy (precisa ter rodado antes no terminal:
# python -m spacy download pt_core_news_sm)
# ------------------------------------------------------------
print("Carregando modelo spaCy...")
nlp = None  # Inicializado sob demanda para economizar memória

def get_nlp():
    """Carrega o modelo spaCy sob demanda"""
    global nlp
    if nlp is None:
        print("Inicializando modelo spaCy...")
        nlp = spacy.load("pt_core_news_sm", disable=["ner"])
    return nlp

# Paralelização (podemos usar mais recursos com 32GB)
N_PROCESS = max(2, min(4, (os.cpu_count() or 1) - 1))  # Mais paralelo para 32GB

# Stopwords extras (ajuste à vontade)
STOPWORDS_SET = set(STOP_WORDS)

# ------------------------------------------------------------
# Funções de limpeza
# ------------------------------------------------------------
def clean_timestamps(text: str) -> str:
    """Remove timestamps do tipo [00:10] ou [01:02:33]."""
    return re.sub(r"\[\d{1,2}:\d{2}(?::\d{2})?\]", " ", text)

def clean_urls(text: str) -> str:
    """Remove URLs simples."""
    return re.sub(r"(https?://\S+|www\.\S+)", " ", text)

def normalize_spaces(text: str) -> str:
    """Colapsa espaços e trim."""
    return re.sub(r"\s+", " ", text).strip()

def text_generator(texts: List[str]) -> Generator[str, None, None]:
    """Gera textos pré-processados um a um para economizar memória."""
    for text in texts:
        if not isinstance(text, str):
            yield ""
            continue
            
        # Pré-limpeza básica
        text = text.replace("\n", " ").replace("\r", " ")
        text = clean_timestamps(text)
        text = clean_urls(text)
        text = text.lower()
        text = normalize_spaces(text)
        
        yield text

def preprocess_text_batch(texts, batch_size: int = 64, n_process: int = N_PROCESS, show_progress: bool = True):
    """Limpa e normaliza transcripts usando processamento em lotes menores."""
    # Garante iterável indexável
    texts_list = list(texts)
    total = len(texts_list)
    results = [""] * total
    
    if total == 0:
        return results

    step = max(1, total // 20)  # atualiza a cada ~5%
    processed = 0
    
    # Processar em lotes maiores para melhor performance com mais memória
    sub_batch_size = min(200, batch_size)  # Aumentado de 50 para 200
    
    # Pré-processar todos os textos de uma vez, mas armazenar em uma lista
    # para evitar problemas com o gerador
    pre_cleaned_texts = []
    for text in texts_list:
        if not isinstance(text, str):
            pre_cleaned_texts.append("")
            continue
            
        # Pré-limpeza básica
        text = text.replace("\n", " ").replace("\r", " ")
        text = clean_timestamps(text)
        text = clean_urls(text)
        text = text.lower()
        text = normalize_spaces(text)
        pre_cleaned_texts.append(text)
    
    # Liberar memória
    del texts_list
    check_memory_usage()
    
    # Carregar o modelo apenas quando necessário
    model = get_nlp()
    
    # Processar em sub-lotes para melhor controle de memória
    for start_idx in range(0, total, sub_batch_size):
        end_idx = min(start_idx + sub_batch_size, total)
        sub_batch_texts = pre_cleaned_texts[start_idx:end_idx]
        
        # Verifique memória antes de processar o lote
        if processed % MEMORY_CHECK_FREQUENCY == 0:
            check_memory_usage()
        
        # Usar pipe com batch_size maior
        docs = list(model.pipe(sub_batch_texts, batch_size=min(32, len(sub_batch_texts))))  # Aumentado de 16 para 32
        
        for i, doc in enumerate(docs):
            tokens = []
            for token in doc:
                if not token.is_alpha:
                    continue
                lemma = token.lemma_.lower()
                lemma_norm = unicodedata.normalize("NFKD", lemma).encode("ascii", "ignore").decode("ascii")
                if len(lemma) <= 2:
                    continue
                if lemma in STOPWORDS_SET or lemma_norm in STOPWORDS_SET:
                    continue
                tokens.append(lemma)
            
            results[start_idx + i] = " ".join(tokens)
            
            # Liberar memória do doc após processar
            del doc
            
            processed += 1
            if show_progress and (processed % step == 0 or processed == total):
                pct_done = processed * 100.0 / total
                pct_left = 100.0 - pct_done
                mem_usage = get_memory_usage()
                print(f"\rProgresso: {pct_done:6.2f}% | Restante: {pct_left:6.2f}% | Memória: {mem_usage:.1f}%", end="", flush=True)
        
        # Liberar memória após cada sub-lote
        del sub_batch_texts
        del docs
        
        # Verificar memória depois de processar o lote
        if processed % MEMORY_CHECK_FREQUENCY == 0:
            check_memory_usage(force_gc=True)
    
    # Liberar memória dos textos pré-processados
    del pre_cleaned_texts
    check_memory_usage()

    if show_progress:
        print()

    return results

# ------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------
def process_chunk(chunk):
    """Processa um chunk de dados."""
    # Converte 'publishedAt' para datetime, tratando erros
    chunk['publishedAt'] = pd.to_datetime(chunk['publishedAt'], errors='coerce')
    
    # Verifica se as colunas de métricas existem e converte para o tipo numérico
    engagement_metrics = ['viewCount', 'likeCount', 'commentCount']
    for metric in engagement_metrics:
        if metric in chunk.columns:
            chunk[metric] = pd.to_numeric(chunk[metric], errors='coerce')
    
    if len(chunk) == 0:
        return None
    
    print(f"Processando chunk com {len(chunk)} registros...")
    
    # Lista as colunas presentes para verificação
    print(f"Colunas disponíveis: {chunk.columns.tolist()}")
    
    # Verifica se as métricas de engajamento estão presentes
    metrics_present = [metric for metric in engagement_metrics if metric in chunk.columns]
    if metrics_present:
        print(f"Métricas de engajamento incluídas: {metrics_present}")
    else:
        print("AVISO: Nenhuma métrica de engajamento encontrada nos dados.")
    
    # Mais otimizações para usar mais memória disponível
    chunk["cleanTranscript"] = preprocess_text_batch(
        chunk["videoTranscript"].tolist(),
        batch_size=64,
        n_process=N_PROCESS, 
        show_progress=True
    )
    
    # Remove a coluna videoTranscript para economizar espaço
    chunk = chunk.drop(columns=['videoTranscript'])
    
    # Garantir que valores nulos nas métricas sejam substituídos por zeros
    for metric in metrics_present:
        chunk[metric] = chunk[metric].fillna(0).astype(int)
    
    return chunk

def main():
    # Add global declaration for CHUNK_SIZE
    global CHUNK_SIZE
    
    print(f"Lendo banco em: {RAW_DB_PATH}")
    
    # Define o período de filtro (novembro de 2022 a julho de 2023, inclusivo)
    start_date = '2022-11-01'
    end_date = '2023-08-01' # O limite superior é exclusivo para incluir todo o dia 31/07
    
    # Criar o arquivo de saída com cabeçalhos
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(RAW_DB_PATH)
    
    # Contar total de registros para informação
    count_query = "SELECT COUNT(*) FROM Videos"
    total_records = pd.read_sql_query(count_query, conn).iloc[0, 0]
    print(f"Total de vídeos no banco: {total_records}")
    
    # Processar em chunks
    chunks_processed = 0
    total_processed = 0
    
    # Vamos modificar a consulta para buscar apenas as colunas necessárias
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
        LIMIT ? OFFSET ?
    """
    
    # Processar o primeiro chunk e criar o arquivo
    first_chunk = True
    offset = 0
    
    while True:
        # Verifica uso de memória antes de carregar novo chunk
        check_memory_usage(force_gc=True)
        
        print(f"\nCarregando chunk {chunks_processed+1} (offset {offset}, tamanho {CHUNK_SIZE})...")
        try:
            chunk = pd.read_sql_query(query, conn, params=[CHUNK_SIZE, offset])
        except Exception as e:
            print(f"Erro ao carregar chunk: {str(e)}")
            # Tentar com um chunk menor em caso de erro
            CHUNK_SIZE = max(MIN_CHUNK_SIZE, CHUNK_SIZE // 2)
            print(f"Reduzindo para chunk de tamanho {CHUNK_SIZE} e tentando novamente...")
            chunk = pd.read_sql_query(query, conn, params=[CHUNK_SIZE, offset])
        
        if len(chunk) == 0:
            break
        
        # Verificar se o chunk é muito grande - aumentamos o limite para 32GB
        if len(chunk) > 0 and chunk.memory_usage(deep=True).sum() / (1024**2) > 2000:  # Aumentado de 500MB para 2GB
            print("Chunk muito grande! Tentando reduzir...")
            CHUNK_SIZE = max(MIN_CHUNK_SIZE, CHUNK_SIZE // 2)
            print(f"Reduzindo para {CHUNK_SIZE} registros por chunk.")
            continue  # Tente novamente com chunk menor
            
        processed_chunk = process_chunk(chunk)
        
        # Liberar memória do chunk original imediatamente
        del chunk
        gc.collect()
        
        if processed_chunk is not None and len(processed_chunk) > 0:
            # Filtra o DataFrame para o período desejado APÓS o processamento
            mask = (processed_chunk['publishedAt'] >= start_date) & (processed_chunk['publishedAt'] < end_date)
            df_filtrado = processed_chunk[mask].copy()
            
            if len(df_filtrado) == 0:
                print("Nenhum registro no período para este chunk.")
                del processed_chunk
                del df_filtrado
                gc.collect()
                offset += CHUNK_SIZE
                continue
            
            engagement_metrics = ['viewCount', 'likeCount', 'commentCount']
            metrics_present = [metric for metric in engagement_metrics if metric in df_filtrado.columns]
            
            if metrics_present:
                print(f"Exportando com métricas de engajamento: {metrics_present}")
                # Mostrar estatísticas básicas
                for metric in metrics_present:
                    print(f"  - {metric}: média = {df_filtrado[metric].mean():.1f}, máx = {df_filtrado[metric].max()}")
            else:
                print("AVISO: Nenhuma métrica de engajamento será exportada.")
            
            # Escrever para o arquivo em modo append
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            
            # Escrever em pedaços maiores para economizar operações de I/O
            write_chunk_size = min(500, len(df_filtrado))  # Aumentado de 100 para 500
            for i in range(0, len(df_filtrado), write_chunk_size):
                sub_df = df_filtrado.iloc[i:i+write_chunk_size]
                sub_df.to_csv(
                    OUTPUT_PATH, 
                    index=False, 
                    encoding="utf-8", 
                    mode=mode, 
                    header=header
                )
                # Apenas o primeiro sub-chunk do primeiro chunk tem header
                mode = 'a'
                header = False
                del sub_df  # Liberar memória
                
            if first_chunk:
                first_chunk = False
            
            total_processed += len(df_filtrado)
            print(f"Salvos {len(df_filtrado)} registros no arquivo. Total: {total_processed}")
        
        chunks_processed += 1
        offset += CHUNK_SIZE
        
        # Liberar memória do chunk processado explicitamente
        del processed_chunk
        if 'df_filtrado' in locals():
            del df_filtrado
        gc.collect()
        
        # Verificar uso de memória e ajustar o tamanho do chunk se necessário
        mem_usage = get_memory_usage()
        print(f"Uso de memória atual: {mem_usage:.1f}%")
    
    conn.close()
    
    print(f"\n✅ Processamento concluído em {chunks_processed} chunks.")
    print(f"✅ Transcripts processados salvos em: {OUTPUT_PATH}")
    
    # Verificar as colunas no arquivo final
    try:
        # Ler apenas o cabeçalho
        csv_header = pd.read_csv(OUTPUT_PATH, nrows=0).columns.tolist()
        print(f"Colunas no arquivo CSV final: {csv_header}")
        
        # Verificar métricas no arquivo final
        final_metrics = [m for m in engagement_metrics if m in csv_header]
        if final_metrics:
            print(f"✅ Métricas de engajamento incluídas no arquivo final: {final_metrics}")
        else:
            print("❌ ERRO: Nenhuma métrica de engajamento encontrada no arquivo final!")
    except Exception as e:
        print(f"Erro ao verificar arquivo final: {str(e)}")
    
    print(f"Total de tokens removidos como stopwords: {len(STOPWORDS_SET)}")
    print(f"Total de registros processados: {total_processed}")

# ------------------------------------------------------------
if __name__ == "__main__":
    # Verificar dependências
    try:
        import psutil
    except ImportError:
        print("Instalando dependência psutil...")
        import subprocess
        subprocess.check_call(["pip", "install", "psutil"])
        import psutil
    
    # Verificar recursos disponíveis
    mem_available = psutil.virtual_memory().available / (1024**3)
    mem_total = psutil.virtual_memory().total / (1024**3)
    print(f"Memória do sistema: {mem_total:.2f} GB total, {mem_available:.2f} GB disponível")
    
    # Ajustar o tamanho do chunk com base na memória disponível
    if mem_available < 8:  # Menos de 8GB disponível (era 2GB)
        INITIAL_CHUNK_SIZE = MIN_CHUNK_SIZE
        CHUNK_SIZE = INITIAL_CHUNK_SIZE
        print(f"Pouca memória disponível! Utilizando chunks pequenos: {CHUNK_SIZE}")
    elif mem_available > 16:  # Se tiver mais de 16GB disponível, usar chunks maiores
        INITIAL_CHUNK_SIZE = 3000
        CHUNK_SIZE = INITIAL_CHUNK_SIZE
        print(f"Muita memória disponível! Utilizando chunks grandes: {CHUNK_SIZE}")
    
    main()
