import csv
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time

# Configurações
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
MAX_RESULTS_PER_PAGE = 50

YOUTUBE_API_KEY = ""

def build_youtube_service():
    """Constrói o serviço da API do YouTube usando a chave fixa."""
    try:
        return build(API_SERVICE_NAME, API_VERSION, developerKey=YOUTUBE_API_KEY)
    except Exception as e:
        print(f"Erro ao construir serviço YouTube: {e}")
        return None

def get_channel_stats(youtube, channel_id: str) -> Dict[str, Any]:
    """Obtém estatísticas atualizadas do canal pelo ID."""
    try:
        request = youtube.channels().list(
            part="statistics,snippet",
            id=channel_id
        )
        response = request.execute()
        
        if not response.get('items'):
            print(f"Nenhum canal encontrado para o ID: {channel_id}")
            return {"subscribers": 0, "videos": 0, "valid": False}
        
        channel_info = response['items'][0]
        statistics = channel_info['statistics']
        
        return {
            "subscribers": int(statistics.get('subscriberCount', 0)),
            "videos": int(statistics.get('videoCount', 0)),
            "valid": True
        }
    except HttpError as e:
        print(f"Erro HTTP ao buscar dados para o canal {channel_id}: {e}")
        return {"subscribers": 0, "videos": 0, "valid": False}
    except Exception as e:
        print(f"Erro inesperado ao buscar dados para o canal {channel_id}: {e}")
        return {"subscribers": 0, "videos": 0, "valid": False}

def update_channels_data(csv_path: str, output_path: str, current_year: int):
    """Atualiza os dados dos canais a partir do CSV original."""
    # Ler o CSV com pandas
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Erro ao ler arquivo CSV: {e}")
        return False
    
    # Construir o serviço com a chave fixa
    youtube = build_youtube_service()
    if not youtube:
        print("Falha ao criar serviço YouTube. Verifique a chave de API.")
        return False
    
    # Adicionar novas colunas para o ano atual
    df[f'videos{current_year}'] = 0
    df[f'inscritos{current_year}'] = 0
    df[f'dataColetada{current_year}'] = None
    
    print(f"Atualizando dados de {len(df)} canais...")
    
    # Contador para mostrar progresso
    processed = 0
    updated = 0
    failed = 0
    
    # Data de coleta
    collection_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Processar cada canal
    for index, row in df.iterrows():
        channel_id = row['id']
        processed += 1
        
        # Status de progresso
        if processed % 5 == 0 or processed == len(df):
            print(f"Processado {processed}/{len(df)} canais ({updated} atualizados, {failed} falhas)")
        
        # Pular linhas vazias
        if not channel_id or pd.isna(channel_id):
            continue
        
        print(f"Buscando dados para: {row['name']} (ID: {channel_id})")
        
        # Obter estatísticas atualizadas
        stats = get_channel_stats(youtube, channel_id)
        
        if stats['valid']:
            # Atualizar DataFrame
            df.at[index, f'videos{current_year}'] = stats['videos']
            df.at[index, f'inscritos{current_year}'] = stats['subscribers']
            df.at[index, f'dataColetada{current_year}'] = collection_date
            updated += 1
            print(f"  ✓ Canal atualizado: {stats['subscribers']} inscritos, {stats['videos']} vídeos")
        else:
            failed += 1
            print(f"  ✗ Falha ao atualizar canal")
        
        # Salvar periodicamente para não perder progressos em caso de erro
        if processed % 10 == 0:
            try:
                df.to_csv(f"{output_path}.temp", index=False)
                print(f"Progresso salvo temporariamente após {processed} canais")
            except Exception as e:
                print(f"Aviso: Não foi possível salvar progresso temporário: {e}")
        
        # Adicionar um pequeno atraso para evitar exceder limites de API
        time.sleep(1.0)  # Aumento do delay para evitar problemas de quota
    
    # Salvar o DataFrame atualizado
    try:
        df.to_csv(output_path, index=False)
        print(f"Dados salvos com sucesso em {output_path}")
        print(f"Total de {updated} canais atualizados de {processed} processados.")
        return True
    except Exception as e:
        print(f"Erro ao salvar o arquivo atualizado: {e}")
        return False

def main():
    """Função principal."""
    # Definir caminhos
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    input_file = os.path.join(data_dir, "ytChannels.csv")
    
    # Obter o ano atual
    current_year = datetime.now().year
    
    # Nome do arquivo de saída
    output_file = os.path.join(data_dir, f"ytChannels_atualizado_{current_year}.csv")
    
    print(f"Iniciando atualização de dados dos canais do YouTube para {current_year}")
    print(f"Arquivo original: {input_file}")
    print(f"Arquivo de saída: {output_file}")
    
    # Verificar se o arquivo existe
    if not os.path.exists(input_file):
        print(f"ERRO: O arquivo {input_file} não foi encontrado.")
        return 1
    
    # Atualizar os dados
    success = update_channels_data(input_file, output_file, current_year)
    
    if success:
        print("Atualização concluída com sucesso!")
        return 0
    else:
        print("Falha na atualização dos dados.")
        return 1

if __name__ == "__main__":
    sys.exit(main())