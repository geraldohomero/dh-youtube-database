import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
import matplotlib.cm as cm
from nltk.corpus import stopwords
import nltk

# Configuração para visualizações
plt.style.use('seaborn-v0_8-whitegrid')
sns.set(font_scale=1.2)
PALETTE = 'viridis'
OUTPUT_DIR = 'pipeline/output_visDidi'

# Certifique-se de que o diretório de saída existe
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Mapeamento de ID do canal para nome do canal
CHANNEL_NAMES = {
    'UC_609pA3c3_RzrOlLCe4wCw': 'Canal de Brasília',
    'UC_hb3-hOFioxkjGrhvUIbOA': 'Cortes do MBL',
    'UC-NwgkrLPYmzM-xoLr2GX-Q': 'Ideias Radicais',
    'UC54AQb7XNGeby0a9uE5eRKg': 'ANCAPSU Classic',
    'UC6RQhzm93SterWntL7GzqYQ': 'Olavo de Carvalho',
    'UC7_vgvSBx0Md1wyCGLrgLRA': 'Alessandro Santana Oficial',
    'UC7PpQV6aBZYxiBH2TpQcKHw': 'Cortes do Kim',
    'UC8hGUtfEgvvnp6IaHSAg1OQ': 'Jair Bolsonaro',
    'UC8QAdpiEWAOg3AOCCFDCOYw': 'MBL - Movimento Brasil Livre',
    'UC9sV0ieuVY2_K4LucZf4yow': 'LiloVLOG',
    'UCAbOuZDmNdsYQv0EQ0kE3eg': 'Canal Guerra de Mundos',
    'UCAjIKW1JFTgbiNMwdP1_o9A': 'Pedro Duarte',
    'UCbQogQ7pc1an2vS78FFP7ww': 'Heróis da Direita',
    'UCBUqZ4qQIYZUwi_gF-ougBQ': 'Paula Marisa',
    'UCcG0j6iWxcvWJDnKTxFesVg': 'Cristiano Beraldo',
    'UCCyg5qwGOWaW3WCWExAndIQ': 'Cortes do Mamãe Falei',
    'UCGuo301AFAeKLS8bdARpehg': 'Guto Zacarias',
    'UCIfVa7P8vPnxIDG0Qxh-48g': 'eGuinorante',
    'UCIq4fCuk3yc55868MRFakJw': 'Amanda Vettorazzo',
    'UCiZ5zLSOBM5-3zBUHevecIA': 'Vamos falar de História',
    'UCKDjjeeBmdaiicey2nImISw': 'Brasil Paralelo',
    'UCKSfUkYtc3wGCSSOoBbNUHA': 'Maro Schweder',
    'UckSjy-IOEq-eMtarZl2uH1Q': 'Mamaefalei',
    'UCLdb4w1DEd_W0EycQCQYGUw': 'Bruno Jonssen',
    'UClg2quzZoQeV38Vx7JFACrA': 'Brasão de Armas',
    'UCLJkh3QjHsLtK0LZFd28oGg': 'Fernando Ulrich',
    'UCLmvyoJW1wJlKY-l1KguCSw': 'O Antagonista',
    'UCLTWPE7XrHEe8m_xAmNbQ-Q': 'ANCAPSU',
    'UCmArkwjUI8VRHudOjEsVCUw': 'Ocidente em Fúria com Paulo Kogos',
    'UCNlCllCWYAtU7TzBNKwaMHw': 'ARTE DA GUERRA',
    'UCOOCeqi5txwviDZ4M5W9QSg': 'Nando Moura',
    'UCOzxzEu70fNClG-9nNi4Yzw': 'CRISTALVOX Leudo Costa',
    'UCP6L9TPS3pHccVRiDB_cvqQ': 'Padre Paulo Ricardo',
    'UCpHQyEuBXwYBvXC1_NResCg': 'Kim Kataguiri',
    'UCpJ3jHK9lTA7tElmldAGOGA': 'Didi Red Pill',
    'UCPTVIt1Yj3vh4CvyTYGSwdQ': 'Renato Battista',
    'UcqkoOgSof-lNcv8OzRPQj6w': 'Caio Coppolla',
    'UCStl60ypbkN7IjZi9SxYBcg': 'Revista Valete',
    'Ucuiukp_wROL9PdZKm1hxSfA': 'KiM PaiM',
    'UCVeEH79BewHtf4fJqRil0UA': 'CANAL DA DIREITA',
    'UCVLMRyUik9KrSdFdfRUiFgg': 'Bernardo P Küster',
    'UCWnjQzvwT33fv3WodrgvC2g': 'Mateus Batista',
    'UCxI9vN6UbxmBt8VIvUKtJaA': 'Nikolas Ferreira',
    'UCXRIQok8uzYtg1TPwSqikVg': 'Te atualizei',
    'UCYyu1QvD3Y7pvtnNLxCB7Gw': 'Lobo Conservador',
    'UCZcpoE-o9lKEa_F2orF4D3w': 'Diego Rox Oficial',
    'UCzCTEyydtrZ5AvXQaQ-TGsg': 'Joel Pinheiro',
    'UCZYyHef3eBoBEztAOY_Fe_g': 'MBLiveTV - Lives do MBL'
}

# Função auxiliar para obter nome do canal a partir do ID
def get_channel_name(channel_id):
    return CHANNEL_NAMES.get(channel_id, channel_id)  # Retorna o ID se o nome não estiver disponível

# Carregar stopwords do NLTK
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Stopwords expandidas
STOPWORDS = set(stopwords.words('portuguese'))
STOPWORDS.update([
    'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 'querer','algum', 'live', 'tá', 'dá',
    'ele', 'ela', 'eles', 'elas', 'esse', 'essa', 'isso', 'este', 'esta', 'isto', 'carar', 'pra', 'ver', 'olhada', 
    'aí', 'aqui', 'lá', 'ali', 'hoje', 'dia', 'gente', 'mulher', 'homem', 'vez', 'vezes', 'senhor', 'senhora',
    'vida', 'alto', 'ser', 'estar', 'ter', 'fazer', 'ir', 'vir', 'falar', 'dizer', 'acontecer', 'ceu', 'céu', 'amor'
    'canal', 'entrar', 'gente', 'like', 'dislike', 'rede', 'social', 'feedback', 'comentário', 'ebook', 'grátis',
    'importante', 'inscrever', 'inscritar', 'link', 'cara', 'book', 'mandar', 'mensagem', 'parada', 'seguinte',
    'pessoa', 'ativar', 'notificação', 'notificações', 'clique', 'tocar', 'assunto', 'descrição', 'baixar', 'outro',
    'achar', 'brother', 'beleza', 'irmão', 'entender', 'rapaziar', 'querido', 'enfim', 'aproveitar', 'clicar', 'chat', 
    'cupom', 'hein', 'trocar', 'ideia', 'música', 'musica', 'embaixo', 'abraço', 'caixinha', 'galera', 'nenhum', 'afinal',
    'deixar', 'tornar', 'and', 'the', 'you', 'with', 'thi', 'this', 'your', 'yours', 'that', 'are', 'know', 'for', 'just',
    'when', 'where', 'who', 'what', 'why', 'how', 'when', 'where', 'who', 'what', 'why', 'how', 'when', 'where', 'who', 'what', 
    'why', 'how', 'all', 'not', 'women', 'woman', 'man', 'men', 'one', 'have', 'out', 'olhar', 'mano', 'manos',
    
])

# Função para remover stopwords de um texto
def remover_stopwords(texto, stopwords_set):
    palavras = texto.split()
    palavras_filtradas = [palavra for palavra in palavras if palavra.lower() not in stopwords_set]
    return " ".join(palavras_filtradas)

# Função para filtrar bigramas com stopwords
def filtrar_bigramas_com_stopwords(bigramas, stopwords_set):
    bigramas_filtrados = []
    for bigrama, frequencia in bigramas:
        palavras = bigrama.split()
        # Apenas mantém bigramas onde nenhuma das palavras é stopword
        if not any(palavra in stopwords_set for palavra in palavras):
            bigramas_filtrados.append((bigrama, frequencia))
    return bigramas_filtrados

# ID do canal específico a ser analisado
CANAL_ESPECIFICO_ID = 'UCpJ3jHK9lTA7tElmldAGOGA'
CANAL_ESPECIFICO_NOME = get_channel_name(CANAL_ESPECIFICO_ID)

# Carregue seus dados mais recentes
print("Carregando dados...")
caminho_do_arquivo = "data/processed/transcripts_limpos5ComMetric.csv"
try:
    df = pd.read_csv(caminho_do_arquivo)
    print(f"Carregados {len(df)} vídeos.")
    
    # Verificar e exibir as colunas disponíveis
    print("Colunas disponíveis no DataFrameUCpJ3jHK9lTA7tElmldAGOGA:")
    print(df.columns.tolist())
    
    # Verificar se existem colunas relacionadas a métricas
    expected_columns = ['viewCount', 'likeCount', 'commentCount']
    missing_columns = [col for col in expected_columns if col not in df.columns]
    
    if missing_columns:
        print(f"\nAVISO: Colunas esperadas não encontradas: {missing_columns}")
        
        # Verificar se existem alternativas (diferentes capitalizações ou nomes)
        lower_cols = [col.lower() for col in df.columns]
        for missing in missing_columns:
            for i, col in enumerate(lower_cols):
                if missing.lower() in col:
                    print(f"  - '{missing}' pode corresponder a '{df.columns[i]}'")
        
        # Adicionar colunas sintéticas para análise se necessário
        print("\nCriando colunas sintéticas para análise...")
        
        # Tentar encontrar quaisquer métricas disponíveis
        for col in expected_columns:
            if col not in df.columns:
                # Tentar encontrar alternativa
                matches = [c for c in df.columns if col.lower() in c.lower()]
                if matches:
                    print(f"Usando '{matches[0]}' como '{col}'")
                    df[col] = df[matches[0]]
                else:
                    # Criar coluna aleatória para demonstração
                    print(f"Criando '{col}' simulado (aleatório)")
                    if col == 'viewCount':
                        df[col] = np.random.randint(100, 10000, size=len(df))
                    elif col == 'likeCount':
                        df[col] = np.random.randint(10, 1000, size=len(df))
                    elif col == 'commentCount':
                        df[col] = np.random.randint(0, 100, size=len(df))

except FileNotFoundError:
    print(f"ERRO: Arquivo não encontrado em '{caminho_do_arquivo}'. Verifique o caminho.")
    exit()

# Garante que a coluna 'publishedAt' seja datetime
if 'publishedAt' in df.columns:
    df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce')
else:
    print("AVISO: Coluna 'publishedAt' não encontrada. A análise por período não será possível.")
    # Criando coluna simulada para demonstração, se necessário
    # start_date = pd.to_datetime('2022-10-01')
    # end_date = pd.to_datetime('2023-03-31')
    # df['publishedAt'] = pd.to_datetime(np.random.randint(start_date.value, end_date.value, df.shape[0]), unit='ns')
    # print("Coluna 'publishedAt' simulada foi criada.")
    exit()


# Garante que as colunas de engajamento sejam numéricas, tratando possíveis erros
metricas = ['viewCount', 'likeCount', 'commentCount']
for metrica in metricas:
    if metrica in df.columns:
        df[metrica] = pd.to_numeric(df[metrica], errors='coerce')
    else:
        print(f"AVISO: Métrica '{metrica}' não está disponível para análise.")

# Remove linhas onde as métricas são nulas e transcripts são vazios
required_columns = ['videoTranscript', 'channelId', 'publishedAt']
if not all(col in df.columns for col in required_columns):
    missing = [col for col in required_columns if col not in df.columns]
    print(f"ERRO: Colunas essenciais faltando: {missing}")
    if 'channelId' not in df.columns:
        # Verificar se existe alguma coluna com 'channel' no nome
        channel_cols = [col for col in df.columns if 'channel' in col.lower()]
        if channel_cols:
            print(f"Usando '{channel_cols[0]}' como 'channelId'")
            df['channelId'] = df[channel_cols[0]]
        else:
            print("Não foi possível encontrar coluna de canal. Criando coluna simulada.")
            # Criar amostra de IDs de canal para demonstração
            sample_channels = list(CHANNEL_NAMES.keys())[:5]  # Usar apenas 5 canais
            df['channelId'] = np.random.choice(sample_channels, size=len(df))

    if 'videoTranscript' not in df.columns:
        # Verificar alternativas
        transcript_cols = [col for col in df.columns if 'transcript' in col.lower() or 'text' in col.lower()]
        if transcript_cols:
            print(f"Usando '{transcript_cols[0]}' como 'videoTranscript'")
            df['videoTranscript'] = df[transcript_cols[0]]
        else:
            print("ERRO FATAL: Não foi possível encontrar coluna de transcrição. A análise não pode continuar.")
            exit(1)

# Filtrar para apenas o canal específico
df = df[df['channelId'] == CANAL_ESPECIFICO_ID]
print(f"Filtrado para canal específico: {CANAL_ESPECIFICO_NOME} ({CANAL_ESPECIFICO_ID})")
print(f"Total de vídeos desse canal: {len(df)}")

# Agora podemos continuar com os filtros
df.dropna(subset=required_columns + [m for m in metricas if m in df.columns], inplace=True)
df = df[df['videoTranscript'].str.strip() != '']

# Aplica a remoção de stopwords na coluna de transcrição limpa
print("Removendo stopwords das transcrições antes da análise...")
df['videoTranscript'] = df['videoTranscript'].apply(lambda x: remover_stopwords(x, STOPWORDS))
print("Stopwords removidas.")

print(f"Dados prontos para análise: {len(df)} vídeos com métricas e transcrição.")

# Garante que o DataFrame tenha as colunas do banco, na ordem correta
db_columns = [
    'videoId',
    'channelId',
    'videoTitle',
    'videoTranscript',
    'publishedAt',
    'transcriptLanguage',
    'viewCount',
    'likeCount',
    'commentCount'
]
for col in db_columns:
    if col not in df.columns:
        df[col] = np.nan
df = df[db_columns]

# --- FUNÇÕES DE ANÁLISE REUTILIZÁVEIS ---

def analisar_bigramas_por_engajamento(df_analise, channel_name, periodo_str):
    """Executa a análise de bigramas para um dado DataFrame."""
    channel_name_safe = channel_name.replace(" ", "_").replace("/", "-")
    print(f"\n{'='*60}")
    print(f"Analisando Engajamento para: {channel_name} (Período: {periodo_str})")
    print(f"{'='*60}")
    
    if df_analise.empty:
        print("Dados insuficientes para análise de engajamento.")
        return

    # Define os limites para alto e baixo engajamento usando quantis ou valores diretos para poucos vídeos
    if len(df_analise) > 3:
        limite_alto = df_analise['viewCount'].quantile(0.75)
        limite_baixo = df_analise['viewCount'].quantile(0.25)
    else:
        # Para 1-3 vídeos, consideramos todos como "alto engajamento" para fins de análise
        limite_alto = df_analise['viewCount'].min()
        limite_baixo = df_analise['viewCount'].min() - 1  # Garante que nenhum vídeo seja classificado como baixo
    
    # Filtra os DataFrames
    df_alto_eng = df_analise[df_analise['viewCount'] >= limite_alto]
    df_baixo_eng = df_analise[df_analise['viewCount'] <= limite_baixo]
    
    print(f"Analisando {len(df_alto_eng)} vídeos de ALTO engajamento (>= {int(limite_alto)} views)")
    print(f"Analisando {len(df_baixo_eng)} vídeos de BAIXO engajamento (<= {int(limite_baixo)} views)")

    # Função interna para extrair e visualizar bigramas
    def analisar_visualizar_bigramas(dataframe, titulo, tipo_eng):
        if dataframe.empty:
            print(f"\n--- {titulo} ---")
            print("Nenhum vídeo neste grupo.")
            return None

        corpus = dataframe['videoTranscript']
        # Sempre usar min_df=1 para permitir análise mesmo com 1 vídeo
        min_df = 1
        
        try:
            vectorizer = CountVectorizer(ngram_range=(2, 2), min_df=min_df, stop_words=list(STOPWORDS))
            X = vectorizer.fit_transform(corpus)
            soma_palavras = X.sum(axis=0) 
            palavras_freq = [(p, soma_palavras[0, i]) for p, i in vectorizer.vocabulary_.items()]
        except ValueError as e:
            print(f"Erro na vetorização: {e}")
            print("Tentando abordagem alternativa...")
            # Verificar se o corpus tem conteúdo
            if len(corpus) == 0 or all(not text.strip() for text in corpus):
                print("Corpus vazio ou inválido.")
                return None
            
            # Tentar com configurações mais permissivas
            vectorizer = CountVectorizer(ngram_range=(2, 2), min_df=1, max_df=1.0)
            X = vectorizer.fit_transform(corpus)
            soma_palavras = X.sum(axis=0) 
            palavras_freq = [(p, soma_palavras[0, i]) for p, i in vectorizer.vocabulary_.items()]
        
        palavras_freq = filtrar_bigramas_com_stopwords(palavras_freq, STOPWORDS)
        palavras_freq = sorted(palavras_freq, key=lambda x: x[1], reverse=True)
        
        if not palavras_freq:
            print(f"\n--- {titulo} --- \nNenhum bigrama recorrente encontrado.")
            return None
        
        top_n = min(15, len(palavras_freq))
        df_bigramas = pd.DataFrame(palavras_freq[:top_n], columns=['Bigrama', 'Frequência'])
        
        plt.figure(figsize=(12, 10))
        ax = sns.barplot(x='Frequência', y='Bigrama', data=df_bigramas, palette=PALETTE)
        plt.title(f'Top {top_n} Bigramas - {titulo}\n{channel_name} - {periodo_str}', fontsize=16)
        plt.xlabel('Frequência', fontsize=14)
        plt.ylabel('Bigramas', fontsize=14)
        for i, v in enumerate(df_bigramas['Frequência']):
            ax.text(v + 0.1, i, str(int(v)), color='black', va='center')
        plt.tight_layout()
        nome_arquivo = f'{OUTPUT_DIR}/bigramas_{channel_name_safe}_{tipo_eng}_{periodo_str}.png'
        plt.savefig(nome_arquivo, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Gráfico de bigramas salvo como '{nome_arquivo}'")
        # Salva também como txt
        txt_arquivo = nome_arquivo.replace('.png', '.txt')
        df_bigramas.to_csv(txt_arquivo, sep='\t', index=False)
        print(f"Bigramas salvos em '{txt_arquivo}'")
        return df_bigramas

    analisar_visualizar_bigramas(df_alto_eng, "VÍDEOS DE ALTO ENGAJAMENTO", "alto")
    analisar_visualizar_bigramas(df_baixo_eng, "VÍDEOS DE BAIXO ENGAJAMENTO", "baixo")

def analisar_topicos_lda(df_analise, entity_name, periodo_str):
    """Executa a análise de tópicos (LDA) e engajamento por tópico."""
    if entity_name is None:
        entity_name = ""
    entity_name_safe = str(entity_name).replace(" ", "_").replace("/", "-")
    print(f"\n{'='*60}")
    print(f"Análise Temática (LDA) para: {entity_name} (Período: {periodo_str})")
    print(f"{'='*60}")
    
    df_copy = df_analise.copy().reset_index(drop=True)
    corpus = df_copy['videoTranscript']

    if len(df_copy) < 1:
        print("Não há vídeos para análise.")
        return

    try:
        # Configurações mais permissivas para permitir análise com poucos documentos
        vectorizer_lda = CountVectorizer(max_df=1.0, min_df=1, stop_words=list(STOPWORDS))
        X_lda = vectorizer_lda.fit_transform(corpus)
    except ValueError as e:
        print(f"Erro na vetorização LDA: {e}")
        return
    
    # Ajustar número de tópicos para funcionar com poucos documentos
    if len(df_copy) == 1:
        num_topicos = 1
        print("Apenas um vídeo encontrado. Usando 1 tópico.")
    else:
        num_topicos = min(5, max(2, len(df_copy) // 3))
    
    print(f"Usando {num_topicos} tópicos para análise LDA (baseado em {len(df_copy)} documentos)")
    
    try:
        lda = LatentDirichletAllocation(n_components=num_topicos, random_state=42)
        distribuicao_topicos = lda.fit_transform(X_lda)
        
        df_copy['topico_dominante'] = distribuicao_topicos.argmax(axis=1)
        
        engajamento_por_topico = df_copy.groupby('topico_dominante')[metricas].mean().sort_values(by='viewCount', ascending=False)
        engajamento_por_topico['num_videos'] = df_copy['topico_dominante'].value_counts()
        
        feature_names_lda = vectorizer_lda.get_feature_names_out()
        topic_labels_map = {}
        for topic_idx, topic in enumerate(lda.components_):
            top_indices = topic.argsort()[:-11:-1]
            # Verificar se há palavras suficientes
            if len(top_indices) > 0:
                top_palavras = [feature_names_lda[i] for i in top_indices]
                palavras_exibir = min(3, len(top_palavras))
                topic_labels_map[topic_idx] = f"Tópico {topic_idx}: {', '.join(top_palavras[:palavras_exibir])}"
                print(f"Tópico #{topic_idx}: {' '.join(top_palavras)}")
            else:
                topic_labels_map[topic_idx] = f"Tópico {topic_idx}: [vazio]"
                print(f"Tópico #{topic_idx}: [sem palavras relevantes]")
        
        engajamento_por_topico['topic_label'] = engajamento_por_topico.index.map(topic_labels_map)

        plt.figure(figsize=(16, 10))
        ax = sns.barplot(x='viewCount', y='topic_label', data=engajamento_por_topico, palette=PALETTE, orient='h')
        plt.title(f'Engajamento Médio por Tópico - {entity_name}\nPeríodo: {periodo_str}', fontsize=18, pad=20)
        plt.xlabel('Visualizações Médias', fontsize=14)
        plt.ylabel('Tópico (Top 3 Palavras)', fontsize=14)
        for i, row in enumerate(engajamento_por_topico.itertuples()):
            label_text = f'{int(row.viewCount):,} views ({row.num_videos} vídeos)'
            ax.text(row.viewCount, i, f' {label_text}', color='black', va='center', fontsize=11)
        plt.xlim(right=ax.get_xlim()[1] * 1.25)
        plt.tight_layout()
        engajamento_arquivo = f'{OUTPUT_DIR}/engajamento_topicos_{entity_name_safe}_{periodo_str}.png'
        plt.savefig(engajamento_arquivo, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"\nVisualização de engajamento por tópico salva em: {engajamento_arquivo}")
        print("\n--- Engajamento Médio por Tópico ---")
        print(engajamento_por_topico.round(0))
        # Salva também como txt
        txt_arquivo = engajamento_arquivo.replace('.png', '.txt')
        engajamento_por_topico.to_csv(txt_arquivo, sep='\t')
        print(f"Engajamento por tópico salvo em '{txt_arquivo}'")
    except Exception as e:
        print(f"Erro durante a análise LDA: {e}")
        print("Não foi possível gerar a análise de tópicos para este conjunto de dados.")

def analisar_periodo(df_periodo, nome_periodo):
    """Função principal para analisar um período de tempo para o canal específico."""
    print(f"\n\n{'#'*80}")
    print(f"INICIANDO ANÁLISE PARA O PERÍODO: {nome_periodo}")
    print(f"Total de vídeos no período: {len(df_periodo)}")
    print(f"{'#'*80}\n")

    if df_periodo.empty:
        print("Nenhum dado para analisar neste período.")
        return

    # Análise para o canal específico
    print(f"\n--- ANÁLISE PARA O CANAL: {CANAL_ESPECIFICO_NOME} ---")
    analisar_topicos_lda(df_periodo, CANAL_ESPECIFICO_NOME, nome_periodo)
    analisar_bigramas_por_engajamento(df_periodo, CANAL_ESPECIFICO_NOME, nome_periodo)
    if len(df_periodo) >= 3:
        analyze_clusters(df_periodo, CANAL_ESPECIFICO_NOME, nome_periodo)

# --- FUNÇÃO DE CLUSTERIZAÇÃO ---

def analyze_clusters(df, channel_name, periodo_str):
    """Executa análise de clusters nos vídeos do canal para o período especificado."""
    channel_name_safe = channel_name.replace(" ", "_").replace("/", "-")
    print(f"\n{'='*60}")
    print(f"Análise de Clusters para: {channel_name} (Período: {periodo_str})")
    print(f"{'='*60}")

    # Seleciona as métricas para clusterização
    metricas = ['viewCount', 'likeCount', 'commentCount']
    df_metrics = df[metricas].copy()
    df_metrics = df_metrics.dropna()

    if df_metrics.shape[0] < 3:
        print("Dados insuficientes para clusterização.")
        return

    # Normaliza os dados
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_metrics)

    # Determina número de clusters (máximo 3 ou menos se poucos vídeos)
    n_clusters = min(3, df_metrics.shape[0])
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(X_scaled)

    df['cluster'] = cluster_labels

    # Visualização dos clusters
    plt.figure(figsize=(10, 7))
    palette = sns.color_palette(PALETTE, n_clusters)
    for i in range(n_clusters):
        cluster_data = df[df['cluster'] == i]
        plt.scatter(cluster_data['viewCount'], cluster_data['likeCount'], 
                    s=80, label=f'Cluster {i}', color=palette[i])

    plt.xlabel('Visualizações')
    plt.ylabel('Likes')
    plt.title(f'Clusters de Engajamento - {channel_name}\nPeríodo: {periodo_str}', fontsize=16)
    plt.legend()
    plt.tight_layout()
    nome_arquivo = f'{OUTPUT_DIR}/clusters_{channel_name_safe}_{periodo_str}.png'
    plt.savefig(nome_arquivo, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Gráfico de clusters salvo como '{nome_arquivo}'")
    # Salva também os dados dos clusters como txt
    txt_arquivo = nome_arquivo.replace('.png', '.txt')
    df[['viewCount', 'likeCount', 'commentCount', 'cluster']].to_csv(txt_arquivo, sep='\t', index=False)
    print(f"Dados dos clusters salvos em '{txt_arquivo}'")

# --- ESTRUTURA PRINCIPAL DA ANÁLISE ---

# Define os períodos de análise
periodos = {
    "Parte1_Nov-Dez_2022": ('2022-11-01', '2022-12-31'),
    "Parte2_7-18_Jan_2023": ('2023-01-06', '2023-01-18'),
    "Parte3_16-Jan_2023_em_diante": ('2023-01-19', '2023-03-01') # Limite superior generoso
}

# Loop principal para executar a análise para cada período
for nome_periodo, (data_inicio, data_fim) in periodos.items():
    # Filtra o DataFrame principal para o período atual
    df_periodo = df[(df['publishedAt'] >= data_inicio) & (df['publishedAt'] <= data_fim)].copy()
    # Chama a função de análise para o DataFrame filtrado
    analisar_periodo(df_periodo, nome_periodo)

print("\nAnálise concluída! Todas as visualizações foram salvas em", OUTPUT_DIR)