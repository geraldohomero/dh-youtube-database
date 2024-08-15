import os
import whisper 
from moviepy.editor import VideoFileClip

def transcrever_videos(diretorio_videos, diretorio_transcricoes):
    # Carregar o modelo Whisper
    modelo = whisper.load_model("base")

    # Verificar se o diretório de transcrições existe, caso contrário, criar
    if not os.path.exists(diretorio_transcricoes):
        os.makedirs(diretorio_transcricoes)

    # Iterar sobre os arquivos de vídeo no diretório especificado
    for nome_arquivo in os.listdir(diretorio_videos):
        if nome_arquivo.endswith(('.mp4', '.avi', '.mov', '.mkv')):
            caminho_video = os.path.join(diretorio_videos, nome_arquivo)
            caminho_transcricao = os.path.join(diretorio_transcricoes, f"{os.path.splitext(nome_arquivo)[0]}.txt")

            # Extrair áudio do vídeo
            video = VideoFileClip(caminho_video)
            caminho_audio = f"{os.path.splitext(caminho_video)[0]}.wav"
            video.audio.write_audiofile(caminho_audio)

            # Transcrever o áudio usando Whisper
            resultado = modelo.transcribe(caminho_audio)

            # Salvar a transcrição em um arquivo de texto
            with open(caminho_transcricao, 'w') as arquivo_transcricao:
                arquivo_transcricao.write(resultado['text'])

            # Remover o arquivo de áudio temporário
            os.remove(caminho_audio)

            print(f"Transcrição de {nome_arquivo} concluída e salva em {caminho_transcricao}")

# Exemplo de uso
diretorio_videos = "$HOME/Documents/videos"
diretorio_transcricoes = "$HOME/Documents/videos/transcricoes"
transcrever_videos(diretorio_videos, diretorio_transcricoes)