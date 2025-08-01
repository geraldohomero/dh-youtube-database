## Digital Humanities Youtube Research Database (PhD Research Proposal)

[![DOI](https://zenodo.org/badge/929105417.svg)](https://doi.org/10.5281/zenodo.15258448)

> Projeto de pesquisa financiado pelo [Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq)](https://cnpq.br), Brasil

## Base de dados

Essa base de dados irá contar com as seguintes ferramentas para análise

### **Python e Whisper (OpenAI)**
- Transcrição dos vídeos (Python e Whisper/OpenAI)
- Web scrapping (Python) e Google API V3 YouTube
-  [Whisper](https://openai.com/index/whisper/) - [GitHub](https://github.com/openai/whisper)
### **YouTube data tools e Google API** 
>Para cada vídeo:
- Autores (Canais)
- URLs
- Data do vídeo 
- Número de visualizações
- Número de likes
- Número de comentários 
- Conteúdo dos comentários 
- Número de likes dos comentários 

### Arquivo `.env`:

```python
YOUTUBE_API_KEYS=<API_KEYS,API_KEYS,API_KEYS>
CHANNEL_IDS=<CHANNEL_ID>
DB_CONFIG=./db/YouTubeStats.sqlite3
```
## ER Diagrama

![image](https://github.com/user-attachments/assets/ba7f69a9-1ee5-4d73-869b-984c032c4f5e)

