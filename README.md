## Digital Humanities YouTube Research Database (PhD Research Proposal)

[![DOI](https://zenodo.org/badge/929105417.svg)](https://doi.org/10.5281/zenodo.15258448)

Projeto de pesquisa desenvolvido como parte da minha tese de doutorado sendo realizada no [Programa de Pós-Graduação em Ciência Política](https://www.ifch.unicamp.br/pos/ciencia-politica) da Universidade Estadual de Campinas (Unicamp). O projeto é
financiado pelo [Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq)](https://cnpq.br), Brasil.

## Visão geral

Este repositório reúne scripts e dados para coleta, atualização e análise de conteúdo de canais do YouTube no contexto de pesquisa em Humanidades Digitais.

Principais objetivos:

- Coletar metadados de vídeos e comentários.
- Atualizar periodicamente os dados dos canais monitorados.
- Armazenar os resultados em base relacional (SQLite).
- Apoiar análises exploratórias e de NLP (incluindo transcrição de áudio).

## Tecnologias e ferramentas

### Python + Whisper (OpenAI)

- Download das transcrições com `YouTube Transcript API` + Whisper/OpenAI + `yt-dlp`. Usa-se Whisper quando não há transcrição automática disponível ou para comparar resultados.
- Coleta (scraping/API) com Python e YouTube Data API v3.

### Referências

- [Whisper](https://openai.com/index/whisper/)
- [GitHub Whisper](https://github.com/openai/whisper)
- [WhisperCPP](https://github.com/ggerganov/whisper.cpp) 
- [EasyWhisperUi](https://github.com/mehtabmahir/easy-whisper-ui)
- [YouTube Data API v3](https://developers.google.com/youtube/v3)
- [YouTube Transcript API](https://github.com/jdepoix/youtube-transcript-api)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp)
- [Google API Python Client](https://github.com/googleapis/google-api-python-client)
- [SQLite3](https://www.sqlite.org/index.html)

### YouTube Data Tools + Google API

Para cada vídeo, o projeto busca armazenar:

- Autor/canal
- URL
- Data de publicação
- Número de visualizações
- Número de likes
- Número de comentários
- Conteúdo dos comentários
- Likes dos comentários

## Estrutura do projeto (resumo)

- `api/` — cliente e integrações de API.
- `audio/` — rotinas de áudio/transcrição.
- `data/` — arquivos CSV e saídas processadas.
- `db/` — base SQLite local.
- `pipeline/` — análises e visualizações.
- `services/`, `models/`, `database/` — camadas auxiliares de dados e domínio.

## Configuração

### 1) Clonar e instalar dependências

```bash
git clone https://github.com/geraldohomero/dh-youtube-database.git
cd dh-youtube-database
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
```

### 2) Configurar variáveis de ambiente

Crie/edite o arquivo `.env` com os campos abaixo:

```env
YOUTUBE_API_KEYS=<API_KEYS...>
CHANNEL_IDS=<CHANNEL_ID...>
DB_CONFIG=<DB_PATH>
WEBSHARE_PROXY_USERNAME=<USERNAME>
WEBSHARE_PROXY_PASSWORD=<PASSWORD>
```

## Como executar

Fluxo mínimo sugerido:

1. **Atualização anual/geral de canais**
   - Script: `canaisAtualizacaoAnual.py`

2. **Persistência em banco**
   - Script: `toDatabase.py` ou `periodo.py` (dependendo do escopo da atualização)
3. **Análise de canal específico (pipeline)**
   - Script: `pipeline/canalEspecifico.py`

Exemplo de execução:

```bash
python canaisAtualizacaoAnual.py
python toDatabase.py
python pipeline/canalEspecifico.py
```

## Banco de dados

- Script SQL base: `database.sql`
- Banco SQLite local: `db/YouTubeStats.sqlite3`

## Diagrama ER

![ER Diagram](/assets/ER_Diagram.png)


## Licença e citação

- Consulte [CITATION.cff](CITATION.cff) para referência acadêmica do projeto.
- Se usar esta base em pesquisa, cite o DOI indicado no topo deste documento.