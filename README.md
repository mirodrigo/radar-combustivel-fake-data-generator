# Plataforma Radar Combustível

> Trabalho final — **MBA FIAP em Engenharia de Dados**
> Pipeline streaming **MongoDB → Redis** com visualização em **Streamlit**.

A Plataforma Radar Combustível é um estudo de caso de pipeline de dados em
tempo (quase) real: o MongoDB é a fonte de eventos (cadastro de postos,
atualizações de preço, buscas de usuários, localização) e o Redis atua
como camada de *serving* de baixa latência — alimentando rankings, mapa
geográfico e séries temporais consumidas por uma interface Streamlit.

## Integrantes

| RM       | Nome                               |
|----------|------------------------------------|
| 361560   | Enio Roberto Lourenço              |
| 360485   | Luis Henrique Kalil Duarte         |
| 363586   | Leila Moreira Gomes Roque          |
| 365533   | Adonias Ferreira Barros            |

---

## Arquitetura

```
┌────────────────────────────────────────────────────────────────────┐
│                        FONTES DE EVENTOS                           │
│  seed_radar_combustivel.py  ──┐                                    │
│  scripts/gerar_eventos.py  ───┼──►  MongoDB (replica set rs0)      │
│  Integrações reais (futuro) ──┘     • postos                       │
│                                     • eventos_preco                │
│                                     • buscas_usuarios              │
│                                     • avaliacoes_interacoes        │
│                                     • localizacoes_postos          │
└──────────────────────────────┬─────────────────────────────────────┘
                               │  Change Stream (oplog, rs0)
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                 PIPELINE (pipeline/mongodb_consumer.py)            │
│  • Backfill inicial via bootstrap.py                               │
│  • Watchers de eventos_preco e buscas_usuarios                     │
│  • Normalização + enriquecimento com cadastro do posto             │
│  • Logs em PT-BR + contadores em pipeline:metricas                 │
└──────────────────────────────┬─────────────────────────────────────┘
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                   REDIS STACK (serving layer)                      │
│  Hash   posto:{id}                         • cadastro resumido     │
│  GEO    geo:postos                         • proximidade           │
│  ZSET   ranking:menor_preco:{comb}[:{uf}]  • preço (asc)           │
│  ZSET   ranking:buscas:postos|regioes      • demanda               │
│  TS     ts:preco:{posto}:{combustivel}     • evolução histórica    │
│  TS     ts:preco_medio:{combustivel}       • referência global     │
│  STREAM stream:eventos_preco               • auditoria/debug       │
│  FT     idx:postos (RediSearch)            • busca textual         │
└──────────────────────────────┬─────────────────────────────────────┘
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                  INTERFACE (app/streamlit_app.py)                  │
│  • KPIs globais, ranking por preço, mapa pydeck (GEOSEARCH)        │
│  • Séries temporais comparando posto x média do combustível        │
│  • Busca textual RediSearch + histórico de eventos no stream       │
└────────────────────────────────────────────────────────────────────┘
```

Detalhes, decisões técnicas e diagramas estão em
[`docs/ARQUITETURA.md`](docs/ARQUITETURA.md).

---

## Pré-requisitos

- Docker 24+ e Docker Compose v2
- (Opcional, para rodar scripts fora do container) Python 3.11+

Portas utilizadas:

| Porta  | Serviço                   |
|--------|---------------------------|
| 27017  | MongoDB                   |
| 6379   | Redis Stack               |
| 5540   | RedisInsight              |
| 8501   | Streamlit                 |

---

## Como executar (Docker — recomendado)

1. **Clonar o repositório** e entrar na pasta:

   ```bash
   git clone https://github.com/mirodrigo/radar-combustivel-fake-data-generator.git
   cd radar-combustivel-fake-data-generator
   ```

2. **Subir a infraestrutura** (MongoDB + Redis + RedisInsight):

   ```bash
   docker compose up -d mongo mongo-init redis redis-insight
   ```

3. **Popular o MongoDB** (carga inicial — 2.000 registros por coleção
   já é suficiente para validar o pipeline):

   ```bash
   docker compose run --rm \
     -e N=2000 \
     pipeline python seed_radar_combustivel.py
   ```

   Para uma carga maior (demo pesada): `-e N=20000`.

4. **Subir o pipeline, o gerador contínuo e o Streamlit**:

   ```bash
   docker compose up -d pipeline gerador streamlit
   ```

5. **Acessar a interface**: <http://localhost:8501>
   RedisInsight (opcional): <http://localhost:5540>

6. **Acompanhar os logs em tempo real**:

   ```bash
   docker compose logs -f pipeline gerador
   ```

### Encerrando

```bash
docker compose down           # para containers (mantém dados)
docker compose down -v        # remove também os volumes
```

---

## Como executar sem Docker

Requer Python 3.11+, MongoDB 7 com replica set (`rs0`) e Redis Stack
locais já em execução. Em seguida:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

python seed_radar_combustivel.py         # carga inicial no Mongo
python -m scripts.bootstrap              # hidratação inicial do Redis
python -m pipeline.mongodb_consumer      # consumer do change stream
python -m scripts.gerar_eventos          # (terminal 2) gerador contínuo
streamlit run app/streamlit_app.py       # (terminal 3) UI
```

---

## Estrutura do repositório

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── seed_radar_combustivel.py          # carga inicial (MongoDB)
├── app/
│   └── streamlit_app.py               # UI
├── pipeline/
│   ├── chaves_redis.py                # convenção de chaves Redis
│   ├── event_transformer.py           # normalização de eventos
│   ├── logger.py                      # logger padronizado
│   ├── mongodb_consumer.py            # change stream -> redis
│   ├── redis_writer.py                # camada de gravação no Redis
│   └── settings.py
├── scripts/
│   ├── bootstrap.py                   # hidratação inicial do Redis
│   └── gerar_eventos.py               # gerador contínuo de eventos
└── docs/
    ├── ARQUITETURA.md                 # relatório técnico + integrantes
    └── imagens/                       # prints para o relatório
```

---

## Funcionalidades da interface (Streamlit)

- **KPIs globais** — número de postos cadastrados, combustíveis ativos,
  menor preço do filtro atual, eventos recentes no stream.
- **Ranking de menor preço** por combustível, com filtro opcional por UF.
- **Mapa** com `GEOSEARCH` em raio ajustável (pydeck). Ao passar o mouse
  o tooltip exibe nome, bandeira, cidade/UF e preço.
- **Séries temporais** comparando o posto selecionado contra o preço
  médio do combustível (`TS.RANGE`).
- **Busca textual** via RediSearch (`idx:postos`) com filtro por
  bandeira, cidade, UF e nome fantasia.
- **Auditoria** — últimos eventos consumidos (stream
  `stream:eventos_preco`).

---

## Observabilidade

- Logs estruturados em **Português do Brasil** (pipeline, bootstrap,
  gerador e consumer).
- Contadores em `pipeline:metricas` (Hash) atualizados pelo consumer:
  `eventos_preco_processados`, `buscas_processadas`,
  `erros_processamento`.
- Stream `stream:eventos_preco` mantém as últimas 5.000 ocorrências
  para depuração (consumido na interface Streamlit).

---

## Tratamento de falhas

- **Retries exponenciais** (`tenacity`) na conexão com o MongoDB no
  bootstrap e no gerador de eventos.
- **Reconexão automática** do change stream em caso de
  `OperationFailure`/`PyMongoError`/`RedisConnectionError` — o loop
  externo aguarda 2–3 segundos e reabre o `watch`.
- **`TS.CREATE` sob demanda** para séries temporais novas, com
  `DUPLICATE_POLICY=LAST` e retenção de 7 dias.
- **Encerramento gracioso** via `SIGTERM`/`SIGINT` no consumer.

---

## Referências

- [`mirodrigo/lab-streaming-mongo-redis`](https://github.com/mirodrigo/lab-streaming-mongo-redis)
  — padrão de arquitetura/programação.
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Redis Stack — Sorted Sets, TimeSeries, Geo, RediSearch](https://redis.io/docs/data-types/)
