# Plataforma Radar Combustível — Relatório Técnico

> **MBA FIAP em Engenharia de Dados** — Trabalho final
> Pipeline de dados em tempo quase real **MongoDB → Redis**, com camada
> de visualização em Streamlit.

## Integrantes

| RM       | Nome                               |
|----------|------------------------------------|
| 361560   | Enio Roberto Lourenço              |
| 360485   | Luis Henrique Kalil Duarte         |
| 363586   | Leila Moreira Gomes Roque          |
| 365533   | Adonias Ferreira Barros            |

---

## 1. Objetivo

Construir uma plataforma de observação de preços de combustíveis que
reflete mudanças da base operacional (MongoDB) em uma camada de
*serving* de baixa latência (Redis) em segundos, expondo rankings,
consultas por proximidade, séries temporais e buscas textuais para uma
interface Streamlit.

## 2. Visão geral da arquitetura

```
 ┌───────────────────────┐   Change Streams    ┌──────────────────────────┐
 │     MongoDB (rs0)     │ ───────────────────►│   Pipeline Python        │
 │  postos               │                     │   (pipeline/)            │
 │  eventos_preco        │                     │   - mongodb_consumer.py  │
 │  buscas_usuarios      │                     │   - event_transformer.py │
 │  avaliacoes_interacoes│                     │   - redis_writer.py      │
 │  localizacoes_postos  │                     └──────────────┬───────────┘
 └───────────────────────┘                                    │
            ▲                                                 ▼
            │ Inserts em lote               ┌──────────────────────────┐
 ┌──────────┴────────────┐                  │       Redis Stack        │
 │ seed_radar_combustivel│                  │  Hash   posto:{id}       │
 │ scripts/gerar_eventos │                  │  GEO    geo:postos       │
 └───────────────────────┘                  │  ZSET   ranking:*        │
                                            │  TS     ts:preco:*       │
                                            │  FT     idx:postos       │
                                            │  STREAM stream:eventos_* │
                                            └──────────────┬───────────┘
                                                           │ TS.RANGE, ZRANGE,
                                                           │ GEOSEARCH, FT.SEARCH
                                                           ▼
                                            ┌──────────────────────────┐
                                            │   Streamlit UI           │
                                            │   app/streamlit_app.py   │
                                            └──────────────────────────┘
```

## 3. Coleções no MongoDB

| Coleção                   | Conteúdo principal                                         |
|---------------------------|------------------------------------------------------------|
| `postos`                  | Cadastro de postos com endereço + `location` GeoJSON.      |
| `eventos_preco`           | Atualizações de preço por posto/combustível (stream-like). |
| `buscas_usuarios`         | Buscas/filtros + `geo_centro`, latência, contagem.         |
| `avaliacoes_interacoes`   | Avaliações, favoritos, check-ins, denúncias.               |
| `localizacoes_postos`     | 1:1 com `postos`, com IBGE, bairro e `geo` indexável.      |

Índices criados automaticamente pelo `seed_radar_combustivel.py`:

- `postos.location` (2dsphere)
- `eventos_preco.(posto_id, ocorrido_em)` e `(combustivel, ocorrido_em)`
- `buscas_usuarios.consultado_em` e `(estado, cidade)`
- `localizacoes_postos.posto_id` (único), `localizacoes_postos.geo` (2dsphere)

## 4. Pipeline de streaming

### 4.1 Pré-requisito: Replica Set

O *Change Stream* do MongoDB exige replica set. O `docker-compose.yml`
sobe o Mongo com `--replSet rs0` e um serviço auxiliar
`mongo-init` executa `rs.initiate(...)` idempotente.

### 4.2 Fases do consumer

1. **Bootstrap** (`scripts/bootstrap.py`):
   - Percorre `postos`, escreve `posto:{id}` (Hash) e adiciona ao
     `geo:postos`.
   - Para cada `(posto_id, combustivel)` extrai o evento mais recente
     com `$sort + $group` e alimenta os sorted sets
     `ranking:menor_preco:*` e a TimeSeries `ts:preco:{posto}:{comb}`.
   - Cria o índice RediSearch `idx:postos` (idempotente).

2. **Watch** (`pipeline/mongodb_consumer.py`):
   - Duas threads consumindo `col.watch(...)` em
     `eventos_preco` e `buscas_usuarios`.
   - Cada evento é normalizado em `event_transformer.py`, enriquecido
     com o Hash `posto:{id}` e aplicado às estruturas de serving.
   - Contadores são incrementados em `pipeline:metricas` e cada evento
     é publicado em `stream:eventos_preco` (auditoria).

3. **Gerador contínuo** (`scripts/gerar_eventos.py`): insere lotes de
   eventos em `eventos_preco` para simular tráfego real — útil em
   apresentação/defesa.

### 4.3 Enriquecimento

Como os eventos de preço não trazem a UF do posto, o consumer consulta
`posto:{id}` no Redis (com fallback para MongoDB) para inferir UF e
alimentar tanto o ranking global quanto o por UF.

## 5. Modelagem no Redis

| Estrutura     | Chave                                        | Justificativa técnica                                                                                  |
|---------------|----------------------------------------------|--------------------------------------------------------------------------------------------------------|
| Hash          | `posto:{id}`                                 | `HGETALL` O(1) para compor tabelas e tooltips da UI sem round-trip ao Mongo.                           |
| GEO           | `geo:postos`                                 | `GEOSEARCH BYRADIUS` para mapa do Streamlit; natural para consultas por proximidade.                    |
| Sorted Set    | `ranking:menor_preco:{comb}[:{uf}]`          | *Score = preço*; `ZRANGE` O(log N) devolve o Top-N mais barato; filtro por UF em chaves separadas.     |
| Sorted Set    | `ranking:buscas:postos` / `ranking:buscas:regioes` | Contadores incrementais (`ZINCRBY`) dão ranking de demanda sem agregação ad-hoc.                   |
| TimeSeries    | `ts:preco:{posto}:{comb}`                    | Retenção de 7 dias, ideal para gráfico histórico; agregações nativas em `TS.RANGE`.                    |
| TimeSeries    | `ts:preco_medio:{comb}`                      | Média recalculada a cada evento (baseada no Top-100 do ranking global) para comparativo visual.        |
| Stream        | `stream:eventos_preco`                       | Últimas 5.000 entradas para auditoria/trace — `XREVRANGE` alimenta a seção "Últimos eventos" da UI.    |
| Set           | `set:combustiveis`                           | Descoberta dinâmica de combustíveis para popular selectboxes.                                          |
| Índice FT     | `idx:postos` (RediSearch)                    | Busca textual por nome/bandeira/UF/cidade com filtros por tag.                                         |
| Hash          | `pipeline:metricas`                          | Contadores de observabilidade (`eventos_preco_processados`, `erros_processamento`, etc.).              |

## 6. Decisões técnicas

- **Change Streams** em vez de polling: consumo nativo do oplog,
  sem gap de leitura e sem carga extra em `find`/cursors.
- **Serving layer única (Redis Stack)**: consolida Hashes, Sorted Sets,
  GEO, TimeSeries e RediSearch em um único cluster — reduz superfície
  operacional em comparação com adotar Elastic, Postgres/TimescaleDB e
  Redis separadamente.
- **Idempotência** no bootstrap e no consumer: `HSET`/`ZADD`/`TS.ADD`
  (`ON_DUPLICATE LAST`) tornam o pipeline tolerante a reinicializações.
- **Consistência eventual**: rankings e métricas podem apresentar atraso
  de centenas de milissegundos — aceitável para casos de uso analíticos
  e comparativos.
- **Multi-coleção por threads independentes**: `eventos_preco` e
  `buscas_usuarios` têm cadências distintas e falhas isoladas não
  afetam o outro consumidor.
- **Separação pipeline / UI / gerador** no Docker Compose: facilita
  subir só a UI para demonstrações ou escalar horizontalmente o
  pipeline.

## 7. Observabilidade e falhas

- Logger padronizado (`pipeline/logger.py`) com timestamp, nível e nome
  do módulo.
- Contadores de eventos e erros em `pipeline:metricas`.
- Auto-reconexão do change stream com backoff simples.
- Retries exponenciais (`tenacity`) nos scripts utilitários
  (`bootstrap`, `gerar_eventos`).
- Encerramento gracioso por `SIGTERM`/`SIGINT` (consumer e gerador).

## 8. Evidências visuais

Após subir a stack (`docker compose up -d`), capture os prints abaixo e
salve em `docs/imagens/`:

| Arquivo esperado                    | Descrição                                                           |
|------------------------------------|---------------------------------------------------------------------|
| `docs/imagens/01-streamlit.png`    | Visão geral do Streamlit com KPIs e ranking.                        |
| `docs/imagens/02-mapa.png`         | Mapa de postos (GEOSEARCH) com tooltip visível.                     |
| `docs/imagens/03-serie-temporal.png` | Série temporal preço do posto x média do combustível.             |
| `docs/imagens/04-pipeline.png`     | Logs do consumer processando eventos em tempo real.                  |
| `docs/imagens/05-redisinsight.png` | RedisInsight mostrando Hashes, Sorted Sets, GEO e TimeSeries.       |

Inserir no relatório final (este arquivo ou PDF convertido) com
`![descrição](imagens/arquivo.png)`.

## 9. Como gerar o PDF

Recomenda-se converter este Markdown para PDF usando
[Pandoc](https://pandoc.org/):

```bash
pandoc docs/ARQUITETURA.md \
  -o docs/Radar_Combustivel_Relatorio.pdf \
  --from=gfm \
  --pdf-engine=xelatex \
  -V geometry:margin=2cm
```

## 10. Roadmap / próximos passos (fora do escopo atual)

- Adicionar autenticação no Redis e no Mongo (atualmente o ambiente é
  pensado para laboratório).
- Substituir o gerador por ingestão real da API da ANP.
- Persistir métricas históricas em Parquet/S3 via sink de batch.
- Adicionar alertas (ex.: variação de preço > 10% em < 1h).
