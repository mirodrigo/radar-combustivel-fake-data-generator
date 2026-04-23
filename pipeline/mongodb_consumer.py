"""Consumidor de Change Streams do MongoDB que alimenta o Redis.

Executa em loop infinito assistindo ``eventos_preco`` (principal) e
``buscas_usuarios`` (volume de buscas). Cada evento é normalizado e
aplicado atomicamente no Redis em estruturas adequadas:

- Hash ``posto:{id}`` — cadastro enxuto do posto.
- Sorted Set ``ranking:menor_preco:{combustivel}[:{uf}]`` — ranking global e por UF.
- Sorted Set ``ranking:buscas:postos`` e ``ranking:buscas:regioes`` — buscas.
- Geo ``geo:postos`` — consultas por proximidade.
- TimeSeries ``ts:preco:{posto_id}:{combustivel}`` — evolução de preço.
- TimeSeries ``ts:preco_medio:{combustivel}`` — EMA por combustível.
"""

from __future__ import annotations

import argparse
import signal
import threading
import time
from typing import Any, Dict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure, PyMongoError
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from pipeline import chaves_redis as ck
from pipeline import redis_writer
from pipeline.event_transformer import (
    normalizar_busca,
    normalizar_evento_preco,
    normalizar_posto,
)
from pipeline.logger import configurar_logger
from pipeline.settings import carregar

LOG = configurar_logger(__name__)

# Evento usado para encerramento gracioso em sinais POSIX.
_parar = threading.Event()


def _handler_saida(_sig, _frame) -> None:
    LOG.info("Sinal recebido — encerrando consumer de forma graciosa...")
    _parar.set()


signal.signal(signal.SIGINT, _handler_saida)
signal.signal(signal.SIGTERM, _handler_saida)


def _cache_posto(redis: Redis, mongo_db, posto_id: str) -> Dict[str, Any] | None:
    """Recupera (ou faz cache) o cadastro do posto para enriquecer o evento."""
    chave = f"{ck.PREFIXO_POSTO}:{posto_id}"
    cadastro = redis.hgetall(chave)
    if cadastro and "estado" in cadastro:
        return cadastro

    doc = mongo_db.postos.find_one({"_id": posto_id}) or mongo_db.postos.find_one(
        {"_id": _tentar_object_id(posto_id)}
    )
    if not doc:
        return None
    posto = normalizar_posto(doc)
    redis_writer.upsert_posto_hash(redis, posto)
    redis_writer.registrar_geo(redis, posto["posto_id"], posto["lon"], posto["lat"])
    return redis.hgetall(chave)


def _tentar_object_id(valor: str):
    """Tenta converter string para ``ObjectId`` silenciosamente."""
    try:
        from bson import ObjectId

        return ObjectId(valor)
    except Exception:
        return valor


def processar_evento_preco(redis: Redis, mongo_db, raw: Dict[str, Any]) -> None:
    """Aplica um evento de atualização de preço nas estruturas do Redis."""
    evento = normalizar_evento_preco(raw)
    cadastro = _cache_posto(redis, mongo_db, evento["posto_id"])
    uf = (cadastro or {}).get("estado", "")

    redis_writer.atualizar_ranking_preco(
        redis, evento["combustivel"], uf, evento["posto_id"], evento["preco_novo"]
    )
    redis_writer.registrar_ts_preco(
        redis,
        evento["posto_id"],
        evento["combustivel"],
        evento["ocorrido_em_ms"],
        evento["preco_novo"],
    )
    redis.sadd(ck.SET_COMBUSTIVEIS, evento["combustivel"])

    # Preço médio móvel simples por combustível (amostra rápida da última janela).
    media = _media_preco_atual(redis, evento["combustivel"])
    if media > 0:
        redis_writer.atualizar_ts_preco_medio(
            redis, evento["combustivel"], evento["ocorrido_em_ms"], media
        )

    redis_writer.registrar_evento_no_stream(
        redis,
        {
            "tipo": "preco",
            "posto_id": evento["posto_id"],
            "combustivel": evento["combustivel"],
            "preco_novo": evento["preco_novo"],
            "variacao_pct": evento["variacao_pct"],
            "uf": uf,
            "ocorrido_em": evento["ocorrido_em_iso"],
        },
    )
    redis_writer.incrementar_metrica(redis, "eventos_preco_processados")

    LOG.info(
        "[PRECO] posto=%s comb=%s preco=%.3f var=%.2f%% uf=%s",
        evento["posto_id"],
        evento["combustivel"],
        evento["preco_novo"],
        evento["variacao_pct"],
        uf,
    )


def _media_preco_atual(redis: Redis, combustivel: str) -> float:
    """Calcula o preço médio atual do combustível a partir do Sorted Set de ranking."""
    try:
        scores = redis.zrange(
            ck.rk_menor_preco(combustivel), 0, 99, withscores=True
        )
        if not scores:
            return 0.0
        precos = [float(s) for _, s in scores if float(s) > 0]
        if not precos:
            return 0.0
        return round(sum(precos) / len(precos), 4)
    except Exception as exc:  # defensivo — não derruba o pipeline
        LOG.warning("Falha ao calcular média: %s", exc)
        return 0.0


def processar_busca(redis: Redis, raw: Dict[str, Any]) -> None:
    """Aplica um evento de busca nas métricas de demanda."""
    busca = normalizar_busca(raw)
    regiao_key = f"{busca['estado']}::{busca['cidade']}".strip(":")
    redis_writer.incrementar_buscas(redis, [], regiao_key)
    redis_writer.incrementar_metrica(redis, "buscas_processadas")
    LOG.info(
        "[BUSCA] uf=%s cidade=%s comb=%s resultados=%d",
        busca["estado"],
        busca["cidade"],
        busca["tipo_combustivel"],
        busca["resultado_count"],
    )


def _backfill(
    redis: Redis, mongo_db, limite: int = 20_000
) -> None:
    """Processa eventos já existentes no Mongo para preencher o Redis na primeira execução."""
    LOG.info("Iniciando backfill (limite=%d) — isso pode levar alguns segundos...", limite)
    colecao: Collection = mongo_db.eventos_preco
    processados = 0
    for doc in colecao.find({}).sort("ocorrido_em", 1).limit(limite):
        processar_evento_preco(redis, mongo_db, doc)
        processados += 1
        if processados % 2000 == 0:
            LOG.info("Backfill: %d eventos aplicados...", processados)
    LOG.info("Backfill concluído: %d eventos aplicados.", processados)


def _watch_colecao(
    mongo_client: MongoClient,
    redis: Redis,
    db_name: str,
    colecao_nome: str,
) -> None:
    """Loop de consumo de Change Stream para uma coleção específica."""
    pipeline_match = [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]
    db = mongo_client[db_name]
    colecao = db[colecao_nome]

    while not _parar.is_set():
        try:
            LOG.info("Assistindo change stream em %s.%s", db_name, colecao_nome)
            with colecao.watch(pipeline_match, full_document="updateLookup") as stream:
                for change in stream:
                    if _parar.is_set():
                        break
                    doc = change.get("fullDocument") or {}
                    if not doc:
                        continue
                    try:
                        if colecao_nome == "eventos_preco":
                            processar_evento_preco(redis, db, doc)
                        elif colecao_nome == "buscas_usuarios":
                            processar_busca(redis, doc)
                    except Exception:  # pragma: no cover - defensivo
                        LOG.exception("Erro ao processar evento de %s", colecao_nome)
                        redis_writer.incrementar_metrica(redis, "erros_processamento")
        except OperationFailure as exc:
            LOG.error("Falha no change stream (%s): %s — aguardando replica set", colecao_nome, exc)
            time.sleep(3)
        except PyMongoError as exc:
            LOG.error("Erro MongoDB (%s): %s — tentando reconectar", colecao_nome, exc)
            time.sleep(2)
        except RedisConnectionError as exc:
            LOG.error("Erro Redis (%s): %s — tentando reconectar", colecao_nome, exc)
            time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumer Radar Combustível (MongoDB -> Redis).")
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Não processa eventos pré-existentes ao iniciar.",
    )
    parser.add_argument(
        "--backfill-limit",
        type=int,
        default=20_000,
        help="Número máximo de eventos históricos processados no boot.",
    )
    args = parser.parse_args()

    cfg = carregar()
    LOG.info("Consumer iniciando | mongo=%s redis=%s:%s", cfg.mongo_uri, cfg.redis_host, cfg.redis_port)

    mongo = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=20_000)
    redis = redis_writer.conectar(cfg.redis_host, cfg.redis_port, cfg.redis_password)

    # Garante que o índice RediSearch existe.
    redis_writer.criar_indice_postos(redis)

    if not args.skip_backfill:
        _backfill(redis, mongo[cfg.db_name], limite=args.backfill_limit)

    # Duas threads: uma por coleção assistida.
    threads = [
        threading.Thread(
            target=_watch_colecao,
            args=(mongo, redis, cfg.db_name, "eventos_preco"),
            daemon=True,
            name="watch-eventos-preco",
        ),
        threading.Thread(
            target=_watch_colecao,
            args=(mongo, redis, cfg.db_name, "buscas_usuarios"),
            daemon=True,
            name="watch-buscas",
        ),
    ]
    for t in threads:
        t.start()

    LOG.info("Consumer em execução. Pressione Ctrl+C para encerrar.")
    while not _parar.is_set():
        time.sleep(1)

    LOG.info("Consumer encerrado.")


if __name__ == "__main__":
    main()
