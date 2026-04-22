"""Carga inicial Redis a partir do MongoDB.

Executa um único ciclo de preenchimento do Redis com:
  - Hashes ``posto:*`` (cadastro enxuto).
  - Conjunto GEO ``geo:postos``.
  - Índice RediSearch ``idx:postos``.
  - Sorted Sets e TimeSeries de preços a partir do último evento
    de preço conhecido por (posto, combustível).

Idempotente: pode ser executado várias vezes sem duplicação.
"""

from __future__ import annotations

import sys
from typing import Dict, Tuple

from pymongo import DESCENDING, MongoClient
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline import chaves_redis as ck
from pipeline import redis_writer
from pipeline.event_transformer import normalizar_evento_preco, normalizar_posto
from pipeline.logger import configurar_logger
from pipeline.settings import carregar

LOG = configurar_logger("bootstrap")


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, max=10))
def _conectar_mongo(uri: str) -> MongoClient:
    cliente = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    cliente.admin.command("ping")
    return cliente


def _hidratar_postos(redis, mongo_db) -> Tuple[int, int]:
    """Preenche Hashes ``posto:*`` e o conjunto GEO."""
    total = 0
    geolocalizados = 0
    for doc in mongo_db.postos.find({}, batch_size=500):
        posto = normalizar_posto(doc)
        redis_writer.upsert_posto_hash(redis, posto)
        if posto["lon"] or posto["lat"]:
            redis_writer.registrar_geo(redis, posto["posto_id"], posto["lon"], posto["lat"])
            geolocalizados += 1
        total += 1
        if total % 2000 == 0:
            LOG.info("Hidratando postos... %d processados", total)
    LOG.info("Postos hidratados: %d (geolocalizados=%d).", total, geolocalizados)
    return total, geolocalizados


def _hidratar_precos(redis, mongo_db) -> int:
    """Carrega o último preço por (posto, combustível) no Redis.

    Utiliza pipeline de agregação no Mongo (``$sort + $group``) para
    pegar o registro mais recente de cada chave e alimenta sorted sets
    de ranking e as TimeSeries de preço.
    """
    pipeline = [
        {"$sort": {"ocorrido_em": DESCENDING}},
        {
            "$group": {
                "_id": {"posto_id": "$posto_id", "combustivel": "$combustivel"},
                "preco_novo": {"$first": "$preco_novo"},
                "preco_anterior": {"$first": "$preco_anterior"},
                "variacao_pct": {"$first": "$variacao_pct"},
                "unidade": {"$first": "$unidade"},
                "fonte": {"$first": "$fonte"},
                "ocorrido_em": {"$first": "$ocorrido_em"},
                "revisado": {"$first": "$revisado"},
            }
        },
    ]

    cache_uf: Dict[str, str] = {}
    processados = 0
    for doc in mongo_db.eventos_preco.aggregate(pipeline, allowDiskUse=True):
        chave = doc["_id"]
        payload = {
            "posto_id": chave["posto_id"],
            "combustivel": chave["combustivel"],
            "preco_novo": doc["preco_novo"],
            "preco_anterior": doc.get("preco_anterior"),
            "variacao_pct": doc.get("variacao_pct"),
            "unidade": doc.get("unidade"),
            "fonte": doc.get("fonte"),
            "ocorrido_em": doc.get("ocorrido_em"),
            "revisado": doc.get("revisado"),
        }
        evento = normalizar_evento_preco(payload)

        uf = cache_uf.get(evento["posto_id"])
        if uf is None:
            hash_key = f"{ck.PREFIXO_POSTO}:{evento['posto_id']}"
            uf = redis.hget(hash_key, "estado") or ""
            cache_uf[evento["posto_id"]] = uf

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

        processados += 1
        if processados % 2000 == 0:
            LOG.info("Carga de preços: %d chaves processadas", processados)

    LOG.info("Carga de preços concluída: %d chaves (posto,combustível).", processados)
    return processados


def main() -> int:
    cfg = carregar()
    LOG.info(
        "Bootstrap Redis | mongo=%s db=%s redis=%s:%s",
        cfg.mongo_uri,
        cfg.db_name,
        cfg.redis_host,
        cfg.redis_port,
    )
    try:
        mongo = _conectar_mongo(cfg.mongo_uri)
    except Exception as exc:
        LOG.error("Não foi possível conectar ao MongoDB: %s", exc)
        return 1

    redis = redis_writer.conectar(cfg.redis_host, cfg.redis_port, cfg.redis_password)
    redis_writer.criar_indice_postos(redis)

    db = mongo[cfg.db_name]
    if db.postos.estimated_document_count() == 0:
        LOG.warning(
            "Coleção 'postos' vazia — execute 'python seed_radar_combustivel.py' antes do bootstrap."
        )
        return 0

    _hidratar_postos(redis, db)
    _hidratar_precos(redis, db)
    LOG.info("Bootstrap concluído com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
