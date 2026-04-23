"""Gerador contínuo de eventos para demonstrar o pipeline em funcionamento.

A cada ``INTERVALO_SEGUNDOS``, insere ``EVENTOS_POR_CICLO`` eventos novos
na coleção ``eventos_preco`` (e opcionalmente em ``buscas_usuarios``),
fazendo com que os Change Streams propaguem tudo para o Redis.
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import List

from bson import ObjectId
from pymongo import MongoClient
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline.logger import configurar_logger
from pipeline.settings import carregar

LOG = configurar_logger("gerador")

COMBUSTIVEIS = (
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
)

FONTES = ("app_usuario", "api_anp", "operador_posto", "crawler")

_parar = False


def _handler(_sig, _frame) -> None:
    global _parar
    LOG.info("Sinal recebido — encerrando gerador.")
    _parar = True


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, max=10))
def _conectar_mongo(uri: str) -> MongoClient:
    cliente = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    cliente.admin.command("ping")
    return cliente


def _ids_de_postos(db, amostra: int = 200) -> List:
    """Retorna uma amostra pequena de postos para eventos realistas."""
    return [doc["_id"] for doc in db.postos.aggregate([{"$sample": {"size": amostra}}])]


def _criar_evento_preco(posto_id) -> dict:
    combustivel = random.choice(COMBUSTIVEIS)
    preco_novo = round(random.uniform(4.5, 8.9), 3)
    preco_ant = round(max(3.0, preco_novo + random.uniform(-0.6, 0.6)), 3)
    variacao = round((preco_novo - preco_ant) / preco_ant * 100, 4) if preco_ant else 0.0
    return {
        "_id": ObjectId(),
        "posto_id": posto_id,
        "combustivel": combustivel,
        "preco_anterior": preco_ant,
        "preco_novo": preco_novo,
        "variacao_pct": variacao,
        "unidade": "BRL_L",
        "fonte": random.choice(FONTES),
        "ocorrido_em": datetime.now(timezone.utc),
        "revisado": random.random() > 0.1,
    }


def main() -> int:
    cfg = carregar()
    intervalo = float(os.getenv("INTERVALO_SEGUNDOS", "2"))
    por_ciclo = int(os.getenv("EVENTOS_POR_CICLO", "3"))
    amostra_postos = int(os.getenv("AMOSTRA_POSTOS", "200"))

    LOG.info(
        "Gerador iniciando | intervalo=%.2fs eventos/ciclo=%d mongo=%s",
        intervalo,
        por_ciclo,
        cfg.mongo_uri,
    )

    try:
        cliente = _conectar_mongo(cfg.mongo_uri)
    except Exception as exc:
        LOG.error("Impossível conectar ao MongoDB: %s", exc)
        return 1

    db = cliente[cfg.db_name]
    postos = _ids_de_postos(db, amostra=amostra_postos)
    if not postos:
        LOG.error("Nenhum posto encontrado. Rode 'python seed_radar_combustivel.py' antes.")
        return 1

    total = 0
    while not _parar:
        batch = [_criar_evento_preco(random.choice(postos)) for _ in range(por_ciclo)]
        try:
            db.eventos_preco.insert_many(batch, ordered=False)
        except Exception as exc:  # pragma: no cover - defensivo
            LOG.exception("Falha ao inserir lote de eventos: %s", exc)
            time.sleep(min(intervalo * 2, 10))
            continue

        total += len(batch)
        LOG.info("Inseridos %d eventos (total=%d).", len(batch), total)
        time.sleep(intervalo)

    LOG.info("Gerador encerrado. Total inserido: %d eventos.", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
