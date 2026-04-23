"""Camada fina de gravação no Redis para o pipeline.

Cada função encapsula uma única responsabilidade (single writer principle)
para facilitar teste unitário e reduzir acoplamento com o consumer.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable

from redis import Redis
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.exceptions import ResponseError

from pipeline import chaves_redis as ck
from pipeline.logger import configurar_logger

LOG = configurar_logger(__name__)

INDICE_POSTOS = "idx:postos"
# Retenção de 7 dias em milissegundos para as TimeSeries.
RETENCAO_MS_SEMANA = 7 * 24 * 60 * 60 * 1000


def conectar(host: str, port: int, password: str | None = None) -> Redis:
    """Cria cliente Redis configurado para strings (``decode_responses``)."""
    return Redis(host=host, port=port, password=password, decode_responses=True)


def upsert_posto_hash(redis: Redis, posto: Dict[str, Any]) -> None:
    """Grava/atualiza o Hash ``posto:{id}`` com dados cadastrais."""
    chave = f"{ck.PREFIXO_POSTO}:{posto['posto_id']}"
    mapping = {
        "posto_id": posto["posto_id"],
        "cnpj": posto["cnpj"],
        "nome_fantasia": posto["nome_fantasia"],
        "bandeira": posto["bandeira"],
        "bairro": posto["bairro"],
        "cidade": posto["cidade"],
        "estado": posto["estado"],
        "cep": posto["cep"],
        "ativo": posto["ativo"],
        "lon": str(posto["lon"]),
        "lat": str(posto["lat"]),
    }
    redis.hset(chave, mapping=mapping)


def registrar_geo(redis: Redis, posto_id: str, lon: float, lat: float) -> None:
    """Adiciona o posto no conjunto GEO usado para consultas por proximidade."""
    if lon == 0.0 and lat == 0.0:
        return
    redis.geoadd(ck.geo_postos(), (lon, lat, posto_id))


def atualizar_ranking_preco(
    redis: Redis, combustivel: str, uf: str, posto_id: str, preco: float
) -> None:
    """Atualiza os sorted sets de menor preço (global + por UF)."""
    if preco <= 0:
        return
    redis.zadd(ck.rk_menor_preco(combustivel), {posto_id: preco})
    if uf:
        redis.zadd(ck.rk_menor_preco(combustivel, uf), {posto_id: preco})


def registrar_ts_preco(
    redis: Redis,
    posto_id: str,
    combustivel: str,
    timestamp_ms: int,
    preco: float,
) -> None:
    """Insere um ponto na TimeSeries do preço do combustível."""
    chave = ck.ts_preco(posto_id, combustivel)
    try:
        redis.execute_command(
            "TS.ADD",
            chave,
            timestamp_ms,
            preco,
            "ON_DUPLICATE",
            "LAST",
        )
    except ResponseError as exc:
        mensagem = str(exc)
        if "TSDB" not in mensagem and "key does not exist" not in mensagem:
            raise
        redis.execute_command(
            "TS.CREATE",
            chave,
            "RETENTION",
            RETENCAO_MS_SEMANA,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            "posto_id",
            posto_id,
            "combustivel",
            combustivel.lower(),
            "tipo",
            "preco",
        )
        redis.execute_command("TS.ADD", chave, timestamp_ms, preco, "ON_DUPLICATE", "LAST")


def atualizar_ts_preco_medio(
    redis: Redis, combustivel: str, timestamp_ms: int, preco_medio: float
) -> None:
    """Atualiza a TimeSeries do preço médio por combustível."""
    chave = ck.ts_preco_medio(combustivel)
    try:
        redis.execute_command(
            "TS.ADD",
            chave,
            timestamp_ms,
            preco_medio,
            "ON_DUPLICATE",
            "LAST",
        )
    except ResponseError as exc:
        mensagem = str(exc)
        if "TSDB" not in mensagem and "key does not exist" not in mensagem:
            raise
        redis.execute_command(
            "TS.CREATE",
            chave,
            "RETENTION",
            RETENCAO_MS_SEMANA,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            "combustivel",
            combustivel.lower(),
            "tipo",
            "preco_medio",
        )
        redis.execute_command(
            "TS.ADD", chave, timestamp_ms, preco_medio, "ON_DUPLICATE", "LAST"
        )


def incrementar_buscas(redis: Redis, posto_ids: Iterable[str], regiao_key: str) -> None:
    """Incrementa contador de buscas para postos retornados + região."""
    pipe = redis.pipeline()
    for posto_id in posto_ids:
        pipe.zincrby(ck.rk_busca_por_posto(), 1, posto_id)
    if regiao_key:
        pipe.zincrby(ck.rk_busca_por_regiao(), 1, regiao_key)
    pipe.execute()


def registrar_evento_no_stream(redis: Redis, payload: Dict[str, Any]) -> None:
    """Publica o evento bruto no stream para fins de observabilidade."""
    redis.xadd(
        ck.STREAM_EVENTOS,
        {k: str(v) for k, v in payload.items()},
        maxlen=5000,
        approximate=True,
    )


def incrementar_metrica(redis: Redis, nome: str, incremento: int = 1) -> None:
    """Contadores simples para observabilidade do pipeline."""
    redis.hincrby(ck.HASH_METRICAS_PIPELINE, nome, incremento)


def criar_indice_postos(redis: Redis) -> None:
    """Cria (idempotente) o índice RediSearch sobre os Hashes ``posto:*``."""
    try:
        redis.ft(INDICE_POSTOS).info()
        LOG.info("Índice %s já existe — pulando criação.", INDICE_POSTOS)
        return
    except ResponseError:
        pass

    try:
        redis.ft(INDICE_POSTOS).create_index(
            fields=[
                TextField("nome_fantasia", weight=2.0),
                TagField("bandeira"),
                TagField("estado"),
                TagField("cidade"),
                TagField("bairro"),
                TagField("ativo"),
                NumericField("lon", sortable=True),
                NumericField("lat", sortable=True),
            ],
            definition=IndexDefinition(
                prefix=[f"{ck.PREFIXO_POSTO}:"], index_type=IndexType.HASH
            ),
        )
        LOG.info("Índice %s criado com sucesso.", INDICE_POSTOS)
    except ResponseError as exc:
        if "Index already exists" in str(exc):
            return
        raise
