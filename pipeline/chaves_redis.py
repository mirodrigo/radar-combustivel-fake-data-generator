"""Convenção de chaves do Redis para a Plataforma Radar Combustível.

Centralizar as convenções em um único módulo evita divergência entre
pipeline, bootstrap e camada de consulta (Streamlit).
"""

from __future__ import annotations

# Hash com cadastro resumido de um posto.
PREFIXO_POSTO = "posto"

# Sorted Sets de ranking.
def rk_menor_preco(combustivel: str, uf: str | None = None) -> str:
    """Ranking de postos pelo menor preço de um combustível (score = preço)."""
    if uf:
        return f"ranking:menor_preco:{combustivel.lower()}:{uf.lower()}"
    return f"ranking:menor_preco:{combustivel.lower()}"


def rk_busca_por_posto() -> str:
    """Ranking global de postos por volume de buscas (score = nº de buscas)."""
    return "ranking:buscas:postos"


def rk_busca_por_regiao() -> str:
    """Ranking global por UF/cidade pelo volume de buscas."""
    return "ranking:buscas:regioes"


# GEO: um set com todos os postos para consultas por proximidade.
def geo_postos() -> str:
    return "geo:postos"


# TimeSeries: evolução do preço por posto/combustível.
def ts_preco(posto_id: str, combustivel: str) -> str:
    return f"ts:preco:{posto_id}:{combustivel.lower()}"


def ts_preco_medio(combustivel: str) -> str:
    """TimeSeries com o preço médio por combustível (amostragem recente)."""
    return f"ts:preco_medio:{combustivel.lower()}"


# Stream/lista circular com últimos eventos processados (observabilidade).
STREAM_EVENTOS = "stream:eventos_preco"

# Conjunto auxiliar com combustíveis conhecidos.
SET_COMBUSTIVEIS = "set:combustiveis"

# Hash de métricas operacionais do pipeline (contadores, último offset).
HASH_METRICAS_PIPELINE = "pipeline:metricas"
