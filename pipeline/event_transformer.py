"""Normalização de eventos do MongoDB antes de gravar no Redis."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict


def _para_iso(valor: Any) -> str:
    """Converte datetime/str para ISO-8601. Retorna string vazia em caso de falha."""
    if isinstance(valor, datetime):
        if valor.tzinfo is None:
            valor = valor.replace(tzinfo=timezone.utc)
        return valor.isoformat()
    if isinstance(valor, str):
        return valor
    return ""


def _para_ms(valor: Any) -> int:
    """Converte datetime para timestamp em milissegundos (UTC)."""
    if isinstance(valor, datetime):
        if valor.tzinfo is None:
            valor = valor.replace(tzinfo=timezone.utc)
        return int(valor.timestamp() * 1000)
    if isinstance(valor, (int, float)):
        return int(valor)
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def normalizar_evento_preco(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza documento da coleção ``eventos_preco``."""
    return {
        "posto_id": str(raw["posto_id"]),
        "combustivel": str(raw["combustivel"]).upper(),
        "preco_anterior": float(raw.get("preco_anterior") or 0.0),
        "preco_novo": float(raw["preco_novo"]),
        "variacao_pct": float(raw.get("variacao_pct") or 0.0),
        "unidade": str(raw.get("unidade", "BRL_L")),
        "fonte": str(raw.get("fonte", "")),
        "ocorrido_em_iso": _para_iso(raw.get("ocorrido_em")),
        "ocorrido_em_ms": _para_ms(raw.get("ocorrido_em")),
        "revisado": bool(raw.get("revisado", False)),
    }


def normalizar_busca(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza documento da coleção ``buscas_usuarios``."""
    geo = raw.get("geo_centro") or {}
    coords = geo.get("coordinates") or [0.0, 0.0]
    return {
        "usuario_id": str(raw.get("usuario_id", "")),
        "tipo_combustivel": str(raw.get("tipo_combustivel", "")).upper(),
        "cidade": str(raw.get("cidade", "")),
        "estado": str(raw.get("estado", "")).upper(),
        "raio_km": int(raw.get("raio_km") or 0),
        "lon": float(coords[0]) if coords else 0.0,
        "lat": float(coords[1]) if len(coords) > 1 else 0.0,
        "consultado_em_iso": _para_iso(raw.get("consultado_em")),
        "consultado_em_ms": _para_ms(raw.get("consultado_em")),
        "resultado_count": int(raw.get("resultado_count") or 0),
    }


def normalizar_posto(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza documento da coleção ``postos`` para um Hash enxuto no Redis."""
    endereco = raw.get("endereco") or {}
    location = raw.get("location") or {}
    coords = location.get("coordinates") or [0.0, 0.0]
    return {
        "posto_id": str(raw["_id"]),
        "cnpj": str(raw.get("cnpj", "")),
        "nome_fantasia": str(raw.get("nome_fantasia", "")),
        "bandeira": str(raw.get("bandeira", "")),
        "bairro": str(endereco.get("bairro", "")),
        "cidade": str(endereco.get("cidade", "")),
        "estado": str(endereco.get("estado", "")).upper(),
        "cep": str(endereco.get("cep", "")),
        "ativo": "1" if raw.get("ativo", True) else "0",
        "lon": float(coords[0]) if coords else 0.0,
        "lat": float(coords[1]) if len(coords) > 1 else 0.0,
    }
