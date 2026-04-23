"""Carrega configurações de ambiente utilizadas pelo pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Permite .env em desenvolvimento local (dentro do container os valores vêm do compose).
load_dotenv(".env.local")
load_dotenv()


@dataclass(frozen=True)
class Configuracao:
    """Configurações imutáveis compartilhadas entre scripts."""

    mongo_uri: str
    db_name: str
    redis_host: str
    redis_port: int
    redis_password: str | None


def carregar() -> Configuracao:
    """Retorna configurações lidas das variáveis de ambiente."""
    return Configuracao(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"),
        db_name=os.getenv("DB_NAME", "radar_combustivel"),
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_password=os.getenv("REDIS_PASSWORD") or None,
    )
