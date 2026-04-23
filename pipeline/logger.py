"""Configuração central de logging em Português."""

from __future__ import annotations

import logging
import os
import sys

_FORMATO = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def configurar_logger(nome: str) -> logging.Logger:
    """Cria (ou recupera) um logger com formato padronizado.

    O nível é controlado por variável de ambiente ``LOG_LEVEL``
    (default ``INFO``). Todas as mensagens são escritas em stdout
    para facilitar captura por ``docker logs``.
    """
    nivel = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(nome)
    if logger.handlers:
        return logger

    logger.setLevel(nivel)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMATO))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
