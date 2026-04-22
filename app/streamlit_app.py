"""Interface Streamlit da Plataforma Radar Combustível.

Consome apenas o Redis (serving layer) — o pipeline é responsável por
manter as estruturas atualizadas em tempo (quase) real a partir do
MongoDB.
"""

from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from redis import Redis
from redis.commands.search.query import Query
from redis.exceptions import ResponseError

from pipeline import chaves_redis as ck
from pipeline.redis_writer import INDICE_POSTOS, conectar

st.set_page_config(
    page_title="Plataforma Radar Combustível",
    layout="wide",
    page_icon="⛽",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None


@st.cache_resource(show_spinner=False)
def _redis() -> Redis:
    return conectar(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)


def _combustiveis_disponiveis(redis: Redis) -> List[str]:
    conhecidos = sorted(redis.smembers(ck.SET_COMBUSTIVEIS))
    if conhecidos:
        return conhecidos
    return [
        "GASOLINA_COMUM",
        "GASOLINA_ADITIVADA",
        "ETANOL",
        "DIESEL_S10",
        "DIESEL_COMUM",
        "GNV",
    ]


def _ufs_disponiveis(redis: Redis) -> List[str]:
    """Deriva UFs a partir das chaves ``ranking:menor_preco:{combustivel}:{uf}``."""
    vistos = set()
    for padrao in ("ranking:menor_preco:*:*",):
        for chave in redis.scan_iter(match=padrao, count=200):
            partes = chave.split(":")
            if len(partes) == 4 and len(partes[3]) == 2:
                vistos.add(partes[3].upper())
    return sorted(vistos)


def _ler_postos(redis: Redis, posto_ids: List[str]) -> Dict[str, Dict[str, str]]:
    if not posto_ids:
        return {}
    pipe = redis.pipeline()
    for pid in posto_ids:
        pipe.hgetall(f"{ck.PREFIXO_POSTO}:{pid}")
    resultados = pipe.execute()
    return {pid: (dados or {}) for pid, dados in zip(posto_ids, resultados)}


def _metricas_pipeline(redis: Redis) -> Dict[str, int]:
    crus = redis.hgetall(ck.HASH_METRICAS_PIPELINE)
    return {k: int(v) for k, v in crus.items()}


def _ultimos_eventos(redis: Redis, n: int = 15) -> List[Dict[str, str]]:
    try:
        registros = redis.xrevrange(ck.STREAM_EVENTOS, count=n)
    except ResponseError:
        return []
    return [campos for (_id, campos) in registros]


# =============================================================================
# Sidebar — controles globais
# =============================================================================
redis = _redis()

st.sidebar.title("⛽ Radar Combustível")
st.sidebar.caption("Serving layer: Redis | Origem: MongoDB Change Stream")

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
intervalo = st.sidebar.number_input(
    "Intervalo de refresh (s)", min_value=2, max_value=60, value=5, step=1
)

combustiveis = _combustiveis_disponiveis(redis)
combustivel = st.sidebar.selectbox("Combustível", combustiveis, index=0)

ufs = _ufs_disponiveis(redis)
uf_opcoes = ["(Todos)"] + ufs
uf_escolhida = st.sidebar.selectbox("UF", uf_opcoes, index=0)
uf_filtro = None if uf_escolhida == "(Todos)" else uf_escolhida

top_n = st.sidebar.slider("Tamanho do ranking", 5, 50, 15, step=5)

st.sidebar.markdown("---")
st.sidebar.markdown("**Observabilidade**")
metricas = _metricas_pipeline(redis)
st.sidebar.metric("Eventos de preço", metricas.get("eventos_preco_processados", 0))
st.sidebar.metric("Buscas processadas", metricas.get("buscas_processadas", 0))
st.sidebar.metric("Erros no processamento", metricas.get("erros_processamento", 0))

# =============================================================================
# Header
# =============================================================================
st.title("Plataforma Radar Combustível")
st.caption(
    "Monitoramento em tempo quase real de preços de combustíveis — pipeline MongoDB → Redis."
)

agora = datetime.now(timezone.utc).astimezone().strftime("%d/%m/%Y %H:%M:%S")
st.caption(f"Atualizado em {agora} | Redis em `{REDIS_HOST}:{REDIS_PORT}`")

# =============================================================================
# KPIs globais
# =============================================================================
kpi_cols = st.columns(4)
total_postos = redis.zcard(ck.geo_postos())
total_combustiveis = redis.scard(ck.SET_COMBUSTIVEIS)
eventos_stream = redis.xlen(ck.STREAM_EVENTOS) if redis.exists(ck.STREAM_EVENTOS) else 0

chave_ranking = ck.rk_menor_preco(combustivel, uf_filtro)
preco_min_score = redis.zrange(chave_ranking, 0, 0, withscores=True)
preco_min = float(preco_min_score[0][1]) if preco_min_score else math.nan
total_no_ranking = redis.zcard(chave_ranking)

kpi_cols[0].metric("Postos cadastrados", f"{total_postos:,}".replace(",", "."))
kpi_cols[1].metric("Combustíveis ativos", total_combustiveis)
kpi_cols[2].metric(
    "Menor preço no filtro atual",
    f"R$ {preco_min:.3f}" if not math.isnan(preco_min) else "—",
)
kpi_cols[3].metric("Eventos recentes (stream)", eventos_stream)

# =============================================================================
# Ranking de menor preço
# =============================================================================
st.subheader(f"🏆 Top {top_n} postos com menor preço — {combustivel}" +
             (f" (UF: {uf_filtro})" if uf_filtro else ""))
ranking = redis.zrange(chave_ranking, 0, top_n - 1, withscores=True)
if not ranking:
    st.info("Sem dados de ranking para o filtro atual. Aguardando eventos do pipeline.")
else:
    ids = [pid for pid, _ in ranking]
    postos = _ler_postos(redis, ids)
    linhas = []
    for posicao, (posto_id, preco) in enumerate(ranking, start=1):
        p = postos.get(posto_id, {})
        linhas.append(
            {
                "#": posicao,
                "posto_id": posto_id,
                "Nome Fantasia": p.get("nome_fantasia", "—"),
                "Bandeira": p.get("bandeira", "—"),
                "Cidade": p.get("cidade", "—"),
                "UF": p.get("estado", "—"),
                "Preço (R$)": round(float(preco), 3),
            }
        )
    df_rank = pd.DataFrame(linhas)
    col_a, col_b = st.columns([2, 1])
    with col_a:
        fig = px.bar(
            df_rank.sort_values("Preço (R$)", ascending=False),
            x="Preço (R$)",
            y="Nome Fantasia",
            orientation="h",
            color="Bandeira",
            title="Preços por posto (quanto menor, melhor)",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col_b:
        st.dataframe(
            df_rank.drop(columns=["posto_id"]),
            use_container_width=True,
            hide_index=True,
        )

# =============================================================================
# Mapa (GEORADIUS)
# =============================================================================
st.subheader("🗺️ Mapa — postos com menor preço no filtro atual")
col_map, col_cfg = st.columns([3, 1])
with col_cfg:
    raio_km = st.slider("Raio (km)", 1, 200, 50, step=1)
    lat_padrao = st.number_input("Latitude central", value=-23.55, format="%.4f")
    lon_padrao = st.number_input("Longitude central", value=-46.63, format="%.4f")
    limite = st.slider("Máx. postos no mapa", 10, 500, 100, step=10)

try:
    vizinhos = redis.execute_command(
        "GEOSEARCH",
        ck.geo_postos(),
        "FROMLONLAT",
        lon_padrao,
        lat_padrao,
        "BYRADIUS",
        raio_km,
        "km",
        "ASC",
        "COUNT",
        limite,
        "WITHCOORD",
    )
except ResponseError as exc:
    vizinhos = []
    st.warning(f"Consulta GEOSEARCH falhou: {exc}")

mapa_pontos: List[Dict[str, float]] = []
if vizinhos:
    ids_vizinhos = [item[0] for item in vizinhos]
    coords = {item[0]: (float(item[1][0]), float(item[1][1])) for item in vizinhos}
    postos = _ler_postos(redis, ids_vizinhos)
    precos = redis.zmscore(chave_ranking, ids_vizinhos) if ids_vizinhos else []
    for pid, preco in zip(ids_vizinhos, precos):
        dados = postos.get(pid, {})
        lon, lat = coords[pid]
        mapa_pontos.append(
            {
                "posto_id": pid,
                "nome": dados.get("nome_fantasia", "—"),
                "bandeira": dados.get("bandeira", "—"),
                "cidade": dados.get("cidade", "—"),
                "uf": dados.get("estado", "—"),
                "preco": round(float(preco), 3) if preco is not None else None,
                "lon": lon,
                "lat": lat,
            }
        )

if mapa_pontos:
    df_mapa = pd.DataFrame(mapa_pontos)
    with col_map:
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_mapa,
            get_position="[lon, lat]",
            get_radius=600,
            get_fill_color=[255, 99, 71, 180],
            pickable=True,
        )
        tooltip = {
            "html": "<b>{nome}</b><br/>{bandeira} • {cidade}/{uf}<br/>R$ {preco}",
            "style": {"backgroundColor": "#111", "color": "white"},
        }
        view_state = pdk.ViewState(latitude=lat_padrao, longitude=lon_padrao, zoom=8)
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))
    st.dataframe(
        df_mapa.sort_values("preco", na_position="last").head(30),
        use_container_width=True,
        hide_index=True,
    )
else:
    with col_map:
        st.info("Nenhum posto encontrado nesse raio — amplie o raio ou mude o centro.")

# =============================================================================
# Time Series — evolução do preço
# =============================================================================
st.subheader(f"📈 Evolução de preço — {combustivel}")
col_ts, col_info = st.columns([3, 1])

posto_para_ts = None
if ranking:
    posto_para_ts = ranking[0][0]
with col_info:
    posto_para_ts = st.text_input(
        "Posto (ID)", value=posto_para_ts or "", help="Identificador do posto (ObjectId)."
    )

if posto_para_ts:
    chave_ts = ck.ts_preco(posto_para_ts, combustivel)
    try:
        serie_posto = redis.execute_command("TS.RANGE", chave_ts, "-", "+")
    except ResponseError:
        serie_posto = []
else:
    serie_posto = []

chave_media = ck.ts_preco_medio(combustivel)
try:
    serie_media = redis.execute_command("TS.RANGE", chave_media, "-", "+")
except ResponseError:
    serie_media = []


def _serie_para_df(serie, label: str) -> pd.DataFrame:
    if not serie:
        return pd.DataFrame(columns=["ts", "preco", "serie"])
    df = pd.DataFrame(serie, columns=["ts", "preco"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df["preco"] = df["preco"].astype(float)
    df["serie"] = label
    return df


df_serie_posto = _serie_para_df(serie_posto, f"Posto {posto_para_ts[:8]}…" if posto_para_ts else "")
df_media = _serie_para_df(serie_media, "Preço médio")
df_merged = pd.concat([df_serie_posto, df_media], ignore_index=True)

with col_ts:
    if df_merged.empty:
        st.info("Sem série temporal disponível. Gere eventos para popular `ts:preco:*`.")
    else:
        fig_ts = px.line(
            df_merged,
            x="ts",
            y="preco",
            color="serie",
            title="Evolução do preço (R$ / L)",
            markers=True,
        )
        st.plotly_chart(fig_ts, use_container_width=True)

# =============================================================================
# Ranking por região (buscas)
# =============================================================================
st.subheader("🔥 Regiões com mais buscas")
buscas = redis.zrevrange(ck.rk_busca_por_regiao(), 0, 19, withscores=True)
if buscas:
    df_buscas = pd.DataFrame(buscas, columns=["Região (UF::Cidade)", "Buscas"])
    df_buscas["Buscas"] = df_buscas["Buscas"].astype(int)
    st.bar_chart(df_buscas, x="Região (UF::Cidade)", y="Buscas", use_container_width=True)
else:
    st.info("Sem dados de buscas. O pipeline preenche `ranking:buscas:regioes` conforme novas buscas chegam.")

# =============================================================================
# RediSearch — busca textual
# =============================================================================
st.subheader("🔎 Busca textual (RediSearch)")
col_txt, col_resultado = st.columns([1, 3])
with col_txt:
    termo = st.text_input("Nome fantasia / bandeira", "")
    bandeira = st.text_input("Filtrar bandeira (tag)", "")
    cidade_filtro = st.text_input("Filtrar cidade (tag)", "")
    max_resultados = st.slider("Máx. resultados", 5, 100, 20, step=5)

def _escapar_tag(valor: str) -> str:
    """Escapa espaços em tags para o parser RediSearch."""
    barra = chr(92)  # backslash — isolado porque f-strings <3.12 não aceitam '\'.
    return valor.replace(" ", barra + " ")


partes = []
if termo.strip():
    partes.append(f"@nome_fantasia:{termo.strip()}")
if bandeira.strip():
    partes.append(f"@bandeira:{{{_escapar_tag(bandeira.strip())}}}")
if cidade_filtro.strip():
    partes.append(f"@cidade:{{{_escapar_tag(cidade_filtro.strip())}}}")
if uf_filtro:
    partes.append(f"@estado:{{{uf_filtro}}}")

query_text = " ".join(partes) if partes else "*"
try:
    resultado = redis.ft(INDICE_POSTOS).search(
        Query(query_text).paging(0, max_resultados)
    )
    docs = [
        {
            "posto_id": getattr(d, "posto_id", d.id.split(":")[-1]),
            "Nome Fantasia": getattr(d, "nome_fantasia", "—"),
            "Bandeira": getattr(d, "bandeira", "—"),
            "Cidade": getattr(d, "cidade", "—"),
            "UF": getattr(d, "estado", "—"),
            "Bairro": getattr(d, "bairro", "—"),
        }
        for d in resultado.docs
    ]
    with col_resultado:
        st.caption(f"Consulta: `{query_text}` — {resultado.total} resultado(s)")
        if docs:
            st.dataframe(pd.DataFrame(docs), use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum posto encontrado para o filtro informado.")
except ResponseError as exc:
    with col_resultado:
        st.error(f"Busca falhou: {exc}")

# =============================================================================
# Últimos eventos (stream)
# =============================================================================
with st.expander("📜 Últimos eventos observados (stream Redis)", expanded=False):
    ultimos = _ultimos_eventos(redis, n=20)
    if ultimos:
        st.dataframe(pd.DataFrame(ultimos), use_container_width=True, hide_index=True)
    else:
        st.info("Stream `stream:eventos_preco` ainda vazio.")

# =============================================================================
# Auto-refresh
# =============================================================================
if auto_refresh:
    # st.rerun() agenda novo ciclo depois de `intervalo` segundos.
    import time as _time

    _time.sleep(float(intervalo))
    st.rerun()
