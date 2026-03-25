"""
CryptoStream Dashboard
──────────────────────
Dashboard em tempo real com:
  • Preços ao vivo (Redis cache)
  • Gráficos de candlestick (Plotly)
  • Médias móveis sobrepostas
  • Feed de alertas em tempo real
  • Atualização automática a cada 5s
"""

import os
import json
import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text
import redis

# ── Config ────────────────────────────────────────────────────────────────────
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://cryptouser:cryptopass@localhost:5432/cryptodb")
REDIS_HOST   = os.getenv("REDIS_HOST",   "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
REFRESH_SECS = 5
SYMBOLS      = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "BNB-USD"]
SYMBOL_COLORS = {
    "BTC-USD":  "#F7931A",
    "ETH-USD":  "#627EEA",
    "SOL-USD":  "#9945FF",
    "DOGE-USD": "#C3A634",
    "BNB-USD":  "#F3BA2F",
}

# ── Streamlit Page Config ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="CryptoStream Monitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS customizado ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0d1117; }
    .stMetric { background: #161b22; border-radius: 8px; padding: 12px; }
    .stMetric label { color: #8b949e !important; }
    .alert-spike  { background: #1a2e1a; border-left: 3px solid #3fb950; padding: 8px 12px; border-radius: 4px; margin: 4px 0; }
    .alert-drop   { background: #2e1a1a; border-left: 3px solid #f85149; padding: 8px 12px; border-radius: 4px; margin: 4px 0; }
    .alert-anomaly{ background: #2e2a1a; border-left: 3px solid #d29922; padding: 8px 12px; border-radius: 4px; margin: 4px 0; }
</style>
""", unsafe_allow_html=True)


# ── Connections ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(POSTGRES_URL)

@st.cache_resource
def get_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


# ── Data Fetchers ─────────────────────────────────────────────────────────────
def fetch_latest_prices(r: redis.Redis) -> dict:
    """Lê último estado de cada símbolo do Redis (sub-milissegundo)."""
    result = {}
    if r is None:
        return result
    for sym in SYMBOLS:
        data = r.hgetall(f"crypto:latest:{sym}")
        if data:
            result[sym] = {
                "price":      float(data.get("price", 0)),
                "ma10":       float(data.get("ma10") or 0),
                "ma30":       float(data.get("ma30") or 0),
                "change_pct": float(data.get("change_pct") or 0),
                "is_anomaly": data.get("is_anomaly") == "1",
                "updated_at": data.get("updated_at", ""),
            }
    return result


def fetch_candlestick(engine, symbol: str, minutes: int = 60) -> pd.DataFrame:
    query = text("""
        SELECT candle_time, open, high, low, close, tick_count
        FROM candlestick_1min
        WHERE symbol = :symbol
          AND candle_time > NOW() - INTERVAL ':minutes minutes'
        ORDER BY candle_time ASC
        LIMIT 200
    """.replace(":minutes", str(minutes)))
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"symbol": symbol})
        return df
    except Exception:
        return pd.DataFrame()


def fetch_moving_averages(engine, symbol: str) -> pd.DataFrame:
    query = text("""
        SELECT ts, avg_price, ma10, ma30, price_change_pct, is_anomaly
        FROM moving_averages
        WHERE symbol = :symbol
        ORDER BY ts ASC
        LIMIT 200
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"symbol": symbol})
        return df
    except Exception:
        return pd.DataFrame()


def fetch_alerts(engine, limit: int = 20) -> pd.DataFrame:
    query = text("""
        SELECT symbol, alert_type, message, price, change_pct, triggered_at
        FROM crypto_alerts
        ORDER BY triggered_at DESC
        LIMIT :limit
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"limit": limit})
        return df
    except Exception:
        return pd.DataFrame()


# ── Chart Builders ────────────────────────────────────────────────────────────
def build_candlestick_chart(symbol: str, candles: pd.DataFrame, ma_df: pd.DataFrame) -> go.Figure:
    color = SYMBOL_COLORS.get(symbol, "#ffffff")

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.02,
    )

    if not candles.empty:
        fig.add_trace(go.Candlestick(
            x=candles["candle_time"],
            open=candles["open"],
            high=candles["high"],
            low=candles["low"],
            close=candles["close"],
            name=symbol,
            increasing_line_color="#3fb950",
            decreasing_line_color="#f85149",
        ), row=1, col=1)

        fig.add_trace(go.Bar(
            x=candles["candle_time"],
            y=candles["tick_count"],
            name="Ticks",
            marker_color=color,
            opacity=0.5,
        ), row=2, col=1)

    if not ma_df.empty:
        fig.add_trace(go.Scatter(
            x=ma_df["ts"], y=ma_df["ma10"],
            name="MA10", line=dict(color="#58a6ff", width=1.5, dash="dot"),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=ma_df["ts"], y=ma_df["ma30"],
            name="MA30", line=dict(color="#d29922", width=1.5, dash="dash"),
        ), row=1, col=1)

        # Marca anomalias
        anomalies = ma_df[ma_df["is_anomaly"]]
        if not anomalies.empty:
            fig.add_trace(go.Scatter(
                x=anomalies["ts"], y=anomalies["avg_price"],
                mode="markers",
                marker=dict(symbol="x", size=12, color="#f85149"),
                name="Anomalia",
            ), row=1, col=1)

    fig.update_layout(
        height=500,
        paper_bgcolor="#0d1117",
        plot_bgcolor="#0d1117",
        font=dict(color="#c9d1d9"),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(bgcolor="#161b22"),
        margin=dict(l=10, r=10, t=30, b=10),
    )
    fig.update_xaxes(gridcolor="#21262d", showgrid=True)
    fig.update_yaxes(gridcolor="#21262d", showgrid=True)

    return fig


# ── Layout Principal ──────────────────────────────────────────────────────────
def main():
    engine = get_engine()
    r      = get_redis()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## ⚡ CryptoStream")
        st.markdown("---")
        selected_symbol = st.selectbox("Símbolo", SYMBOLS, index=0)
        time_range      = st.slider("Janela (minutos)", 15, 120, 60, step=15)
        alert_threshold = st.number_input("Threshold de alerta (%)", 0.5, 10.0, 2.0, step=0.5)
        st.markdown("---")
        st.markdown(f"**Stack:**")
        st.markdown("- 🟠 Apache Kafka")
        st.markdown("- ⚡ Apache Spark")
        st.markdown("- 🐘 PostgreSQL")
        st.markdown("- 🔴 Redis")
        st.markdown("---")
        st.markdown(f"🔄 Atualiza a cada **{REFRESH_SECS}s**")
        last_update = st.empty()

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("# 📈 CryptoStream Monitor")
    st.markdown("*Streaming em tempo real com Kafka + Spark Structured Streaming*")
    st.markdown("---")

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    prices = fetch_latest_prices(r)
    cols   = st.columns(len(SYMBOLS))

    for i, sym in enumerate(SYMBOLS):
        with cols[i]:
            data = prices.get(sym, {})
            price      = data.get("price", 0)
            change_pct = data.get("change_pct", 0)
            is_anomaly = data.get("is_anomaly", False)

            label = f"{'⚠️ ' if is_anomaly else ''}{sym.replace('-USD', '')}"
            delta_str = f"{change_pct:+.2f}%" if change_pct else "—"
            price_str = f"${price:,.2f}" if price else "Aguardando..."

            st.metric(label=label, value=price_str, delta=delta_str)

    st.markdown("---")

    # ── Candlestick ───────────────────────────────────────────────────────────
    col_chart, col_alerts = st.columns([2, 1])

    with col_chart:
        st.subheader(f"📊 {selected_symbol} — Candlestick + Médias Móveis")
        candles = fetch_candlestick(engine, selected_symbol, time_range)
        ma_df   = fetch_moving_averages(engine, selected_symbol)
        fig     = build_candlestick_chart(selected_symbol, candles, ma_df)
        st.plotly_chart(fig, use_container_width=True)

        # Info técnica
        if not ma_df.empty:
            latest = ma_df.iloc[-1]
            c1, c2, c3 = st.columns(3)
            c1.metric("MA10", f"${latest.get('ma10', 0):,.4f}" if latest.get('ma10') else "—")
            c2.metric("MA30", f"${latest.get('ma30', 0):,.4f}" if latest.get('ma30') else "—")
            c3.metric("Δ% Janela", f"{latest.get('price_change_pct', 0):+.2f}%" if latest.get('price_change_pct') is not None else "—")

    # ── Feed de Alertas ───────────────────────────────────────────────────────
    with col_alerts:
        st.subheader("🔔 Alertas Recentes")
        alerts_df = fetch_alerts(engine, limit=15)

        if alerts_df.empty:
            st.info("Nenhum alerta ainda. O sistema detecta variações maiores que "
                    f"{alert_threshold}% e anomalias por Z-score.")
        else:
            for _, row in alerts_df.iterrows():
                alert_type = row["alert_type"]
                ts = pd.to_datetime(row["triggered_at"]).strftime("%H:%M:%S")
                change = f"{row['change_pct']:+.2f}%" if row["change_pct"] else ""

                if alert_type == "PRICE_SPIKE":
                    css_class = "alert-spike"
                    icon = "🟢"
                elif alert_type == "PRICE_DROP":
                    css_class = "alert-drop"
                    icon = "🔴"
                else:
                    css_class = "alert-anomaly"
                    icon = "⚠️"

                st.markdown(
                    f'<div class="{css_class}">'
                    f'<strong>{icon} {row["symbol"]}</strong> <small>{ts}</small><br>'
                    f'<small>{row["message"]}</small>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Tabela histórica ──────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📋 Últimas Métricas Agregadas (Spark Output)")
    try:
        with engine.connect() as conn:
            hist_df = pd.read_sql(
                text("""
                    SELECT symbol, window_start, window_end,
                           ROUND(avg_price::numeric, 4) AS avg_price,
                           ROUND(price_change_pct::numeric, 2) AS change_pct,
                           trade_count, is_anomaly
                    FROM crypto_metrics
                    ORDER BY window_end DESC
                    LIMIT 30
                """),
                conn,
            )
        if not hist_df.empty:
            st.dataframe(
                hist_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "is_anomaly": st.column_config.CheckboxColumn("Anomalia"),
                    "change_pct": st.column_config.NumberColumn("Δ%", format="%.2f%%"),
                },
            )
    except Exception as e:
        st.warning(f"Aguardando dados do Spark... ({e})")

    # ── Footer / Status ───────────────────────────────────────────────────────
    now = datetime.now().strftime("%H:%M:%S")
    last_update.markdown(f"Última atualização: **{now}**")

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    time.sleep(REFRESH_SECS)
    st.rerun()


if __name__ == "__main__":
    main()
