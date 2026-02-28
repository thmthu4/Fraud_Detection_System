"""
Streamlit Dashboard — Real-Time Fraud Detection Monitor.

Clean, professional dashboard with:
- KPI metric cards
- Full-width transaction feed
- Horizontal fraud alert cards
- Analytics charts
- Model performance metrics
- Auto-refreshing every 5 seconds
"""

import sys
import time
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import settings

# ─────────────────────────────────────────────
# Page Configuration
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Fraud Detection — Live Monitor",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp {
        background: #0f1117;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Header */
    .dash-header {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 16px 24px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .dash-header h1 {
        margin: 0;
        font-size: 22px;
        font-weight: 600;
        color: #f0f6fc;
    }
    .dash-header .sub {
        color: #8b949e;
        font-size: 13px;
        margin-top: 2px;
    }
    .live-tag {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #0d1117;
        border: 1px solid #238636;
        padding: 4px 12px;
        border-radius: 16px;
        font-size: 12px;
        color: #3fb950;
        font-weight: 500;
    }
    .live-tag::before {
        content: '';
        width: 6px;
        height: 6px;
        background: #3fb950;
        border-radius: 50%;
        animation: blink 1.5s infinite;
    }
    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }

    /* KPI Row */
    .kpi {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
    .kpi .label {
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #8b949e;
        margin-bottom: 6px;
    }
    .kpi .value {
        font-size: 28px;
        font-weight: 700;
        color: #f0f6fc;
    }
    .kpi .value.red { color: #f85149; }
    .kpi .value.green { color: #3fb950; }
    .kpi .value.yellow { color: #d29922; }
    .kpi .value.blue { color: #58a6ff; }

    /* Section headers */
    .sec-head {
        font-size: 15px;
        font-weight: 600;
        color: #f0f6fc;
        margin: 20px 0 10px 0;
    }

    /* Alert strip */
    .alert-strip {
        display: flex;
        gap: 12px;
        overflow-x: auto;
        padding: 4px 0 10px 0;
    }
    .alert-strip::-webkit-scrollbar { height: 4px; }
    .alert-strip::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }

    .a-card {
        background: #161b22;
        border: 1px solid #f8514930;
        border-left: 3px solid #f85149;
        border-radius: 6px;
        padding: 10px 14px;
        min-width: 240px;
        flex-shrink: 0;
    }
    .a-card .a-name {
        font-size: 13px;
        font-weight: 600;
        color: #f85149;
        margin-bottom: 2px;
    }
    .a-card .a-email {
        font-size: 11px;
        color: #8b949e;
        margin-bottom: 6px;
    }
    .a-card .a-info {
        font-size: 11px;
        color: #8b949e;
        line-height: 1.6;
    }
    .a-card .a-info b {
        color: #c9d1d9;
    }

    /* Model metric */
    .m-badge {
        display: inline-block;
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 8px 14px;
        text-align: center;
    }
    .m-badge .m-label {
        font-size: 10px;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .m-badge .m-val {
        font-size: 18px;
        font-weight: 700;
        color: #58a6ff;
    }

    /* Hide Streamlit stuff */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }
    .stDataFrame { border-radius: 6px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    try:
        from database.mongo_client import MongoDBClient
        return MongoDBClient()
    except Exception:
        return None


@st.cache_resource
def get_redis_client():
    try:
        from feature_store.redis_client import RedisFeatureStore
        return RedisFeatureStore()
    except Exception:
        return None


def load_stats(mongo, redis_store):
    stats = {
        "total_transactions": 0, "total_fraud": 0,
        "fraud_rate": 0.0, "avg_amount": 0.0,
    }
    if redis_store:
        try:
            rs = redis_store.get_global_stats()
            stats["total_transactions"] = rs.get("total_transactions", 0)
            stats["total_fraud"] = rs.get("total_frauds", 0)
            stats["fraud_rate"] = rs.get("fraud_rate", 0.0)
        except Exception:
            pass
    if mongo:
        try:
            ms = mongo.get_fraud_stats()
            if stats["total_transactions"] == 0:
                stats.update(ms)
            else:
                stats["avg_amount"] = ms.get("avg_amount", 0)
        except Exception:
            pass
    return stats


def load_recent_transactions(mongo, redis_store, limit=30):
    if redis_store:
        try:
            txns = redis_store.get_recent_transactions(count=limit)
            if txns:
                return pd.DataFrame(txns)
        except Exception:
            pass
    if mongo:
        try:
            txns = mongo.get_recent_transactions(limit=limit)
            if txns:
                for tx in txns:
                    tx.pop("_id", None)
                return pd.DataFrame(txns)
        except Exception:
            pass
    return pd.DataFrame()


def load_fraud_alerts(redis_store, limit=20):
    if redis_store:
        try:
            return redis_store.get_recent_alerts(count=limit)
        except Exception:
            pass
    return []


def load_model_metrics(mongo):
    if mongo:
        try:
            m = mongo.get_latest_metrics()
            if m:
                m.pop("_id", None)
                return m
        except Exception:
            pass
    return None


def load_fraud_by_channel(mongo):
    if mongo:
        try:
            return mongo.get_fraud_by_channel()
        except Exception:
            pass
    return []


def load_timeline(mongo):
    if mongo:
        try:
            return mongo.get_transactions_over_time(interval_minutes=1)
        except Exception:
            pass
    return []


# ─────────────────────────────────────────────
# Render Functions
# ─────────────────────────────────────────────
def render_header():
    st.markdown("""
    <div class="dash-header">
        <div>
            <h1>🛡️ Fraud Detection Monitor</h1>
            <div class="sub">Real-time digital wallet transaction analysis</div>
        </div>
        <div class="live-tag">LIVE</div>
    </div>
    """, unsafe_allow_html=True)


def render_kpis(stats):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="kpi">
            <div class="label">Total Transactions</div>
            <div class="value blue">{stats['total_transactions']:,}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi">
            <div class="label">Fraud Detected</div>
            <div class="value red">{stats['total_fraud']:,}</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi">
            <div class="label">Fraud Rate</div>
            <div class="value yellow">{stats['fraud_rate']:.2f}%</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        avg = stats.get("avg_amount", 0)
        st.markdown(f"""<div class="kpi">
            <div class="label">Avg Transaction</div>
            <div class="value green">${avg:,.2f}</div>
        </div>""", unsafe_allow_html=True)


def render_transactions(df):
    st.markdown('<div class="sec-head">📋 Recent Transactions</div>', unsafe_allow_html=True)
    if df.empty:
        st.info("No transactions yet. Start the pipeline to see live data.")
        return

    cols = [c for c in ["username", "email", "channel", "amount_src",
                        "ip_country", "kyc_tier", "fraud_probability",
                        "prediction"] if c in df.columns]
    if not cols:
        cols = df.columns.tolist()[:8]

    tdf = df[cols].copy()

    if "amount_src" in tdf.columns:
        tdf["amount_src"] = tdf["amount_src"].apply(
            lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
    if "fraud_probability" in tdf.columns:
        tdf["fraud_probability"] = tdf["fraud_probability"].apply(
            lambda x: f"{x:.4f}" if pd.notna(x) else "0.0000")
    if "prediction" in tdf.columns:
        tdf["prediction"] = tdf["prediction"].apply(
            lambda x: "🚨 FRAUD" if x == 1.0 else "✅ Legit")

    tdf = tdf.rename(columns={
        "username": "Name", "email": "Email", "channel": "Channel",
        "amount_src": "Amount", "ip_country": "Country",
        "kyc_tier": "KYC", "fraud_probability": "Fraud Prob",
        "prediction": "Status",
    })

    st.dataframe(tdf, use_container_width=True, hide_index=True, height=400)


def render_alerts(alerts):
    if not alerts:
        return

    n = len(alerts)
    st.markdown(f'<div class="sec-head">🚨 Incoming Fraud Alerts ({n})</div>',
                unsafe_allow_html=True)

    # Render each alert card individually to avoid Streamlit HTML truncation
    cards = []
    for alert in alerts[:12]:
        data = alert.get("data", alert)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                data = {}

        name = data.get("username", "Unknown")
        email = data.get("email", "")
        amt = data.get("amount_src", 0)
        try:
            amt = float(amt)
        except (ValueError, TypeError):
            amt = 0.0
        prob = data.get("fraud_probability", 0)
        try:
            prob = float(prob)
        except (ValueError, TypeError):
            prob = 0.0
        ch = data.get("channel", "—")
        co = str(data.get("ip_country", "—")).upper()
        risk = data.get("risk_score_internal", 0)
        try:
            risk = float(risk)
        except (ValueError, TypeError):
            risk = 0.0

        cards.append(
            f'<div class="a-card">'
            f'<div class="a-name">⚠️ {name}</div>'
            f'<div class="a-email">{email}</div>'
            f'<div class="a-info">💰 <b>${amt:,.2f}</b> · {ch} · {co}<br>'
            f'🎯 Prob: <b>{prob:.3f}</b> · Risk: <b>{risk:.3f}</b></div>'
            f'</div>'
        )

    # Render as horizontal scrollable strip
    strip_html = '<div class="alert-strip">' + ''.join(cards) + '</div>'
    st.markdown(strip_html, unsafe_allow_html=True)


def render_charts(mongo):
    st.markdown('<div class="sec-head">📊 Analytics</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)

    with c1:
        fbc = load_fraud_by_channel(mongo)
        if fbc:
            ch_df = pd.DataFrame(fbc).rename(columns={"_id": "channel"})
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=ch_df["channel"], y=ch_df["total"],
                name="Total", marker_color="#58a6ff", opacity=0.6,
            ))
            fig.add_trace(go.Bar(
                x=ch_df["channel"], y=ch_df["fraud_count"],
                name="Fraud", marker_color="#f85149",
            ))
            fig.update_layout(
                title="Transactions by Channel", barmode="overlay",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8b949e"),
                legend=dict(orientation="h", y=-0.15),
                margin=dict(t=40, b=40, l=40, r=20), height=320,
            )
            fig.update_xaxes(gridcolor="#21262d")
            fig.update_yaxes(gridcolor="#21262d")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No channel data yet.")

    with c2:
        if mongo:
            try:
                s = mongo.get_fraud_stats()
                tf = s.get("total_fraud", 0)
                tl = s.get("total_transactions", 0) - tf
                if tf > 0 or tl > 0:
                    fig = go.Figure(data=[go.Pie(
                        labels=["Legitimate", "Fraud"],
                        values=[tl, tf],
                        marker=dict(colors=["#3fb950", "#f85149"]),
                        hole=0.55, textinfo="label+percent",
                        textfont=dict(size=12),
                    )])
                    fig.update_layout(
                        title="Transaction Distribution",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#8b949e"),
                        margin=dict(t=40, b=20, l=20, r=20), height=320,
                        showlegend=True,
                        legend=dict(orientation="h", y=-0.1),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No distribution data yet.")
            except Exception:
                st.info("No distribution data yet.")
        else:
            st.info("MongoDB not connected.")

    # Timeline
    tl_data = load_timeline(mongo)
    if tl_data:
        tl_df = pd.DataFrame(tl_data).rename(columns={"_id": "time"})
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(
            x=tl_df["time"], y=tl_df["total"], name="Transactions",
            line=dict(color="#58a6ff", width=2),
            fill="tozeroy", fillcolor="rgba(88,166,255,0.05)",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=tl_df["time"], y=tl_df["fraud_count"], name="Fraud",
            line=dict(color="#f85149", width=2, dash="dot"),
            fill="tozeroy", fillcolor="rgba(248,81,73,0.05)",
        ), secondary_y=True)
        fig.update_layout(
            title="Transaction Volume Over Time",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8b949e"),
            legend=dict(orientation="h", y=-0.15),
            margin=dict(t=40, b=40, l=40, r=40), height=320,
        )
        fig.update_xaxes(gridcolor="#21262d")
        fig.update_yaxes(gridcolor="#21262d", secondary_y=False)
        fig.update_yaxes(gridcolor="#21262d", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)


def render_model_metrics(metrics):
    st.markdown('<div class="sec-head">🧠 Model Performance</div>', unsafe_allow_html=True)
    if not metrics:
        st.info("No model metrics available. Train a model first.")
        return

    items = [
        ("AUC-ROC", metrics.get("auc_roc", 0)),
        ("Accuracy", metrics.get("accuracy", 0)),
        ("Precision", metrics.get("precision", 0)),
        ("Recall", metrics.get("recall", 0)),
        ("F1 Score", metrics.get("f1_score", 0)),
    ]
    cols = st.columns(5)
    for col, (label, val) in zip(cols, items):
        with col:
            st.markdown(f"""<div class="m-badge">
                <div class="m-label">{label}</div>
                <div class="m-val">{val:.4f}</div>
            </div>""", unsafe_allow_html=True)

    ts = metrics.get("train_size", 0)
    tes = metrics.get("test_size", 0)
    st.caption(f"Trained on {ts:,} samples · Tested on {tes:,} samples")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    mongo = get_mongo_client()
    redis_store = get_redis_client()

    render_header()
    render_kpis(load_stats(mongo, redis_store))

    st.markdown("")

    # Full-width transaction table
    render_transactions(load_recent_transactions(mongo, redis_store))

    st.markdown("")

    # Fraud alerts as horizontal strip
    render_alerts(load_fraud_alerts(redis_store))

    st.markdown("")

    # Charts
    render_charts(mongo)

    st.markdown("")

    # Model metrics
    render_model_metrics(load_model_metrics(mongo))

    # Footer
    st.markdown("---")
    fc1, fc2 = st.columns(2)
    with fc1:
        conns = []
        conns.append("MongoDB ✅" if mongo else "MongoDB ❌")
        conns.append("Redis ✅" if redis_store else "Redis ❌")
        st.caption(" · ".join(conns) + f" · Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    with fc2:
        st.caption("Auto-refresh: 5s")

    time.sleep(5)
    st.rerun()


if __name__ == "__main__":
    main()
