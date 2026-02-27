"""
Streamlit Dashboard — Real-Time Fraud Detection Monitor.

A polished dark-themed dashboard with:
- KPI metric cards (total transactions, fraud detected, fraud rate, avg amount)
- Real-time transaction feed with fraud probability color-coding
- Live fraud alerts panel
- Time-series charts, fraud distribution, and channel breakdowns
- Model performance metrics
- Auto-refreshing every 5 seconds

Adapted for the Digital Wallet Transaction dataset.
"""

import sys
import time
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
    page_title="🛡️ Fraud Detection — Real-Time Monitor",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ─────────────────────────────────────────────
# Custom CSS — Premium Dark Theme
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* ── Base Theme ── */
    .stApp {
        background: linear-gradient(135deg, #0a0a1a 0%, #0d1117 50%, #0a0a1a 100%);
        font-family: 'Inter', sans-serif;
    }

    /* ── Header ── */
    .main-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 16px;
        padding: 24px 32px;
        margin-bottom: 24px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        box-shadow: 0 8px 32px rgba(99, 102, 241, 0.15);
    }
    .main-header h1 {
        margin: 0;
        font-size: 28px;
        font-weight: 700;
        background: linear-gradient(135deg, #818cf8, #a78bfa, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -0.5px;
    }
    .main-header .subtitle {
        color: #94a3b8;
        font-size: 14px;
        margin-top: 4px;
    }
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: rgba(16, 185, 129, 0.1);
        border: 1px solid rgba(16, 185, 129, 0.3);
        padding: 6px 14px;
        border-radius: 20px;
        font-size: 13px;
        color: #10b981;
        font-weight: 500;
    }
    .live-dot {
        width: 8px;
        height: 8px;
        background: #10b981;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); }
        50% { opacity: 0.7; box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
    }

    /* ── KPI Cards ── */
    .kpi-card {
        background: linear-gradient(135deg, #1e1e3f 0%, #1a1a2e 100%);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 16px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.2);
    }
    .kpi-label {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #94a3b8;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: #e2e8f0;
        line-height: 1.2;
    }
    .kpi-value.fraud { color: #f87171; }
    .kpi-value.success { color: #34d399; }
    .kpi-value.warning { color: #fbbf24; }
    .kpi-value.info { color: #818cf8; }

    /* ── Alert Cards ── */
    .alerts-container {
        max-height: 420px;
        overflow-y: auto;
        padding-right: 4px;
        scrollbar-width: thin;
        scrollbar-color: rgba(248,113,113,0.3) transparent;
    }
    .alerts-container::-webkit-scrollbar {
        width: 4px;
    }
    .alerts-container::-webkit-scrollbar-thumb {
        background: rgba(248,113,113,0.3);
        border-radius: 4px;
    }
    .alert-card {
        background: linear-gradient(135deg, #2d1b1b 0%, #1a1a2e 100%);
        border: 1px solid rgba(248, 113, 113, 0.4);
        border-left: 4px solid #f87171;
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 8px;
        box-shadow: 0 2px 8px rgba(248, 113, 113, 0.1);
        animation: alertPopIn 0.4s cubic-bezier(0.34, 1.56, 0.64, 1) both;
    }
    .alert-card:nth-child(1) { animation-delay: 0s; }
    .alert-card:nth-child(2) { animation-delay: 0.08s; }
    .alert-card:nth-child(3) { animation-delay: 0.16s; }
    .alert-card:nth-child(4) { animation-delay: 0.24s; }
    .alert-card:nth-child(5) { animation-delay: 0.32s; }
    @keyframes alertPopIn {
        0% {
            opacity: 0;
            transform: translateX(30px) scale(0.95);
        }
        100% {
            opacity: 1;
            transform: translateX(0) scale(1);
        }
    }
    .alert-card .alert-title {
        font-size: 13px;
        font-weight: 600;
        color: #f87171;
        margin-bottom: 3px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .alert-card .alert-detail {
        font-size: 11px;
        color: #94a3b8;
        margin: 1px 0;
        line-height: 1.4;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .alert-card .alert-detail strong {
        color: #e2e8f0;
    }
    .alert-count {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(248, 113, 113, 0.15);
        border: 1px solid rgba(248, 113, 113, 0.3);
        padding: 4px 12px;
        border-radius: 16px;
        font-size: 12px;
        color: #f87171;
        font-weight: 600;
        margin-bottom: 8px;
    }

    /* ── Section Titles ── */
    .section-title {
        font-size: 18px;
        font-weight: 600;
        color: #e2e8f0;
        margin: 24px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(99, 102, 241, 0.3);
    }

    /* ── Model Metric Badge ── */
    .metric-badge {
        display: inline-block;
        background: rgba(99, 102, 241, 0.1);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 8px;
        padding: 8px 16px;
        margin: 4px;
        text-align: center;
    }
    .metric-badge .badge-label {
        font-size: 11px;
        color: #94a3b8;
        text-transform: uppercase;
    }
    .metric-badge .badge-value {
        font-size: 20px;
        font-weight: 700;
        color: #818cf8;
    }

    /* ── Hide Streamlit defaults ── */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }

    /* ── Dataframe styling ── */
    .stDataFrame { border-radius: 12px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Data Loading Functions
# ─────────────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    """Get a cached MongoDB client."""
    try:
        from database.mongo_client import MongoDBClient
        return MongoDBClient()
    except Exception:
        return None


@st.cache_resource
def get_redis_client():
    """Get a cached Redis client."""
    try:
        from feature_store.redis_client import RedisFeatureStore
        return RedisFeatureStore()
    except Exception:
        return None


def load_stats(mongo, redis_store):
    """Load statistics from MongoDB and Redis."""
    stats = {
        "total_transactions": 0,
        "total_fraud": 0,
        "fraud_rate": 0.0,
        "avg_amount": 0.0,
        "total_amount": 0.0,
        "fraud_amount": 0.0,
    }

    if redis_store:
        try:
            redis_stats = redis_store.get_global_stats()
            stats["total_transactions"] = redis_stats.get("total_transactions", 0)
            stats["total_fraud"] = redis_stats.get("total_frauds", 0)
            stats["fraud_rate"] = redis_stats.get("fraud_rate", 0.0)
        except Exception:
            pass

    if mongo:
        try:
            mongo_stats = mongo.get_fraud_stats()
            if stats["total_transactions"] == 0:
                stats.update(mongo_stats)
            else:
                stats["avg_amount"] = mongo_stats.get("avg_amount", 0)
                stats["total_amount"] = mongo_stats.get("total_amount", 0)
                stats["fraud_amount"] = mongo_stats.get("fraud_amount", 0)
        except Exception:
            pass

    return stats


def load_recent_transactions(mongo, redis_store, limit=30):
    """Load recent transactions."""
    if redis_store:
        try:
            transactions = redis_store.get_recent_transactions(count=limit)
            if transactions:
                return pd.DataFrame(transactions)
        except Exception:
            pass

    if mongo:
        try:
            transactions = mongo.get_recent_transactions(limit=limit)
            if transactions:
                for tx in transactions:
                    tx.pop("_id", None)
                return pd.DataFrame(transactions)
        except Exception:
            pass

    return pd.DataFrame()


def load_fraud_alerts(redis_store, limit=20):
    """Load recent fraud alerts from Redis."""
    if redis_store:
        try:
            return redis_store.get_recent_alerts(count=limit)
        except Exception:
            pass
    return []


def load_model_metrics(mongo):
    """Load the latest model metrics."""
    if mongo:
        try:
            metrics = mongo.get_latest_metrics()
            if metrics:
                metrics.pop("_id", None)
                return metrics
        except Exception:
            pass
    return None


def load_fraud_by_channel(mongo):
    """Load fraud counts by channel."""
    if mongo:
        try:
            return mongo.get_fraud_by_channel()
        except Exception:
            pass
    return []


def load_timeline(mongo):
    """Load transaction timeline data."""
    if mongo:
        try:
            return mongo.get_transactions_over_time(interval_minutes=1)
        except Exception:
            pass
    return []


# ─────────────────────────────────────────────
# Dashboard Layout
# ─────────────────────────────────────────────
def render_header():
    """Render the dashboard header."""
    st.markdown("""
    <div class="main-header">
        <div>
            <h1>🛡️ Fraud Detection Monitor</h1>
            <div class="subtitle">Real-Time Digital Wallet Transaction Analysis</div>
        </div>
        <div class="live-indicator">
            <div class="live-dot"></div>
            LIVE
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_kpi_cards(stats):
    """Render the KPI metric cards row."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📊 Total Transactions</div>
            <div class="kpi-value info">{stats['total_transactions']:,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">🚨 Fraud Detected</div>
            <div class="kpi-value fraud">{stats['total_fraud']:,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📈 Fraud Rate</div>
            <div class="kpi-value warning">{stats['fraud_rate']:.2f}%</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        avg_amt = stats.get("avg_amount", 0)
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">💰 Avg Transaction</div>
            <div class="kpi-value success">${avg_amt:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)


def render_transaction_feed(df):
    """Render the real-time transaction feed table."""
    st.markdown('<div class="section-title">📋 Recent Transactions</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("No transactions yet. Start the pipeline to see live data.")
        return

    # Select display columns
    display_cols = []
    for c in ["username", "email", "channel", "amount_src",
              "ip_country", "kyc_tier",
              "fraud_probability", "prediction", "timestamp"]:
        if c in df.columns:
            display_cols.append(c)

    if not display_cols:
        display_cols = df.columns.tolist()[:8]

    display_df = df[display_cols].copy()

    # Format columns
    if "amount_src" in display_df.columns:
        display_df["amount_src"] = display_df["amount_src"].apply(
            lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00"
        )
    if "fraud_probability" in display_df.columns:
        display_df["fraud_probability"] = display_df["fraud_probability"].apply(
            lambda x: f"{x:.4f}" if pd.notna(x) else "0.0000"
        )
    if "prediction" in display_df.columns:
        display_df["prediction"] = display_df["prediction"].apply(
            lambda x: "🚨 FRAUD" if x == 1.0 else "✅ Legit"
        )
    if "device_trust_score" in display_df.columns:
        display_df["device_trust_score"] = display_df["device_trust_score"].apply(
            lambda x: f"{x:.3f}" if pd.notna(x) else "N/A"
        )

    # Rename for display
    rename_map = {
        "username": "Username",
        "email": "Email",
        "channel": "Channel",
        "amount_src": "Amount",
        "ip_country": "Country",
        "kyc_tier": "KYC Tier",
        "fraud_probability": "Fraud Prob",
        "prediction": "Status",
        "timestamp": "Timestamp",
    }
    display_df = display_df.rename(columns={
        k: v for k, v in rename_map.items() if k in display_df.columns
    })

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=350,
    )


def render_fraud_alerts(alerts):
    """Render the fraud alerts panel with scrollable container and pop-in animation."""
    st.markdown('<div class="section-title">🚨 Fraud Alerts</div>', unsafe_allow_html=True)

    if not alerts:
        st.info("No fraud alerts detected yet.")
        return

    alert_count = len(alerts)
    st.markdown(f'<div class="alert-count">🔔 {alert_count} alert{"s" if alert_count != 1 else ""}</div>', unsafe_allow_html=True)

    # Build all alert cards inside a scrollable container
    cards_html = '<div class="alerts-container">'

    for alert in alerts[:15]:
        data = alert.get("data", {})
        if isinstance(data, str):
            import json
            try:
                data = json.loads(data)
            except Exception:
                data = {}

        username = data.get("username", "N/A")
        email = data.get("email", "N/A")
        amount = data.get("amount_src", 0)
        prob = data.get("fraud_probability", 0)
        channel = data.get("channel", "N/A")
        country = data.get("ip_country", "N/A").upper()
        risk = data.get("risk_score_internal", 0)

        cards_html += f'''
        <div class="alert-card">
            <div class="alert-title">⚠️ {username}</div>
            <div class="alert-detail">👤 {email}</div>
            <div class="alert-detail">💰 <strong>${amount:,.2f}</strong> · {channel} · {country}</div>
            <div class="alert-detail">🎯 Prob: <strong>{prob:.3f}</strong> · Risk: <strong>{risk:.3f}</strong></div>
        </div>
        '''

    cards_html += '</div>'
    st.markdown(cards_html, unsafe_allow_html=True)


def render_charts(mongo):
    """Render the analytics charts."""
    st.markdown('<div class="section-title">📊 Analytics</div>', unsafe_allow_html=True)

    chart_col1, chart_col2 = st.columns(2)

    # ── Fraud by Channel ──
    with chart_col1:
        fraud_by_channel = load_fraud_by_channel(mongo)
        if fraud_by_channel:
            ch_df = pd.DataFrame(fraud_by_channel)
            ch_df = ch_df.rename(columns={"_id": "channel"})

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=ch_df["channel"],
                y=ch_df["total"],
                name="Total",
                marker_color="#818cf8",
                opacity=0.7,
            ))
            fig.add_trace(go.Bar(
                x=ch_df["channel"],
                y=ch_df["fraud_count"],
                name="Fraud",
                marker_color="#f87171",
            ))
            fig.update_layout(
                title="Transactions by Channel",
                barmode="overlay",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#94a3b8"),
                legend=dict(orientation="h", y=-0.15),
                margin=dict(t=40, b=40, l=40, r=20),
                height=350,
            )
            fig.update_xaxes(gridcolor="rgba(99,102,241,0.1)")
            fig.update_yaxes(gridcolor="rgba(99,102,241,0.1)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No channel data available yet.")

    # ── Fraud Distribution Pie ──
    with chart_col2:
        if mongo:
            try:
                stats = mongo.get_fraud_stats()
                total_fraud = stats.get("total_fraud", 0)
                total_legit = stats.get("total_transactions", 0) - total_fraud
                if total_fraud > 0 or total_legit > 0:
                    fig = go.Figure(data=[go.Pie(
                        labels=["Legitimate", "Fraud"],
                        values=[total_legit, total_fraud],
                        marker=dict(colors=["#34d399", "#f87171"]),
                        hole=0.6,
                        textinfo="label+percent",
                        textfont=dict(size=13),
                    )])
                    fig.update_layout(
                        title="Transaction Distribution",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#94a3b8"),
                        margin=dict(t=40, b=20, l=20, r=20),
                        height=350,
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

    # ── Timeline Chart ──
    timeline_data = load_timeline(mongo)
    if timeline_data:
        timeline_df = pd.DataFrame(timeline_data)
        timeline_df = timeline_df.rename(columns={"_id": "time"})

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(
                x=timeline_df["time"],
                y=timeline_df["total"],
                name="Transactions",
                line=dict(color="#818cf8", width=2),
                fill="tozeroy",
                fillcolor="rgba(129, 140, 248, 0.1)",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=timeline_df["time"],
                y=timeline_df["fraud_count"],
                name="Fraud",
                line=dict(color="#f87171", width=2, dash="dot"),
                fill="tozeroy",
                fillcolor="rgba(248, 113, 113, 0.1)",
            ),
            secondary_y=True,
        )
        fig.update_layout(
            title="Transaction Volume Over Time",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8"),
            legend=dict(orientation="h", y=-0.15),
            margin=dict(t=40, b=40, l=40, r=40),
            height=350,
        )
        fig.update_xaxes(gridcolor="rgba(99,102,241,0.1)")
        fig.update_yaxes(gridcolor="rgba(99,102,241,0.1)", secondary_y=False)
        fig.update_yaxes(gridcolor="rgba(248,113,113,0.1)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)


def render_model_metrics(metrics):
    """Render the model performance section."""
    st.markdown('<div class="section-title">🧠 Model Performance</div>', unsafe_allow_html=True)

    if not metrics:
        st.info("No model metrics available. Train a model first.")
        return

    cols = st.columns(5)
    metric_items = [
        ("AUC-ROC", metrics.get("auc_roc", 0), "#818cf8"),
        ("Accuracy", metrics.get("accuracy", 0), "#34d399"),
        ("Precision", metrics.get("precision", 0), "#fbbf24"),
        ("Recall", metrics.get("recall", 0), "#f87171"),
        ("F1 Score", metrics.get("f1_score", 0), "#818cf8"),
    ]

    for col_item, (label, value, color) in zip(cols, metric_items):
        with col_item:
            st.markdown(f"""
            <div class="metric-badge">
                <div class="badge-label">{label}</div>
                <div class="badge-value" style="color: {color}">{value:.4f}</div>
            </div>
            """, unsafe_allow_html=True)

    train_size = metrics.get("train_size", 0)
    test_size = metrics.get("test_size", 0)
    version = metrics.get("version", "N/A")
    st.caption(
        f"Model v{version} • "
        f"Trained on {train_size:,} samples • "
        f"Tested on {test_size:,} samples"
    )


# ─────────────────────────────────────────────
# Main Application
# ─────────────────────────────────────────────
def main():
    """Main dashboard render loop."""
    mongo = get_mongo_client()
    redis_store = get_redis_client()

    # ── Header ──
    render_header()

    # ── KPI Cards ──
    stats = load_stats(mongo, redis_store)
    render_kpi_cards(stats)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Main Layout: Transaction Feed + Alerts ──
    feed_col, alert_col = st.columns([3, 1])

    with feed_col:
        tx_df = load_recent_transactions(mongo, redis_store)
        render_transaction_feed(tx_df)

    with alert_col:
        alerts = load_fraud_alerts(redis_store)
        render_fraud_alerts(alerts)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts ──
    render_charts(mongo)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Model Metrics ──
    model_metrics = load_model_metrics(mongo)
    render_model_metrics(model_metrics)

    # ── Footer ──
    st.markdown("---")
    footer_col1, footer_col2, footer_col3 = st.columns(3)
    with footer_col1:
        st.caption(f"🕐 Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    with footer_col2:
        connected = []
        if mongo:
            connected.append("MongoDB ✅")
        else:
            connected.append("MongoDB ❌")
        if redis_store:
            connected.append("Redis ✅")
        else:
            connected.append("Redis ❌")
        st.caption(" | ".join(connected))
    with footer_col3:
        st.caption("Auto-refresh: 5 seconds")

    # ── Auto Refresh ──
    time.sleep(5)
    st.rerun()


if __name__ == "__main__":
    main()
