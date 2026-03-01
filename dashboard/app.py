"""
Streamlit Dashboard — Fraud Detection Monitor (Main Page).

Shows live monitoring: KPIs, transactions, fraud alerts, charts, model metrics.
Case management and reports are on separate pages.
"""

import sys
import time
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import settings

st.set_page_config(
    page_title="Fraud Detection — Monitor",
    page_icon="shield",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    .stApp { background: #0f1117; font-family: 'Inter', sans-serif; }
    .dash-header {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 8px; padding: 16px 24px; margin-bottom: 20px;
        display: flex; align-items: center; justify-content: space-between;
    }
    .dash-header h1 { margin: 0; font-size: 20px; font-weight: 600; color: #f0f6fc; }
    .dash-header .sub { color: #8b949e; font-size: 12px; margin-top: 2px; }
    .live-tag {
        display: inline-flex; align-items: center; gap: 6px;
        background: #0d1117; border: 1px solid #238636;
        padding: 4px 12px; border-radius: 16px;
        font-size: 11px; color: #3fb950; font-weight: 500;
    }
    .live-tag::before {
        content: ''; width: 6px; height: 6px;
        background: #3fb950; border-radius: 50%; animation: blink 1.5s infinite;
    }
    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
    .kpi {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 8px; padding: 14px; text-align: center;
    }
    .kpi .label { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #8b949e; margin-bottom: 4px; }
    .kpi .value { font-size: 24px; font-weight: 700; color: #f0f6fc; }
    .kpi .value.red { color: #f85149; }
    .kpi .value.green { color: #3fb950; }
    .kpi .value.yellow { color: #d29922; }
    .kpi .value.blue { color: #58a6ff; }
    .kpi .value.orange { color: #db6d28; }
    .sec-head { font-size: 13px; font-weight: 600; color: #f0f6fc; margin: 20px 0 10px 0; text-transform: uppercase; letter-spacing: 0.5px; }
    .alert-strip { display: flex; gap: 10px; overflow-x: auto; padding: 4px 0 10px 0; }
    .alert-strip::-webkit-scrollbar { height: 4px; }
    .alert-strip::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
    .a-card {
        background: #161b22; border: 1px solid #f8514930;
        border-left: 3px solid #f85149; border-radius: 6px;
        padding: 10px 14px; min-width: 230px; flex-shrink: 0;
    }
    .a-card .a-name { font-size: 13px; font-weight: 600; color: #f85149; margin-bottom: 2px; }
    .a-card .a-email { font-size: 11px; color: #8b949e; margin-bottom: 6px; }
    .a-card .a-info { font-size: 11px; color: #8b949e; line-height: 1.6; }
    .a-card .a-info b { color: #c9d1d9; }
    .a-card .a-action {
        display: inline-block; margin-top: 6px; padding: 2px 8px;
        border-radius: 10px; font-size: 10px; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.3px;
    }
    .action-pending { background: #d2992220; color: #d29922; border: 1px solid #d2992240; }
    .alert-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px; }
    .alert-count { font-size: 11px; color: #f85149; background: #f8514915; border: 1px solid #f8514930; padding: 2px 10px; border-radius: 12px; font-weight: 500; }
    .m-badge {
        display: inline-block; background: #161b22; border: 1px solid #30363d;
        border-radius: 6px; padding: 8px 14px; text-align: center;
    }
    .m-badge .m-label { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
    .m-badge .m-val { font-size: 18px; font-weight: 700; color: #58a6ff; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_mongo():
    try:
        from database.mongo_client import MongoDBClient
        return MongoDBClient()
    except Exception:
        return None

@st.cache_resource
def get_redis():
    try:
        from feature_store.redis_client import RedisFeatureStore
        return RedisFeatureStore()
    except Exception:
        return None


def main():
    mongo = get_mongo()
    redis_store = get_redis()

    # Header
    st.markdown("""
    <div class="dash-header">
        <div><h1>Fraud Detection Monitor</h1><div class="sub">Real-time digital wallet transaction analysis</div></div>
        <div class="live-tag">LIVE</div>
    </div>""", unsafe_allow_html=True)

    # Refresh
    rc1, rc2 = st.columns([6, 1])
    with rc2:
        if st.button("Refresh", use_container_width=True):
            st.rerun()

    # KPIs
    stats = {"total_transactions": 0, "total_fraud": 0, "fraud_rate": 0.0, "avg_amount": 0.0, "pending": 0}
    if redis_store:
        try:
            rs = redis_store.get_global_stats()
            stats["total_transactions"] = rs.get("total_transactions", 0)
            stats["total_fraud"] = rs.get("total_frauds", 0)
            stats["fraud_rate"] = rs.get("fraud_rate", 0.0)
        except Exception: pass
    if mongo:
        try:
            ms = mongo.get_fraud_stats()
            if stats["total_transactions"] == 0: stats.update(ms)
            else: stats["avg_amount"] = ms.get("avg_amount", 0)
            stats["pending"] = mongo.transactions.count_documents({"case_status": "pending"})
        except Exception: pass

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.markdown(f'<div class="kpi"><div class="label">Transactions</div><div class="value blue">{stats["total_transactions"]:,}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="kpi"><div class="label">Fraud Detected</div><div class="value red">{stats["total_fraud"]:,}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="kpi"><div class="label">Pending Review</div><div class="value orange">{stats["pending"]:,}</div></div>', unsafe_allow_html=True)
    with c4: st.markdown(f'<div class="kpi"><div class="label">Fraud Rate</div><div class="value yellow">{stats["fraud_rate"]:.2f}%</div></div>', unsafe_allow_html=True)
    with c5: st.markdown(f'<div class="kpi"><div class="label">Avg Amount</div><div class="value green">${stats.get("avg_amount",0):,.2f}</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # Transaction table
    st.markdown('<div class="sec-head">Recent Transactions</div>', unsafe_allow_html=True)
    df = pd.DataFrame()
    if redis_store:
        try:
            txns = redis_store.get_recent_transactions(count=30)
            if txns: df = pd.DataFrame(txns)
        except Exception: pass
    if df.empty and mongo:
        try:
            txns = mongo.get_recent_transactions(limit=30)
            if txns:
                for tx in txns: tx.pop("_id", None)
                df = pd.DataFrame(txns)
        except Exception: pass

    if not df.empty:
        cols = [c for c in ["username", "email", "channel", "amount_src", "ip_country",
                            "fraud_probability", "action", "prediction"] if c in df.columns]
        tdf = df[cols].copy() if cols else df.iloc[:, :8].copy()
        if "amount_src" in tdf.columns: tdf["amount_src"] = tdf["amount_src"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
        if "fraud_probability" in tdf.columns: tdf["fraud_probability"] = tdf["fraud_probability"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "0.0000")
        if "prediction" in tdf.columns: tdf["prediction"] = tdf["prediction"].apply(lambda x: "FRAUD" if x == 1.0 else "Legit")
        if "action" in tdf.columns: tdf["action"] = tdf["action"].apply(lambda x: str(x).replace("_", " ").upper() if pd.notna(x) else "-")
        tdf = tdf.rename(columns={"username": "Name", "email": "Email", "channel": "Channel", "amount_src": "Amount", "ip_country": "Country", "fraud_probability": "Prob", "action": "Action", "prediction": "Status"})
        st.dataframe(tdf, use_container_width=True, hide_index=True, height=360)
    else:
        st.info("No transactions yet. Start the pipeline.")

    st.markdown("")

    # Fraud alerts
    alerts = []
    if redis_store:
        try: alerts = redis_store.get_recent_alerts(count=20)
        except Exception: pass

    if alerts:
        n = len(alerts)
        st.markdown(f'<div class="alert-header"><div class="sec-head" style="margin:0">Fraud Alerts</div><div class="alert-count">{n} flagged</div></div>', unsafe_allow_html=True)
        cards = []
        for alert in alerts[:12]:
            data = alert.get("data", alert)
            if isinstance(data, str):
                try: data = json.loads(data)
                except Exception: data = {}
            name = data.get("username", "Unknown")
            email = data.get("email", "")
            amt = float(data.get("amount_src", 0) or 0)
            prob = float(data.get("fraud_probability", 0) or 0)
            ch = data.get("channel", "-")
            action = data.get("action", "pending_review")
            cards.append(
                f'<div class="a-card"><div class="a-name">{name}</div>'
                f'<div class="a-email">{email}</div>'
                f'<div class="a-info"><b>${amt:,.2f}</b> &middot; {ch} &middot; Prob: <b>{prob:.3f}</b></div>'
                f'<span class="a-action action-pending">PENDING</span></div>'
            )
        st.markdown('<div class="alert-strip">' + ''.join(cards) + '</div>', unsafe_allow_html=True)

    st.markdown("")

    # Charts
    st.markdown('<div class="sec-head">Analytics</div>', unsafe_allow_html=True)
    chart_theme = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#8b949e", family="Inter"), margin=dict(t=36, b=36, l=36, r=20), height=280)
    c1, c2 = st.columns(2)
    with c1:
        fbc = []
        if mongo:
            try: fbc = mongo.get_fraud_by_channel()
            except Exception: pass
        if fbc:
            ch_df = pd.DataFrame(fbc).rename(columns={"_id": "channel"})
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ch_df["channel"], y=ch_df["total"], name="Total", marker_color="#58a6ff", opacity=0.5))
            fig.add_trace(go.Bar(x=ch_df["channel"], y=ch_df["fraud_count"], name="Fraud", marker_color="#f85149"))
            fig.update_layout(title="By Channel", barmode="overlay", legend=dict(orientation="h", y=-0.2), **chart_theme)
            fig.update_xaxes(gridcolor="#21262d"); fig.update_yaxes(gridcolor="#21262d")
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        if mongo:
            try:
                s = mongo.get_fraud_stats()
                tf, tl = s.get("total_fraud", 0), s.get("total_transactions", 0) - s.get("total_fraud", 0)
                if tf > 0 or tl > 0:
                    fig = go.Figure(data=[go.Pie(labels=["Legitimate", "Fraud"], values=[tl, tf], marker=dict(colors=["#3fb950", "#f85149"]), hole=0.55, textinfo="label+percent")])
                    fig.update_layout(title="Distribution", showlegend=True, legend=dict(orientation="h", y=-0.1), **chart_theme)
                    st.plotly_chart(fig, use_container_width=True)
            except Exception: pass

    st.markdown("")

    # Model metrics
    st.markdown('<div class="sec-head">Model Performance</div>', unsafe_allow_html=True)
    metrics = None
    if mongo:
        try:
            m = mongo.get_latest_metrics()
            if m: m.pop("_id", None); metrics = m
        except Exception: pass
    if metrics:
        items = [("AUC-ROC", metrics.get("auc_roc", 0)), ("Accuracy", metrics.get("accuracy", 0)), ("Precision", metrics.get("precision", 0)), ("Recall", metrics.get("recall", 0)), ("F1", metrics.get("f1_score", 0))]
        cols = st.columns(5)
        for col, (label, val) in zip(cols, items):
            with col: st.markdown(f'<div class="m-badge"><div class="m-label">{label}</div><div class="m-val">{val:.4f}</div></div>', unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')} | Click Refresh to update")


if __name__ == "__main__":
    main()
