"""
Reports — Generate and download fraud detection reports.
"""

import sys
import io
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Reports", page_icon="shield", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    .stApp { background: #0f1117; font-family: 'Inter', sans-serif; }
    .sec-head { font-size: 13px; font-weight: 600; color: #f0f6fc; margin: 16px 0 10px 0; text-transform: uppercase; letter-spacing: 0.5px; }
    .report-card {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 8px; padding: 16px 20px; margin-bottom: 12px;
    }
    .report-card .rc-title { font-size: 14px; font-weight: 600; color: #f0f6fc; margin-bottom: 4px; }
    .report-card .rc-desc { font-size: 12px; color: #8b949e; }
    .stat-row {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 6px; padding: 12px 16px; margin-bottom: 6px;
    }
    .stat-row .sr-label { font-size: 11px; color: #8b949e; }
    .stat-row .sr-val { font-size: 16px; font-weight: 600; color: #f0f6fc; }
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


def generate_summary_report(mongo):
    """Generate a text summary report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = mongo.get_fraud_stats()
    case_stats = mongo.get_case_stats()
    fbc = mongo.get_fraud_by_channel()

    total = stats.get("total_transactions", 0)
    fraud = stats.get("total_fraud", 0)
    rate = (fraud / total * 100) if total > 0 else 0
    avg = stats.get("avg_amount", 0)
    blocked = mongo.transactions.count_documents({"action": "blocked"})
    alerted = mongo.transactions.count_documents({"action": "alerted"})
    dismissed = mongo.transactions.count_documents({"action": "dismissed"})

    report = []
    report.append("=" * 60)
    report.append("FRAUD DETECTION — SUMMARY REPORT")
    report.append(f"Generated: {now}")
    report.append("=" * 60)
    report.append("")
    report.append("TRANSACTION OVERVIEW")
    report.append("-" * 40)
    report.append(f"  Total Transactions:     {total:,}")
    report.append(f"  Fraud Detected:         {fraud:,}")
    report.append(f"  Fraud Rate:             {rate:.2f}%")
    report.append(f"  Average Amount:         ${avg:,.2f}")
    report.append("")
    report.append("ACTIONS TAKEN")
    report.append("-" * 40)
    report.append(f"  Blocked & Notified:     {blocked:,}")
    report.append(f"  Alert Emails Sent:      {alerted:,}")
    report.append(f"  Dismissed (FP):         {dismissed:,}")
    report.append("")
    report.append("CASE STATUS")
    report.append("-" * 40)
    report.append(f"  Pending Review:         {case_stats.get('pending', 0):,}")
    report.append(f"  Confirmed Fraud:        {case_stats.get('confirmed', 0):,}")
    report.append(f"  False Positive:         {case_stats.get('false_positive', 0):,}")
    report.append(f"  Under Review:           {case_stats.get('under_review', 0):,}")
    report.append("")
    report.append("FRAUD BY CHANNEL")
    report.append("-" * 40)
    for ch in fbc:
        ch_name = ch.get("_id", "unknown")
        ch_total = ch.get("total", 0)
        ch_fraud = ch.get("fraud_count", 0)
        ch_rate = (ch_fraud / ch_total * 100) if ch_total > 0 else 0
        report.append(f"  {ch_name:<20} {ch_fraud:>4} / {ch_total:>5}  ({ch_rate:.1f}%)")
    report.append("")

    # Model metrics
    metrics = mongo.get_latest_metrics()
    if metrics:
        report.append("MODEL PERFORMANCE")
        report.append("-" * 40)
        report.append(f"  Algorithm:              {metrics.get('algorithm', 'N/A')}")
        report.append(f"  AUC-ROC:                {metrics.get('auc_roc', 0):.4f}")
        report.append(f"  Accuracy:               {metrics.get('accuracy', 0):.4f}")
        report.append(f"  Precision:              {metrics.get('precision', 0):.4f}")
        report.append(f"  Recall:                 {metrics.get('recall', 0):.4f}")
        report.append(f"  F1 Score:               {metrics.get('f1_score', 0):.4f}")
        report.append("")

    # Notifications
    try:
        from notifications.notifier import NotificationService
        ns = NotificationService(mongo_client=mongo)
        n_stats = ns.get_notification_stats()
        report.append("NOTIFICATIONS")
        report.append("-" * 40)
        report.append(f"  Total Sent:             {n_stats.get('total', 0):,}")
        report.append(f"  Block Notices:          {n_stats.get('blocked', 0):,}")
        report.append(f"  Fraud Alerts:           {n_stats.get('flagged', 0):,}")
        report.append("")
    except Exception:
        pass

    report.append("=" * 60)
    report.append("END OF REPORT")
    report.append("=" * 60)

    return "\n".join(report)


def generate_csv_export(mongo, export_type="all"):
    """Export transactions as CSV."""
    if export_type == "fraud":
        txns = mongo.get_fraud_transactions(limit=10000)
    elif export_type == "blocked":
        txns = list(mongo.transactions.find({"action": "blocked"}, {"_id": 0}).limit(10000))
    elif export_type == "notifications":
        try:
            from notifications.notifier import NotificationService
            ns = NotificationService(mongo_client=mongo)
            txns = ns.get_recent_notifications(limit=10000)
        except Exception:
            txns = []
    else:
        txns = mongo.get_recent_transactions(limit=10000)

    if txns:
        for t in txns:
            t.pop("_id", None)
        return pd.DataFrame(txns)
    return pd.DataFrame()


def main():
    mongo = get_mongo()

    st.markdown("## Reports")
    st.caption("Generate reports and export data for analysis.")

    if not mongo:
        st.error("MongoDB not connected.")
        return

    # Summary Report
    st.markdown('<div class="sec-head">Summary Report</div>', unsafe_allow_html=True)

    if st.button("Generate Summary Report", type="primary", use_container_width=False):
        with st.spinner("Generating report..."):
            report_text = generate_summary_report(mongo)
            st.session_state["report_text"] = report_text

    if "report_text" in st.session_state:
        st.code(st.session_state["report_text"], language="text")

        # Download button
        st.download_button(
            label="Download Report (.txt)",
            data=st.session_state["report_text"],
            file_name=f"fraud_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain",
        )

    st.markdown("")
    st.markdown("---")

    # Data Export
    st.markdown('<div class="sec-head">Data Export</div>', unsafe_allow_html=True)

    export_type = st.selectbox("Select data to export", [
        ("all", "All Transactions"),
        ("fraud", "Fraud Transactions Only"),
        ("blocked", "Blocked Transactions"),
        ("notifications", "Notification Log"),
    ], format_func=lambda x: x[1])

    if st.button("Export as CSV", use_container_width=False):
        with st.spinner("Exporting..."):
            df = generate_csv_export(mongo, export_type[0])

        if not df.empty:
            st.success(f"Exported {len(df):,} records.")
            st.dataframe(df.head(20), use_container_width=True, hide_index=True, height=300)

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            st.download_button(
                label="Download CSV",
                data=csv_buffer.getvalue(),
                file_name=f"fraud_export_{export_type[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )
        else:
            st.warning("No data found for the selected export type.")

    st.markdown("")
    st.markdown("---")

    # Quick Stats
    st.markdown('<div class="sec-head">Quick Statistics</div>', unsafe_allow_html=True)

    stats = mongo.get_fraud_stats()
    case_stats = mongo.get_case_stats()
    total = stats.get("total_transactions", 0)
    fraud = stats.get("total_fraud", 0)
    rate = (fraud / total * 100) if total > 0 else 0
    blocked = mongo.transactions.count_documents({"action": "blocked"})
    alerted = mongo.transactions.count_documents({"action": "alerted"})

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="stat-row"><div class="sr-label">Total Transactions</div><div class="sr-val">{total:,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">Fraud Detected</div><div class="sr-val">{fraud:,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">Fraud Rate</div><div class="sr-val">{rate:.2f}%</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="stat-row"><div class="sr-label">Blocked</div><div class="sr-val">{blocked:,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">Alerted</div><div class="sr-val">{alerted:,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">Avg Amount</div><div class="sr-val">${stats.get("avg_amount", 0):,.2f}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="stat-row"><div class="sr-label">Pending</div><div class="sr-val">{case_stats.get("pending", 0):,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">Confirmed</div><div class="sr-val">{case_stats.get("confirmed", 0):,}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="stat-row"><div class="sr-label">False Positive</div><div class="sr-val">{case_stats.get("false_positive", 0):,}</div></div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
