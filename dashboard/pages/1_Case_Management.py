"""
Case Management — Review, approve, and take action on flagged fraud cases.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Case Management", page_icon="shield", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    .stApp { background: #0f1117; font-family: 'Inter', sans-serif; }
    .sec-head { font-size: 13px; font-weight: 600; color: #f0f6fc; margin: 16px 0 10px 0; text-transform: uppercase; letter-spacing: 0.5px; }
    .case-stat {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 6px; padding: 10px; text-align: center;
    }
    .case-stat .cs-label { font-size: 10px; color: #8b949e; text-transform: uppercase; }
    .case-stat .cs-val { font-size: 20px; font-weight: 700; }
    .email-preview {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 8px; padding: 16px; margin: 10px 0;
        font-family: monospace; font-size: 12px; color: #c9d1d9;
        line-height: 1.6; white-space: pre-wrap;
    }
    .email-preview .ep-header {
        color: #8b949e; font-size: 11px; margin-bottom: 8px;
        padding-bottom: 8px; border-bottom: 1px solid #30363d;
    }
    .email-preview .ep-header b { color: #58a6ff; }
    .action-log-wrap {
        max-height: 360px; overflow-y: auto; padding-right: 4px;
        scrollbar-width: thin; scrollbar-color: #30363d transparent;
    }
    .action-log-wrap::-webkit-scrollbar { width: 4px; }
    .action-log-wrap::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
    .action-log {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 6px; padding: 8px 12px; margin-bottom: 4px;
    }
    .action-log .al-action {
        font-size: 10px; font-weight: 600; text-transform: uppercase;
        padding: 2px 8px; border-radius: 10px; display: inline-block;
    }
    .al-blocked { background: #db6d2820; color: #db6d28; }
    .al-alerted { background: #f8514920; color: #f85149; }
    .al-dismissed { background: #3fb95020; color: #3fb950; }
    .action-log .al-info { font-size: 11px; color: #8b949e; margin-top: 2px; }
    .action-log .al-info b { color: #c9d1d9; }
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


def admin_block_and_notify(mongo, tx_id):
    from notifications.notifier import NotificationService
    notifier = NotificationService(mongo_client=mongo)
    tx = mongo.transactions.find_one({"transaction_id": tx_id}, {"_id": 0})
    if tx:
        mongo.update_case_status(tx_id, "confirmed")
        mongo.transactions.update_one({"transaction_id": tx_id}, {"$set": {"action": "blocked"}})
        return notifier.send_block_notice(tx)
    return None


def admin_send_alert(mongo, tx_id):
    from notifications.notifier import NotificationService
    notifier = NotificationService(mongo_client=mongo)
    tx = mongo.transactions.find_one({"transaction_id": tx_id}, {"_id": 0})
    if tx:
        mongo.update_case_status(tx_id, "confirmed")
        mongo.transactions.update_one({"transaction_id": tx_id}, {"$set": {"action": "alerted"}})
        return notifier.send_fraud_alert(tx)
    return None


def admin_dismiss(mongo, tx_id):
    mongo.update_case_status(tx_id, "false_positive")
    mongo.transactions.update_one({"transaction_id": tx_id}, {"$set": {"action": "dismissed"}})


def generate_reason(tx):
    """Generate a human-readable reason for why a transaction was flagged."""
    reasons = []
    amt = float(tx.get("amount_src", 0) or 0)
    prob = float(tx.get("fraud_probability", 0) or 0)
    risk = float(tx.get("risk_score_internal", 0) or 0)
    ip_risk = float(tx.get("ip_risk_score", 0) or 0)
    device_trust = float(tx.get("device_trust_score", 1) or 1)
    vel_1h = int(tx.get("txn_velocity_1h", 0) or 0)
    vel_24h = int(tx.get("txn_velocity_24h", 0) or 0)
    new_device = tx.get("new_device_flag", tx.get("new_device", False))
    loc_mismatch = tx.get("location_mismatch_flag", tx.get("location_mismatch", False))
    chargebacks = int(tx.get("chargeback_history_count", 0) or 0)
    corridor = float(tx.get("corridor_risk", 0) or 0)

    if amt > 3000:
        reasons.append(f"Very high amount (${amt:,.2f})")
    elif amt > 1000:
        reasons.append(f"High amount (${amt:,.2f})")

    if risk > 0.6:
        reasons.append(f"High internal risk score ({risk:.2f})")
    elif risk > 0.4:
        reasons.append(f"Elevated risk score ({risk:.2f})")

    if ip_risk > 0.5:
        reasons.append(f"High-risk IP ({ip_risk:.2f})")

    if device_trust < 0.3:
        reasons.append(f"Untrusted device ({device_trust:.2f})")

    if new_device:
        reasons.append("New/unrecognized device")

    if loc_mismatch:
        reasons.append("Location mismatch detected")

    if vel_1h >= 5:
        reasons.append(f"High velocity: {vel_1h} txns in 1h")
    elif vel_1h >= 3:
        reasons.append(f"Elevated velocity: {vel_1h} txns in 1h")

    if vel_24h >= 15:
        reasons.append(f"Excessive 24h activity: {vel_24h} txns")

    if chargebacks >= 2:
        reasons.append(f"Chargeback history ({chargebacks})")

    if corridor > 0.5:
        reasons.append(f"High-risk corridor ({corridor:.2f})")

    if prob > 0.9:
        reasons.insert(0, f"Model confidence: {prob:.1%}")
    elif prob > 0.7:
        reasons.insert(0, f"Model confidence: {prob:.1%}")

    if not reasons:
        reasons.append("Multiple low-level signals combined")

    return "; ".join(reasons[:4])  # Max 4 reasons for readability


def main():
    mongo = get_mongo()

    st.markdown("## Case Management")
    st.caption("Review flagged fraud cases, take action, and send notifications to customers.")

    if not mongo:
        st.error("MongoDB not connected.")
        return

    # Case stats
    try:
        cs = mongo.get_case_stats()
    except Exception:
        cs = {"pending": 0, "confirmed": 0, "false_positive": 0, "under_review": 0}

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(f'<div class="case-stat"><div class="cs-label">Pending</div><div class="cs-val" style="color:#d29922">{cs["pending"]}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="case-stat"><div class="cs-label">Confirmed</div><div class="cs-val" style="color:#f85149">{cs["confirmed"]}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="case-stat"><div class="cs-label">False Positive</div><div class="cs-val" style="color:#3fb950">{cs["false_positive"]}</div></div>', unsafe_allow_html=True)
    with c4: st.markdown(f'<div class="case-stat"><div class="cs-label">Under Review</div><div class="cs-val" style="color:#58a6ff">{cs["under_review"]}</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # Pending cases
    st.markdown('<div class="sec-head">Pending Cases</div>', unsafe_allow_html=True)
    try:
        cases = mongo.get_fraud_cases(status_filter="pending", limit=30)
    except Exception:
        cases = []

    if not cases:
        st.success("No pending cases. All caught up.")
    else:
        # Add reason column
        for c in cases:
            c["reason"] = generate_reason(c)

        case_df = pd.DataFrame(cases)
        display_cols = [col for col in ["transaction_id", "username", "amount_src",
                                     "channel", "fraud_probability", "action", "reason"] if col in case_df.columns]
        if display_cols:
            cdf = case_df[display_cols].copy()
            if "amount_src" in cdf.columns: cdf["amount_src"] = cdf["amount_src"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
            if "fraud_probability" in cdf.columns: cdf["fraud_probability"] = cdf["fraud_probability"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "0.0000")
            if "action" in cdf.columns: cdf["action"] = cdf["action"].apply(lambda x: str(x).replace("_", " ").upper() if pd.notna(x) else "-")
            cdf = cdf.rename(columns={"transaction_id": "ID", "username": "Name", "amount_src": "Amount", "channel": "Channel", "fraud_probability": "Prob", "action": "Risk", "reason": "Reason"})
            st.dataframe(cdf, use_container_width=True, hide_index=True, height=320)

        st.markdown("---")
        st.markdown("**Take action:**")

        # Dropdown
        pending_ids = [c.get("transaction_id", "") for c in cases if c.get("transaction_id")]
        case_labels = {}
        case_lookup = {}
        for c in cases:
            tid = c.get("transaction_id", "")
            name = c.get("username", "?")
            amt = float(c.get("amount_src", 0) or 0)
            case_labels[tid] = f"{tid} — {name} · ${amt:,.2f}"
            case_lookup[tid] = c

        selected_id = st.selectbox("Select case", pending_ids, format_func=lambda x: case_labels.get(x, x))

        # Case detail panel
        if selected_id and selected_id in case_lookup:
            tx = case_lookup[selected_id]
            reason = tx.get("reason", generate_reason(tx))

            st.markdown(f"""
            <div style="background:#161b22; border:1px solid #30363d; border-left:3px solid #f85149; border-radius:6px; padding:14px 18px; margin:10px 0;">
                <div style="font-size:11px; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px;">Flagged Reason</div>
                <div style="font-size:13px; color:#f0f6fc; line-height:1.5;">{reason}</div>
                <div style="margin-top:10px; font-size:11px; color:#8b949e; line-height:1.8;">
                    <b style="color:#c9d1d9">Customer:</b> {tx.get("username","?")} ({tx.get("email","")}) &nbsp;
                    <b style="color:#c9d1d9">Amount:</b> ${float(tx.get("amount_src",0) or 0):,.2f} &nbsp;
                    <b style="color:#c9d1d9">Channel:</b> {tx.get("channel","-")} &nbsp;
                    <b style="color:#c9d1d9">Country:</b> {str(tx.get("ip_country","-")).upper()}<br>
                    <b style="color:#c9d1d9">Risk Score:</b> {float(tx.get("risk_score_internal",0) or 0):.2f} &nbsp;
                    <b style="color:#c9d1d9">IP Risk:</b> {float(tx.get("ip_risk_score",0) or 0):.2f} &nbsp;
                    <b style="color:#c9d1d9">Device Trust:</b> {float(tx.get("device_trust_score",0) or 0):.2f} &nbsp;
                    <b style="color:#c9d1d9">Velocity:</b> {tx.get("txn_velocity_1h",0)}/1h, {tx.get("txn_velocity_24h",0)}/24h &nbsp;
                    <b style="color:#c9d1d9">Chargebacks:</b> {tx.get("chargeback_history_count",0)}
                </div>
            </div>
            """, unsafe_allow_html=True)

        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            if st.button("Block & Notify", type="primary", use_container_width=True):
                notif = admin_block_and_notify(mongo, selected_id)
                if notif:
                    st.session_state["last_email"] = notif
                    st.session_state["last_action"] = "blocked"
                    st.rerun()
        with ac2:
            if st.button("Send Alert Email", use_container_width=True):
                notif = admin_send_alert(mongo, selected_id)
                if notif:
                    st.session_state["last_email"] = notif
                    st.session_state["last_action"] = "alerted"
                    st.rerun()
        with ac3:
            if st.button("Dismiss", use_container_width=True):
                admin_dismiss(mongo, selected_id)
                st.session_state["last_email"] = None
                st.session_state["last_action"] = "dismissed"
                st.rerun()

    st.markdown("")

    # Email preview
    if st.session_state.get("last_email"):
        email = st.session_state["last_email"]
        action = st.session_state.get("last_action", "sent")
        label = "BLOCKED & NOTIFIED" if action == "blocked" else "ALERT SENT"
        st.markdown(f'<div class="sec-head">Email Sent — {label}</div>', unsafe_allow_html=True)
        st.markdown(f'''<div class="email-preview">
<div class="ep-header">To: <b>{email.get("to","")}</b>
Subject: <b>{email.get("subject","")}</b>
Status: <b>SENT</b> at {email.get("created_at","")[:19]}</div>
{email.get("body","")}
</div>''', unsafe_allow_html=True)
    elif st.session_state.get("last_action") == "dismissed":
        st.success("Case dismissed as false positive.")

    st.markdown("")

    # Action log
    st.markdown('<div class="sec-head">Recent Actions</div>', unsafe_allow_html=True)
    try:
        from notifications.notifier import NotificationService
        ns = NotificationService(mongo_client=mongo)
        notifs = ns.get_recent_notifications(limit=10)
    except Exception:
        notifs = []

    if notifs:
        cards = []
        for n in notifs:
            action = n.get("action", "flagged")
            cls = "al-blocked" if action == "blocked" else ("al-alerted" if action == "flagged" else "al-dismissed")
            label = action.replace("_", " ").upper()
            cards.append(
                f'<div class="action-log"><span class="al-action {cls}">{label}</span>'
                f'<div class="al-info"><b>{n.get("username","")}</b> &middot; {n.get("to","")} &middot; ${n.get("amount",0):,.2f} &middot; {n.get("created_at","")[:19]}</div></div>'
            )
        st.markdown('<div class="action-log-wrap">' + ''.join(cards) + '</div>', unsafe_allow_html=True)
    else:
        st.caption("No actions taken yet.")


if __name__ == "__main__":
    main()
