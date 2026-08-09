"""Phase 2 Step 6D -- read-only shadow operating dashboard.
PHASE2_TG_DASHBOARD_FINAL_SPEC_HANDOFF.md Section 1.

Single page, S1-S7, top to bottom. Reads ONLY the canonical
operating_state_latest view (Section 3's "one publisher, two renderers"
rule) via a real, separately-provisioned read-only Postgres role
(phase2_dashboard_reader -- see phase2/scripts/setup_dashboard_reader_
role.py) -- never the service-role DATABASE_URL, never any other table.

Hard rules enforced here, structurally, not just by convention:
- No execution/approval/edit/config-changing control anywhere in this
  file. Grep-verifiable: no st.button labeled approve/execute/confirm/
  edit, no st.form that writes anything, no database INSERT/UPDATE/
  DELETE statement anywhere in this module.
- Every action label reads PAPER BUY / PAPER SELL / PAPER HOLD -- the
  word PAPER is mandatory everywhere an action appears (Section 1.1).
- Read-only is enforced at the credential level (the role itself has no
  write grant on anything), not by this file's own discipline alone.
"""
import hashlib
import json
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

MAXDD_REFERENCE_PCT = -35.46  # sealed backtest MaxDD reference line, Section 1.4 S4
ABOVE_FOLD_STATUS_ORDER = ("GREEN", "WARNING", "REVIEW_REQUIRED", "HALT")
STATUS_COLORS = {
    "GREEN": "#1a7f37", "WARNING": "#9a6700", "REVIEW_REQUIRED": "#bc4c00", "HALT": "#cf222e",
}
HEALTH_STATUS_TO_DISPLAY = {
    "HEALTHY_FOR_SHADOW_SIGNAL": "GREEN",
    "HEALTHY_WITH_WARNINGS": "WARNING",
    "NEEDS_REVIEW_DATA_ISSUE": "REVIEW_REQUIRED",
    "HALT_SIGNAL_GENERATION_REAL_FAULT": "HALT",
}


def get_db_url() -> str:
    """Streamlit Cloud secrets first (st.secrets), local env var
    fallback for dev/testing -- never a hardcoded connection string."""
    try:
        if "PHASE2_DASHBOARD_READONLY_DATABASE_URL" in st.secrets:
            return st.secrets["PHASE2_DASHBOARD_READONLY_DATABASE_URL"]
    except Exception:
        pass
    return os.environ.get("PHASE2_DASHBOARD_READONLY_DATABASE_URL", "")


@st.cache_resource
def get_connection(db_url: str):
    conn = psycopg2.connect(db_url)
    # Real bug found 2026-08-09: this connection is cached across every
    # Streamlit rerun for the app's lifetime. Without autocommit, each
    # real SELECT leaves the session 'idle in transaction' indefinitely
    # -- a real Supabase pooler connection held for as long as the app
    # stays warm (observed: ~21 hours), contributing to real connection
    # drops elsewhere. The role is read-only (no write grant exists at
    # all), so autocommit is safe by construction, not just convenient.
    conn.autocommit = True
    return conn


def load_latest_snapshot(db_url: str) -> dict | None:
    conn = get_connection(db_url)
    cols = ["id", "as_of_date", "latest_bhavcopy_date", "scanner_status", "target_book", "paper_positions",
            "paper_actions", "paper_pnl", "health_status", "health_gates", "unresolved_issue_overlap",
            "materiality_flags", "quarantine_warnings", "ca_watch_status", "asm_gsm_status", "seam_status",
            "continuity_status", "archive_status", "supabase_status", "b2_status", "failed_gate",
            "exact_failure_reason", "capital_status", "exact_next_action", "recommendation", "created_at"]
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(cols)} FROM operating_state_latest")
            row = cur.fetchone()
    except Exception:
        conn.rollback()
        return None
    return dict(zip(cols, row)) if row else None


def _jsonb(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def render_status_banner(snap: dict) -> None:
    """S1 -- always above the fold. Overall status, dates, scanner
    status, capital status; failed_gate + exact_failure_reason inline,
    never hidden behind a click, when not GREEN."""
    display = HEALTH_STATUS_TO_DISPLAY.get(snap["health_status"], "UNKNOWN")
    color = STATUS_COLORS.get(display, "#57606a")
    st.markdown(
        f"<div style='background:{color};color:white;padding:16px 20px;border-radius:6px;'>"
        f"<span style='font-size:2rem;font-weight:700;'>{display}</span></div>",
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Signal date (as_of_date)", str(snap["as_of_date"]))
    c2.metric("Data date (latest_bhavcopy_date)", str(snap["latest_bhavcopy_date"]))
    if snap["as_of_date"] != snap["latest_bhavcopy_date"]:
        st.warning("as_of_date and latest_bhavcopy_date differ -- WARNING-level fact per spec.")
    c3.metric("Scanner status", snap["scanner_status"])
    c4.metric("Capital status", snap["capital_status"])
    if display in ("HALT", "REVIEW_REQUIRED"):
        st.error(f"Failed gate: **{snap['failed_gate']}** -- {snap['exact_failure_reason']}")


def render_next_action_strip(snap: dict) -> None:
    """S2 -- above the fold. exact_next_action verbatim + recommendation."""
    st.info(f"**Next:** {snap['exact_next_action']}")
    st.caption(snap["recommendation"])


def render_target_book(snap: dict) -> None:
    """S3 -- header row above the fold. Symbol/rank/score/weight/PAPER
    action chip; boundary indicator within 50 of 101/750; unresolved-
    issue-overlap row highlight."""
    st.subheader("S3 -- Target Book")
    tb = _jsonb(snap["target_book"]) or []
    overlap = {o.get("symbol"): o.get("reason") for o in (_jsonb(snap["unresolved_issue_overlap"]) or [])}
    if not tb:
        st.write("No target book generated.")
        return
    rows = []
    for r in tb:
        rank = r.get("rank")
        boundary = ""
        if isinstance(rank, (int, float)):
            if abs(rank - 101) <= 50:
                boundary = "near lower band (101)"
            elif abs(rank - 750) <= 50:
                boundary = "near upper band (750)"
        action = r.get("action", "HOLD")
        rows.append({
            "Symbol": r.get("symbol"), "Rank": rank, "Score": r.get("score"),
            "Weight": r.get("weight"), "Action": f"PAPER {action}",
            "Boundary": boundary, "Overlap": overlap.get(r.get("symbol"), ""),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch")


def render_paper_portfolio(snap: dict) -> None:
    """S4 -- positions, cycle actions (PAPER BUY/SELL/HOLD + reason
    code), paper_pnl block with the sealed backtest MaxDD reference."""
    st.subheader("S4 -- Paper Portfolio")
    pos = _jsonb(snap["paper_positions"]) or []
    if pos:
        st.dataframe(pd.DataFrame(pos), width="stretch")
    else:
        st.write("No open paper positions.")

    actions = _jsonb(snap["paper_actions"]) or []
    if actions:
        rows = [{"Symbol": a.get("symbol"), "Action": f"PAPER {a.get('intended_action', 'HOLD').replace('PAPER_', '')}",
                  "Reason": a.get("reason_code")} for a in actions]
        st.dataframe(pd.DataFrame(rows), width="stretch")

    pnl = _jsonb(snap["paper_pnl"]) or {}
    c1, c2, c3 = st.columns(3)
    c1.metric("Cumulative P&L (INR)", pnl.get("cumulative_pnl_inr", "n/a"))
    c2.metric("Cumulative P&L (%)", pnl.get("cumulative_pnl_pct", "n/a"))
    c3.metric("Sealed backtest MaxDD reference", f"{MAXDD_REFERENCE_PCT}%")


def render_health_gates(snap: dict) -> None:
    """S5 -- one row per Tier-2 gate: name, status, value, threshold,
    last-evaluated timestamp."""
    st.subheader("S5 -- Health Gates (Tier-2)")
    gates = _jsonb(snap["health_gates"]) or []
    if not gates:
        st.write("No gate results available.")
        return
    st.dataframe(pd.DataFrame(gates), width="stretch")


def render_data_pipeline_status(snap: dict) -> None:
    """S6 -- compact status-chip row + materiality-counted quarantine
    warnings (counts alone never determine status -- materiality class
    does)."""
    st.subheader("S6 -- Data Pipeline Status")
    chips = {
        "CA watch": snap["ca_watch_status"], "ASM/GSM watch": snap["asm_gsm_status"],
        "Price seam": snap["seam_status"], "Continuity": snap["continuity_status"],
        "Archive (aggregate)": snap["archive_status"], "Supabase": snap["supabase_status"],
        "B2": snap["b2_status"],
    }
    st.dataframe(pd.DataFrame([{"Component": k, "Status": v} for k, v in chips.items()]), width="stretch")

    mf = _jsonb(snap["materiality_flags"]) or {}
    qw = _jsonb(snap["quarantine_warnings"]) or {}
    c1, c2 = st.columns(2)
    c1.write("Materiality flags:"); c1.json(mf)
    c2.write("Quarantine warnings:"); c2.json(qw)


def snapshot_identity_line(snap: dict) -> str:
    """Snapshot parity footer -- operator ruling 2026-08-09 ("D. SNAPSHOT
    PARITY -- REQUIRED ON BOTH SURFACES"). Canonical hash definition
    identical to shadow_signal_output.compute_snapshot_identity() (the TG
    renderer): SHA-256 over json.dumps(row, sort_keys=True,
    separators=(',',':'), default=str) of the exact operating_state_latest
    row this dashboard just queried -- same row, same hash, provable
    parity with the TG digest, not just an assertion."""
    payload = json.dumps(snap, sort_keys=True, separators=(",", ":"), default=str)
    full_hash = hashlib.sha256(payload.encode()).hexdigest()
    return f"snapshot id={snap['id']} · as_of={snap['as_of_date']} · sha256={full_hash[:12]}"


def render_drill_down(snap: dict) -> None:
    """S7 -- collapsed by default, NOT above the fold. Read-only display
    of sealed records only."""
    with st.expander("S7 -- Drill-down (raw snapshot, read-only)", expanded=False):
        st.json({k: (str(v) if k in ("as_of_date", "latest_bhavcopy_date", "created_at") else v)
                 for k, v in snap.items()})


def main():
    st.set_page_config(page_title="Phase 2 Shadow Operating Dashboard", layout="wide")
    st.title("Phase 2 -- Shadow Operating Dashboard")
    st.caption("Read-only. Shadow/paper only. No execution, no broker, no MTF, no capital deployed.")

    db_url = get_db_url()
    if not db_url:
        st.error("PHASE2_DASHBOARD_READONLY_DATABASE_URL is not configured (Streamlit secrets or environment).")
        return

    snap = load_latest_snapshot(db_url)
    if snap is None:
        st.warning("No operating_state_snapshot row found yet -- pipeline has not published a real snapshot.")
        return

    render_status_banner(snap)      # S1, above the fold
    render_next_action_strip(snap)  # S2, above the fold
    render_target_book(snap)        # S3 header row, above the fold
    st.divider()
    render_paper_portfolio(snap)    # S4
    render_health_gates(snap)       # S5
    render_data_pipeline_status(snap)  # S6
    render_drill_down(snap)         # S7, collapsed, not above the fold

    st.caption(f"Snapshot published: {snap['created_at']} | Auto-refresh every 60s on Streamlit Cloud.")
    st.caption(snapshot_identity_line(snap))


if __name__ == "__main__":
    main()
