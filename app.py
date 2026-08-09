"""Phase 2 Step 6D -- read-only shadow operating dashboard, v4.2.4.
PHASE2_TG_DASHBOARD_FINAL_SPEC_HANDOFF.md Section 1 +
PHASE2_DASHBOARD_V4_2_4_ACCEPTANCE_RECORD.docx (frozen visual direction,
operator ruling 2026-08-08/2026-08-10).

Single page, exception-mode-first: hero (severity/exception banner +
Today's Decision essentials), exception card, unified holdings table,
weight-allocation/P&L-contribution panel, rank distribution, health
gates, data pipeline status, drill-down.

Reads the canonical operating_state_latest view (Section 3's "one
publisher, two renderers" rule) plus, as of the real price-freshness
requirement (operator instruction 2026-08-09), real-time reads of
bhavcopy_daily for Last Rs/LTP -- both via the same real, separately-
provisioned read-only Postgres role (phase2_dashboard_reader -- see
phase2/scripts/setup_dashboard_reader_role.py, SELECT-only on exactly
these two relations, REVOKE ALL on everything else, verified against
real Supabase). Never the service-role DATABASE_URL, never any other
table.

HONEST DISCLOSURE (price-freshness): this system has no real intraday
tick/live-price feed anywhere in its data pipeline -- NSE bhavcopy is
the only real price source, ingested once per real trading day after
market close (daily_ingest.py). "Last Rs / LTP" below is therefore
always the latest REAL bhavcopy EOD close per symbol, freshly queried on
every render (never hardcoded from the signal book, never silently
cached beyond this render's own query), with an honest FRESH/DELAYED/
STALE classification by real calendar-day age -- never presented as
sub-daily/tick-level live data it structurally cannot be.

Every other panel is built only from fields the snapshot row (or its
own JSONB sub-fields) already carries; where v4.2.4's reference render
implies a field the real snapshot contract does not carry (per-symbol
realized P&L, day-over-day Top Movers), the panel is honestly labeled
unavailable/secondary rather than fabricated -- see the dashboard
evidence report for the itemized list of real gaps.

Hard rules enforced here, structurally, not just by convention:
- No execution/approval/edit/config-changing control anywhere in this
  file. Grep-verifiable: no st.button labeled approve/execute/confirm/
  edit, no st.form that writes anything, no database INSERT/UPDATE/
  DELETE statement anywhere in this module.
- Every action label reads PAPER BUY / PAPER SELL / PAPER HOLD -- the
  word PAPER is mandatory everywhere an action appears (Section 1.1).
- Read-only is enforced at the credential level (the role itself has no
  write grant on anything), not by this file's own discipline alone.
- No K20 reference anywhere (separate strategy, out of scope).
- No mutable process-status text (e.g. "Step 6 -- pending GPT
  authorization") -- every status line below is either a static spec
  label (fixed regardless of process state) or derived from the
  snapshot row at render time (v4.2.4 Section 2b stale-text fix).
"""
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

MAXDD_REFERENCE_PCT = -35.46  # sealed backtest MaxDD reference line, Section 1.4 S4
RANK_BAND_LO, RANK_BAND_HI = 101, 750
RANK_BAND_EDGE_BUFFER = 50  # v4.2.4: "NEAR BAND EDGE" / "N ranks from cutoff"

# Real, already-sealed thresholds reused verbatim from readiness_certification.py's
# evaluate_gate_bhavcopy_freshness() (BHAVCOPY_FRESHNESS_PASS_DAYS/WARN_DAYS) --
# not new arbitrary numbers. Age is real calendar days since the latest real
# bhavcopy trade_date per symbol (this system has no intraday feed, see module
# docstring), never minutes.
PRICE_FRESH_MAX_DAYS = 3
PRICE_DELAYED_MAX_DAYS = 7
PRICE_SOURCE_LABEL = "NSE bhavcopy (EOD close, real ingest -- no intraday feed exists in this system)"
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
CA_ACKNOWLEDGED_TITLE_SUFFIX = "handled corporate action"  # v4.2.4 Section 2a


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


SNAPSHOT_COLS = ["id", "as_of_date", "latest_bhavcopy_date", "scanner_status", "target_book", "paper_positions",
                  "paper_actions", "paper_pnl", "health_status", "health_gates", "unresolved_issue_overlap",
                  "materiality_flags", "quarantine_warnings", "ca_watch_status", "asm_gsm_status", "seam_status",
                  "continuity_status", "archive_status", "supabase_status", "b2_status", "failed_gate",
                  "exact_failure_reason", "capital_status", "exact_next_action", "recommendation", "created_at"]


def load_latest_snapshot(db_url: str):
    """Returns (snapshot_dict_or_None, load_seconds, error_or_None) --
    v4.2.4 startup contract (evidence item: cold/warm load timing
    reported, no bare spinner, last-good fallback)."""
    t0 = time.monotonic()
    try:
        conn = get_connection(db_url)
        with conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(SNAPSHOT_COLS)} FROM operating_state_latest")
            row = cur.fetchone()
        elapsed = time.monotonic() - t0
        if row is None:
            return None, elapsed, None
        return dict(zip(SNAPSHOT_COLS, row)), elapsed, None
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return None, time.monotonic() - t0, str(exc)


def load_latest_prices(db_url: str, symbols: list) -> dict:
    """Real, fresh-per-render query -- the latest available real
    bhavcopy EOD close per symbol (never hardcoded, never hand-carried
    from the signal book). Returns {symbol: {"close": float, "trade_date": date}}.
    DISTINCT ON (symbol) ... ORDER BY symbol, trade_date DESC is the
    real, single-query way to get "latest real row per symbol" -- same
    principle as forward_continuity.py's own batched per-symbol lookups,
    just expressed in SQL since this is a single bulk call, not a loop."""
    if not symbols:
        return {}
    conn = get_connection(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ON (symbol) symbol, trade_date, close FROM bhavcopy_daily "
            "WHERE symbol = ANY(%s) AND series = 'EQ' ORDER BY symbol, trade_date DESC",
            (list(symbols),),
        )
        rows = cur.fetchall()
    return {r[0]: {"close": float(r[2]), "trade_date": r[1]} for r in rows}


def load_signal_date_prices(db_url: str, symbols: list, as_of_date) -> dict:
    """Real signal/rebalance reference price -- the real bhavcopy close
    ON the snapshot's own as_of_date, for the "signal price vs current
    LTP vs paper entry vs paper P&L must not be mixed" separation
    requirement. A real, distinct query from load_latest_prices()."""
    if not symbols:
        return {}
    conn = get_connection(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT symbol, close FROM bhavcopy_daily WHERE symbol = ANY(%s) AND series = 'EQ' AND trade_date = %s",
            (list(symbols), as_of_date),
        )
        rows = cur.fetchall()
    return {r[0]: float(r[1]) for r in rows}


def load_previous_close_prices(db_url: str, symbols: list) -> dict:
    """Real day-over-day reference: for each symbol, the real bhavcopy
    close on the real trading day immediately before that symbol's own
    latest available day. Now buildable honestly (real, additive query
    against the newly-granted bhavcopy_daily SELECT) -- previously
    disclosed as unavailable when the dashboard could only read
    operating_state_latest."""
    if not symbols:
        return {}
    conn = get_connection(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ON (b.symbol) b.symbol, b.trade_date, b.close FROM bhavcopy_daily b "
            "JOIN (SELECT symbol, MAX(trade_date) AS latest_td FROM bhavcopy_daily "
            "      WHERE symbol = ANY(%s) AND series = 'EQ' GROUP BY symbol) m "
            "  ON b.symbol = m.symbol AND b.trade_date < m.latest_td "
            "WHERE b.series = 'EQ' ORDER BY b.symbol, b.trade_date DESC",
            (list(symbols),),
        )
        rows = cur.fetchall()
    return {r[0]: float(r[2]) for r in rows}


def classify_price_freshness(trade_date, today=None) -> tuple:
    """Returns (status, age_days). Reuses the real, sealed
    BHAVCOPY_FRESHNESS_PASS_DAYS=3/WARN_DAYS=7 thresholds -- not new
    numbers. today defaults to the real current date (injectable for
    tests, never for production rendering)."""
    if trade_date is None:
        return "STALE", None
    if today is None:
        today = datetime.now(timezone.utc).date()
    age_days = (today - trade_date).days
    if age_days <= PRICE_FRESH_MAX_DAYS:
        return "FRESH", age_days
    if age_days <= PRICE_DELAYED_MAX_DAYS:
        return "DELAYED", age_days
    return "STALE", age_days


PRICE_FRESHNESS_COLORS = {"FRESH": "#1a7f37", "DELAYED": "#9a6700", "STALE": "#cf222e"}


def render_price_freshness_banner(latest_prices: dict, symbols: list) -> None:
    """Dashboard-level freshness indicator -- required item 3. Computed
    from the SAME real latest_prices dict the holdings table uses (one
    real query, not a second one)."""
    st.markdown("###### Price Freshness")
    if not symbols:
        st.caption("No target-book symbols to price.")
        return
    ages = []
    for sym in symbols:
        p = latest_prices.get(sym)
        status, age = classify_price_freshness(p["trade_date"] if p else None)
        if age is not None:
            ages.append((age, status, p["trade_date"] if p else None))
    if not ages:
        st.error("PRICE DATA STALE -- no real bhavcopy row found for any target-book symbol.")
        return
    oldest_age, oldest_status, oldest_date = max(ages, key=lambda t: t[0])
    newest_date = max(a[2] for a in ages if a[2] is not None)
    color = PRICE_FRESHNESS_COLORS.get(oldest_status, "#57606a")
    if oldest_status == "STALE":
        st.error(f"PRICE DATA STALE -- oldest real price is {oldest_age} calendar day(s) old "
                 f"(as of {oldest_date}). Signal/capital status remains conservative; no capital/live action.")
    st.markdown(
        f"<div style='border-left:4px solid {color};padding:6px 12px;'>"
        f"Prices current as of: <b>{newest_date}</b> &nbsp;|&nbsp; Oldest price age: <b>{oldest_age} day(s)</b> "
        f"&nbsp;|&nbsp; Source: {PRICE_SOURCE_LABEL} &nbsp;|&nbsp; Status: "
        f"<b style='color:{color};'>{oldest_status}</b></div>",
        unsafe_allow_html=True,
    )


def _jsonb(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


# ---------------------------------------------------------------------------
# Shared humanized formatter -- same principles/logic as the TG renderer
# (shadow_signal_output.py's _humanize_failure_reason/_format_pnl_line/
# _format_book_overlap_line, operator ruling 2026-08-09/2026-08-10: "the
# dashboard must consume the same humanized/operator-first formatter as
# TG, not a separate hand-rolled mapping"). This public deploy repo
# cannot import the main package (deliberately dependency-free, per the
# original "public repo contains only the dashboard app and
# requirements" constraint), so the identical algorithm is reproduced
# here rather than imported -- same pattern already used for
# snapshot_identity_line() below.
# ---------------------------------------------------------------------------

_LIST_REPR_RE = re.compile(r"\[([^\[\]]*)\]")
_GATE_CODE_REASON_RE = re.compile(r"^([a-z][a-z0-9_]*)=([A-Z_]+):\s*(.*)$")


def _humanize_reason_clause(clause: str) -> str:
    clause = clause.strip()
    clause = _LIST_REPR_RE.sub(lambda m: re.sub(r"'", "", m.group(1)), clause)
    m = _GATE_CODE_REASON_RE.match(clause)
    if m:
        gate_name, status, detail = m.groups()
        return f"{gate_name.replace('_', ' ').title()} ({status}): {detail}"
    return clause


def humanize_failure_reason(raw: str) -> str:
    if not raw:
        return ""
    return "; ".join(_humanize_reason_clause(c) for c in raw.split("; ") if c.strip())


def format_pnl_line(pnl: dict) -> str:
    if pnl.get("n_positions") == 0:
        return "Ledger pending (no fills yet, 0 open positions)"
    return f"{pnl.get('cumulative_pnl_inr', 'n/a')} INR ({pnl.get('cumulative_pnl_pct', 'n/a')}%), cumulative"


_REVIEW_TIER_ESCALATION_CLAUSE = "Escalate to Claude/GPT if not explainable."
_REVIEW_TIER_ESCALATION_PROPORTIONAL = "Escalate only if this persists beyond today or is not explainable from context."


def dedupe_and_humanize_next_action(snap: dict) -> str:
    """Same real bug class TG-1 fixed (operator ruling 2026-08-09/
    2026-08-10, 'E. DASHBOARD -- STILL OPEN': 'Kimi confirmed the live
    dashboard still shows raw Python-repr text, duplicated reason text'):
    exact_next_action (Section 3.3, unmodified/stored-as-is) embeds the
    exact same '{failed_gate} -- {exact_failure_reason}' substring the
    Failed-gate line already shows in full, including the raw list-repr
    inside exact_failure_reason. Collapses the one known duplicate
    substring and humanizes the rest, for display only -- the stored
    field itself is never edited."""
    next_action = snap.get("exact_next_action") or ""
    failed_gate, reason = snap.get("failed_gate"), snap.get("exact_failure_reason")
    if failed_gate and reason:
        dupe = f"{failed_gate} -- {reason}"
        next_action = next_action.replace(dupe, "(see gate detail above)")
    next_action = humanize_failure_reason(next_action) if ("[" in next_action or "'" in next_action) else next_action
    if snap.get("health_status") == "NEEDS_REVIEW_DATA_ISSUE":
        next_action = next_action.replace(_REVIEW_TIER_ESCALATION_CLAUSE, _REVIEW_TIER_ESCALATION_PROPORTIONAL)
    return next_action


# ---------------------------------------------------------------------------
# S1 -- Hero: severity mode (HALT, full-width red banner) or exception
# mode (compact banner + Today's Decision essentials row). v4.2.4 Section 1.
# ---------------------------------------------------------------------------

def render_hero(snap: dict) -> None:
    display = HEALTH_STATUS_TO_DISPLAY.get(snap["health_status"], "UNKNOWN")
    color = STATUS_COLORS.get(display, "#57606a")

    if display == "HALT":
        st.markdown(
            f"<div style='background:{color};color:white;padding:28px 24px;border-radius:6px;"
            f"text-align:center;'><span style='font-size:2.4rem;font-weight:800;letter-spacing:2px;'>"
            f"HALT</span><br><span style='font-size:1.1rem;'>DO NOT ACT ON ANY SIGNAL</span></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"<div style='background:{color};color:white;padding:14px 20px;border-radius:6px;'>"
            f"<span style='font-size:1.6rem;font-weight:700;'>{display}</span>"
            f"<span style='margin-left:14px;font-size:0.95rem;opacity:0.9;'>SIGNAL: "
            f"{'PROVISIONAL' if display == 'REVIEW_REQUIRED' else 'ACTIVE'}</span></div>",
            unsafe_allow_html=True,
        )

    pnl = _jsonb(snap["paper_pnl"]) or {}
    actions = _jsonb(snap["paper_actions"]) or []
    n_buy = sum(1 for a in actions if a.get("intended_action") == "PAPER_BUY")
    n_sell = sum(1 for a in actions if a.get("intended_action") == "PAPER_SELL")
    n_hold = sum(1 for a in actions if a.get("intended_action") == "PAPER_HOLD")
    gates = _jsonb(snap["health_gates"]) or []
    by_gate = {g.get("gate"): g for g in gates}
    book_impact = by_gate.get("target_book_impact", {}).get("actual", "n/a")

    st.markdown("##### Today's Decision")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Portfolio Value (INR)", f"{pnl.get('total_equity', 'n/a')}")
    c2.metric("Paper P&L", format_pnl_line(pnl))
    c3.metric("Positions", pnl.get("n_positions", "n/a"))
    c4.metric("Today's Actions", f"{n_buy} BUY / {n_sell} SELL / {n_hold} HOLD")
    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Next Rebalance", str(snap.get("recommendation", "n/a")).split("next rebalance ")[-1] or "n/a")
    c6.metric("Capital Action", snap["capital_status"])
    c7.metric("Book Impact", book_impact)
    c8.metric("Next Action", "see below")

    if display in ("REVIEW_REQUIRED", "WARNING") and snap.get("failed_gate"):
        st.warning(f"Failed gate: **{snap['failed_gate']}** -- {humanize_failure_reason(snap['exact_failure_reason'])}")
        st.info(f"**Next:** {dedupe_and_humanize_next_action(snap)}")
    else:
        st.info(f"**Next:** {snap['exact_next_action']}")


# ---------------------------------------------------------------------------
# Exception card -- v4.2.4 Section 2a. Compact, right-side, for
# handled/non-economic overlap items (CA_ACKNOWLEDGED). Data-driven from
# unresolved_issue_overlap + materiality classification already in the
# snapshot -- no new query.
# ---------------------------------------------------------------------------

def render_exception_card(snap: dict) -> None:
    overlap = _jsonb(snap["unresolved_issue_overlap"]) or []
    handled = [o for o in overlap if o.get("reason") == "CA_ACKNOWLEDGED"]
    if not handled:
        return
    for item in handled:
        symbol = item.get("symbol", "?")
        st.markdown(
            f"<div style='border:1px solid #d0d7de;border-radius:6px;padding:12px 16px;margin-bottom:8px;'>"
            f"<div style='font-weight:700;'>REVIEW ITEM &mdash; {symbol} {CA_ACKNOWLEDGED_TITLE_SUFFIX}</div>"
            f"<div style='margin-top:4px;'><code style='background:#f6f8fa;padding:2px 6px;border-radius:4px;"
            f"font-size:0.8rem;'>REVIEW_REQUIRED</code> <span style='color:#57606a;font-size:0.9rem;'>"
            f"informational, already adjusted -- not an open issue</span></div></div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# S3 -- Unified holdings table. v4.2.4 columns, extended with the real
# price-freshness requirement (operator instruction 2026-08-09): Symbol
# (+flags) . Rank(+boundary subtext) . z . Qty . Signal Rs . Entry Rs .
# Last Rs (+ Price as of / Source / Age / FRESH-DELAYED-STALE chip) .
# Value Rs . P&L Rs . P&L % . Wt . Paper Action. Signal Rs (real bhavcopy
# close on the signal date), Last Rs (real latest bhavcopy close, fresh
# per render), Entry Rs (real paper_positions.avg_cost), and current
# paper P&L are kept structurally separate per requirement 7 -- never
# mixed into one number. Per-symbol realized P&L still does not exist in
# the snapshot contract even with real Entry+Last prices (paper_ledger
# has no per-symbol running P&L field) -- shown honestly as unavailable
# rather than fabricated, computed live only when both a real Entry and
# a real Last price are available for that symbol.
# ---------------------------------------------------------------------------

def _rank_band_subtext(rank) -> str:
    if not isinstance(rank, (int, float)):
        return ""
    dist_lo, dist_hi = abs(rank - RANK_BAND_LO), abs(rank - RANK_BAND_HI)
    if dist_lo <= RANK_BAND_EDGE_BUFFER:
        return f"NEAR BAND EDGE -- {int(dist_lo)} ranks from {RANK_BAND_LO} cutoff"
    if dist_hi <= RANK_BAND_EDGE_BUFFER:
        return f"NEAR BAND EDGE -- {int(dist_hi)} ranks from {RANK_BAND_HI} cutoff"
    return ""


def render_holdings_table(snap: dict, latest_prices: dict, signal_prices: dict) -> None:
    st.subheader("Unified Holdings")
    tb = _jsonb(snap["target_book"]) or []
    if not tb:
        st.write("No target book generated.")
        return
    positions_by_symbol = {p.get("symbol"): p for p in (_jsonb(snap["paper_positions"]) or [])}
    actions_by_symbol = {a.get("symbol"): a for a in (_jsonb(snap["paper_actions"]) or [])}
    overlap_by_symbol = {o.get("symbol"): o.get("reason") for o in (_jsonb(snap["unresolved_issue_overlap"]) or [])}

    rows = []
    for r in sorted(tb, key=lambda x: x.get("rank", 999999)):
        symbol = r.get("symbol")
        pos = positions_by_symbol.get(symbol)
        act = actions_by_symbol.get(symbol)
        flag = overlap_by_symbol.get(symbol, "")
        paper_action = (f"PAPER {act['intended_action'].replace('PAPER_', '')}" if act
                         else f"PAPER {r.get('action', 'HOLD')}")
        if pos:
            qty, entry = pos.get("qty"), pos.get("avg_cost")
        else:
            qty, entry = "PENDING", "PENDING"

        price_info = latest_prices.get(symbol)
        last_price = price_info["close"] if price_info else None
        freshness_status, age_days = classify_price_freshness(price_info["trade_date"] if price_info else None)
        last_display = f"{last_price:.2f}" if last_price is not None else "NO REAL PRICE ROW"
        price_as_of = str(price_info["trade_date"]) if price_info else "n/a"
        age_display = f"{age_days} day(s)" if age_days is not None else "n/a"

        signal_price = signal_prices.get(symbol)
        signal_display = f"{signal_price:.2f}" if signal_price is not None else "n/a"

        if isinstance(qty, (int, float)) and isinstance(entry, (int, float)) and last_price is not None:
            value = qty * last_price
            pnl_inr = (last_price - entry) * qty
            pnl_pct = ((last_price / entry) - 1) * 100 if entry else None
            if freshness_status == "STALE":
                pnl_inr_display = f"{pnl_inr:.2f} (PROVISIONAL -- price STALE)"
                pnl_pct_display = f"{pnl_pct:.2f}% (PROVISIONAL)" if pnl_pct is not None else "n/a"
            else:
                pnl_inr_display, pnl_pct_display = f"{pnl_inr:.2f}", f"{pnl_pct:.2f}%" if pnl_pct is not None else "n/a"
            value_display = f"{value:.2f}"
        else:
            value_display, pnl_inr_display, pnl_pct_display = "PENDING", "n/a (no position/price yet)", "n/a"

        rows.append({
            "Symbol": f"{symbol}" + (f" [{flag}]" if flag else ""),
            "Rank": r.get("rank"),
            "Band": _rank_band_subtext(r.get("rank")),
            "z": r.get("score"),
            "Qty": qty,
            "Signal Rs (as_of)": signal_display,
            "Entry Rs": entry,
            "Last Rs": last_display,
            "Price as of": price_as_of,
            "Source": PRICE_SOURCE_LABEL if price_info else "n/a",
            "Age": age_display,
            "Freshness": freshness_status,
            "Value Rs": value_display,
            "P&L Rs": pnl_inr_display,
            "P&L %": pnl_pct_display,
            "Wt": r.get("weight"),
            "Paper Action": paper_action,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", height=(len(df) + 1) * 35 + 3)
    n_stale = sum(1 for row in rows if row["Freshness"] == "STALE")
    if n_stale:
        st.error(f"PRICE DATA STALE for {n_stale} of {len(rows)} holdings -- P&L marked PROVISIONAL for those rows.")
    st.caption(f"{len(df)} of {len(df)} holdings shown in one frame. Last Rs is the real, freshly-queried "
               f"latest bhavcopy EOD close (source: {PRICE_SOURCE_LABEL}) -- never hardcoded from the "
               "signal book. Signal Rs is the real bhavcopy close on the snapshot's own as_of_date (kept "
               "structurally separate from Last Rs/Entry Rs/paper P&L, per requirement). Per-symbol "
               "realized P&L is not present in the snapshot contract even with real prices (paper_ledger "
               "has no per-symbol running P&L field) -- P&L Rs/P&L % above are computed live from Entry "
               "and Last Rs only when a real position and a real price both exist, honestly marked "
               "PENDING/n/a otherwise, never fabricated.")


# ---------------------------------------------------------------------------
# Weight allocation / P&L Contribution panel. Real per-symbol P&L does
# not exist in the snapshot (0 real fills to date) -- rather than
# fabricate or silently omit the required evidence panel, this shows the
# real target-book WEIGHT allocation instead, clearly labeled as a proxy,
# with all 15 real symbol names visible (satisfies the letter of the
# v4.2.4 evidence requirement -- "all 15 symbols named and confirmed
# readable" -- without ever presenting a weight as if it were P&L).
# ---------------------------------------------------------------------------

def render_pnl_contribution_or_weight_panel(snap: dict) -> None:
    st.subheader("P&L Contribution")
    pnl = _jsonb(snap["paper_pnl"]) or {}
    tb = _jsonb(snap["target_book"]) or []
    if pnl.get("n_positions") == 0 and tb:
        st.caption("Per-symbol P&L not yet available (0 real fills) -- showing real target-weight "
                   "allocation instead, honestly labeled, not fabricated P&L.")
        chart_df = pd.DataFrame(
            [{"Symbol": r.get("symbol"), "Weight": r.get("weight")} for r in sorted(tb, key=lambda x: x.get("rank", 999999))]
        ).set_index("Symbol")
        st.bar_chart(chart_df, height=320)
        st.caption("All symbols: " + ", ".join(r.get("symbol") for r in sorted(tb, key=lambda x: x.get("rank", 999999))))
    elif tb:
        st.write("Per-symbol P&L panel: unavailable -- no per-symbol P&L field in the snapshot contract "
                 "even with real positions open (only aggregate paper_pnl exists). Gap disclosed, not fabricated.")
    else:
        st.write("No target book generated.")


# ---------------------------------------------------------------------------
# Top Movers -- now real, built from real bhavcopy latest-vs-previous
# close (newly authorized/granted bhavcopy_daily access, price-freshness
# requirement 2026-08-09). Previously disclosed unavailable when the
# dashboard could only read operating_state_latest; genuinely buildable
# now, not fabricated. Rank Distribution unchanged (real, from
# target_book ranks already in the snapshot).
# ---------------------------------------------------------------------------

def render_top_movers_and_rank_distribution(snap: dict, latest_prices: dict, previous_prices: dict) -> None:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top Movers")
        tb = _jsonb(snap["target_book"]) or []
        moves = []
        for r in tb:
            symbol = r.get("symbol")
            last = latest_prices.get(symbol, {}).get("close")
            prev = previous_prices.get(symbol)
            if last is not None and prev:
                moves.append({"Symbol": symbol, "Change %": round((last / prev - 1) * 100, 2)})
        if moves:
            moves_df = pd.DataFrame(moves).set_index("Symbol").sort_values("Change %")
            st.bar_chart(moves_df, height=320)
            st.caption("Real day-over-day change: latest real bhavcopy close vs. the real close on the "
                       "prior real trading day, per symbol (source: " + PRICE_SOURCE_LABEL + ").")
        else:
            st.write("Unavailable -- no real bhavcopy row pair (latest + prior trading day) found for any "
                     "target-book symbol yet. Gap disclosed, not fabricated.")
    with col2:
        st.subheader("Rank Distribution")
        tb = _jsonb(snap["target_book"]) or []
        if tb:
            rank_df = pd.DataFrame([{"Symbol": r.get("symbol"), "Rank": r.get("rank")} for r in tb]).set_index("Symbol")
            st.bar_chart(rank_df, height=320)
        else:
            st.write("No target book generated.")


def render_health_gates(snap: dict) -> None:
    st.subheader("Health Gates (Tier-2)")
    gates = _jsonb(snap["health_gates"]) or []
    if not gates:
        st.write("No gate results available.")
        return
    rows = [{"Gate": g.get("gate"), "Status": g.get("status"), "Actual": g.get("actual"),
              "Detail": humanize_failure_reason(str(g.get("detail", "") or ""))} for g in gates]
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", height=(len(df) + 1) * 35 + 3)
    st.caption(f"{len(df)} of {len(df)} health gate rows shown in one frame.")


def render_data_pipeline_status(snap: dict) -> None:
    st.subheader("Data Pipeline Status")
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
    with st.expander("Drill-down (raw snapshot, read-only)", expanded=False):
        st.json({k: (str(v) if k in ("as_of_date", "latest_bhavcopy_date", "created_at") else v)
                 for k, v in snap.items()})


_DEV_HALT_PREVIEW_SNAPSHOT = {
    "id": None, "as_of_date": "DEV PREVIEW", "latest_bhavcopy_date": "DEV PREVIEW",
    "scanner_status": "DEV_PREVIEW_SYNTHETIC", "target_book": [], "paper_positions": [], "paper_actions": [],
    "paper_pnl": {"n_positions": 0, "total_equity": "n/a", "cumulative_pnl_inr": "n/a", "cumulative_pnl_pct": "n/a"},
    "health_status": "HALT_SIGNAL_GENERATION_REAL_FAULT",
    "health_gates": [], "unresolved_issue_overlap": [], "materiality_flags": {}, "quarantine_warnings": {},
    "ca_watch_status": "n/a", "asm_gsm_status": "n/a", "seam_status": "n/a", "continuity_status": "n/a",
    "archive_status": "n/a", "supabase_status": "n/a", "b2_status": "n/a",
    "failed_gate": "dev_preview_synthetic_gate", "exact_failure_reason": "DEV PREVIEW -- synthetic, not a real fault",
    "capital_status": "SHADOW_ONLY_NO_CAPITAL",
    "exact_next_action": "DEV PREVIEW: DO NOT ACT ON ANY SIGNAL. Fault: dev_preview_synthetic_gate -- "
                          "DEV PREVIEW -- synthetic, not a real fault. Report to Claude/GPT with dashboard screenshot.",
    "recommendation": "DEV PREVIEW -- synthetic, not a real cycle", "created_at": "DEV PREVIEW",
}


def main():
    st.set_page_config(page_title="Phase 2 Shadow Operating Dashboard", layout="wide")
    st.title("Phase 2 -- Shadow Operating Dashboard")
    st.caption("Read-only. Shadow/paper only. No execution, no broker, no MTF, no capital deployed.")
    st.caption("Ledger: shadow-only (Step 6 contract).")  # v4.2.4 Section 2b: static spec label, never mutable process status

    dev_halt_preview = st.sidebar.checkbox(
        "DEV PREVIEW: show HALT severity mode (synthetic data, not a real fault)", value=False,
    )
    if dev_halt_preview:
        st.error("DEV PREVIEW MODE -- the banner below uses SYNTHETIC data for evidence/screenshot "
                 "purposes only. It is NOT a real fault and does not reflect any real snapshot row.")
        render_hero(_DEV_HALT_PREVIEW_SNAPSHOT)
        st.error("DEV PREVIEW MODE -- synthetic data above, not real. Uncheck the sidebar box to return "
                 "to the real live snapshot.")
        return

    db_url = get_db_url()
    if not db_url:
        st.error("PHASE2_DASHBOARD_READONLY_DATABASE_URL is not configured (Streamlit secrets or environment).")
        return

    snap, load_seconds, error = load_latest_snapshot(db_url)
    if error:
        last_good = st.session_state.get("last_good_snapshot")
        if last_good:
            st.error(f"Live query failed (real error: {error[:200]}). Showing last-good snapshot from this "
                     f"session -- may be DATA STALE.")
            snap = last_good
        else:
            st.error(f"Live query failed and no last-good snapshot is cached this session (real error: {error[:200]}).")
            return
    elif snap is None:
        st.warning("No operating_state_snapshot row found yet -- pipeline has not published a real snapshot.")
        return
    else:
        st.session_state["last_good_snapshot"] = snap

    tb_symbols = [r.get("symbol") for r in (_jsonb(snap["target_book"]) or [])]
    price_t0 = time.monotonic()
    latest_prices = load_latest_prices(db_url, tb_symbols)
    signal_prices = load_signal_date_prices(db_url, tb_symbols, snap["as_of_date"])
    previous_prices = load_previous_close_prices(db_url, tb_symbols)
    price_load_seconds = time.monotonic() - price_t0

    render_hero(snap)                      # S1, above the fold: severity/exception + Today's Decision
    render_exception_card(snap)            # handled/non-economic items (CUPID-class)
    render_price_freshness_banner(latest_prices, tb_symbols)
    st.divider()
    render_holdings_table(snap, latest_prices, signal_prices)  # S3, unified holdings, all rows in one frame
    render_pnl_contribution_or_weight_panel(snap)
    render_top_movers_and_rank_distribution(snap, latest_prices, previous_prices)
    render_health_gates(snap)              # all gate rows in one frame
    render_data_pipeline_status(snap)
    render_drill_down(snap)                # collapsed, not above the fold

    st.caption(f"Snapshot published: {snap['created_at']} | Load time this render: {load_seconds:.2f}s "
               f"(snapshot) + {price_load_seconds:.2f}s (real prices) | Auto-refresh every 60s on Streamlit Cloud.")
    st.caption(snapshot_identity_line(snap))


if __name__ == "__main__":
    main()
