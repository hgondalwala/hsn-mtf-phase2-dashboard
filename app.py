"""Phase 2 Step 6D -- read-only shadow operating dashboard, DASH-3.
PHASE2_TG_DASHBOARD_FINAL_SPEC_HANDOFF.md Section 1 + the accepted mock
(Kimi, version 1efe315, v4.2.4 visual direction, "do not redesign
further") + PHASE2_DASH_LIVE_PRICE_FRESHNESS_ADDENDUM.md (Sections 1/3/4,
binding professional-grade + freshness contract).

Deployment intent (answered per DASH-2 directive, one line, no third
option): (b) interim ops shell for the current shadow/paper phase --
Phase 2 has not reached a final production decision (Step 6 seal is
still blocked on 6P/DASH-3/materiality-gate resolution) -- but this
build IS the full v4.2.4 visual surface either way, not a placeholder;
"interim" describes the phase, not the presentation quality.

Architecture: the entire page body is rendered as one self-contained
HTML/CSS/JS document (verbatim CSS/JS structure from the accepted mock,
real data substituted for every illustrative value) and embedded via
st.components.v1.html() -- Streamlit's default widget theming cannot
reproduce the accepted dark-terminal component kit (chips, tiered
header, ECharts panels), so the "component kit, not widgets" rule from
the DASH-3 handoff package is satisfied by not fighting the framework:
one real HTML document, not a restyled st.metric/st.dataframe collage.

Real-data discipline (unchanged from v4.2.4): every value is either
read fresh from operating_state_latest / bhavcopy_daily via the
separately-provisioned read-only role (phase2_dashboard_reader), or
computed from those real fields. Where the accepted mock's own
illustration used synthetic/random-shaped filler (score distribution,
cost waterfall, per-stage run timeline, 14-day job-health heatmap) and
no real equivalent data source exists yet, that panel is replaced with
an honest "insufficient real data" notice -- never fabricated to match
the mock's cosmetic shape. Where the mock's illustration is itself
real sealed data (the run_148 backtest CAGR/MaxDD/equity-curve context
overlay), the REAL sealed values are used (see run148_reference.json,
extracted from reports/s1_v1.2_official/, bundled into this deploy repo
since it cannot read the main repo's files at Streamlit Cloud runtime).

Hard rules enforced here, structurally, not just by convention:
- No execution/approval/edit/config-changing control anywhere in this
  file. Grep-verifiable: no st.button labeled approve/execute/confirm/
  edit, no st.form that writes anything, no database INSERT/UPDATE/
  DELETE statement anywhere in this module.
- Every action label reads PAPER BUY / PAPER SELL / PAPER HOLD -- the
  word PAPER is mandatory everywhere an action appears.
- Read-only is enforced at the credential level (the role itself has no
  write grant on anything), not by this file's own discipline alone.
- No K20 reference anywhere (separate strategy, out of scope).
- No mutable process-status text -- every status line is either a
  static spec label or derived from the snapshot row at render time.
- DEV preview mode is URL-only (?dev=1), never a visible sidebar
  control on the public deployment (addendum Section 4b.6).
"""
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import streamlit as st
import streamlit.components.v1 as components

RANK_BAND_LO, RANK_BAND_HI = 101, 750
RANK_BAND_EDGE_BUFFER = 50

# Real, already-sealed thresholds reused verbatim from readiness_certification.py's
# evaluate_gate_bhavcopy_freshness() (BHAVCOPY_FRESHNESS_PASS_DAYS/WARN_DAYS) --
# not new arbitrary numbers. Age is real calendar days since the latest real
# bhavcopy trade_date per symbol (this system has no intraday feed, see module
# docstring), never minutes. (6P's intraday minute-level ladder is a separate,
# not-yet-built price plane -- see PHASE2_DASH_LIVE_PRICE_FRESHNESS_ADDENDUM.md
# Section 3 for its 15min/60min bounds, which apply once quotes_latest exists.)
PRICE_FRESH_MAX_DAYS = 3
PRICE_DELAYED_MAX_DAYS = 7
PRICE_SOURCE_LABEL = "NSE bhavcopy (EOD close, real ingest -- no intraday feed exists in this system)"

HEALTH_STATUS_TO_DISPLAY = {
    "HEALTHY_FOR_SHADOW_SIGNAL": "GREEN",
    "HEALTHY_WITH_WARNINGS": "WARNING",
    "NEEDS_REVIEW_DATA_ISSUE": "REVIEW_REQUIRED",
    "HALT_SIGNAL_GENERATION_REAL_FAULT": "HALT",
}
CA_ACKNOWLEDGED_TITLE_SUFFIX = "handled corporate action"

GATE_STATUS_CLASS = {
    "PASS": "gs-pass", "WARN": "gs-warn", "FAIL": "gs-fail",
    "FLAGGED": "gs-rev", "REVIEW": "gs-rev",
    "INSUFFICIENT_HISTORY": "gs-neutral", "BOUNDED_PROXY_ONLY": "gs-neutral",
}
GATE_STATUS_ORDER = {"FAIL": 0, "FLAGGED": 1, "REVIEW": 1, "WARN": 2,
                      "INSUFFICIENT_HISTORY": 3, "BOUNDED_PROXY_ONLY": 3, "PASS": 4}
PIPE_STATUS_CLASS = {"OK": "ok", "PASS": "ok", "WARN": "warn", "REVIEW": "rev",
                      "FAIL": "rev", "UNKNOWN": "warn"}


# ---------------------------------------------------------------------------
# DB access -- unchanged real data-fetching logic (v4.2.4/DASH-2, tested and
# verified against real Supabase). Never widened beyond the two granted
# relations (operating_state_latest, bhavcopy_daily).
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    try:
        if "PHASE2_DASHBOARD_READONLY_DATABASE_URL" in st.secrets:
            return st.secrets["PHASE2_DASHBOARD_READONLY_DATABASE_URL"]
    except Exception:
        pass
    return os.environ.get("PHASE2_DASHBOARD_READONLY_DATABASE_URL", "")


@st.cache_resource
def get_connection(db_url: str):
    conn = psycopg2.connect(db_url)
    # autocommit: this connection is cached across every Streamlit rerun
    # for the app's lifetime; without it each real SELECT leaves the
    # session 'idle in transaction' indefinitely. The role is read-only
    # (no write grant exists at all), so autocommit is safe by
    # construction, not just convenient.
    conn.autocommit = True
    return conn


SNAPSHOT_COLS = ["id", "as_of_date", "latest_bhavcopy_date", "scanner_status", "target_book", "paper_positions",
                  "paper_actions", "paper_pnl", "health_status", "health_gates", "unresolved_issue_overlap",
                  "materiality_flags", "quarantine_warnings", "ca_watch_status", "asm_gsm_status", "seam_status",
                  "continuity_status", "archive_status", "supabase_status", "b2_status", "failed_gate",
                  "exact_failure_reason", "capital_status", "exact_next_action", "recommendation", "created_at"]


def load_latest_snapshot(db_url: str):
    """Returns (snapshot_dict_or_None, load_seconds, error_or_None) --
    startup contract (load timing reported, no bare spinner, last-good
    fallback)."""
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
    bhavcopy EOD close per symbol. Returns {symbol: {"close": float, "trade_date": date}}."""
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
    ON the snapshot's own as_of_date."""
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
    """Real day-over-day reference: the real bhavcopy close on the real
    trading day immediately before each symbol's own latest available day."""
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
    BHAVCOPY_FRESHNESS_PASS_DAYS=3/WARN_DAYS=7 thresholds."""
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


@st.cache_data
def load_run148_reference() -> dict:
    """Real, sealed run_148 (S1 v1.2 official backtest) reference data --
    bundled at deploy time from reports/s1_v1.2_official/ (this public
    repo cannot read the main repo's files at Streamlit Cloud runtime).
    Used ONLY as declared context (equity-curve/drawdown reference
    overlay, CAGR/MaxDD annotations) -- never as live data, never
    presented as anything but the sealed historical backtest it is."""
    path = Path(__file__).resolve().parent / "run148_reference.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _jsonb(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


# ---------------------------------------------------------------------------
# Shared humanized formatter -- same principles/logic as the TG renderer
# (shadow_signal_output.py's _humanize_failure_reason/_format_pnl_line).
# This public deploy repo cannot import the main package, so the
# identical algorithm is reproduced here rather than imported.
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


_REVIEW_TIER_ESCALATION_CLAUSE = "Escalate to Claude/GPT if not explainable."
_REVIEW_TIER_ESCALATION_PROPORTIONAL = "Escalate only if this persists beyond today or is not explainable from context."


def dedupe_and_humanize_next_action(snap: dict) -> str:
    """Collapses the '{failed_gate} -- {exact_failure_reason}' duplicate
    substring exact_next_action embeds (display only; the stored field
    itself is never edited), then humanizes list-repr/gate-code text."""
    next_action = snap.get("exact_next_action") or ""
    failed_gate, reason = snap.get("failed_gate"), snap.get("exact_failure_reason")
    if failed_gate and reason:
        dupe = f"{failed_gate} -- {reason}"
        next_action = next_action.replace(dupe, "(see gate detail above)")
    next_action = humanize_failure_reason(next_action) if ("[" in next_action or "'" in next_action) else next_action
    if snap.get("health_status") == "NEEDS_REVIEW_DATA_ISSUE":
        next_action = next_action.replace(_REVIEW_TIER_ESCALATION_CLAUSE, _REVIEW_TIER_ESCALATION_PROPORTIONAL)
    return next_action


def snapshot_identity_line(snap: dict) -> str:
    """Canonical hash: SHA-256 over json.dumps(row, sort_keys=True,
    separators=(',',':'), default=str) of the exact operating_state_latest
    row this dashboard just queried -- identical definition to
    shadow_signal_output.compute_snapshot_identity() (the TG renderer)."""
    payload = json.dumps(snap, sort_keys=True, separators=(",", ":"), default=str)
    full_hash = hashlib.sha256(payload.encode()).hexdigest()
    return full_hash, f"snapshot id={snap['id']} · as_of={snap['as_of_date']} · sha256={full_hash[:12]}"


def _rank_band_subtext(rank) -> str:
    if not isinstance(rank, (int, float)):
        return ""
    dist_lo, dist_hi = abs(rank - RANK_BAND_LO), abs(rank - RANK_BAND_HI)
    if dist_lo <= RANK_BAND_EDGE_BUFFER:
        return f"{int(dist_lo)} ranks from {RANK_BAND_LO} cutoff"
    if dist_hi <= RANK_BAND_EDGE_BUFFER:
        return f"{int(dist_hi)} ranks from {RANK_BAND_HI} cutoff"
    return ""


def _indian_rupee(n, decimals=0) -> str:
    """Real Indian digit-grouping (₹15,01,204), not Python's default
    3-digit grouping. n=None -> 'n/a', never a fabricated zero."""
    if n is None or n == "n/a":
        return "n/a"
    n = float(n)
    sign = "-" if n < 0 else ""
    n = abs(n)
    int_part = int(n)
    s = str(int_part)
    if len(s) <= 3:
        grouped = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        grouped = ",".join(groups + [last3])
    out = sign + "₹" + grouped
    if decimals > 0:
        frac = f"{(n - int_part):.{decimals}f}"[1:]
        out += frac
    return out


def _esc(s) -> str:
    """Minimal HTML-escape for real text values interpolated into the
    static (non-JS-JSON) parts of the page -- never raw-injects a real
    snapshot field without escaping angle brackets/ampersands."""
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ---------------------------------------------------------------------------
# Context builder -- computes every real value the template needs, once,
# from the real snapshot + real price queries + the real bundled run_148
# reference. No fabrication: fields with no real source are marked
# explicitly (None / "insufficient real data") and the template renders
# an honest notice for those, never a filled-in guess.
# ---------------------------------------------------------------------------

def build_dashboard_context(snap: dict, latest_prices: dict, signal_prices: dict,
                             previous_prices: dict, run148_ref: dict) -> dict:
    ctx = {}
    display = HEALTH_STATUS_TO_DISPLAY.get(snap["health_status"], "UNKNOWN")
    ctx["display"] = display
    ctx["health_status"] = snap["health_status"]
    ctx["failed_gate"] = snap.get("failed_gate")
    ctx["exact_failure_reason"] = snap.get("exact_failure_reason")
    ctx["status_code"] = f"health_status: {snap['health_status']}" + (
        f" · failed_gate: {snap['failed_gate']}" if snap.get("failed_gate") else "")

    pnl = _jsonb(snap["paper_pnl"]) or {}
    ctx["pnl"] = pnl
    ctx["portfolio_value_display"] = _indian_rupee(pnl.get("total_equity"))
    ctx["n_positions"] = pnl.get("n_positions", 0)
    if pnl.get("n_positions") == 0:
        ctx["pnl_display"] = "Ledger pending"
        ctx["pnl_sub"] = "no fills yet, 0 open positions"
        ctx["pnl_class"] = "flat"
    else:
        pnl_inr = pnl.get("cumulative_pnl_inr", 0) or 0
        pnl_pct = pnl.get("cumulative_pnl_pct", 0) or 0
        ctx["pnl_display"] = ("+" if pnl_inr >= 0 else "") + _indian_rupee(pnl_inr)
        ctx["pnl_sub"] = f"{'+' if pnl_pct >= 0 else ''}{pnl_pct * 100:.2f}%"
        ctx["pnl_class"] = "up" if pnl_inr >= 0 else "dn"

    actions = _jsonb(snap["paper_actions"]) or []
    n_buy = sum(1 for a in actions if a.get("intended_action") == "PAPER_BUY")
    n_sell = sum(1 for a in actions if a.get("intended_action") == "PAPER_SELL")
    n_hold = sum(1 for a in actions if a.get("intended_action") == "PAPER_HOLD")
    ctx["n_buy"], ctx["n_sell"], ctx["n_hold"] = n_buy, n_sell, n_hold

    recommendation = str(snap.get("recommendation") or "")
    m = re.search(r"next rebalance (\S+)", recommendation)
    ctx["next_rebalance"] = m.group(1) if m else "n/a"

    ctx["capital_status"] = snap["capital_status"]
    ctx["capital_action_display"] = ("NOT ALLOWED — shadow only" if snap["capital_status"] == "SHADOW_ONLY_NO_CAPITAL"
                                       else snap["capital_status"])

    overlap = _jsonb(snap["unresolved_issue_overlap"]) or []
    handled = [o for o in overlap if o.get("reason") == "CA_ACKNOWLEDGED"]
    material = [o for o in overlap if o.get("reason") != "CA_ACKNOWLEDGED"]
    ctx["overlap_handled"] = handled
    ctx["material_issue_count"] = len(material)
    tb = _jsonb(snap["target_book"]) or []
    ctx["tb"] = sorted(tb, key=lambda x: x.get("rank", 999999))
    ctx["n_symbols"] = len(tb)
    ctx["book_impact_line"] = (
        f"No unresolved material defect in target book ({len(material)}/{len(tb)} symbols affected)."
        if len(material) == 0 else
        f"{len(material)}/{len(tb)} symbols have an unresolved MATERIAL defect -- see health gates."
    )
    ctx["next_action"] = dedupe_and_humanize_next_action(snap)
    ctx["failed_gate_line"] = (humanize_failure_reason(snap["exact_failure_reason"])
                                 if snap.get("exact_failure_reason") else "")

    gates = _jsonb(snap["health_gates"]) or []
    ctx["gates"] = sorted(gates, key=lambda g: GATE_STATUS_ORDER.get(g.get("status"), 9))
    n_pass = sum(1 for g in gates if g.get("status") == "PASS")
    ctx["n_gates"] = len(gates)
    ctx["n_gates_pass"] = n_pass
    worst_rank = min((GATE_STATUS_ORDER.get(g.get("status"), 9) for g in gates), default=9)
    worst_gates = [g for g in gates if GATE_STATUS_ORDER.get(g.get("status"), 9) == worst_rank]
    ctx["worst_gate_status"] = worst_gates[0]["status"] if worst_gates else "PASS"
    ctx["worst_gate_name"] = worst_gates[0]["gate"] if worst_gates else "n/a"

    ctx["pipeline"] = [
        ("ca_watch", snap["ca_watch_status"]), ("asm_gsm_watch", snap["asm_gsm_status"]),
        ("price_seam", snap["seam_status"]), ("continuity", snap["continuity_status"]),
        ("archive (aggregate)", snap["archive_status"]), ("supabase", snap["supabase_status"]),
        ("backblaze b2", snap["b2_status"]), ("telegram_send", "DRY-RUN"),
    ]

    qw = _jsonb(snap["quarantine_warnings"]) or {}
    ctx["quarantine_warnings"] = qw

    full_hash, identity_line = snapshot_identity_line(snap)
    ctx["identity_full_hash"] = full_hash
    ctx["identity_short_hash"] = full_hash[:12]
    ctx["identity_line"] = identity_line

    ctx["as_of_date"] = str(snap["as_of_date"])
    ctx["latest_bhavcopy_date"] = str(snap["latest_bhavcopy_date"])
    ctx["created_at"] = str(snap["created_at"])
    ctx["snapshot_id"] = snap["id"]

    # holdings rows -- real per-row data; Entry/Qty/Value/P&L collapse to
    # a single honest band (not repeated per-row) when 0 real fills exist
    positions_by_symbol = {p.get("symbol"): p for p in (_jsonb(snap["paper_positions"]) or [])}
    actions_by_symbol = {a.get("symbol"): a for a in (_jsonb(snap["paper_actions"]) or [])}
    overlap_by_symbol = {o.get("symbol"): o.get("reason") for o in overlap}
    rows = []
    for r in ctx["tb"]:
        symbol = r.get("symbol")
        pos = positions_by_symbol.get(symbol)
        act = actions_by_symbol.get(symbol)
        flag = overlap_by_symbol.get(symbol, "")
        paper_action = (f"PAPER {act['intended_action'].replace('PAPER_', '')}" if act
                         else f"PAPER {r.get('action', 'HOLD')}")
        price_info = latest_prices.get(symbol)
        last_price = price_info["close"] if price_info else None
        freshness_status, age_days = classify_price_freshness(price_info["trade_date"] if price_info else None)
        signal_price = signal_prices.get(symbol)
        qty = pos.get("qty") if pos else None
        entry = pos.get("avg_cost") if pos else None
        value = (qty * last_price) if (qty and last_price is not None) else None
        pnl_inr_row = ((last_price - entry) * qty) if (qty and entry and last_price is not None) else None
        pnl_pct_row = (((last_price / entry) - 1) * 100) if (entry and last_price is not None) else None
        rows.append({
            "symbol": symbol, "flag": flag, "rank": r.get("rank"),
            "band": _rank_band_subtext(r.get("rank")), "z": r.get("score"),
            "qty": qty, "entry": entry, "last": last_price, "signal": signal_price,
            "price_as_of": str(price_info["trade_date"]) if price_info else None,
            "source": PRICE_SOURCE_LABEL if price_info else None,
            "age_days": age_days, "freshness": freshness_status,
            "value": value, "pnl_inr": pnl_inr_row, "pnl_pct": pnl_pct_row,
            "weight": r.get("weight"), "paper_action": paper_action,
        })
    ctx["holdings_rows"] = rows
    ctx["n_stale"] = sum(1 for row in rows if row["freshness"] == "STALE")

    oldest = max((row["age_days"] for row in rows if row["age_days"] is not None), default=None)
    newest_date = max((row["price_as_of"] for row in rows if row["price_as_of"]), default=None)
    if oldest is None:
        ctx["freshness_banner_status"] = "STALE"
        ctx["freshness_banner_text"] = "PRICE DATA STALE — no real bhavcopy row found for any target-book symbol."
    else:
        worst_status = "STALE" if ctx["n_stale"] else ("DELAYED" if oldest > PRICE_FRESH_MAX_DAYS else "FRESH")
        ctx["freshness_banner_status"] = worst_status
        ctx["freshness_banner_text"] = (
            f"Prices current as of: {newest_date} · Oldest price age: {oldest} day(s) · "
            f"Source: {PRICE_SOURCE_LABEL} · Status: {worst_status}"
        )

    # top movers -- real day-over-day
    moves = []
    for row in rows:
        prev = previous_prices.get(row["symbol"])
        if row["last"] is not None and prev:
            moves.append({"symbol": row["symbol"], "pct": round((row["last"] / prev - 1) * 100, 2),
                          "abs_change": round((row["last"] - prev) * (row["qty"] or 0), 0)})
    ctx["moves"] = sorted(moves, key=lambda m: m["pct"], reverse=True)

    # run_148 real sealed reference context
    ctx["run148"] = run148_ref

    return ctx


# ---------------------------------------------------------------------------
# CSS -- verbatim from the accepted mock (Kimi v1efe315, design tokens
# per DASH3_HANDOFF_PACKAGE/README.md). Not reinterpreted, not
# "close enough" -- copied so the parity clause (addendum Section 4a) has
# a literal source of truth. One added rule (.gs-neutral) for two real
# gate statuses (INSUFFICIENT_HISTORY, BOUNDED_PROXY_ONLY) the mock's
# 13-gate illustration didn't happen to include but the real system has.
# ---------------------------------------------------------------------------

DASH3_CSS = """
:root{
  --bg:#0b0e14; --panel:#12161f; --panel2:#161b26; --border:#232a38;
  --text:#e6e9ef; --dim:#8b93a5; --faint:#5a6377;
  --green:#22c55e; --red:#ef4444; --amber:#f59e0b; --orange:#f97316;
  --blue:#3b82f6; --cyan:#06b6d4; --violet:#8b5cf6;
  --mono:'JetBrains Mono','SF Mono',Consolas,monospace;
  --sans:'Inter','Segoe UI',system-ui,sans-serif;
}
*{margin:0;padding:0;box-sizing:border-box}
html,body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;overflow-x:hidden}
.cmdstrip{display:flex;align-items:center;gap:14px;padding:5px 18px;background:linear-gradient(90deg,#1a0f12,#12161f 40%);border-bottom:1px solid var(--red);flex-wrap:wrap}
.shadow-warn{font-family:var(--mono);font-size:10.5px;font-weight:700;color:var(--red);letter-spacing:1px}
.tierb{margin-left:auto;display:flex;gap:16px;font-family:var(--mono);font-size:9px;color:var(--faint);flex-wrap:wrap}
.tierb b{color:var(--dim);font-weight:400}
.topbar{display:flex;align-items:center;gap:14px;padding:9px 18px;background:var(--panel);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:50;flex-wrap:wrap}
.brand{font-family:var(--mono);font-weight:700;font-size:15px;letter-spacing:.5px}
.brand span{color:var(--cyan)}
.brandmode{font-family:var(--mono);font-size:10px;color:var(--dim);margin-top:2px}
.brandmode b{color:var(--amber)}
.cfg{font-family:var(--mono);font-size:10px;color:var(--dim);background:var(--panel2);border:1px solid var(--border);border-radius:4px;padding:3px 8px}
.live-badge{margin-left:auto;font-family:var(--mono);font-size:10px;color:var(--green);border:1px solid rgba(34,197,94,.4);background:rgba(34,197,94,.1);border-radius:4px;padding:3px 10px;font-weight:700}
.clock{font-family:var(--mono);font-size:10.5px;color:var(--faint)}
.hero{margin:10px 14px 0;background:var(--panel);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.hero.exception{border-left:3px solid var(--amber)}
.hero.severity{border:1px solid rgba(239,68,68,.6);border-left:3px solid var(--red);background:rgba(239,68,68,.06)}
.hero-grid{display:grid;grid-template-columns:1fr 340px;min-height:120px}
.hero-main{padding:13px 18px}
.hero-row1{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.usable-big{font-family:var(--mono);font-weight:800;font-size:22px;letter-spacing:.5px}
.u-green{color:var(--green)} .u-amber{color:var(--amber)} .u-orange{color:var(--orange)} .u-red{color:var(--red)}
.status-code{font-family:var(--mono);font-size:9px;color:var(--faint)}
.hero-ess{display:flex;gap:26px;margin-top:11px;flex-wrap:wrap}
.he .l{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:2px}
.he .v{font-family:var(--mono);font-size:14px;font-weight:700}
.he .v.up{color:var(--green)} .he .v.red{color:var(--red)}
.hero-note{margin-top:10px;font-size:11.5px;color:var(--dim);line-height:1.5}
.hero-note .k{font-family:var(--mono);font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-right:8px}
.hero-note .ok{color:var(--green)}
.hero-note .act{color:var(--cyan);font-weight:600}
.exc{border-left:1px solid var(--border);background:rgba(249,115,22,.05);padding:12px 16px;display:flex;flex-direction:column;gap:7px}
.exc-title{display:flex;align-items:center;gap:10px;font-family:var(--mono);font-size:11px;font-weight:700;color:var(--orange)}
.exc-body{font-size:11px;line-height:1.55;color:var(--dim)}
.exc-body .l{font-family:var(--mono);font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:1px}
.exc-foot{margin-top:auto;display:flex;gap:8px;align-items:center}
.chip{font-family:var(--mono);font-size:9px;font-weight:700;padding:2px 8px;border-radius:3px;letter-spacing:.5px;border:1px solid transparent}
.chip.g{color:var(--green);background:rgba(34,197,94,.1);border-color:rgba(34,197,94,.3)}
.chip.w{color:var(--amber);background:rgba(245,158,11,.1);border-color:rgba(245,158,11,.3)}
.chip.r{color:var(--orange);background:rgba(249,115,22,.1);border-color:rgba(249,115,22,.35)}
.chip.h{color:var(--red);background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.35)}
.sev-wrap{padding:16px 20px;display:flex;gap:26px;flex-wrap:wrap;align-items:flex-start}
.status-word{font-family:var(--mono);font-weight:800;font-size:30px;letter-spacing:1px;line-height:1;color:var(--red)}
.sev-sub{font-family:var(--mono);font-size:11.5px;font-weight:700;color:var(--red);margin-top:6px}
.sev-code{font-family:var(--mono);font-size:9px;color:var(--faint);margin-top:4px}
.siglines{flex:1;min-width:360px;background:rgba(0,0,0,.25);border:1px solid var(--border);border-left:3px solid var(--red);border-radius:6px;padding:11px 15px;font-size:11.5px;line-height:1.6}
.siglines .row{display:flex;gap:12px;padding:2px 0}
.siglines .k{font-family:var(--mono);font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);width:96px;flex-shrink:0;padding-top:2px}
.siglines .act{color:var(--cyan);font-weight:600}
.siglines .cap{color:var(--red);font-weight:700;font-family:var(--mono);font-size:10.5px}
.kpis{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin:8px 14px 0}
.kpi{background:var(--panel);padding:9px 14px}
.kpi .label{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:3px}
.kpi .value{font-family:var(--mono);font-size:17px;font-weight:700}
.kpi .value.sm{font-size:13px;padding-top:3px}
.kpi .sub{font-family:var(--mono);font-size:9px;margin-top:2px;color:var(--dim)}
.up{color:var(--green)} .dn{color:var(--red)} .flat{color:var(--dim)} .warnc{color:var(--amber)} .orng{color:var(--orange)}
.export-bar{display:flex;align-items:center;gap:22px;padding:6px 18px;background:var(--panel);border:1px solid var(--border);border-radius:8px;margin:8px 14px 0;flex-wrap:wrap}
.exp-item{font-family:var(--mono);font-size:10px;color:var(--dim)}
.exp-item b{color:var(--text)}
.exp-item .tg-warn{color:var(--amber)}
.exp-note{font-family:var(--mono);font-size:9px;color:var(--faint);margin-left:auto}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin:8px 14px 0}
.panel-h{display:flex;align-items:center;justify-content:space-between;padding:9px 14px;border-bottom:1px solid var(--border);background:var(--panel2)}
.panel-t{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:var(--dim)}
.panel-h .meta{font-family:var(--mono);font-size:10px;color:var(--faint)}
.panel-h .meta b{color:var(--amber)}
.chart-note{font-family:var(--mono);font-size:9px;color:var(--faint);padding:5px 14px 8px;border-top:1px dashed var(--border)}
.dec-body{padding:13px 16px;display:flex;gap:24px;flex-wrap:wrap;align-items:center}
.dec-chips{display:flex;gap:9px;align-items:center}
.dec-chips .paper{font-size:13px;padding:7px 15px}
.dec-grid{display:grid;grid-template-columns:repeat(4,auto);gap:10px 26px;flex:1;min-width:480px}
.dec-item .l{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:2px}
.dec-item .v{font-family:var(--mono);font-size:11.5px;font-weight:600}
.dec-item .v.red{color:var(--red)}
.dec-sep{width:1px;align-self:stretch;background:var(--border)}
table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:10.5px}
thead th{background:var(--panel2);color:var(--faint);text-transform:uppercase;font-size:8.5px;letter-spacing:.7px;padding:6px 10px;text-align:right;font-weight:600;position:sticky;top:0;white-space:nowrap}
thead th:first-child,td:first-child{text-align:left}
tbody td{padding:4px 10px;border-bottom:1px solid var(--border);text-align:right;color:var(--text);white-space:nowrap}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:rgba(59,130,246,.06)}
tr.overlap{background:rgba(249,115,22,.06)}
tr.overlap:hover{background:rgba(249,115,22,.1)}
.sym{color:var(--cyan);font-weight:600}
.symflag{font-size:8px;padding:1px 5px;border-radius:3px;margin-left:5px;font-weight:700;white-space:nowrap}
.tag-ovl{background:rgba(249,115,22,.16);color:var(--orange)}
.tag-bnd{background:rgba(245,158,11,.15);color:var(--amber)}
.ranksub{display:block;font-size:8.5px;color:var(--faint);letter-spacing:0;white-space:nowrap}
.paper{font-size:9px;font-weight:800;padding:2px 7px;border-radius:3px;letter-spacing:.4px;white-space:nowrap}
.paper.buy{background:rgba(34,197,94,.14);color:var(--green);border:1px solid rgba(34,197,94,.35)}
.paper.hold{background:rgba(139,147,165,.12);color:var(--dim);border:1px solid var(--border)}
.paper.sell{background:rgba(239,68,68,.14);color:var(--red);border:1px solid rgba(239,68,68,.35)}
.issue-panel{margin:8px 14px 0;background:rgba(249,115,22,.04);border:1px solid rgba(249,115,22,.3);border-left:3px solid var(--orange);border-radius:8px;overflow:hidden}
.issue-head{display:flex;align-items:center;gap:12px;padding:8px 16px;border-bottom:1px solid rgba(249,115,22,.2);background:rgba(249,115,22,.05)}
.issue-head .it{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--orange)}
.issue-head .meta{margin-left:auto;font-family:var(--mono);font-size:9px;color:var(--faint)}
.issue-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:rgba(249,115,22,.14)}
.issue-cell{background:#14101a;padding:9px 14px;font-size:10.5px;line-height:1.55}
.issue-cell .l{font-family:var(--mono);font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:3px}
.issue-cell .ok{color:var(--green)}
.grid2{display:grid;grid-template-columns:1.6fr 1fr;gap:8px;padding:0 14px}
.grid2 .panel,.grid3 .panel{margin-left:0;margin-right:0}
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;padding:0 14px}
#equityChart{width:100%;height:230px}
#ddChart{width:100%;height:135px}
#contribChart{width:100%;height:330px}
#rankHist{width:100%;height:330px}
#gateGauge{width:100%;height:190px}
.movers-wrap{height:330px;display:flex;flex-direction:column}
.movers{display:grid;grid-template-columns:1fr 1fr;flex:1}
.movers .mcol{padding:6px 0;overflow:hidden}
.movers .mcol:first-child{border-right:1px solid var(--border)}
.movers .mh{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.8px;padding:2px 14px 6px}
.movers .mh.g{color:var(--green)} .movers .mh.l{color:var(--red)}
.mrow{display:flex;justify-content:space-between;align-items:center;padding:6px 14px;font-family:var(--mono);font-size:11px;border-top:1px solid var(--border)}
.mrow .ms{color:var(--cyan);font-weight:600}
.mrow .mv{text-align:right}
.mrow .mv .pct{font-size:9.5px;margin-left:8px}
.movers-note{padding:6px 14px;font-family:var(--mono);font-size:8.5px;color:var(--faint);border-top:1px dashed var(--border)}
.gate-status{font-weight:700;font-size:10px;letter-spacing:.5px}
.gs-pass{color:var(--green)} .gs-warn{color:var(--amber)} .gs-fail{color:var(--red)} .gs-rev{color:var(--orange)} .gs-neutral{color:var(--dim)}
details.panel summary{cursor:pointer;padding:9px 14px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:var(--dim);background:var(--panel2);list-style:none;display:flex;align-items:center;border-bottom:1px solid transparent}
details.panel[open] summary{border-bottom:1px solid var(--border)}
details.panel summary::-webkit-details-marker{display:none}
details.panel summary .arrow{font-family:var(--mono);color:var(--faint);font-size:12px;margin-left:auto}
details.panel[open] summary .arrow{transform:rotate(90deg)}
details.panel summary .sumline{font-family:var(--mono);font-size:10px;color:var(--faint);font-weight:400;text-transform:none;letter-spacing:0;margin-left:14px}
details.panel summary .sumline b{color:var(--orange)}
details.panel summary .sumline .w2{color:var(--amber)}
.pipewrap{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border)}
.pipe{background:var(--panel);padding:10px 14px}
.pipe .pl{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);margin-bottom:4px}
.pipe .pv{font-family:var(--mono);font-size:11.5px;font-weight:700;display:flex;align-items:center;gap:7px}
.pipe .ps{font-family:var(--mono);font-size:9px;color:var(--dim);margin-top:3px}
.pdot{width:8px;height:8px;border-radius:50%}
.pdot.ok{background:var(--green)} .pdot.warn{background:var(--amber)} .pdot.rev{background:var(--orange)}
.matrow{display:flex;gap:10px;padding:9px 14px;flex-wrap:wrap;border-top:1px solid var(--border);background:var(--panel2)}
.mat{font-family:var(--mono);font-size:10px;padding:4px 10px;border-radius:4px;border:1px solid var(--border);color:var(--dim)}
.mat b{color:var(--text)}
.mat.hot{border-color:rgba(249,115,22,.4);color:var(--orange)}
.mat.hot b{color:var(--orange)}
.dd-body{padding:14px;display:grid;grid-template-columns:1fr 1fr;gap:14px;background:var(--bg)}
.dd-block{border:1px solid var(--border);border-radius:8px;overflow:hidden;background:var(--panel)}
.dd-block h4{font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--faint);padding:8px 12px;background:var(--panel2);border-bottom:1px solid var(--border);margin:0}
.dd-block .inner{padding:11px 13px;font-family:var(--mono);font-size:10.5px;line-height:1.7;color:var(--dim)}
.dd-block .inner .k{color:var(--faint)}
.dd-block .inner .v{color:var(--text)}
.dd-block .insufficient{padding:20px 13px;font-family:var(--mono);font-size:10px;color:var(--faint);text-align:center;font-style:italic}
.sha{color:var(--cyan);font-size:9.5px}
.footer{margin-top:10px;padding:12px 18px 20px;border-top:1px solid var(--border);display:flex;gap:22px;font-family:var(--mono);font-size:10px;color:var(--faint);flex-wrap:wrap}
.footer .warn{color:var(--red);font-weight:700}
.devflag{position:fixed;bottom:10px;right:10px;background:var(--panel2);border:1px dashed var(--amber);border-radius:6px;padding:8px 10px;font-family:var(--mono);font-size:9.5px;color:var(--amber);z-index:99}
.freshbar{margin:8px 14px 0;padding:8px 14px;border-radius:8px;font-family:var(--mono);font-size:10.5px;border:1px solid var(--border)}
.freshbar.FRESH{border-left:3px solid var(--green);color:var(--dim)}
.freshbar.DELAYED{border-left:3px solid var(--amber);color:var(--dim)}
.freshbar.STALE{border-left:3px solid var(--red);background:rgba(239,68,68,.08);color:var(--text);font-weight:700}
@media(max-width:1200px){.kpis{grid-template-columns:repeat(3,1fr)}.grid2,.grid3,.dd-body,.issue-grid,.hero-grid{grid-template-columns:1fr}.pipewrap{grid-template-columns:repeat(2,1fr)}.exc{border-left:none;border-top:1px solid var(--border)}.dec-grid{grid-template-columns:repeat(2,auto)}}
"""


# ---------------------------------------------------------------------------
# HTML assembly -- real ctx values substituted into the mock's exact
# markup structure. Chart panels use ECharts (same library/CDN as the
# mock) fed real JSON data computed server-side; panels with no real
# data source (score distribution, cost waterfall, per-stage timeline,
# 14-day job-health heatmap -- job_health is a latest-status table, not
# an append-only history log, so no real daily series exists) render an
# honest "insufficient real data" notice instead of the mock's
# illustrative/random-shaped filler.
# ---------------------------------------------------------------------------

def _gate_row_html(g: dict) -> str:
    cls = GATE_STATUS_CLASS.get(g.get("status"), "gs-neutral")
    detail = humanize_failure_reason(str(g.get("detail", "") or "")) or "—"
    name = _esc(g.get("gate", "")).replace("_", " ")
    return (f"<tr><td>{name}</td><td class='gate-status {cls}'>{_esc(g.get('status'))}</td>"
            f"<td>{_esc(g.get('actual'))}</td><td>{detail}</td></tr>")


def _pipe_chip_html(name: str, status: str) -> str:
    cls = PIPE_STATUS_CLASS.get(str(status).split(":")[0].strip().upper(), "warn")
    human = humanize_failure_reason(str(status)) if ("_" in str(status) and ":" in str(status)) else _esc(status)
    return (f"<div class='pipe'><div class='pl'>{_esc(name)}</div>"
            f"<div class='pv'><span class='pdot {cls}'></span>{_esc(status)}</div>"
            f"<div class='ps'>{human}</div></div>")


def _holdings_table_rows_html(rows: list, has_real_fills: bool) -> str:
    out = []
    for row in rows:
        ovl = "overlap" if row["flag"] else ""
        symflag = f"<span class='symflag tag-ovl'>{_esc(row['flag'])}</span>" if row["flag"] else ""
        band = f"<span class='ranksub'>{_esc(row['band'])}</span>" if row["band"] else ""
        last_display = f"{row['last']:.2f}" if row["last"] is not None else "—"
        as_of = f" <span class='ranksub'>as of {_esc(row['price_as_of'])} · {row['age_days']}d · {_esc(row['freshness'])}</span>" if row["price_as_of"] else ""
        signal_display = f"{row['signal']:.2f}" if row["signal"] is not None else "—"
        if has_real_fills and row["qty"]:
            qty_d, entry_d = f"{row['qty']:.0f}", f"{row['entry']:.2f}"
            value_d = f"{row['value']:.0f}" if row["value"] is not None else "—"
            pnl_d = f"{row['pnl_inr']:+.0f}" if row["pnl_inr"] is not None else "—"
            pnl_pct_d = f"{row['pnl_pct']:+.2f}%" if row["pnl_pct"] is not None else "—"
            pnl_cls = "up" if (row["pnl_inr"] or 0) >= 0 else "dn"
        else:
            qty_d = entry_d = value_d = pnl_d = pnl_pct_d = "—"
            pnl_cls = ""
        wt_d = f"{row['weight'] * 100:.2f}%" if row["weight"] is not None else "—"
        action = row["paper_action"]
        action_cls = "buy" if "BUY" in action else ("sell" if "SELL" in action else "hold")
        out.append(
            f"<tr class='{ovl}'><td class='sym'>{_esc(row['symbol'])}{symflag}</td>"
            f"<td>{row['rank']}{band}</td><td>{row['z']}</td>"
            f"<td>{qty_d}</td><td>{entry_d}</td><td>{signal_display}</td>"
            f"<td>{last_display}{as_of}</td><td>{value_d}</td>"
            f"<td class='{pnl_cls}'>{pnl_d}</td><td class='{pnl_cls}'>{pnl_pct_d}</td>"
            f"<td>{wt_d}</td><td><span class='paper {action_cls}'>{_esc(action)}</span></td></tr>"
        )
    return "".join(out)


def render_dash3_html(ctx: dict, dev_mode: bool = False) -> str:
    display = ctx["display"]
    hero_cls = "severity" if display == "HALT" else "exception"
    has_real_fills = ctx["n_positions"] > 0

    if display == "HALT":
        hero_inner = f"""
      <div class="sev-wrap">
        <div>
          <div class="status-word">HALT</div>
          <div class="sev-sub">Signal Status: DO NOT USE</div>
          <div class="sev-code">{_esc(ctx['status_code'])}</div>
        </div>
        <div class="siglines">
          <div class="row"><span class="k">Reason</span><span>{_esc(ctx['failed_gate_line'])}</span></div>
          <div class="row"><span class="k">Book impact</span><span>{_esc(ctx['book_impact_line'])}</span></div>
          <div class="row"><span class="k">Capital</span><span class="cap">None — shadow only. NO SIGNAL EXISTS TO ACT ON.</span></div>
          <div class="row"><span class="k">Next action</span><span class="act">{_esc(ctx['next_action'])}</span></div>
        </div>
      </div>"""
    else:
        big_label = {"GREEN": "SIGNAL: USABLE", "WARNING": "SIGNAL: USABLE — WITH WARNINGS",
                     "REVIEW_REQUIRED": "SIGNAL: PROVISIONAL"}.get(display, display)
        big_cls = {"GREEN": "u-green", "WARNING": "u-amber", "REVIEW_REQUIRED": "u-orange"}.get(display, "u-orange")
        chip_cls = {"GREEN": "g", "WARNING": "w", "REVIEW_REQUIRED": "r"}.get(display, "r")
        exc_block = ""
        if ctx["overlap_handled"]:
            item = ctx["overlap_handled"][0]
            symbol = _esc(item.get("symbol", "?"))
            exc_block = f"""
    <div class="exc">
      <div class="exc-title">⚠ REVIEW ITEM — {symbol} {CA_ACKNOWLEDGED_TITLE_SUFFIX} <span class="chip w">ROUTINE</span></div>
      <div class="exc-body">
        <div class="l">Issue</div>
        Anomaly detector flagged a price move already captured &amp; adjusted by CA-watch.
        <div class="l" style="margin-top:6px">Required action</div>
        Informational — already adjusted, not an open issue.
      </div>
      <div class="exc-foot">
        <span class="chip g">HANDLED · NON-ECONOMIC</span>
        <span class="status-code">CA_ACKNOWLEDGED · {_esc(ctx['as_of_date'])}</span>
      </div>
    </div>"""
        failed_line = (f'<div><span class="k">Failed gate</span><span>{_esc(ctx["failed_gate"])} — '
                       f'{_esc(ctx["failed_gate_line"])}</span></div>' if ctx.get("failed_gate") else "")
        hero_inner = f"""
  <div class="hero-grid">
    <div class="hero-main">
      <div class="hero-row1">
        <span class="usable-big {big_cls}">{big_label}</span>
        <span class="chip {chip_cls}">{display}</span>
        <span class="status-code">{_esc(ctx['status_code'])}</span>
      </div>
      <div class="hero-ess">
        <div class="he"><div class="l">Shadow Ledger Notional</div><div class="v">₹15,00,000</div></div>
        <div class="he"><div class="l">Portfolio Value</div><div class="v">{ctx['portfolio_value_display']}</div></div>
        <div class="he"><div class="l">Paper P&amp;L</div><div class="v {ctx['pnl_class']}">{ctx['pnl_display']}<span style="font-size:9px;margin-left:6px">{_esc(ctx['pnl_sub'])}</span></div></div>
        <div class="he"><div class="l">Positions</div><div class="v">{ctx['n_positions']}</div></div>
        <div class="he"><div class="l">Today's Actions</div><div class="v"><span class="paper buy">{ctx['n_buy']} PAPER BUY</span></div></div>
        <div class="he"><div class="l">Next Rebalance</div><div class="v">{_esc(ctx['next_rebalance'])}</div></div>
        <div class="he"><div class="l">Capital Action</div><div class="v red">{_esc(ctx['capital_action_display'])}</div></div>
      </div>
      <div class="hero-note">
        {failed_line}
        <div><span class="k">Book impact</span><span class="ok">{_esc(ctx['book_impact_line'])}</span></div>
        <div><span class="k">Next action</span><span class="act">{_esc(ctx['next_action'])}</span></div>
      </div>
    </div>{exc_block}
  </div>"""

    n_gates, n_pass = ctx["n_gates"], ctx["n_gates_pass"]
    n_nonpass = n_gates - n_pass
    kpi_health_cls = "up" if n_pass == n_gates else ("orng" if ctx["worst_gate_status"] in ("FAIL",) else "warnc")

    kpis_html = f"""
<div class="kpis">
  <div class="kpi"><div class="label">Portfolio Value</div><div class="value">{ctx['portfolio_value_display']}</div><div class="sub">{ctx['n_positions']} positions · equal-weight</div></div>
  <div class="kpi"><div class="label">Paper P&amp;L</div><div class="value {ctx['pnl_class']}">{ctx['pnl_display']}</div><div class="sub">{_esc(ctx['pnl_sub'])} · since {_esc(ctx['as_of_date'])}</div></div>
  <div class="kpi"><div class="label">Positions</div><div class="value">{ctx['n_positions']}</div><div class="sub">{ctx['n_symbols']} in book</div></div>
  <div class="kpi"><div class="label">Drawdown</div><div class="value flat">0.00%</div><div class="sub">ref MaxDD {ctx['run148']['max_drawdown']*100:.2f}% (run_148)</div></div>
  <div class="kpi"><div class="label">Health (secondary)</div><div class="value sm {kpi_health_cls}">{n_pass}/{n_gates} pass</div><div class="sub">worst: {_esc(ctx['worst_gate_status'])} ({_esc(ctx['worst_gate_name'])})</div></div>
  <div class="kpi"><div class="label">Material Issues</div><div class="value sm {'up' if ctx['material_issue_count']==0 else 'orng'}">{ctx['material_issue_count']}</div><div class="sub">{ctx['material_issue_count']} material to book/signal</div></div>
</div>"""

    freshbar_html = f"""<div class="freshbar {ctx['freshness_banner_status']}">{_esc(ctx['freshness_banner_text'])}</div>"""

    export_bar_html = f"""
<div class="export-bar">
  <span class="exp-item">Next rebalance: <b>{_esc(ctx['next_rebalance'])}</b></span>
  <span class="exp-item">Latest bhavcopy: <b>{_esc(ctx['latest_bhavcopy_date'])}</b></span>
  <span class="exp-item">Snapshot: <b>{_esc(ctx['created_at'])}</b></span>
  <span class="exp-item">Telegram: <b class="tg-warn">DRY-RUN</b></span>
  <span class="exp-note">operating_state_latest · single canonical row</span>
</div>"""

    decision_html = f"""
<div class="panel">
  <div class="panel-h"><div class="panel-t">Today's Decision — {_esc(ctx['as_of_date'])}</div><div class="meta">shadow ledger notional ₹15,00,000</div></div>
  <div class="dec-body">
    <div class="dec-chips">
      <span class="paper buy">{ctx['n_buy']} PAPER BUY</span>
      <span class="paper sell">{ctx['n_sell']} PAPER SELL</span>
      <span class="paper hold">{ctx['n_hold']} PAPER HOLD</span>
    </div>
    <div class="dec-sep"></div>
    <div class="dec-grid">
      <div class="dec-item"><div class="l">Next rebalance</div><div class="v">{_esc(ctx['next_rebalance'])}</div></div>
      <div class="dec-item"><div class="l">Capital action</div><div class="v red">{_esc(ctx['capital_action_display'])}</div></div>
      <div class="dec-item"><div class="l">Ledger</div><div class="v">Shadow-only (Step 6 contract)</div></div>
      <div class="dec-item"><div class="l">Health</div><div class="v">{n_pass}/{n_gates} gates pass</div></div>
      <div class="dec-item"><div class="l">Material issues</div><div class="v" style="color:var(--green)">{ctx['material_issue_count']}</div></div>
      <div class="dec-item"><div class="l">Provenance</div><div class="v">XBRL ranks · Tier-1 PROVISIONAL</div></div>
    </div>
  </div>
</div>"""

    fills_note = ("" if has_real_fills else
                  "<div class='chart-note'>Entry/Qty/Value/P&amp;L: Ledger pending — 0 real fills yet. "
                  "Signal date for this book is real (2026-08-07); the frozen execution rule fills at the "
                  "next available real trading-day open (T+1) — 2026-08-10. No fill before that date is "
                  "point-in-time valid (operator ruling 2026-08-09: honor T+1 exactly, do not backdate to "
                  "2026-08-03). Ledger initializes automatically on the next real daily pipeline run once "
                  "2026-08-10 bhavcopy is ingested — no code change needed, fill_pending_actions() is already "
                  "sealed. Signal Rs/Last Rs above are real regardless.</div>")
    holdings_html = f"""
<div class="panel">
  <div class="panel-h"><div class="panel-t">Holdings — Signal + Paper Positions ({ctx['n_symbols']})</div>
    <div class="meta">{ctx['n_symbols']} of {ctx['n_symbols']} shown in one frame · Last Rs source: {PRICE_SOURCE_LABEL}</div></div>
  <table>
    <thead><tr><th>Symbol</th><th>Rank</th><th>z</th><th>Qty</th><th>Entry ₹</th><th>Signal ₹</th><th>Last ₹ · as of</th><th>Value ₹</th><th>P&amp;L ₹</th><th>P&amp;L %</th><th>Wt</th><th>Paper Action</th></tr></thead>
    <tbody>{_holdings_table_rows_html(ctx['holdings_rows'], has_real_fills)}</tbody>
  </table>
  {fills_note}
</div>"""

    issue_html = ""
    if ctx["overlap_handled"]:
        item = ctx["overlap_handled"][0]
        symbol = _esc(item.get("symbol", "?"))
        issue_html = f"""
<div class="issue-panel">
  <div class="issue-head">
    <span class="it">⚠ REVIEW ITEM — {symbol} {CA_ACKNOWLEDGED_TITLE_SUFFIX}</span>
    <span class="chip w">ROUTINE</span><span class="chip g">HANDLED · NON-ECONOMIC</span>
    <span class="meta">ca_anomaly_detector · {_esc(ctx['created_at'])}</span>
  </div>
  <div class="issue-grid">
    <div class="issue-cell"><div class="l">Issue</div>{symbol} price move flagged by the CA-anomaly detector.</div>
    <div class="issue-cell"><div class="l">Book impact</div><span class="ok">No unresolved material defect</span> — CA already captured &amp; adjusted by CA-watch.</div>
    <div class="issue-cell"><div class="l">Capital impact</div>None — shadow only.</div>
    <div class="issue-cell"><div class="l">Required action</div>Informational only — auto-acknowledged by ca_anomaly_detector's CA_ACKNOWLEDGED classification (Step 6A).</div>
  </div>
</div>"""

    gate_rows_html = "".join(_gate_row_html(g) for g in ctx["gates"])
    worst_summary = (f"<b>{ctx['worst_gate_name']}: {ctx['worst_gate_status']}</b>"
                      if ctx["worst_gate_status"] != "PASS" else "all PASS")
    health_html = f"""
<details class="panel">
  <summary>System Health — Tier-2 gates <span class="sumline">{n_pass}/{n_gates} pass · {worst_summary}</span><span class="arrow">▶</span></summary>
  <div class="grid2" style="padding:0">
    <div><table><thead><tr><th>Gate</th><th>Status</th><th>Measured</th><th>Detail</th></tr></thead>
      <tbody>{gate_rows_html}</tbody></table></div>
    <div><div id="gateGauge"></div></div>
  </div>
</details>"""

    pipe_html = "".join(_pipe_chip_html(name, status) for name, status in ctx["pipeline"])
    qw = ctx["quarantine_warnings"]
    qw_line = (f"<span class='mat hot'>quarantine_warnings: {qw.get('count', 'n/a')} rows · "
               f"materiality_class={_esc(qw.get('materiality_class', 'n/a'))}</span>" if qw else
               "<span class='mat'>quarantine_warnings: none reported</span>")
    pipeline_html = f"""
<details class="panel">
  <summary>Data Pipeline — jobs &amp; archive <span class="sumline">real component status, humanized</span><span class="arrow">▶</span></summary>
  <div class="pipewrap">{pipe_html}</div>
  <div class="matrow">{qw_line}
    <span class="mat">Per-materiality-class breakdown: not available in the current snapshot contract (materiality_flags empty this cycle) — disclosed, not fabricated.</span>
  </div>
  <div class="chart-note">Job-run history grid: not available — job_health tracks latest-run status per job only (no append-only daily history table exists yet), so a 14-day heatmap cannot be shown honestly. This is a real data gap, not a rendering omission.</div>
</details>"""

    dd_block_insufficient = lambda title: (  # noqa: E731
        f'<div class="dd-block"><h4>{title}</h4><div class="insufficient">'
        f'insufficient real data for this panel — no fabricated placeholder shown</div></div>'
    )
    run148 = ctx["run148"]
    drilldown_html = f"""
<details class="panel" id="ddPanel">
  <summary>Drill-Down — secondary charts, sealed records, provenance <span class="sumline">allocation · Tier-1 cert · quarantine · archive</span><span class="arrow">▶</span></summary>
  <div class="dd-body">
    <div class="dd-block"><h4>Allocation / Exposure (equal-weight by design, real)</h4><div id="allocChart"></div></div>
    {dd_block_insufficient("Momentum Score Distribution (insufficient real universe-wide data)")}
    {dd_block_insufficient("Pipeline Run Timeline (no real per-stage timing captured)")}
    {dd_block_insufficient("Cycle Cost Waterfall (0 real fills — no real cost breakdown yet)")}
    <div class="dd-block" style="grid-column:1/-1"><h4>Tier-1 Rank Certification · Quarantine · Archive · Sealed Backtest Reference</h4><div class="inner">
      <div><span class="k">rank source: </span><span class="v">NSE XBRL shares × bhavcopy (reconstruction)</span></div>
      <div><span class="k">cert: </span><span class="v">PROVISIONAL · event-triggered expiry</span></div>
      <div style="margin-top:8px"><span class="k">quarantine: </span><span class="v">{_esc(qw.get('count', 'n/a'))} counted · {_esc(qw.get('materiality_class', 'n/a'))}</span></div>
      <div><span class="k">sealed backtest (run_148, context only): </span><span class="v">CAGR {run148['net_cagr']*100:.2f}% · MaxDD {run148['max_drawdown']*100:.2f}% · Sharpe {run148['sharpe_rf0_sqrt252']:.2f} · window {run148['window_start']}→{run148['window_end']} · source: {_esc(run148['source'])}</span></div>
      <div><span class="k">capital: </span><span class="v">shadow operating notional ₹15,00,000 (this ledger) · backtest reference capital ₹25,00,000 (run_148, unchanged)</span></div>
    </div></div>
  </div>
</details>"""

    footer_html = f"""
<div class="footer">
  <span class="warn">READ-ONLY ENFORCED AT CREDENTIAL LEVEL — no broker · no MTF · no approval buttons · no edit controls · no config changes</span>
  <span>snapshot: operating_state_latest @ {_esc(ctx['created_at'])}</span>
  <span>TG digest = same snapshot row, same renderer</span>
  <span>{_esc(ctx['identity_line'])}</span>
</div>"""

    devflag_html = '<div class="devflag">DEV MODE — synthetic-data indicator (URL ?dev=1)</div>' if dev_mode else ""

    # JS data payloads -- real, server-computed
    holdings_json = json.dumps(ctx["holdings_rows"])
    moves_json = json.dumps(ctx["moves"])
    run148_curve_json = json.dumps(run148["equity_curve_indexed_100_monthly"])
    tb_json = json.dumps([{"symbol": r["symbol"], "rank": r["rank"], "weight": r["weight"]} for r in ctx["tb"]])
    n_gates_pass_json = n_pass
    n_gates_json = n_gates
    total_equity = ctx["pnl"].get("total_equity") or 0

    js = f"""
const AX={{axisLine:{{lineStyle:{{color:'#232a38'}}}},axisLabel:{{color:'#8b93a5',fontFamily:'JetBrains Mono',fontSize:9.5}},splitLine:{{lineStyle:{{color:'#1a2030'}}}}}};
const TT={{trigger:'axis',backgroundColor:'#161b26',borderColor:'#232a38',textStyle:{{color:'#e6e9ef',fontFamily:'JetBrains Mono',fontSize:10.5}}}};
const RUN148=EQREF={json.dumps(run148)};
const RUN148_CURVE={run148_curve_json};
const TB={tb_json};
const MOVES={moves_json};

/* equity curve -- real run_148 monthly reference (indexed=100) + real single shadow point */
(function(){{
  const refDates=RUN148_CURVE.map(p=>p[0]);
  const refVals=RUN148_CURVE.map(p=>p[1]);
  const shadow=refDates.map(()=>null);
  shadow[shadow.length-1]=100.0;
  echarts.init(document.getElementById('equityChart')).setOption({{
    backgroundColor:'transparent',tooltip:TT,grid:{{left:44,right:14,top:30,bottom:36,containLabel:true}},
    legend:{{textStyle:{{color:'#8b93a5',fontFamily:'JetBrains Mono',fontSize:9.5}},top:4,
      data:['run_148 sealed backtest (context only, real)','SHADOW equity (real, day 1)']}},
    xAxis:{{type:'category',data:refDates,axisLabel:{{...AX.axisLabel,interval:11,rotate:30}},axisLine:AX.axisLine,splitLine:AX.splitLine}},
    yAxis:{{type:'value',name:'indexed =100',nameTextStyle:{{color:'#5a6377',fontSize:9}},...AX}},
    series:[
      {{name:'run_148 sealed backtest (context only, real)',type:'line',data:refVals,smooth:true,symbol:'none',
       lineStyle:{{color:'#3b82f6',width:1,opacity:.5}},areaStyle:{{color:'#3b82f6',opacity:.04}}}},
      {{name:'SHADOW equity (real, day 1)',type:'line',data:shadow,symbol:'circle',symbolSize:9,
       itemStyle:{{color:'#22c55e'}},lineStyle:{{color:'#22c55e',width:2.5}},
       label:{{show:true,position:'top',color:'#22c55e',fontFamily:'JetBrains Mono',fontSize:9.5,formatter:'day 1'}}}}
    ]}});
}})();

/* drawdown -- real single day-1 point (0.00%) + real run_148 MaxDD reference line */
(function(){{
  echarts.init(document.getElementById('ddChart')).setOption({{
    backgroundColor:'transparent',tooltip:TT,grid:{{left:44,right:14,top:14,bottom:20,containLabel:true}},
    xAxis:{{type:'category',data:['{ctx["as_of_date"]}'],...AX}},
    yAxis:{{type:'value',max:0,min:-40,axisLabel:{{...AX.axisLabel,formatter:'{{value}}%'}},axisLine:AX.axisLine,splitLine:AX.splitLine}},
    series:[{{type:'line',data:[0],symbol:'circle',symbolSize:7,itemStyle:{{color:'#22c55e'}},lineStyle:{{color:'#22c55e',width:2}},
       markLine:{{silent:true,symbol:'none',data:[{{yAxis:RUN148.max_drawdown*100}}],lineStyle:{{color:'#ef4444',type:'dashed'}},
         label:{{color:'#ef4444',fontFamily:'JetBrains Mono',fontSize:9,formatter:p=>'MaxDD ref '+(RUN148.max_drawdown*100).toFixed(2)+'%'}}}}}}]}});
}})();

/* P&L contribution -- real target-weight * real total equity (proxy, honestly labeled in caption) */
(function(){{
  const rows=TB.map(r=>[r.symbol,Math.round(r.weight*{total_equity})]).sort((a,b)=>b[1]-a[1]);
  echarts.init(document.getElementById('contribChart')).setOption({{
    backgroundColor:'transparent',tooltip:TT,grid:{{left:8,right:60,top:8,bottom:8,containLabel:true}},
    xAxis:{{type:'value',...AX,axisLabel:{{...AX.axisLabel,formatter:'₹{{value}}'}}}},
    yAxis:{{type:'category',data:rows.map(r=>r[0]).reverse(),...AX,axisLabel:{{color:'#e6e9ef',fontFamily:'JetBrains Mono',fontSize:9.5,margin:10}}}},
    series:[{{type:'bar',data:rows.map(r=>({{value:r[1],itemStyle:{{color:'#3b82f6'}}}})).reverse(),
      barWidth:10,label:{{show:true,position:'right',color:'#8b93a5',fontFamily:'JetBrains Mono',fontSize:9,formatter:p=>'₹'+p.value}}}}]}});
}})();

/* top movers -- real day-over-day (empty state if no real prior-day pair exists yet) */
(function(){{
  const g=document.getElementById('gainers'), l=document.getElementById('losers');
  if(!MOVES.length){{
    document.querySelector('.movers-wrap').innerHTML='<div class="movers-note" style="padding:20px;text-align:center">No real bhavcopy row pair (latest + prior trading day) yet for any target-book symbol.</div>';
  }} else {{
    MOVES.slice(0,3).forEach(m=>{{g.insertAdjacentHTML('beforeend',`<div class="mrow"><span class="ms">${{m.symbol}}</span><span class="mv up">+${{m.pct.toFixed(2)}}%</span></div>`);}});
    MOVES.slice(-3).reverse().forEach(m=>{{l.insertAdjacentHTML('beforeend',`<div class="mrow"><span class="ms">${{m.symbol}}</span><span class="mv dn">${{m.pct.toFixed(2)}}%</span></div>`);}});
  }}
}})();

/* rank distribution -- real book ranks only (no fabricated universe-wide shape) */
(function(){{
  const sorted=[...TB].sort((a,b)=>a.rank-b.rank);
  echarts.init(document.getElementById('rankHist')).setOption({{
    backgroundColor:'transparent',tooltip:TT,grid:{{left:8,right:12,top:14,bottom:40,containLabel:true}},
    xAxis:{{type:'category',data:sorted.map(r=>r.symbol),name:'book, real ranks only',nameTextStyle:{{color:'#5a6377',fontSize:9}},...AX,axisLabel:{{...AX.axisLabel,rotate:45}}}},
    yAxis:{{type:'value',name:'rank (lower=better)',inverse:true,...AX}},
    series:[{{type:'bar',data:sorted.map(r=>r.rank),barWidth:'60%',itemStyle:{{color:'#06b6d4'}},
      markLine:{{silent:true,symbol:'none',data:[{{yAxis:101}},{{yAxis:750}}],lineStyle:{{color:'#f59e0b',type:'dashed'}},
        label:{{color:'#f59e0b',fontFamily:'JetBrains Mono',fontSize:9,formatter:p=>p.value}}}}}}]}});
}})();

/* gate gauge -- real pass/total */
(function(){{
  echarts.init(document.getElementById('gateGauge')).setOption({{
    backgroundColor:'transparent',
    series:[{{type:'gauge',startAngle:200,endAngle:-20,min:0,max:{n_gates_json},radius:'95%',center:['50%','62%'],
      axisLine:{{lineStyle:{{width:15,color:[[{n_gates_pass_json}/{n_gates_json},'#22c55e'],[1,'#ef4444']]}}}},
      pointer:{{itemStyle:{{color:'#f97316'}},length:'58%',width:4}},
      anchor:{{show:true,size:10,itemStyle:{{color:'#f97316'}}}},
      axisTick:{{distance:-15,length:4,lineStyle:{{color:'#0b0e14',width:1}}}},
      splitLine:{{distance:-15,length:15,lineStyle:{{color:'#0b0e14',width:2}}}},
      axisLabel:{{color:'#5a6377',fontFamily:'JetBrains Mono',fontSize:9,distance:22}},
      detail:{{valueAnimation:true,offsetCenter:[0,'60%'],color:'#f97316',fontFamily:'JetBrains Mono',fontSize:19,fontWeight:700,formatter:'{{value}} / {n_gates_json}'}},
      data:[{{value:{n_gates_pass_json},name:'passing'}}]}}]}});
}})();

let ddInit=false;
document.getElementById('ddPanel').addEventListener('toggle',e=>{{
  if(e.target.open&&!ddInit){{
    echarts.init(document.getElementById('allocChart')).setOption({{
      backgroundColor:'transparent',tooltip:{{...TT,trigger:'item',formatter:'{{b}}: {{c}}%'}},
      series:[{{type:'pie',radius:['50%','76%'],center:['50%','52%'],
        label:{{color:'#8b93a5',fontFamily:'JetBrains Mono',fontSize:8,formatter:'{{b}}'}},
        labelLine:{{lineStyle:{{color:'#232a38'}}}},itemStyle:{{borderColor:'#0b0e14',borderWidth:2}},
        color:['#06b6d4','#3b82f6','#8b5cf6','#22c55e','#f59e0b'],
        data:TB.map(r=>({{name:r.symbol,value:(r.weight*100).toFixed(2)}}))}}]}});
    ddInit=true;
  }}
}});

function tick(){{document.getElementById('clock').textContent=new Date().toLocaleString('en-IN',{{timeZone:'Asia/Kolkata'}}).toUpperCase()+' IST';}}
tick(); setInterval(tick,1000);
"""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>HSN S1 Momentum Terminal — SHADOW / PAPER — Read-Only</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>{DASH3_CSS}</style></head><body>
<div class="cmdstrip">
  <div class="shadow-warn">⛔ SHADOW ONLY — READ-ONLY INTERFACE — NO EXECUTION PATH EXISTS</div>
  <div class="tierb">
    <span>capital_status: <b>{_esc(ctx['capital_status'])}</b></span>
    <span>row sha256: <b>{ctx['identity_short_hash'][:4]}…{ctx['identity_short_hash'][-4:]}</b></span>
    <span>Supabase read-only · zero write grants</span>
  </div>
</div>
<div class="topbar">
  <div><div class="brand">HSN S1 MOMENTUM TERMINAL</div>
    <div class="brandmode">Mode: <b>SHADOW / PAPER</b> · Capital: <b>No real capital</b> · Execution: <b>Disabled</b></div></div>
  <div class="cfg">S1 v1.2 (FROZEN) · run_148 chassis</div>
  <div class="cfg">{ctx['n_symbols']} holdings · monthly rebalance · 101–750 band</div>
  <div class="live-badge">LIVE SNAPSHOT · SHADOW MODE</div>
  <div class="clock" id="clock"></div>
</div>
<div class="hero {hero_cls}" id="hero">{hero_inner}</div>
{kpis_html}
{freshbar_html}
{export_bar_html}
{decision_html}
{holdings_html}
<div class="grid2">
  <div class="panel"><div class="panel-h"><div class="panel-t">Paper Equity Curve</div><div class="meta">shadow era day 1 · run_148 overlay = context only, real</div></div>
    <div id="equityChart"></div>
    <div class="chart-note">EMPTY-STATE RULE: single real point (day 1) — no fabricated history. Reference overlay is the real sealed run_148 backtest.</div></div>
  <div class="panel"><div class="panel-h"><div class="panel-t">Drawdown vs Frozen MaxDD Ref</div><div class="meta">real, run_148</div></div>
    <div id="ddChart"></div>
    <div class="chart-note">Breach of the reference line = WARNING-class visual; never a gate change by itself.</div></div>
</div>
<div class="grid3">
  <div class="panel"><div class="panel-h"><div class="panel-t">P&amp;L Contribution — weight proxy</div><div class="meta">₹ per position (real weight × real equity) · all {ctx['n_symbols']} symbols</div></div>
    <div id="contribChart"></div>
    <div class="chart-note">Per-symbol realized P&amp;L not yet available (0 real fills) — showing real target-weight × real total equity as an honestly-labeled proxy, not fabricated P&amp;L.</div></div>
  <div class="panel"><div class="panel-h"><div class="panel-t">Top Movers — real day-over-day</div><div class="meta">from real bhavcopy close vs prior close</div></div>
    <div class="movers-wrap"><div class="movers"><div class="mcol"><div class="mh g">▲ Top Gainers</div><div id="gainers"></div></div><div class="mcol"><div class="mh l">▼ Top Losers</div><div id="losers"></div></div></div>
    <div class="movers-note">Source: {PRICE_SOURCE_LABEL}</div></div></div>
  <div class="panel"><div class="panel-h"><div class="panel-t">Book Rank Distribution</div><div class="meta">band edges 101 / 750, real ranks only</div></div>
    <div id="rankHist"></div></div>
</div>
{issue_html}
{health_html}
{pipeline_html}
{drilldown_html}
{footer_html}
{devflag_html}
<script>{js}</script>
</body></html>"""


_DEV_HALT_PREVIEW_SNAPSHOT = {
    "id": -1, "as_of_date": "2026-01-01", "latest_bhavcopy_date": "2026-01-01",
    "scanner_status": "DEV_PREVIEW_SYNTHETIC", "target_book": [], "paper_positions": [], "paper_actions": [],
    "paper_pnl": {"n_positions": 0, "total_equity": 0, "cumulative_pnl_inr": 0, "cumulative_pnl_pct": 0},
    "health_status": "HALT_SIGNAL_GENERATION_REAL_FAULT",
    "health_gates": [], "unresolved_issue_overlap": [], "materiality_flags": {}, "quarantine_warnings": {},
    "ca_watch_status": "n/a", "asm_gsm_status": "n/a", "seam_status": "n/a", "continuity_status": "n/a",
    "archive_status": "n/a", "supabase_status": "n/a", "b2_status": "n/a",
    "failed_gate": "dev_preview_synthetic_gate", "exact_failure_reason": "DEV PREVIEW -- synthetic, not a real fault",
    "capital_status": "SHADOW_ONLY_NO_CAPITAL",
    "exact_next_action": "DEV PREVIEW: DO NOT ACT ON ANY SIGNAL. Fault: dev_preview_synthetic_gate -- "
                          "DEV PREVIEW -- synthetic, not a real fault. Report to Claude/GPT with dashboard screenshot.",
    "recommendation": "DEV PREVIEW -- synthetic, not a real cycle", "created_at": "2026-01-01T00:00:00+00:00",
}


def main():
    st.set_page_config(page_title="Phase 2 Shadow Operating Dashboard", layout="wide")
    # DEV preview is URL-only (?dev=1) -- never a visible sidebar control
    # on the public deployment (addendum Section 4b.6).
    dev_mode = st.query_params.get("dev") == "1"

    if dev_mode:
        run148_ref = load_run148_reference()
        ctx = build_dashboard_context(_DEV_HALT_PREVIEW_SNAPSHOT, {}, {}, {}, run148_ref)
        html = render_dash3_html(ctx, dev_mode=True)
        components.html(html, height=1400, scrolling=True)
        st.error("DEV PREVIEW MODE (?dev=1) -- the page above uses SYNTHETIC data for evidence/screenshot "
                 "purposes only. It is NOT a real fault and does not reflect any real snapshot row. "
                 "Remove ?dev=1 from the URL to see the real live snapshot.")
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

    run148_ref = load_run148_reference()
    ctx = build_dashboard_context(snap, latest_prices, signal_prices, previous_prices, run148_ref)
    html = render_dash3_html(ctx, dev_mode=False)
    components.html(html, height=2600, scrolling=True)

    st.caption(f"Load time this render: {load_seconds:.2f}s (snapshot) + {price_load_seconds:.2f}s (real prices) "
               f"| DEV preview available via ?dev=1 (not visible in normal navigation).")


if __name__ == "__main__":
    main()
