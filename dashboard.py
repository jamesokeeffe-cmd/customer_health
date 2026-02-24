"""Streamlit dashboard for the Health Score Middleware.

Run with:  streamlit run dashboard.py
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.main import HealthScoreOrchestrator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIER_COLORS = {
    "Champion": "#28a745",
    "Healthy": "#17a2b8",
    "At Risk": "#ffc107",
    "Critical": "#dc3545",
}

DIMENSION_LABELS = {
    "support_health": "Support Health",
    "financial_contract": "Financial & Contract",
    "adoption_engagement": "Adoption & Engagement",
    "relationship_expansion": "Relationship & Expansion",
}

PVS_PILLAR_LABELS = {
    "messaging": "Messaging",
    "automations": "Automations",
    "contactless": "Contactless",
    "requests": "Requests",
    "staff_adoption": "Staff Adoption",
}


def _score_bg(score: float | None) -> str:
    if score is None:
        return "#e9ecef"
    if score >= 76:
        return "#d4edda"
    if score >= 60:
        return "#fff3cd"
    return "#f8d7da"


def _traffic_light(score: float | None) -> str:
    if score is None:
        return "\u26aa"
    if score >= 76:
        return "\U0001f7e2"
    if score >= 60:
        return "\U0001f7e1"
    return "\U0001f534"


# ---------------------------------------------------------------------------
# Orchestrator init (cached once per server process)
# ---------------------------------------------------------------------------

@st.cache_resource
def _init_orchestrator() -> HealthScoreOrchestrator:
    load_dotenv()
    orch = HealthScoreOrchestrator(dry_run=True)
    orch.init_clients_from_env()
    return orch


# ---------------------------------------------------------------------------
# Gauge chart
# ---------------------------------------------------------------------------

def _render_gauge(score: float | None):
    val = score if score is not None else 0
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=val,
        number={"suffix": "", "font": {"size": 48}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar": {"color": "#333"},
            "steps": [
                {"range": [0, 59], "color": "#f8d7da"},
                {"range": [59, 75], "color": "#fff3cd"},
                {"range": [75, 89], "color": "#b8e6f0"},
                {"range": [89, 100], "color": "#d4edda"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 3},
                "thickness": 0.8,
                "value": val,
            },
        },
    ))
    fig.update_layout(height=250, margin=dict(t=30, b=10, l=30, r=30))
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Dimension card
# ---------------------------------------------------------------------------

def _render_dimension_card(name: str, dim_result: dict):
    label = DIMENSION_LABELS.get(name, name)
    score = dim_result.get("score")
    coverage = dim_result.get("coverage", 0)
    metric_count = len(dim_result.get("metric_scores", {}))
    available = sum(1 for v in dim_result.get("metric_scores", {}).values() if v is not None)

    bg = _score_bg(score)
    if score is None:
        display = "No data"
        text_color = "#6c757d"
    else:
        display = f"{score:.1f}"
        text_color = "#212529"

    st.markdown(
        f"""<div style="background:{bg}; border-radius:8px; padding:16px; text-align:center; height:140px;">
            <div style="font-size:14px; font-weight:600; color:#495057;">{label}</div>
            <div style="font-size:32px; font-weight:700; color:{text_color}; margin:8px 0;">{display}</div>
            <div style="font-size:12px; color:#6c757d;">{available}/{metric_count} metrics</div>
        </div>""",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Metric drill-down table
# ---------------------------------------------------------------------------

def _render_drill_down(dim_name: str, dim_result: dict, orchestrator: HealthScoreOrchestrator):
    metric_scores = dim_result.get("metric_scores", {})
    if not metric_scores:
        st.info("No metric data available.")
        return

    weights = orchestrator.weights.get(dim_name, {})
    thresholds = orchestrator.thresholds.get(dim_name, {})

    rows = []
    for metric, score in metric_scores.items():
        weight = weights.get(metric, 0)
        t_cfg = thresholds.get(metric, {})
        direction = "Lower is better" if t_cfg.get("lower_is_better") else "Higher is better"
        # Use "paid" thresholds as representative display
        seg_t = t_cfg.get("paid", t_cfg.get("standard", {}))
        green_t = seg_t.get("green", "—")
        yellow_t = seg_t.get("yellow", "—")
        red_t = seg_t.get("red", "—")

        rows.append({
            "Metric": metric.replace("_", " ").title(),
            "Score": f"{score:.1f}" if score is not None else "—",
            "Status": _traffic_light(score),
            "Weight": f"{weight:.0%}",
            "Green": green_t,
            "Yellow": yellow_t,
            "Red": red_t,
            "Direction": direction,
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Single account view
# ---------------------------------------------------------------------------

def _render_single_account(result: dict, orchestrator: HealthScoreOrchestrator):
    composite = result["composite"]
    qualitative = result["qualitative"]
    final_score = qualitative["final_score"]
    quant_score = composite["quantitative_score"]
    churn_risk = composite["churn_risk_score"]
    tier = composite["tier"]
    pvs = result["platform_value"]
    coverage = result["coverage_pct"]

    # --- Row 1: Header ---
    col_gauge, col_metrics, col_tier = st.columns([1, 1, 1])

    with col_gauge:
        _render_gauge(final_score)

    with col_metrics:
        st.metric("Final Score", f"{final_score:.1f}" if final_score is not None else "—")
        st.metric("Quantitative Score", f"{quant_score:.1f}" if quant_score is not None else "—")
        st.metric("Churn Risk Score", f"{churn_risk:.1f}" if churn_risk is not None else "—")
        st.metric("Platform Value", f"{pvs['score']:.1f}" if pvs["score"] is not None else "—")

    with col_tier:
        tier_color = TIER_COLORS.get(tier, "#6c757d")
        st.markdown(
            f'<div style="background:{tier_color}; color:white; border-radius:8px; '
            f'padding:16px; text-align:center; font-size:24px; font-weight:700; margin-bottom:12px;">'
            f'{tier or "N/A"}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(f"**Coverage:** {coverage:.1f}%")
        st.progress(min(coverage / 100, 1.0))
        if qualitative["override_active"]:
            st.warning(f"Qualitative override active (cap: {qualitative['cap_value']})")
        signal_total = qualitative["critical_count"] + qualitative["moderate_count"] + qualitative["watch_count"]
        st.markdown(
            f"**Signals:** {qualitative['critical_count']} critical, "
            f"{qualitative['moderate_count']} moderate, "
            f"{qualitative['watch_count']} watch ({signal_total} total)"
        )

    st.divider()

    # --- Row 2: Dimension cards ---
    st.subheader("Dimension Scores")
    dim_cols = st.columns(4)
    for i, (dim_name, dim_result) in enumerate(result["dimension_scores"].items()):
        with dim_cols[i]:
            _render_dimension_card(dim_name, dim_result)

    st.divider()

    # --- Row 3: Platform Value breakdown ---
    st.subheader("Platform Value Score")
    pvs_composite_col, *pillar_cols = st.columns(6)
    with pvs_composite_col:
        st.metric("PVS Composite", f"{pvs['score']:.1f}" if pvs["score"] is not None else "—")
    pillar_scores = pvs.get("pillar_scores", {})
    pillar_names = list(PVS_PILLAR_LABELS.keys())
    for idx, col in enumerate(pillar_cols):
        if idx < len(pillar_names):
            pname = pillar_names[idx]
            pval = pillar_scores.get(pname)
            with col:
                st.metric(
                    PVS_PILLAR_LABELS[pname],
                    f"{pval:.1f}" if pval is not None else "—",
                )

    st.divider()

    # --- Row 4: Drill-down expanders ---
    st.subheader("Metric Drill-Down")
    for dim_name, dim_result in result["dimension_scores"].items():
        label = DIMENSION_LABELS.get(dim_name, dim_name)
        score = dim_result.get("score")
        header = f"{_traffic_light(score)} {label} — {score:.1f}" if score is not None else f"\u26aa {label} — No data"
        with st.expander(header):
            _render_drill_down(dim_name, dim_result, orchestrator)


# ---------------------------------------------------------------------------
# All accounts view
# ---------------------------------------------------------------------------

def _render_all_accounts(results: list[dict]):
    if not results:
        st.info("No results to display. Score accounts first.")
        return

    # Tier distribution
    st.subheader("Tier Distribution")
    tier_counts = {"Champion": 0, "Healthy": 0, "At Risk": 0, "Critical": 0}
    for r in results:
        t = r["composite"]["tier"]
        if t in tier_counts:
            tier_counts[t] += 1

    tier_cols = st.columns(4)
    for i, (tier_name, count) in enumerate(tier_counts.items()):
        with tier_cols[i]:
            color = TIER_COLORS[tier_name]
            st.markdown(
                f'<div style="background:{color}; color:white; border-radius:8px; '
                f'padding:12px; text-align:center;">'
                f'<div style="font-size:28px; font-weight:700;">{count}</div>'
                f'<div style="font-size:13px;">{tier_name}</div></div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # Summary table
    rows = []
    for r in results:
        final = r["qualitative"]["final_score"]
        tier = r["composite"]["tier"]
        dims = r["dimension_scores"]
        rows.append({
            "Account": r.get("account_name", r.get("account_id", "?")),
            "Segment": r.get("segment", "—"),
            "Final Score": round(final, 1) if final is not None else None,
            "Tier": tier,
            "Support": dims["support_health"]["score"],
            "Financial": dims["financial_contract"]["score"],
            "Adoption": dims["adoption_engagement"]["score"],
            "Relationship": dims["relationship_expansion"]["score"],
            "PVS": r["platform_value"]["score"],
            "Coverage %": r["coverage_pct"],
        })

    df = pd.DataFrame(rows)

    def _style_tier(val):
        color = TIER_COLORS.get(val, "#6c757d")
        return f"background-color: {color}; color: white; font-weight: 600; border-radius: 4px;"

    def _style_score(val):
        bg = _score_bg(val)
        return f"background-color: {bg};"

    score_cols = ["Final Score", "Support", "Financial", "Adoption", "Relationship", "PVS"]
    styled = df.style.map(_style_tier, subset=["Tier"]).map(_style_score, subset=score_cols)
    st.dataframe(styled, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Health Score Dashboard",
        page_icon="\U0001f3af",
        layout="wide",
    )

    st.title("\U0001f3af Health Score Dashboard")

    orchestrator = _init_orchestrator()

    # --- Sidebar ---
    with st.sidebar:
        st.header("Configuration")

        # Extractor status
        st.subheader("Extractor Status")
        extractors = {
            "Intercom": orchestrator.intercom is not None,
            "Jira": orchestrator.jira is not None,
            "Looker": orchestrator.looker is not None,
            "Salesforce": orchestrator.sf_extractor is not None,
        }
        for name, connected in extractors.items():
            icon = "\U0001f7e2" if connected else "\U0001f534"
            st.markdown(f"{icon} **{name}**")

        if not any(extractors.values()):
            st.info("No extractors configured. Set credentials in .env to connect.")

        st.divider()

        # Account mapping check
        accounts = orchestrator.account_mapping
        if not accounts:
            st.warning("No accounts in config/account_mapping.csv")
            st.stop()

        # Account selector
        account_names = [a.get("account_name", a["sf_account_id"]) for a in accounts]
        options = ["All Accounts"] + account_names
        selected = st.selectbox("Account", options)

        # Scoring period
        default_period = datetime.now(timezone.utc).strftime("%Y-%m")
        period = st.text_input("Scoring Period", value=default_period)

        # Score button
        run_scoring = st.button("Score", type="primary", use_container_width=True)

    # --- Scoring logic ---
    if run_scoring:
        if selected == "All Accounts":
            results = []
            progress = st.progress(0, text="Scoring accounts...")
            for i, account in enumerate(accounts):
                name = account.get("account_name", account["sf_account_id"])
                progress.progress((i + 1) / len(accounts), text=f"Scoring {name}...")
                try:
                    result = orchestrator.score_account(account)
                    results.append(result)
                except Exception as e:
                    st.error(f"Failed to score {name}: {e}")
                    st.exception(e)
            progress.empty()
            st.session_state["results"] = results
            st.session_state["mode"] = "all"
        else:
            idx = account_names.index(selected)
            account = accounts[idx]
            try:
                with st.spinner(f"Scoring {selected}..."):
                    result = orchestrator.score_account(account)
                st.session_state["results"] = [result]
                st.session_state["mode"] = "single"
            except Exception as e:
                st.error(f"Failed to score {selected}: {e}")
                st.exception(e)

    # --- Display results ---
    mode = st.session_state.get("mode")
    results = st.session_state.get("results")

    if not results:
        st.info("Select an account and click **Score** to run the pipeline.")
        return

    if mode == "single":
        result = results[0]
        st.header(f"{result.get('account_name', '?')} — {result.get('segment', '').title()}")
        _render_single_account(result, orchestrator)
    elif mode == "all":
        st.header("All Accounts Overview")
        _render_all_accounts(results)


if __name__ == "__main__":
    main()
