"""
Streamlit dashboard for the DeFi Arbitrage Scanner.

Displays live arbitrage opportunities, an interactive graph visualisation,
and historical analysis charts.  Falls back to mock data when backend
services are unavailable.
"""

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to path so we can import sibling packages
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30, show_spinner=False)
def _get_network_stats() -> Dict[str, float]:
    """Fetch current ETH price (USD) and gas price (Gwei) via public APIs."""
    eth_price = 2130.0
    gas_gwei = 20.0
    try:
        import requests
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "ethereum", "vs_currencies": "usd"},
            timeout=5,
        )
        if r.ok:
            eth_price = r.json()["ethereum"]["usd"]
    except Exception:
        pass
    try:
        import requests
        r = requests.get(
            "https://api.etherscan.io/api",
            params={"module": "gastracker", "action": "gasoracle"},
            timeout=5,
        )
        if r.ok:
            data = r.json().get("result", {})
            gas_gwei = float(data.get("ProposeGasPrice", gas_gwei))
    except Exception:
        pass
    return {"eth_price_usd": eth_price, "gas_price_gwei": gas_gwei}


# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DeFi Arbitrage Scanner",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Helpers / backend wrappers
# ---------------------------------------------------------------------------

def _load_config() -> Dict[str, Any]:
    try:
        import yaml

        cfg_path = _ROOT / "config" / "settings.yaml"
        with open(cfg_path) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


@st.cache_resource(show_spinner=False)
def _get_fetcher(config: Dict):
    try:
        from fetcher.dex_fetcher import DEXFetcher

        return DEXFetcher(config)
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _get_finder():
    try:
        from graph.arbitrage_finder import ArbitrageFinder

        return ArbitrageFinder(g=None)  # mock mode
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _get_analyzer(config: Dict):
    try:
        from spark.historical_analysis import HistoricalAnalyzer

        return HistoricalAnalyzer(config)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Data fetching with error recovery
# ---------------------------------------------------------------------------

def fetch_opportunities(
    finder, min_profit: float, eth_price: float, gas_gwei: float
) -> List[Dict[str, Any]]:
    """Fetch live arbitrage opportunities enriched with cost breakdown."""
    if finder is None:
        return []
    try:
        opps = finder.find_triangular_arbitrage(min_profit=min_profit)
        return finder.enrich_with_economics(opps, eth_price, gas_gwei)
    except Exception as exc:
        st.warning(f"Error fetching opportunities: {exc}")
        return []


def fetch_prices(fetcher) -> Dict[str, Dict[str, float]]:
    """Fetch current DEX prices."""
    if fetcher is None:
        return {}
    try:
        return fetcher.fetch_all_prices()
    except Exception:
        return {}


def fetch_historical(analyzer) -> Dict[str, pd.DataFrame]:
    """Fetch historical analysis results."""
    if analyzer is None:
        return {}
    try:
        return analyzer.analyze_opportunities()
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _format_path(path: List[str]) -> str:
    return " -> ".join(path)


def _format_dexes(dexes: List[str]) -> str:
    return " | ".join(dexes)


def _profit_color(profit_ratio: float) -> str:
    if profit_ratio >= 0.01:
        return "green"
    if profit_ratio >= 0.005:
        return "orange"
    return "red"


def _fmt_usd(val) -> str:
    return f"${val:,.0f}" if val is not None else "—"


def render_opportunities_table(
    opportunities: List[Dict], eth_price: float, gas_gwei: float
) -> None:
    """Render opportunities with three execution strategy tabs."""
    if not opportunities:
        st.info("No arbitrage opportunities found above the selected threshold.")
        return

    avg_gas = sum(o.get("gas_cost_usd", 0) for o in opportunities) / len(opportunities)
    best_net = max(o.get("net_spread_pct", 0) for o in opportunities)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ETH Price", f"${eth_price:,.0f}")
    c2.metric("Gas Price", f"{gas_gwei:.1f} Gwei")
    c3.metric("Avg Gas Cost / Cycle", f"${avg_gas:.2f}")
    c4.metric("Best Net Spread", f"{best_net:.3f}%")

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs([
        "🏦 Regular (own capital)",
        "⚡ Flashloan (Aave V3 / Balancer)",
        "🤖 Flashloan + Flashbots (MEV)",
    ])

    # ── Tab 1: Regular ───────────────────────────────────────────────────────
    with tab1:
        st.caption(
            "You execute 3 separate swaps with your own capital. "
            "Risk: frontrunning, gas paid upfront."
        )
        rows = []
        for opp in opportunities:
            dep = opp.get("min_deposit_usd")
            rows.append({
                "Path":          _format_path(opp.get("path", [])),
                "DEXes":         _format_dexes(opp.get("dexes", [])),
                "Gross Spread":  opp.get("gross_spread_pct", 0),
                "DEX Fees":      opp.get("dex_fees_pct", 0),
                "Net Spread":    opp.get("net_spread_pct", 0),
                "Gas (units)":   opp.get("gas_units", 0),
                "Gas Cost $":    opp.get("gas_cost_usd", 0),
                "Min Deposit":   _fmt_usd(dep),
                "Profit @$1K":   opp.get("profit_at_1k_usd", 0),
                "Profit @$10K":  opp.get("profit_at_10k_usd", 0),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True,
            column_config={
                "Gross Spread": st.column_config.NumberColumn("Gross Spread %", format="%.3f%%"),
                "DEX Fees":     st.column_config.NumberColumn("DEX Fees %", format="%.3f%%"),
                "Net Spread":   st.column_config.NumberColumn("Net Spread %", format="%.3f%%"),
                "Gas (units)":  st.column_config.NumberColumn("Gas Units", format="%d"),
                "Gas Cost $":   st.column_config.NumberColumn("Gas Cost", format="$%.2f"),
                "Profit @$1K":  st.column_config.NumberColumn("Profit @$1K", format="$%.2f"),
                "Profit @$10K": st.column_config.NumberColumn("Profit @$10K", format="$%.2f"),
            })

        # Profit vs deposit chart
        deposit_range = [500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]
        fig_reg = go.Figure()
        for opp in opportunities[:5]:
            net = opp.get("net_spread_pct", 0) / 100
            gas = opp.get("gas_cost_usd", 0)
            label = " → ".join(opp.get("path", []))
            fig_reg.add_trace(go.Scatter(
                x=deposit_range,
                y=[max(0, d * net - gas) for d in deposit_range],
                mode="lines+markers", name=label,
            ))
        fig_reg.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Break-even")
        fig_reg.update_layout(
            title="Profit vs Deposit (own capital)",
            xaxis_title="Deposit ($)", yaxis_title="Net Profit ($)",
            xaxis=dict(tickformat="$,.0f"), height=360,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=-0.25), margin=dict(l=20, r=20, t=40, b=80),
        )
        st.plotly_chart(fig_reg, use_container_width=True, key="chart_regular")

    # ── Tab 2: Flashloan ────────────────────────────────────────────────────
    with tab2:
        st.caption(
            "Borrow the full notional in one atomic tx — repay within the same block. "
            "**Capital needed = only gas ETH** (no own trade capital required)."
        )
        fl_cols = st.columns(2)
        for col_idx, fl_name in enumerate(["aave_v3", "balancer"]):
            label = "Aave V3 (fee 0.05%)" if fl_name == "aave_v3" else "Balancer (fee 0%)"
            with fl_cols[col_idx]:
                st.subheader(label)
                rows_fl = []
                for opp in opportunities:
                    fl = opp.get("flashloan", {}).get(fl_name, {})
                    rows_fl.append({
                        "Path":           _format_path(opp.get("path", [])),
                        "Net Spread":     fl.get("fl_net_spread_pct", 0),
                        "Gas $":          fl.get("fl_gas_usd", 0),
                        "Capital Needed": _fmt_usd(fl.get("capital_needed_usd")),
                        "Min Borrow":     _fmt_usd(fl.get("min_notional_usd")),
                        "Profit @$10K":   fl.get("profit_at_10k_usd", 0),
                        "Profit @$50K":   fl.get("profit_at_50k_usd", 0),
                        "Viable":         "✅" if fl.get("viable") else "❌",
                    })
                st.dataframe(pd.DataFrame(rows_fl), use_container_width=True, hide_index=True,
                    column_config={
                        "Net Spread":   st.column_config.NumberColumn("Net Spread %", format="%.3f%%"),
                        "Gas $":        st.column_config.NumberColumn("Gas Cost", format="$%.2f"),
                        "Profit @$10K": st.column_config.NumberColumn("Profit @$10K", format="$%.2f"),
                        "Profit @$50K": st.column_config.NumberColumn("Profit @$50K", format="$%.2f"),
                    })

        st.markdown("---")
        notional_range = [5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000]
        fig_fl = go.Figure()
        for fl_name, dash, color in [("aave_v3", "solid", "#636EFA"), ("balancer", "dash", "#EF553B")]:
            fl_label = "Aave V3" if fl_name == "aave_v3" else "Balancer"
            for opp in opportunities[:3]:
                fl = opp.get("flashloan", {}).get(fl_name, {})
                net = fl.get("fl_net_spread_pct", 0) / 100
                gas = fl.get("fl_gas_usd", 0)
                path = " → ".join(opp.get("path", []))
                fig_fl.add_trace(go.Scatter(
                    x=notional_range,
                    y=[max(0, n * net - gas) for n in notional_range],
                    mode="lines+markers", name=f"{fl_label}: {path}",
                    line=dict(dash=dash, color=color),
                ))
        fig_fl.add_hline(y=0, line_dash="dot", line_color="gray", annotation_text="Break-even")
        fig_fl.update_layout(
            title="Profit vs Borrowed Notional (flashloan, capital needed = gas only)",
            xaxis_title="Borrowed Notional ($)", yaxis_title="Net Profit ($)",
            xaxis=dict(tickformat="$,.0f"), height=380,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=-0.3), margin=dict(l=20, r=20, t=40, b=100),
        )
        st.plotly_chart(fig_fl, use_container_width=True, key="chart_flashloan")

    # ── Tab 3: Flashbots ────────────────────────────────────────────────────
    with tab3:
        fb_share = opportunities[0].get("flashbots", {}).get("builder_share_pct", 85)
        searcher_share = opportunities[0].get("flashbots", {}).get("searcher_share_pct", 15)
        st.caption(
            f"Private bundle via Flashbots — frontrunning/sandwich impossible. "
            f"Builder takes **{fb_share:.0f}%** of arb profit; you keep **{searcher_share:.0f}%**. "
            f"Capital needed = **only gas ETH** (Balancer flashloan for notional)."
        )

        rows_fb = []
        for opp in opportunities:
            fb = opp.get("flashbots", {})
            rows_fb.append({
                "Path":            _format_path(opp.get("path", [])),
                "Gross Spread":    opp.get("gross_spread_pct", 0),
                "Searcher Net %":  fb.get("fb_net_spread_pct", 0),
                "Gas $":           fb.get("fl_gas_usd", 0),
                "Capital Needed":  _fmt_usd(fb.get("capital_needed_usd")),
                "Min Borrow":      _fmt_usd(fb.get("min_notional_usd")),
                "Profit @$50K":    fb.get("profit_at_50k_usd", 0),
                "Profit @$100K":   fb.get("profit_at_100k_usd", 0),
                "Viable":          "✅" if fb.get("viable") else "❌",
            })
        st.dataframe(pd.DataFrame(rows_fb), use_container_width=True, hide_index=True,
            column_config={
                "Gross Spread":   st.column_config.NumberColumn("Gross Spread %", format="%.3f%%"),
                "Searcher Net %": st.column_config.NumberColumn("Searcher Net %", format="%.3f%%"),
                "Gas $":          st.column_config.NumberColumn("Gas Cost", format="$%.2f"),
                "Profit @$50K":   st.column_config.NumberColumn("Profit @$50K", format="$%.2f"),
                "Profit @$100K":  st.column_config.NumberColumn("Profit @$100K", format="$%.2f"),
            })

        notional_range = [10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000]
        fig_fb = go.Figure()
        for opp in opportunities[:5]:
            fb = opp.get("flashbots", {})
            net = fb.get("fb_net_spread_pct", 0) / 100
            gas = fb.get("fl_gas_usd", 0)
            path = " → ".join(opp.get("path", []))
            fig_fb.add_trace(go.Scatter(
                x=notional_range,
                y=[max(0, n * net - gas) for n in notional_range],
                mode="lines+markers", name=path,
            ))
        fig_fb.add_hline(y=0, line_dash="dot", line_color="gray", annotation_text="Break-even")
        fig_fb.update_layout(
            title=f"Searcher Profit vs Notional (Flashbots, builder takes {fb_share:.0f}%)",
            xaxis_title="Borrowed Notional ($)", yaxis_title="Searcher Profit ($)",
            xaxis=dict(tickformat="$,.0f"), height=380,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=-0.25), margin=dict(l=20, r=20, t=40, b=80),
        )
        st.plotly_chart(fig_fb, use_container_width=True, key="chart_flashbots")


def render_graph_visualization(opportunities: List[Dict]) -> None:
    """Render a Plotly network graph of arbitrage cycles."""
    if not opportunities:
        st.info("No cycles to visualise.")
        return

    try:
        import networkx as nx

        G = nx.MultiDiGraph()

        # Build graph from all opportunities
        for opp in opportunities:
            path = opp.get("path", [])
            dexes = opp.get("dexes", [])
            profit = opp.get("profit_ratio", 0)

            for i in range(len(path) - 1):
                src, dst = path[i], path[i + 1]
                G.add_node(src)
                G.add_node(dst)
                dex = dexes[i] if i < len(dexes) else "?"
                G.add_edge(src, dst, dex=dex, profit=profit)

        # Layout
        pos = nx.spring_layout(G, seed=42, k=2.5)

        # Build Plotly traces
        edge_traces = []
        for u, v, data in G.edges(data=True):
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            color = (
                "rgba(0,200,100,0.6)"
                if data.get("profit", 0) >= 0.008
                else "rgba(255,165,0,0.5)"
            )
            edge_traces.append(
                go.Scatter(
                    x=[x0, x1, None],
                    y=[y0, y1, None],
                    mode="lines",
                    line=dict(width=2, color=color),
                    hoverinfo="none",
                    showlegend=False,
                )
            )

        node_x = [pos[n][0] for n in G.nodes()]
        node_y = [pos[n][1] for n in G.nodes()]
        node_labels = list(G.nodes())

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=node_labels,
            textposition="top center",
            hoverinfo="text",
            marker=dict(
                size=28,
                color=[
                    "#1f77b4" if n in ("ETH", "WBTC") else "#ff7f0e"
                    for n in G.nodes()
                ],
                line=dict(width=2, color="white"),
            ),
            showlegend=False,
        )

        fig = go.Figure(
            data=edge_traces + [node_trace],
            layout=go.Layout(
                title="Token Arbitrage Cycle Graph",
                title_font_size=16,
                showlegend=False,
                hovermode="closest",
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=450,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=50, b=20),
            ),
        )
        st.plotly_chart(fig, use_container_width=True, key="arb_cycle_graph")

    except ImportError:
        st.warning("networkx is required for graph visualisation.")
    except Exception as exc:
        st.error(f"Graph rendering error: {exc}")


def render_historical_charts(historical: Dict[str, pd.DataFrame]) -> None:
    """Render historical analysis Plotly charts."""
    col1, col2 = st.columns(2)

    # --- By pair ---
    with col1:
        st.subheader("Average Spread by Token Pair")
        by_pair = historical.get("by_pair")
        if by_pair is not None and not by_pair.empty:
            fig = px.bar(
                by_pair.head(10),
                x="pair",
                y="mean_spread_pct",
                color="mean_spread_pct",
                color_continuous_scale="Viridis",
                labels={
                    "pair": "Token Pair",
                    "mean_spread_pct": "Avg Spread (%)",
                },
                title="Mean Price Spread (%) by Pair",
            )
            fig.update_layout(
                xaxis_tickangle=-30,
                coloraxis_showscale=False,
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True, key="hist_by_pair")
        else:
            st.info("No pair data available.")

    # --- By hour ---
    with col2:
        st.subheader("Opportunity Frequency by Hour (UTC)")
        by_hour = historical.get("by_hour")
        if by_hour is not None and not by_hour.empty:
            fig = px.line(
                by_hour,
                x="hour",
                y="mean_spread_pct",
                markers=True,
                labels={
                    "hour": "Hour of Day (UTC)",
                    "mean_spread_pct": "Avg Spread (%)",
                },
                title="Average Spread by Hour of Day",
            )
            fig.update_traces(line=dict(color="#1f77b4", width=2))
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True, key="hist_by_hour")
        else:
            st.info("No hourly data available.")


def render_price_heatmap(prices: Dict[str, Dict[str, float]]) -> None:
    """Render a heatmap of current prices across DEXes."""
    if not prices:
        return

    pairs = list(prices.keys())
    dexes = sorted({d for v in prices.values() for d in v.keys()})

    matrix = []
    for pair in pairs:
        row = [prices[pair].get(dex, None) for dex in dexes]
        matrix.append(row)

    df = pd.DataFrame(matrix, index=pairs, columns=dexes)

    # Normalise each row to show relative spread
    df_norm = df.div(df.mean(axis=1), axis=0).sub(1).mul(100).round(4)

    fig = px.imshow(
        df_norm,
        labels=dict(x="DEX", y="Pair", color="Deviation from Mean (%)"),
        title="Price Deviation from Mean Across DEXes (%)",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        aspect="auto",
        text_auto=".3f",
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True, key="price_heatmap")


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main() -> None:
    config = _load_config()
    fetcher = _get_fetcher(config)
    finder = _get_finder()
    analyzer = _get_analyzer(config)
    net_stats = _get_network_stats()
    eth_price = net_stats["eth_price_usd"]
    gas_gwei = net_stats["gas_price_gwei"]

    # ------------------------------------------------------------------
    # Sidebar
    # ------------------------------------------------------------------
    with st.sidebar:
        st.title("DeFi Arbitrage Scanner")
        st.markdown("---")

        min_profit_pct = st.slider(
            "Minimum Profit %",
            min_value=0.0,
            max_value=5.0,
            value=0.5,
            step=0.1,
            format="%.1f%%",
        )
        min_profit = min_profit_pct / 100

        st.markdown("---")
        auto_refresh = st.toggle("Auto-Refresh", value=False)
        refresh_interval = st.selectbox(
            "Refresh Interval (seconds)",
            options=[10, 30, 60, 120],
            index=1,
            disabled=not auto_refresh,
        )

        st.markdown("---")
        st.markdown("**Status**")
        fetcher_ok = fetcher is not None
        finder_ok = finder is not None
        analyzer_ok = analyzer is not None
        st.markdown(
            f"- Fetcher: {'Connected' if fetcher_ok else 'Mock'}"
        )
        st.markdown(
            f"- Arbitrage Finder: {'Live' if finder_ok else 'Unavailable'}"
        )
        st.markdown(
            f"- Historical Analyzer: {'Ready' if analyzer_ok else 'Unavailable'}"
        )
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

    # ------------------------------------------------------------------
    # Main content
    # ------------------------------------------------------------------
    st.title("DeFi Arbitrage Scanner")
    st.markdown(
        "Real-time cross-DEX arbitrage opportunities across Uniswap V2/V3 and SushiSwap."
    )

    # Refresh placeholder for auto-refresh
    refresh_placeholder = st.empty()

    # --- Live Opportunities ---
    st.header("Live Arbitrage Opportunities")
    opp_placeholder = st.empty()

    # --- Price Heatmap ---
    st.header("Current Price Spread Across DEXes")

    # --- Graph Visualization ---
    st.header("Arbitrage Cycle Graph")

    # --- Historical Analysis ---
    st.header("Historical Analysis")

    # ------------------------------------------------------------------
    # Render (single pass — auto-refresh uses st.rerun)
    # ------------------------------------------------------------------
    opportunities = fetch_opportunities(finder, min_profit, eth_price, gas_gwei)
    prices = fetch_prices(fetcher)
    historical = fetch_historical(analyzer)

    with opp_placeholder.container():
        render_opportunities_table(opportunities, eth_price, gas_gwei)

    if prices:
        render_price_heatmap(prices)
    else:
        st.info("No live price data available.")

    render_graph_visualization(opportunities)

    if historical:
        render_historical_charts(historical)
    else:
        st.info("Historical data not available.")

    if auto_refresh:
        time.sleep(refresh_interval)
        refresh_placeholder.caption(
            f"Auto-refreshed at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )
        st.rerun()


if __name__ == "__main__":
    main()
