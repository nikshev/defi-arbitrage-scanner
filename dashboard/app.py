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
    finder, min_profit: float
) -> List[Dict[str, Any]]:
    """Fetch live arbitrage opportunities, falling back to empty list."""
    if finder is None:
        return []
    try:
        return finder.find_triangular_arbitrage(min_profit=min_profit)
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


def render_opportunities_table(opportunities: List[Dict]) -> None:
    """Render the live opportunities as a styled dataframe."""
    if not opportunities:
        st.info("No arbitrage opportunities found above the selected threshold.")
        return

    rows = []
    for opp in opportunities:
        rows.append(
            {
                "Path": _format_path(opp.get("path", [])),
                "Profit %": f"{opp.get('profit_ratio', 0) * 100:.3f}%",
                "DEXes": _format_dexes(opp.get("dexes", [])),
                "Timestamp": opp.get("timestamp", ""),
            }
        )

    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Profit %": st.column_config.TextColumn("Profit %", width="small"),
            "Path": st.column_config.TextColumn("Path", width="large"),
            "DEXes": st.column_config.TextColumn("DEXes", width="medium"),
        },
    )


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
        st.plotly_chart(fig, use_container_width=True)

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
            st.plotly_chart(fig, use_container_width=True)
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
            st.plotly_chart(fig, use_container_width=True)
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
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main() -> None:
    config = _load_config()
    fetcher = _get_fetcher(config)
    finder = _get_finder()
    analyzer = _get_analyzer(config)

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
    heatmap_placeholder = st.empty()

    # --- Graph Visualization ---
    st.header("Arbitrage Cycle Graph")
    graph_placeholder = st.empty()

    # --- Historical Analysis ---
    st.header("Historical Analysis")
    hist_placeholder = st.empty()

    # ------------------------------------------------------------------
    # Render loop
    # ------------------------------------------------------------------
    def render_all() -> None:
        opportunities = fetch_opportunities(finder, min_profit)
        prices = fetch_prices(fetcher)
        historical = fetch_historical(analyzer)

        with opp_placeholder.container():
            render_opportunities_table(opportunities)

        with heatmap_placeholder.container():
            if prices:
                render_price_heatmap(prices)
            else:
                st.info("No live price data available.")

        with graph_placeholder.container():
            render_graph_visualization(opportunities)

        with hist_placeholder.container():
            if historical:
                render_historical_charts(historical)
            else:
                st.info("Historical data not available.")

    render_all()

    if auto_refresh:
        for _ in range(1000):
            time.sleep(refresh_interval)
            refresh_placeholder.caption(
                f"Auto-refreshed at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
            )
            render_all()


if __name__ == "__main__":
    main()
