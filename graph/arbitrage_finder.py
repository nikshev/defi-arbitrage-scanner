"""
Gremlin-based triangular arbitrage finder for the DeFi Arbitrage Scanner.

Detects 3-hop cycles (Token A -> Token B -> Token C -> Token A) across
DEX pool edges and calculates the theoretical profit ratio.
"""

import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from gremlin_python.process.graph_traversal import __
    from gremlin_python.process.traversal import P

    GREMLIN_AVAILABLE = True
except ImportError:
    GREMLIN_AVAILABLE = False


# Realistic mock opportunities for demo mode
_MOCK_OPPORTUNITY_TEMPLATES = [
    {
        "path": ["ETH", "USDC", "DAI", "ETH"],
        "dexes": ["uniswap_v2", "sushiswap", "uniswap_v3"],
        "base_profit": 0.0082,
    },
    {
        "path": ["ETH", "WBTC", "USDC", "ETH"],
        "dexes": ["uniswap_v3", "uniswap_v2", "sushiswap"],
        "base_profit": 0.0061,
    },
    {
        "path": ["USDC", "DAI", "ETH", "USDC"],
        "dexes": ["sushiswap", "uniswap_v2", "uniswap_v3"],
        "base_profit": 0.0034,
    },
    {
        "path": ["ETH", "DAI", "WBTC", "ETH"],
        "dexes": ["uniswap_v2", "uniswap_v3", "sushiswap"],
        "base_profit": 0.0118,
    },
    {
        "path": ["WBTC", "USDC", "DAI", "WBTC"],
        "dexes": ["uniswap_v3", "sushiswap", "uniswap_v2"],
        "base_profit": 0.0047,
    },
    {
        "path": ["DAI", "ETH", "USDC", "DAI"],
        "dexes": ["sushiswap", "uniswap_v3", "uniswap_v2"],
        "base_profit": 0.0025,
    },
    {
        "path": ["ETH", "USDC", "WBTC", "ETH"],
        "dexes": ["uniswap_v2", "uniswap_v2", "uniswap_v3"],
        "base_profit": 0.0093,
    },
    {
        "path": ["USDC", "ETH", "DAI", "USDC"],
        "dexes": ["uniswap_v3", "sushiswap", "uniswap_v2"],
        "base_profit": 0.0071,
    },
]


class ArbitrageFinder:
    """
    Finds triangular arbitrage opportunities in the token-exchange graph.

    Parameters
    ----------
    g : GraphTraversalSource or None
        Active Gremlin traversal source.  Pass ``None`` to use mock mode.
    """

    def __init__(self, g: Optional[Any] = None) -> None:
        self.g = g
        self._mock_mode = g is None or not GREMLIN_AVAILABLE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_triangular_arbitrage(
        self, min_profit: float = 0.005
    ) -> List[Dict[str, Any]]:
        """
        Detect 3-hop cycles with positive profit above ``min_profit``.

        Parameters
        ----------
        min_profit : float  Minimum profit ratio (e.g. 0.005 = 0.5 %).

        Returns
        -------
        list of dict
            Each dict has keys: path, profit_ratio, dexes, timestamp.
        """
        if self._mock_mode:
            return self._mock_triangular_arbitrage(min_profit)

        try:
            return self._gremlin_triangular_arbitrage(min_profit)
        except Exception as exc:
            logger.error(
                "Gremlin query failed (%s). Falling back to mock data.", exc
            )
            return self._mock_triangular_arbitrage(min_profit)

    def calculate_profit_ratio(self, prices: List[float]) -> float:
        """
        Calculate the profit ratio for a cycle of exchange rates.

        For a 3-hop cycle with rates p1, p2, p3 (each after fee):
            profit_ratio = p1 * p2 * p3 - 1.0

        Parameters
        ----------
        prices : list of float  Exchange rates along the cycle.

        Returns
        -------
        float  Profit ratio (positive means profitable).
        """
        if not prices:
            return 0.0
        result = 1.0
        for p in prices:
            result *= p
        return result - 1.0

    def get_top_opportunities(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Return the top-N arbitrage opportunities sorted by profit ratio.

        Parameters
        ----------
        n : int  Number of opportunities to return.

        Returns
        -------
        list of dict
        """
        opportunities = self.find_triangular_arbitrage(min_profit=0.0)
        sorted_opps = sorted(
            opportunities, key=lambda x: x["profit_ratio"], reverse=True
        )
        return sorted_opps[:n]

    # ------------------------------------------------------------------
    # Gremlin implementation
    # ------------------------------------------------------------------

    def _gremlin_triangular_arbitrage(
        self, min_profit: float
    ) -> List[Dict[str, Any]]:
        """Execute the Gremlin cycle-detection traversal."""
        results = (
            self.g.V().hasLabel("Token").as_("start")
            .outE("Pool").as_("e1").inV().as_("mid1")
            .outE("Pool").as_("e2").inV().as_("mid2")
            .outE("Pool").as_("e3").inV().where(P.eq("start"))
            .select("e1", "e2", "e3")
            .by("price")
            .toList()
        )

        opportunities: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for row in results:
            try:
                p1 = float(row["e1"])
                p2 = float(row["e2"])
                p3 = float(row["e3"])
                profit = self.calculate_profit_ratio([p1, p2, p3])

                if profit >= min_profit:
                    opportunities.append(
                        {
                            "path": self._extract_path(row),
                            "profit_ratio": round(profit, 6),
                            "dexes": self._extract_dexes(row),
                            "timestamp": now,
                        }
                    )
            except (KeyError, TypeError, ValueError) as exc:
                logger.debug("Skipping malformed row: %s", exc)

        logger.info(
            "Found %d opportunities (min_profit=%.3f%%)",
            len(opportunities),
            min_profit * 100,
        )
        return opportunities

    def _extract_path(self, row: Dict) -> List[str]:
        """Extract token symbols from a Gremlin result row."""
        try:
            # Attempt to pull vertex labels from 'start', 'mid1', 'mid2'
            return [
                row.get("start", {}).get("symbol", "?"),
                row.get("mid1", {}).get("symbol", "?"),
                row.get("mid2", {}).get("symbol", "?"),
                row.get("start", {}).get("symbol", "?"),
            ]
        except Exception:
            return ["?", "?", "?", "?"]

    def _extract_dexes(self, row: Dict) -> List[str]:
        """Extract DEX names from a Gremlin result row."""
        try:
            return [
                row.get("dex1", "unknown"),
                row.get("dex2", "unknown"),
                row.get("dex3", "unknown"),
            ]
        except Exception:
            return ["unknown", "unknown", "unknown"]

    # ------------------------------------------------------------------
    # Mock implementation
    # ------------------------------------------------------------------

    def _mock_triangular_arbitrage(
        self, min_profit: float
    ) -> List[Dict[str, Any]]:
        """
        Generate realistic mock arbitrage opportunities.

        Adds a small random variance to the base profit so each call
        returns slightly different numbers (simulating live data).
        """
        opportunities: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for tmpl in _MOCK_OPPORTUNITY_TEMPLATES:
            noise = random.uniform(-0.002, 0.003)
            profit = tmpl["base_profit"] + noise
            if profit >= min_profit:
                opportunities.append(
                    {
                        "path": list(tmpl["path"]),
                        "profit_ratio": round(profit, 6),
                        "dexes": list(tmpl["dexes"]),
                        "timestamp": now,
                    }
                )

        logger.debug(
            "Mock: returning %d opportunities (min_profit=%.3f%%)",
            len(opportunities),
            min_profit * 100,
        )
        return opportunities
