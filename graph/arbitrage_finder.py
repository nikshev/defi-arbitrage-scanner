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

from typing import Dict

# DEX swap fee tiers (fraction, not percent)
DEX_SWAP_FEES: Dict[str, float] = {
    "uniswap_v2": 0.003,
    "uniswap_v3": 0.0005,
    "sushiswap":  0.003,
    "curve":      0.0004,   # typical 3pool/stableswap fee
    "balancer":   0.003,    # typical 0.3% Balancer weighted pool fee
}

# Gas units per swap (3-tx path, no flashloan)
DEX_GAS_UNITS: Dict[str, int] = {
    "uniswap_v2": 152_000,
    "uniswap_v3": 184_000,
    "sushiswap":  152_000,
    "curve":      200_000,  # StableSwap exchange is slightly heavier
    "balancer":   196_000,  # Balancer V2 vault swap
}

# Extra gas overhead for wrapping 3 swaps inside a flashloan callback
# Aave V3 flashloan base overhead ~80k gas
# dYdX/Balancer ~60k gas
FLASHLOAN_GAS_OVERHEAD: Dict[str, int] = {
    "aave_v3":  80_000,
    "balancer": 60_000,
}

# Flashloan protocol fees (fraction of borrowed amount)
FLASHLOAN_FEES: Dict[str, float] = {
    "aave_v3":  0.0005,   # 0.05%
    "balancer": 0.0,      # 0% (Balancer flash loans are free)
}

# Flashbots: builder tip as fraction of gross profit to be competitive
# Typically 80-90% goes to the builder in competitive MEV markets
FLASHBOTS_BUILDER_SHARE = 0.85   # 85% to builder, 15% to searcher


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

    def enrich_with_economics(
        self,
        opportunities: List[Dict[str, Any]],
        eth_price_usd: float,
        gas_price_gwei: float,
    ) -> List[Dict[str, Any]]:
        """
        Add full cost/profitability breakdown for three execution strategies:
          1. Regular (own capital, 3 separate txs)
          2. Flashloan via Aave V3 or Balancer (1 tx, borrowed capital)
          3. Flashloan + Flashbots bundle (MEV-protected, share profit with builder)
        """
        enriched = []
        for opp in opportunities:
            dexes = opp.get("dexes", [])
            profit_ratio = opp.get("profit_ratio", 0.0)

            # ── DEX fees ────────────────────────────────────────────────────
            dex_fees_ratio = sum(DEX_SWAP_FEES.get(d, 0.003) for d in dexes)
            gross_spread   = profit_ratio + dex_fees_ratio  # before DEX fees
            net_spread     = profit_ratio                    # after DEX fees (already net)

            # ── 1. Regular execution ────────────────────────────────────────
            gas_units_regular = sum(DEX_GAS_UNITS.get(d, 152_000) for d in dexes)
            gas_cost_eth      = gas_units_regular * gas_price_gwei * 1e-9
            gas_cost_usd      = gas_cost_eth * eth_price_usd

            if net_spread > 0:
                min_deposit_regular = gas_cost_usd / net_spread
            else:
                min_deposit_regular = None

            # ── 2. Flashloan (Aave V3 & Balancer) ──────────────────────────
            # One atomic tx: flashloan borrow → 3 swaps → repay
            # Capital needed = 0 (borrowed), but you pay:
            #   • flashloan fee on notional
            #   • higher gas (single tx with overhead)
            flashloan_results = {}
            for fl_name, fl_fee in FLASHLOAN_FEES.items():
                fl_gas_units = gas_units_regular + FLASHLOAN_GAS_OVERHEAD[fl_name]
                fl_gas_eth   = fl_gas_units * gas_price_gwei * 1e-9
                fl_gas_usd   = fl_gas_eth * eth_price_usd

                # Net spread after flashloan fee (paid on notional = 1 unit)
                fl_net = net_spread - fl_fee

                # Capital you actually need = only gas cost (no own notional)
                # But you need ETH to pay gas — typically held separately
                capital_needed_usd = fl_gas_usd

                if fl_net > 0:
                    # Min notional to borrow so profit covers gas:
                    # notional × fl_net ≥ fl_gas_usd
                    min_notional = fl_gas_usd / fl_net
                    profit_at_10k = max(0.0, 10_000 * fl_net - fl_gas_usd)
                    profit_at_50k = max(0.0, 50_000 * fl_net - fl_gas_usd)
                else:
                    min_notional  = None
                    profit_at_10k = 0.0
                    profit_at_50k = 0.0

                flashloan_results[fl_name] = {
                    "fl_fee_pct":         round(fl_fee * 100, 3),
                    "fl_net_spread_pct":  round(fl_net * 100, 3),
                    "fl_gas_units":       fl_gas_units,
                    "fl_gas_usd":         round(fl_gas_usd, 2),
                    "capital_needed_usd": round(capital_needed_usd, 2),
                    "min_notional_usd":   round(min_notional, 0) if min_notional else None,
                    "profit_at_10k_usd":  round(profit_at_10k, 2),
                    "profit_at_50k_usd":  round(profit_at_50k, 2),
                    "viable":             fl_net > 0 and min_notional is not None,
                }

            # ── 3. Flashloan + Flashbots ────────────────────────────────────
            # Submit as a private bundle → no frontrunning/sandwich risk
            # Cost: builder tip = FLASHBOTS_BUILDER_SHARE × gross profit
            # You keep: (1 - FLASHBOTS_BUILDER_SHARE) × gross profit - gas
            # Use Balancer (0% flashloan fee) as the cheapest option
            fl_fb_gas_units = gas_units_regular + FLASHLOAN_GAS_OVERHEAD["balancer"]
            fl_fb_gas_eth   = fl_fb_gas_units * gas_price_gwei * 1e-9
            fl_fb_gas_usd   = fl_fb_gas_eth * eth_price_usd

            # Searcher's share of net profit (after DEX fees, before builder tip)
            searcher_share  = 1.0 - FLASHBOTS_BUILDER_SHARE
            # On 1 unit notional: gross profit from arb = net_spread
            # Builder tip = net_spread × FLASHBOTS_BUILDER_SHARE (paid as priority fee)
            fb_searcher_net = net_spread * searcher_share

            if fb_searcher_net > 0:
                fb_min_notional   = fl_fb_gas_usd / fb_searcher_net
                fb_profit_at_50k  = max(0.0, 50_000  * fb_searcher_net - fl_fb_gas_usd)
                fb_profit_at_100k = max(0.0, 100_000 * fb_searcher_net - fl_fb_gas_usd)
                fb_viable = True
            else:
                fb_min_notional   = None
                fb_profit_at_50k  = 0.0
                fb_profit_at_100k = 0.0
                fb_viable         = False

            flashbots = {
                "builder_share_pct":    round(FLASHBOTS_BUILDER_SHARE * 100, 0),
                "searcher_share_pct":   round(searcher_share * 100, 0),
                "fb_net_spread_pct":    round(fb_searcher_net * 100, 3),
                "fl_gas_usd":           round(fl_fb_gas_usd, 2),
                "capital_needed_usd":   round(fl_fb_gas_usd, 2),  # only gas ETH
                "min_notional_usd":     round(fb_min_notional, 0) if fb_min_notional else None,
                "profit_at_50k_usd":    round(fb_profit_at_50k, 2),
                "profit_at_100k_usd":   round(fb_profit_at_100k, 2),
                "viable":               fb_viable,
            }

            # ── Summary ─────────────────────────────────────────────────────
            enriched.append({
                **opp,
                # generic
                "dex_fees_pct":        round(dex_fees_ratio * 100, 3),
                "gross_spread_pct":    round(gross_spread * 100, 3),
                "net_spread_pct":      round(net_spread * 100, 3),
                # regular
                "gas_units":           gas_units_regular,
                "gas_cost_usd":        round(gas_cost_usd, 2),
                "min_deposit_usd":     round(min_deposit_regular, 0) if min_deposit_regular else None,
                "profit_at_1k_usd":    round(max(0.0, 1_000  * net_spread - gas_cost_usd), 2),
                "profit_at_10k_usd":   round(max(0.0, 10_000 * net_spread - gas_cost_usd), 2),
                # flashloan strategies
                "flashloan":           flashloan_results,
                # flashbots
                "flashbots":           flashbots,
            })

        return enriched

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
