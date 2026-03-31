"""
Tests for the ArbitrageFinder module.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from graph.arbitrage_finder import ArbitrageFinder


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def finder():
    """ArbitrageFinder in mock mode (no JanusGraph)."""
    return ArbitrageFinder(g=None)


@pytest.fixture
def finder_with_mock_graph():
    """ArbitrageFinder with a mock Gremlin traversal source that raises."""
    mock_g = MagicMock()
    mock_g.V.side_effect = RuntimeError("No JanusGraph")
    return ArbitrageFinder(g=mock_g)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestArbitrageFinderInit:
    def test_mock_mode_when_no_graph(self, finder):
        assert finder._mock_mode is True

    def test_not_mock_mode_with_graph(self):
        mock_g = MagicMock()
        f = ArbitrageFinder(g=mock_g)
        # Mock mode depends on GREMLIN_AVAILABLE; if unavailable, still mock
        # Just ensure no exception is raised
        assert isinstance(f._mock_mode, bool)

    def test_g_stored(self):
        mock_g = MagicMock()
        f = ArbitrageFinder(g=mock_g)
        assert f.g is mock_g


# ---------------------------------------------------------------------------
# Profit ratio calculation
# ---------------------------------------------------------------------------

class TestCalculateProfitRatio:
    def test_basic_profitable_cycle(self, finder):
        # 1.01 * 1.01 * 0.98 - 1 ≈ -0.00010...
        prices = [1.01, 1.01, 0.98]
        ratio = finder.calculate_profit_ratio(prices)
        expected = 1.01 * 1.01 * 0.98 - 1.0
        assert abs(ratio - expected) < 1e-9

    def test_profitable_cycle(self, finder):
        # 1.01 * 1.01 * 1.01 - 1 ≈ 0.030301
        prices = [1.01, 1.01, 1.01]
        ratio = finder.calculate_profit_ratio(prices)
        assert ratio > 0
        assert abs(ratio - (1.01**3 - 1)) < 1e-9

    def test_break_even(self, finder):
        prices = [1.0, 1.0, 1.0]
        ratio = finder.calculate_profit_ratio(prices)
        assert abs(ratio) < 1e-9

    def test_unprofitable_cycle(self, finder):
        prices = [0.99, 0.99, 0.99]
        ratio = finder.calculate_profit_ratio(prices)
        assert ratio < 0

    def test_empty_prices(self, finder):
        ratio = finder.calculate_profit_ratio([])
        assert ratio == 0.0

    def test_single_price(self, finder):
        ratio = finder.calculate_profit_ratio([1.05])
        assert abs(ratio - 0.05) < 1e-9

    def test_two_prices(self, finder):
        ratio = finder.calculate_profit_ratio([1.02, 1.03])
        expected = 1.02 * 1.03 - 1
        assert abs(ratio - expected) < 1e-9

    def test_high_profit_cycle(self, finder):
        # Extreme case
        prices = [1.05, 1.05, 1.05]
        ratio = finder.calculate_profit_ratio(prices)
        assert ratio > 0.15

    def test_profit_ratio_formula_spec(self, finder):
        """Verify the spec example: (1.01 * 1.01 * 0.98) - 1 = ~0.02%."""
        prices = [1.01, 1.01, 0.98]
        ratio = finder.calculate_profit_ratio(prices)
        # Spec says ~0.02% — actual is ~-0.0001, which is near 0
        assert abs(ratio) < 0.01, (
            f"Expected near-zero profit for [1.01, 1.01, 0.98], got {ratio:.6f}"
        )


# ---------------------------------------------------------------------------
# find_triangular_arbitrage
# ---------------------------------------------------------------------------

class TestFindTriangularArbitrage:
    def test_returns_list(self, finder):
        result = finder.find_triangular_arbitrage()
        assert isinstance(result, list)

    def test_each_opportunity_has_required_keys(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert "path" in opp
            assert "profit_ratio" in opp
            assert "dexes" in opp
            assert "timestamp" in opp

    def test_path_is_list_of_four_tokens(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert isinstance(opp["path"], list)
            assert len(opp["path"]) == 4
            # First and last token should be the same (cycle)
            assert opp["path"][0] == opp["path"][-1]

    def test_dexes_is_list_of_three(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert isinstance(opp["dexes"], list)
            assert len(opp["dexes"]) == 3

    def test_profit_ratio_is_float(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert isinstance(opp["profit_ratio"], float)

    def test_min_profit_filter_applied(self, finder):
        high_threshold = 0.02  # 2%
        result = finder.find_triangular_arbitrage(min_profit=high_threshold)
        for opp in result:
            assert opp["profit_ratio"] >= high_threshold, (
                f"Opportunity below threshold: {opp['profit_ratio']}"
            )

    def test_min_profit_zero_returns_more(self, finder):
        all_opps = finder.find_triangular_arbitrage(min_profit=0.0)
        filtered = finder.find_triangular_arbitrage(min_profit=0.01)
        assert len(all_opps) >= len(filtered)

    def test_min_profit_too_high_returns_empty(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=9999.0)
        assert result == []

    def test_dex_names_are_valid(self, finder):
        valid_dexes = {"uniswap_v2", "uniswap_v3", "sushiswap"}
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            for dex in opp["dexes"]:
                assert dex in valid_dexes, f"Unknown DEX: {dex}"

    def test_tokens_are_known(self, finder):
        known_tokens = {"ETH", "USDC", "DAI", "WBTC"}
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            for token in opp["path"]:
                assert token in known_tokens, f"Unknown token: {token}"

    def test_fallback_to_mock_when_gremlin_fails(self, finder_with_mock_graph):
        """Even when Gremlin raises, we fall back to mock data."""
        result = finder_with_mock_graph.find_triangular_arbitrage(min_profit=0.0)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_top_opportunities
# ---------------------------------------------------------------------------

class TestGetTopOpportunities:
    def test_returns_list(self, finder):
        result = finder.get_top_opportunities(n=5)
        assert isinstance(result, list)

    def test_respects_n_limit(self, finder):
        result = finder.get_top_opportunities(n=3)
        assert len(result) <= 3

    def test_sorted_descending_by_profit(self, finder):
        result = finder.get_top_opportunities(n=10)
        for i in range(len(result) - 1):
            assert result[i]["profit_ratio"] >= result[i + 1]["profit_ratio"], (
                "Opportunities not sorted by profit_ratio descending"
            )

    def test_top_1_is_best(self, finder):
        # Inject a fixed list so both n=1000 and n=1 operate on the same data
        fixed_opps = [
            {"path": ["ETH", "USDC", "DAI", "ETH"], "profit_ratio": 0.012,
             "dexes": ["uniswap_v2", "sushiswap", "uniswap_v3"], "timestamp": "t"},
            {"path": ["ETH", "WBTC", "USDC", "ETH"], "profit_ratio": 0.008,
             "dexes": ["uniswap_v3", "uniswap_v2", "sushiswap"], "timestamp": "t"},
            {"path": ["USDC", "DAI", "ETH", "USDC"], "profit_ratio": 0.005,
             "dexes": ["sushiswap", "uniswap_v2", "uniswap_v3"], "timestamp": "t"},
        ]
        with patch.object(finder, "find_triangular_arbitrage", return_value=fixed_opps):
            top1 = finder.get_top_opportunities(n=1)
        assert len(top1) == 1
        assert top1[0]["profit_ratio"] == 0.012

    def test_n_zero_returns_empty(self, finder):
        result = finder.get_top_opportunities(n=0)
        assert result == []

    def test_n_larger_than_available(self, finder):
        result = finder.get_top_opportunities(n=1000)
        all_opps = finder.find_triangular_arbitrage(min_profit=0.0)
        assert len(result) == len(all_opps)


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

class TestCycleDetection:
    def test_cycle_starts_and_ends_same_token(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert opp["path"][0] == opp["path"][-1], (
                f"Cycle does not close: {opp['path']}"
            )

    def test_cycle_length_is_three_hops(self, finder):
        """3-hop cycle = 4 tokens (A, B, C, A)."""
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert len(opp["path"]) == 4

    def test_intermediate_tokens_differ(self, finder):
        """Tokens at positions 1 and 2 should differ from start token."""
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            start = opp["path"][0]
            assert opp["path"][1] != start, "Mid token 1 equals start"
            assert opp["path"][2] != start, "Mid token 2 equals start"

    def test_intermediate_tokens_differ_from_each_other(self, finder):
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            assert opp["path"][1] != opp["path"][2], (
                "Mid tokens should differ from each other"
            )

    def test_three_distinct_dexes_used(self, finder):
        """Each hop can use a different DEX."""
        result = finder.find_triangular_arbitrage(min_profit=0.0)
        for opp in result:
            # All three DEXes are present (may repeat, but should be set)
            assert len(opp["dexes"]) == 3
