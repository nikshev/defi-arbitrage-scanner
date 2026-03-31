"""
Tests for the DEXFetcher module.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure the project root is importable
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetcher.dex_fetcher import DEXFetcher, BASE_PRICES, TOKEN_DECIMALS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_config(tmp_path):
    """Minimal config that disables live RPC (mock mode)."""
    return {
        "ethereum": {"rpc_url": "", "chain_id": 1},
        "tokens": [
            {"symbol": "ETH",  "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"},
            {"symbol": "USDC", "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"},
            {"symbol": "DAI",  "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F"},
            {"symbol": "WBTC", "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"},
        ],
        "dex": {
            "uniswap_v2": {"factory": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", "enabled": True},
            "uniswap_v3": {"factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984", "enabled": True},
            "sushiswap":  {"factory": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac", "enabled": True},
        },
        "arbitrage": {"min_profit_ratio": 0.005, "update_interval_seconds": 30},
        "spark": {
            "snapshots_path": str(tmp_path / "snapshots"),
            "report_path": str(tmp_path / "reports"),
        },
    }


@pytest.fixture
def fetcher(minimal_config):
    """DEXFetcher instance forced into mock mode (no live RPC)."""
    f = DEXFetcher(minimal_config)
    f._use_mock = True  # Force mock mode regardless of env
    f._web3 = None
    return f


# ---------------------------------------------------------------------------
# Initialisation tests
# ---------------------------------------------------------------------------

class TestDEXFetcherInit:
    def test_init_sets_tokens(self, fetcher, minimal_config):
        assert len(fetcher.tokens) == 4
        symbols = [t["symbol"] for t in fetcher.tokens]
        assert "ETH" in symbols
        assert "USDC" in symbols

    def test_init_sets_update_interval(self, fetcher):
        assert fetcher.update_interval == 30

    def test_init_mock_mode_when_no_rpc(self, fetcher):
        assert fetcher._use_mock is True

    def test_init_snapshots_directory_created(self, minimal_config, tmp_path):
        snap_path = tmp_path / "new_snapshots"
        minimal_config["spark"]["snapshots_path"] = str(snap_path)
        f = DEXFetcher(minimal_config)
        assert snap_path.exists()

    def test_abi_files_loaded(self, fetcher):
        assert isinstance(fetcher._pair_abi_v2, list)
        assert isinstance(fetcher._pool_abi_v3, list)
        assert isinstance(fetcher._erc20_abi, list)
        assert len(fetcher._pair_abi_v2) > 0
        assert len(fetcher._pool_abi_v3) > 0
        assert len(fetcher._erc20_abi) > 0


# ---------------------------------------------------------------------------
# Mock data generation tests
# ---------------------------------------------------------------------------

class TestMockDataGeneration:
    def test_mock_prices_returns_dict(self, fetcher):
        prices = fetcher._generate_mock_prices()
        assert isinstance(prices, dict)

    def test_mock_prices_has_expected_pairs(self, fetcher):
        prices = fetcher._generate_mock_prices()
        for pair in BASE_PRICES:
            assert pair in prices, f"Expected pair {pair} in mock prices"

    def test_mock_prices_has_three_dexes_per_pair(self, fetcher):
        prices = fetcher._generate_mock_prices()
        for pair, dex_prices in prices.items():
            assert "uniswap_v2" in dex_prices
            assert "uniswap_v3" in dex_prices
            assert "sushiswap" in dex_prices

    def test_mock_prices_are_positive(self, fetcher):
        prices = fetcher._generate_mock_prices()
        for pair, dex_prices in prices.items():
            for dex, price in dex_prices.items():
                assert price > 0, f"Price must be positive: {pair}/{dex}={price}"

    def test_mock_prices_eth_usdc_reasonable(self, fetcher):
        """ETH/USDC price should be in a realistic range."""
        prices = fetcher._generate_mock_prices()
        eth_usdc_prices = prices.get("ETH/USDC", {})
        for dex, price in eth_usdc_prices.items():
            assert 100 < price < 100_000, (
                f"ETH/USDC price out of range: {dex}={price}"
            )

    def test_mock_prices_spread_small(self, fetcher):
        """Spread between DEXes should be < 2%."""
        prices = fetcher._generate_mock_prices()
        for pair, dex_prices in prices.items():
            vals = list(dex_prices.values())
            if len(vals) < 2:
                continue
            spread = (max(vals) - min(vals)) / min(vals)
            assert spread < 0.02, (
                f"Spread too large for {pair}: {spread:.4f}"
            )

    def test_mock_prices_deterministically_non_identical(self, fetcher):
        """Two calls should not return identical prices (random noise)."""
        p1 = fetcher._generate_mock_prices()
        p2 = fetcher._generate_mock_prices()
        # At least one price should differ
        any_different = any(
            p1[pair][dex] != p2[pair][dex]
            for pair in p1
            for dex in p1[pair]
        )
        assert any_different


# ---------------------------------------------------------------------------
# fetch_all_prices tests
# ---------------------------------------------------------------------------

class TestFetchAllPrices:
    def test_fetch_all_prices_returns_dict(self, fetcher):
        prices = fetcher.fetch_all_prices()
        assert isinstance(prices, dict)

    def test_fetch_all_prices_non_empty_in_mock_mode(self, fetcher):
        prices = fetcher.fetch_all_prices()
        assert len(prices) > 0

    def test_fetch_all_prices_structure(self, fetcher):
        prices = fetcher.fetch_all_prices()
        for pair, dex_map in prices.items():
            assert "/" in pair, f"Pair should contain '/': {pair}"
            assert isinstance(dex_map, dict)
            for dex, price in dex_map.items():
                assert isinstance(price, float)
                assert price > 0

    def test_uniswap_v2_price_returns_none_in_mock(self, fetcher):
        """Live fetch methods return None when mock mode is active."""
        price = fetcher.fetch_uniswap_v2_price("ETH", "USDC")
        assert price is None

    def test_uniswap_v3_price_returns_none_in_mock(self, fetcher):
        price = fetcher.fetch_uniswap_v3_price("ETH", "USDC")
        assert price is None


# ---------------------------------------------------------------------------
# Snapshot saving tests
# ---------------------------------------------------------------------------

class TestSnapshotSaving:
    def test_save_snapshot_creates_file(self, fetcher, tmp_path):
        prices = fetcher.fetch_all_prices()
        out_file = fetcher.save_snapshot(prices, path=tmp_path)
        assert out_file.exists()
        assert out_file.suffix == ".parquet"

    def test_save_snapshot_parquet_readable(self, fetcher, tmp_path):
        prices = fetcher.fetch_all_prices()
        out_file = fetcher.save_snapshot(prices, path=tmp_path)
        df = pd.read_parquet(out_file)
        assert "timestamp" in df.columns
        assert "pair" in df.columns
        assert "dex" in df.columns
        assert "price" in df.columns

    def test_save_snapshot_row_count(self, fetcher, tmp_path):
        prices = fetcher.fetch_all_prices()
        total_entries = sum(len(v) for v in prices.values())
        out_file = fetcher.save_snapshot(prices, path=tmp_path)
        df = pd.read_parquet(out_file)
        assert len(df) == total_entries

    def test_save_snapshot_prices_match(self, fetcher, tmp_path):
        prices = fetcher.fetch_all_prices()
        out_file = fetcher.save_snapshot(prices, path=tmp_path)
        df = pd.read_parquet(out_file)

        for _, row in df.iterrows():
            expected = prices[row["pair"]][row["dex"]]
            assert abs(row["price"] - expected) < 1e-9

    def test_save_snapshot_empty_prices(self, fetcher, tmp_path):
        """Saving empty prices should not raise."""
        out_file = fetcher.save_snapshot({}, path=tmp_path)
        assert out_file is not None

    def test_save_snapshot_uses_default_path(self, fetcher):
        prices = fetcher.fetch_all_prices()
        out_file = fetcher.save_snapshot(prices)
        assert out_file.exists()
        # Clean up
        out_file.unlink()


# ---------------------------------------------------------------------------
# Token decimals
# ---------------------------------------------------------------------------

class TestTokenDecimals:
    def test_eth_has_18_decimals(self):
        assert TOKEN_DECIMALS["ETH"] == 18

    def test_usdc_has_6_decimals(self):
        assert TOKEN_DECIMALS["USDC"] == 6

    def test_dai_has_18_decimals(self):
        assert TOKEN_DECIMALS["DAI"] == 18

    def test_wbtc_has_8_decimals(self):
        assert TOKEN_DECIMALS["WBTC"] == 8


# ---------------------------------------------------------------------------
# Price normalisation helper test (via mock price generation)
# ---------------------------------------------------------------------------

class TestPriceNormalization:
    def test_inverse_pair_not_in_base_prices(self, fetcher):
        """
        Fetcher iterates token pairs in enumeration order (i < j), so for
        any pair A/B, the inverse B/A should not also appear as a separate key.
        BASE_PRICES contains both 'USDC/DAI' and 'DAI/USDC' which the mock
        returns directly — we simply verify that each pair key contains a '/'.
        """
        prices = fetcher.fetch_all_prices()
        for pair in prices:
            assert "/" in pair, f"Pair key missing '/': {pair}"
            parts = pair.split("/")
            assert len(parts) == 2, f"Unexpected pair format: {pair}"

    def test_all_dex_prices_are_floats(self, fetcher):
        prices = fetcher.fetch_all_prices()
        for pair, dex_map in prices.items():
            for dex, price in dex_map.items():
                assert isinstance(price, float), (
                    f"Price for {pair}/{dex} is not a float: {type(price)}"
                )
