"""
DEX price fetcher for Uniswap V2, Uniswap V3, and SushiSwap.
Fetches on-chain prices via Web3 and saves snapshots as Parquet files.
Falls back to mock data when RPC is unavailable.
"""

import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def _load_abi(filename: str) -> List[Dict]:
    """Load an ABI JSON file from the abi/ subdirectory."""
    abi_dir = Path(__file__).parent / "abi"
    with open(abi_dir / filename) as f:
        return json.load(f)


# Known pair addresses for demo / fallback (mainnet)
KNOWN_PAIR_ADDRESSES: Dict[str, Dict[str, str]] = {
    "uniswap_v2": {
        "ETH/USDC": "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
        "ETH/DAI":  "0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11",
        "ETH/WBTC": "0xBb2b8038a1640196FbE3e38816F3e67Cba72D940",
        "USDC/DAI": "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5",
    },
    "sushiswap": {
        "ETH/USDC": "0x397FF1542f962076d0BFE58eA045FfA2d347ACa0",
        "ETH/DAI":  "0xC3D03e4F041Fd4cD388c549Ee2A29a9E5075882f",
        "ETH/WBTC": "0xCEfF51756c56CeFFCA006cD410B03FFC46dd3a58",
        "USDC/DAI": "0x23462C79086a78F87Da1e2d15F80A1D06D5a5462",
    },
    "uniswap_v3": {
        "ETH/USDC": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # 0.05% pool
        "ETH/DAI":  "0xC2e9F25Be6257c210d7Adf0D4Cd6E3E881ba25f8",  # 0.3% pool
        "ETH/WBTC": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
        "USDC/DAI": "0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168",
    },
}

# Realistic base prices (used for mock data generation)
BASE_PRICES: Dict[str, float] = {
    "ETH/USDC": 3500.0,
    "ETH/DAI":  3500.0,
    "ETH/WBTC": 0.0555,   # ~18 ETH per BTC
    "USDC/DAI": 1.0,
    "WBTC/USDC": 63000.0,
    "WBTC/DAI":  63000.0,
    "DAI/USDC":  1.0,
}

TOKEN_DECIMALS: Dict[str, int] = {
    "ETH":  18,
    "USDC": 6,
    "DAI":  18,
    "WBTC": 8,
}


class DEXFetcher:
    """
    Fetches current prices from major Ethereum DEXes.

    Parameters
    ----------
    config : dict
        Parsed YAML configuration (see config/settings.yaml).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.eth_config = config.get("ethereum", {})
        self.dex_config = config.get("dex", {})
        self.tokens: List[Dict[str, str]] = config.get("tokens", [])
        self.update_interval: int = config.get("arbitrage", {}).get(
            "update_interval_seconds", 30
        )
        self.snapshots_path = Path(
            config.get("spark", {}).get("snapshots_path", "data/snapshots")
        )
        self.snapshots_path.mkdir(parents=True, exist_ok=True)

        self._web3 = None
        self._pair_abi_v2 = _load_abi("uniswap_v2_pair.json")
        self._pool_abi_v3 = _load_abi("uniswap_v3_pool.json")
        self._erc20_abi = _load_abi("erc20.json")

        self._use_mock = False
        self._connect_web3()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_web3(self) -> None:
        """Try to connect to the Ethereum RPC endpoint."""
        try:
            from web3 import Web3

            rpc_url = os.environ.get("INFURA_RPC_URL") or self.eth_config.get(
                "rpc_url", ""
            )
            # Strip template placeholder if config was not expanded
            if "${" in rpc_url:
                rpc_url = os.environ.get("INFURA_RPC_URL", "")

            if not rpc_url:
                logger.warning("No RPC URL configured. Falling back to mock data.")
                self._use_mock = True
                return

            self._web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
            if not self._web3.is_connected():
                logger.warning("Web3 connection failed. Falling back to mock data.")
                self._use_mock = True
            else:
                logger.info("Connected to Ethereum node at %s", rpc_url)
        except ImportError:
            logger.warning("web3 package not installed. Falling back to mock data.")
            self._use_mock = True
        except Exception as exc:
            logger.warning("Could not connect to RPC (%s). Falling back to mock data.", exc)
            self._use_mock = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_all_prices(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch current prices from all enabled DEXes.

        Returns
        -------
        dict
            ``{token_pair: {dex: price}}``
            Example: ``{"ETH/USDC": {"uniswap_v2": 3498.12, "sushiswap": 3501.44}}``
        """
        if self._use_mock:
            return self._generate_mock_prices()

        prices: Dict[str, Dict[str, float]] = {}
        token_symbols = [t["symbol"] for t in self.tokens]

        for i, sym0 in enumerate(token_symbols):
            for sym1 in token_symbols[i + 1:]:
                pair_key = f"{sym0}/{sym1}"
                prices[pair_key] = {}

                if self.dex_config.get("uniswap_v2", {}).get("enabled"):
                    price = self.fetch_uniswap_v2_price(sym0, sym1)
                    if price is not None:
                        prices[pair_key]["uniswap_v2"] = price

                if self.dex_config.get("sushiswap", {}).get("enabled"):
                    price = self.fetch_sushiswap_price(sym0, sym1)
                    if price is not None:
                        prices[pair_key]["sushiswap"] = price

                if self.dex_config.get("uniswap_v3", {}).get("enabled"):
                    price = self.fetch_uniswap_v3_price(sym0, sym1)
                    if price is not None:
                        prices[pair_key]["uniswap_v3"] = price

        # Drop pairs with no data
        prices = {k: v for k, v in prices.items() if v}
        logger.info("Fetched prices for %d pairs", len(prices))
        return prices

    def fetch_uniswap_v2_price(self, token0: str, token1: str) -> Optional[float]:
        """
        Fetch price from Uniswap V2 pair contract.

        Parameters
        ----------
        token0 : str  Token symbol (e.g. "ETH")
        token1 : str  Token symbol (e.g. "USDC")

        Returns
        -------
        float or None
        """
        return self._fetch_v2_style_price(token0, token1, "uniswap_v2")

    def fetch_sushiswap_price(self, token0: str, token1: str) -> Optional[float]:
        """Fetch price from SushiSwap pair (same ABI as Uniswap V2)."""
        return self._fetch_v2_style_price(token0, token1, "sushiswap")

    def fetch_uniswap_v3_price(self, token0: str, token1: str) -> Optional[float]:
        """
        Fetch price from Uniswap V3 pool using sqrtPriceX96.

        Parameters
        ----------
        token0 : str  Token symbol
        token1 : str  Token symbol

        Returns
        -------
        float or None
        """
        if self._use_mock or self._web3 is None:
            return None

        pair_key = f"{token0}/{token1}"
        pair_addr = KNOWN_PAIR_ADDRESSES.get("uniswap_v3", {}).get(pair_key)
        if not pair_addr:
            logger.debug("No known V3 pool for %s", pair_key)
            return None

        try:
            pool = self._web3.eth.contract(
                address=self._web3.to_checksum_address(pair_addr),
                abi=self._pool_abi_v3,
            )
            slot0 = pool.functions.slot0().call()
            sqrt_price_x96: int = slot0[0]
            fee: int = pool.functions.fee().call()

            dec0 = TOKEN_DECIMALS.get(token0, 18)
            dec1 = TOKEN_DECIMALS.get(token1, 18)

            # price = (sqrtPriceX96 / 2^96)^2 * (10^dec0 / 10^dec1)
            price = (sqrt_price_x96 / (2**96)) ** 2 * (10**dec0 / 10**dec1)
            fee_factor = 1 - fee / 1_000_000
            adjusted_price = price * fee_factor

            logger.debug(
                "V3 %s price: %.6f (fee=%d)", pair_key, adjusted_price, fee
            )
            return adjusted_price
        except Exception as exc:
            logger.error("Error fetching V3 price for %s: %s", pair_key, exc)
            return None

    def save_snapshot(
        self, prices: Dict[str, Dict[str, float]], path: Optional[Path] = None
    ) -> Path:
        """
        Persist prices as a Parquet snapshot.

        Parameters
        ----------
        prices : dict   Output of ``fetch_all_prices()``.
        path : Path, optional   Override the default snapshots directory.

        Returns
        -------
        Path  Path to the saved file.
        """
        snap_dir = path or self.snapshots_path
        snap_dir = Path(snap_dir)
        snap_dir.mkdir(parents=True, exist_ok=True)

        records = []
        ts = datetime.now(timezone.utc)
        for pair, dex_prices in prices.items():
            for dex, price in dex_prices.items():
                records.append(
                    {
                        "timestamp": ts,
                        "pair": pair,
                        "dex": dex,
                        "price": price,
                    }
                )

        if not records:
            logger.warning("No price data to snapshot.")
            return snap_dir / "empty.parquet"

        df = pd.DataFrame(records)
        filename = snap_dir / f"snapshot_{ts.strftime('%Y%m%dT%H%M%S')}.parquet"
        df.to_parquet(filename, index=False)
        logger.info("Saved snapshot to %s (%d rows)", filename, len(df))
        return filename

    def run_loop(self) -> None:
        """Continuously fetch prices and save snapshots."""
        logger.info(
            "Starting price fetch loop (interval=%ds)", self.update_interval
        )
        while True:
            try:
                prices = self.fetch_all_prices()
                self.save_snapshot(prices)
            except KeyboardInterrupt:
                logger.info("Fetch loop stopped by user.")
                break
            except Exception as exc:
                logger.error("Error in fetch loop: %s", exc)
            time.sleep(self.update_interval)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_v2_style_price(
        self, token0: str, token1: str, dex: str
    ) -> Optional[float]:
        """Shared logic for Uniswap-V2-style pair contracts."""
        if self._use_mock or self._web3 is None:
            return None

        pair_key = f"{token0}/{token1}"
        pair_addr = KNOWN_PAIR_ADDRESSES.get(dex, {}).get(pair_key)
        if not pair_addr:
            logger.debug("No known %s pair for %s", dex, pair_key)
            return None

        try:
            pair = self._web3.eth.contract(
                address=self._web3.to_checksum_address(pair_addr),
                abi=self._pair_abi_v2,
            )
            reserves = pair.functions.getReserves().call()
            reserve0: int = reserves[0]
            reserve1: int = reserves[1]

            if reserve0 == 0 or reserve1 == 0:
                logger.debug("%s reserves are zero for %s", dex, pair_key)
                return None

            dec0 = TOKEN_DECIMALS.get(token0, 18)
            dec1 = TOKEN_DECIMALS.get(token1, 18)

            # Adjust for decimals
            adj_reserve0 = reserve0 / (10**dec0)
            adj_reserve1 = reserve1 / (10**dec1)
            price = adj_reserve1 / adj_reserve0

            logger.debug("%s %s price: %.6f", dex, pair_key, price)
            return price
        except Exception as exc:
            logger.error("Error fetching %s price for %s: %s", dex, pair_key, exc)
            return None

    def _generate_mock_prices(self) -> Dict[str, Dict[str, float]]:
        """
        Generate realistic mock prices with slight spreads between DEXes.
        Used when RPC is unavailable.
        """
        dexes = ["uniswap_v2", "uniswap_v3", "sushiswap"]
        prices: Dict[str, Dict[str, float]] = {}

        for pair, base_price in BASE_PRICES.items():
            prices[pair] = {}
            for dex in dexes:
                # Each DEX gets a slightly different price (up to ±0.5%)
                spread = random.uniform(-0.005, 0.005)
                prices[pair][dex] = round(base_price * (1 + spread), 6)

        logger.debug("Generated mock prices for %d pairs", len(prices))
        return prices


# ------------------------------------------------------------------
# Module-level entry point
# ------------------------------------------------------------------

def _load_config(path: str = "config/settings.yaml") -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = _load_config()
    fetcher = DEXFetcher(cfg)
    fetcher.run_loop()
