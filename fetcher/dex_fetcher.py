"""
DEX price fetcher for Uniswap V2/V3, SushiSwap, Curve, and Balancer V2.
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
        "ETH/USDC":  "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
        "ETH/DAI":   "0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11",
        "ETH/WBTC":  "0xBb2b8038a1640196FbE3e38816F3e67Cba72D940",
        "USDC/DAI":  "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5",
        "ETH/USDT":  "0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852",
        "ETH/LINK":  "0xa2107FA5B38d9bbd2C461D6EDf11B11A50F6b974",
        "ETH/UNI":   "0xd3d2E2692501A5c9Ca623199D38826e513033a17",
        "ETH/AAVE":  "0xDFC14d2Af169B0D36C4EFF567Ada9b2E0CAE044f",
        "ETH/MKR":   "0xC2aDdA861F89bBB333c90c492cB837741916A225",
        "USDC/USDT": "0x3041CbD36888bECc7bbCBc0045E3B1f144466f5f",
        "WBTC/USDC": "0x004375Dff511095CC5A197A54140a24eFEF3A416",
    },
    "sushiswap": {
        "ETH/USDC":  "0x397FF1542f962076d0BFE58eA045FfA2d347ACa0",
        "ETH/DAI":   "0xC3D03e4F041Fd4cD388c549Ee2A29a9E5075882f",
        "ETH/WBTC":  "0xCEfF51756c56CeFFCA006cD410B03FFC46dd3a58",
        "USDC/DAI":  "0xAaF5110db6e744ff70fB339DE037B990A20bdace",
        "ETH/USDT":  "0x06da0fd433C1A5d7a4faa01111c044910A184553",
        "ETH/UNI":   "0xDafd66636E2561b0284EDdE37e42d192F2844D40",
        "ETH/AAVE":  "0xD75EA151a61d06868E31F8988D28DFE5E9df57B4",
        "ETH/MKR":   "0xba13afEcda9beB75De5c56BbAF696b880a5A50dD",
    },
    "uniswap_v3": {
        "ETH/USDC":  "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # 0.05%
        "ETH/DAI":   "0xC2e9F25Be6257c210d7Adf0D4Cd6E3E881ba25f8",  # 0.3%
        "ETH/WBTC":  "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",   # 0.3%
        "USDC/DAI":  "0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168",   # 0.01%
        "ETH/USDT":  "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",   # 0.3%
        "ETH/LINK":  "0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8",   # 0.3%
        "ETH/UNI":   "0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801",  # 0.3%
        "ETH/AAVE":  "0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB",  # 0.3%
        "ETH/MKR":   "0xe8c6c9227491C0a8156A0106A0204d881BB7E531",   # 0.3%
        "ETH/CRV":   "0x919Fa96e88d67499339577Fa202345436bcDaf79",   # 0.3%
        "USDC/USDT": "0x3416cF6C708Da44DB2624D63ea0AAef7113527C6",   # 0.01%
        "WBTC/USDC": "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35",   # 0.3%
    },
}

# Curve pools: {pair: (pool_address, i_in, j_out, dec_in, dec_out)}
# 3pool coins: [0]=DAI(18), [1]=USDC(6), [2]=USDT(6)
CURVE_POOLS: Dict[str, tuple] = {
    "USDC/DAI":  ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 1, 0, 6,  18),
    "DAI/USDC":  ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 0, 1, 18, 6),
    "USDC/USDT": ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 1, 2, 6,  6),
    "USDT/USDC": ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 2, 1, 6,  6),
    "DAI/USDT":  ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 0, 2, 18, 6),
    "USDT/DAI":  ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", 2, 0, 6,  18),
}

# Balancer V2: {pair: (pool_id_hex, token0_addr, token1_addr)}
BALANCER_VAULT   = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
BALANCER_POOLS: Dict[str, str] = {
    # WBTC/WETH 50/50
    "ETH/WBTC": "0xa6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e",
    "WBTC/ETH": "0xa6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e",
}

# Realistic base prices (used for mock data generation)
BASE_PRICES: Dict[str, float] = {
    "ETH/USDC":  3500.0,
    "ETH/DAI":   3500.0,
    "ETH/USDT":  3500.0,
    "ETH/WBTC":  0.0555,    # ~18 ETH per BTC
    "ETH/LINK":  280.0,     # ~$280 LINK? No: LINK ~$13, ETH/LINK = 3500/13 ≈ 270
    "ETH/UNI":   530.0,     # ~$6.6 UNI → 3500/6.6 ≈ 530
    "ETH/AAVE":  17.5,      # ~$200 AAVE → 3500/200 ≈ 17.5
    "ETH/MKR":   1.4,       # ~$2500 MKR → 3500/2500 ≈ 1.4
    "ETH/CRV":   3500.0,    # ~$1 CRV → 3500/1 ≈ 3500
    "USDC/DAI":  1.0,
    "USDC/USDT": 1.0,
    "WBTC/USDC": 63000.0,
    "WBTC/DAI":  63000.0,
    "DAI/USDC":  1.0,
}

TOKEN_DECIMALS: Dict[str, int] = {
    "ETH":  18,
    "USDC": 6,
    "DAI":  18,
    "WBTC": 8,
    "USDT": 6,
    "LINK": 18,
    "UNI":  18,
    "AAVE": 18,
    "MKR":  18,
    "CRV":  18,
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
        self._pair_abi_v2      = _load_abi("uniswap_v2_pair.json")
        self._pool_abi_v3      = _load_abi("uniswap_v3_pool.json")
        self._erc20_abi        = _load_abi("erc20.json")
        self._curve_abi        = _load_abi("curve_pool.json")
        self._balancer_vault_abi = _load_abi("balancer_vault.json")
        self._balancer_pool_abi  = _load_abi("balancer_pool.json")

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

                if self.dex_config.get("curve", {}).get("enabled"):
                    price = self.fetch_curve_price(sym0, sym1)
                    if price is not None:
                        prices[pair_key]["curve"] = price

                if self.dex_config.get("balancer", {}).get("enabled"):
                    price = self.fetch_balancer_price(sym0, sym1)
                    if price is not None:
                        prices[pair_key]["balancer"] = price

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

            # Determine actual on-chain token order
            contract_token0 = pool.functions.token0().call().lower()
            expected_addr0 = self._get_token_address(token0)

            if contract_token0 == expected_addr0:
                dec0 = TOKEN_DECIMALS.get(token0, 18)
                dec1 = TOKEN_DECIMALS.get(token1, 18)
                # sqrtPriceX96 encodes sqrt(token1/token0) in raw units
                # price of token0 in token1 = (sqrtPriceX96/2^96)^2 * 10^dec0/10^dec1
                price = (sqrt_price_x96 / (2**96)) ** 2 * (10**dec0 / 10**dec1)
            else:
                # Tokens are swapped — invert the price
                dec0 = TOKEN_DECIMALS.get(token1, 18)
                dec1 = TOKEN_DECIMALS.get(token0, 18)
                price_raw = (sqrt_price_x96 / (2**96)) ** 2 * (10**dec0 / 10**dec1)
                price = 1.0 / price_raw if price_raw else 0.0

            fee_factor = 1 - fee / 1_000_000
            adjusted_price = price * fee_factor

            logger.debug(
                "V3 %s price: %.6f (fee=%d)", pair_key, adjusted_price, fee
            )
            return adjusted_price
        except Exception as exc:
            logger.error("Error fetching V3 price for %s: %s", pair_key, exc)
            return None

    def fetch_curve_price(self, token0: str, token1: str) -> Optional[float]:
        """
        Fetch price from a Curve StableSwap pool using get_dy.

        Parameters
        ----------
        token0 : str  Input token symbol
        token1 : str  Output token symbol

        Returns
        -------
        float or None  Price of token0 denominated in token1
        """
        if self._use_mock or self._web3 is None:
            return None

        pair_key = f"{token0}/{token1}"
        pool_info = CURVE_POOLS.get(pair_key)
        if not pool_info:
            logger.debug("No known Curve pool for %s", pair_key)
            return None

        pool_addr, i_in, j_out, dec_in, dec_out = pool_info
        try:
            pool = self._web3.eth.contract(
                address=self._web3.to_checksum_address(pool_addr),
                abi=self._curve_abi,
            )
            dx = 10 ** dec_in  # 1 unit of input token in raw decimals
            dy = pool.functions.get_dy(i_in, j_out, dx).call()
            price = dy / 10 ** dec_out
            logger.debug("Curve %s price: %.6f", pair_key, price)
            return price
        except Exception as exc:
            logger.error("Error fetching Curve price for %s: %s", pair_key, exc)
            return None

    def fetch_balancer_price(self, token0: str, token1: str) -> Optional[float]:
        """
        Fetch price from a Balancer V2 weighted pool.

        Uses the invariant formula:
            price(token0 in token1) = (balance1 / weight1) / (balance0 / weight0)
                                      * (10^dec0 / 10^dec1)

        Parameters
        ----------
        token0 : str  Input token symbol
        token1 : str  Output token symbol

        Returns
        -------
        float or None
        """
        if self._use_mock or self._web3 is None:
            return None

        pair_key = f"{token0}/{token1}"
        pool_id_hex = BALANCER_POOLS.get(pair_key)
        if not pool_id_hex:
            logger.debug("No known Balancer pool for %s", pair_key)
            return None

        try:
            pool_id_bytes = bytes.fromhex(pool_id_hex.lstrip("0x"))

            vault = self._web3.eth.contract(
                address=self._web3.to_checksum_address(BALANCER_VAULT),
                abi=self._balancer_vault_abi,
            )

            tokens, balances, _ = vault.functions.getPoolTokens(pool_id_bytes).call()
            pool_addr, _ = vault.functions.getPool(pool_id_bytes).call()

            pool = self._web3.eth.contract(
                address=self._web3.to_checksum_address(pool_addr),
                abi=self._balancer_pool_abi,
            )
            weights = pool.functions.getNormalizedWeights().call()

            # Map lowercase token address → (balance, weight)
            token_map: Dict[str, Tuple[int, int]] = {
                addr.lower(): (bal, w)
                for addr, bal, w in zip(tokens, balances, weights)
            }

            addr0 = self._get_token_address(token0)
            addr1 = self._get_token_address(token1)

            if addr0 not in token_map or addr1 not in token_map:
                logger.debug(
                    "Balancer pool does not contain %s or %s", token0, token1
                )
                return None

            bal0, w0 = token_map[addr0]
            bal1, w1 = token_map[addr1]

            dec0 = TOKEN_DECIMALS.get(token0, 18)
            dec1 = TOKEN_DECIMALS.get(token1, 18)

            # Normalized weights are 1e18-scaled; convert to float
            w0_f = w0 / 1e18
            w1_f = w1 / 1e18

            adj_bal0 = bal0 / 10 ** dec0
            adj_bal1 = bal1 / 10 ** dec1

            # Spot price: (Bi/Wi) / (Bo/Wo) — how many token1 per token0
            price = (adj_bal1 / w1_f) / (adj_bal0 / w0_f)
            logger.debug("Balancer %s price: %.6f", pair_key, price)
            return price
        except Exception as exc:
            logger.error("Error fetching Balancer price for %s: %s", pair_key, exc)
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

    def _get_token_address(self, symbol: str) -> str:
        """Return lowercase on-chain address for a token symbol."""
        # WETH is used for ETH on-chain
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        if symbol == "ETH":
            return weth
        for t in self.tokens:
            if t["symbol"] == symbol:
                return t["address"].lower()
        return ""

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

            # Determine actual on-chain token order to apply correct decimals.
            # Uniswap sorts pair tokens by address, so contract token0 may differ
            # from the function argument token0.
            contract_token0 = pair.functions.token0().call().lower()
            expected_addr0 = self._get_token_address(token0)

            if contract_token0 == expected_addr0:
                # Order matches: reserve0 → token0, reserve1 → token1
                dec0 = TOKEN_DECIMALS.get(token0, 18)
                dec1 = TOKEN_DECIMALS.get(token1, 18)
                price = (reserve1 / 10**dec1) / (reserve0 / 10**dec0)
            else:
                # Tokens are swapped on-chain: reserve0 → token1, reserve1 → token0
                dec0 = TOKEN_DECIMALS.get(token1, 18)
                dec1 = TOKEN_DECIMALS.get(token0, 18)
                price = (reserve0 / 10**dec0) / (reserve1 / 10**dec1)

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
        dexes = ["uniswap_v2", "uniswap_v3", "sushiswap", "curve", "balancer"]
        # Curve only supports stablecoin pairs; Balancer only ETH/WBTC pairs
        dex_pair_filter: Dict[str, Optional[set]] = {
            "uniswap_v2": None,   # all pairs
            "uniswap_v3": None,
            "sushiswap":  None,
            "curve":      {"USDC/DAI", "DAI/USDC", "USDC/USDT", "USDT/USDC", "DAI/USDT", "USDT/DAI"},
            "balancer":   {"ETH/WBTC", "WBTC/ETH"},
        }
        prices: Dict[str, Dict[str, float]] = {}

        for pair, base_price in BASE_PRICES.items():
            prices[pair] = {}
            for dex in dexes:
                allowed = dex_pair_filter[dex]
                if allowed is not None and pair not in allowed:
                    continue
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
