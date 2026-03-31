"""
JanusGraph loader for the DeFi Arbitrage Scanner.

Manages token vertices and pool edges, with upsert semantics to avoid
duplicate nodes.  Connects via gremlinpython WebSocket.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    from gremlin_python.driver import serializer
    from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
    from gremlin_python.process.anonymous_traversal import traversal
    from gremlin_python.process.graph_traversal import GraphTraversalSource, __
    from gremlin_python.process.traversal import T

    GREMLIN_AVAILABLE = True
except ImportError:
    GREMLIN_AVAILABLE = False
    logger.warning("gremlinpython not installed. GraphLoader will use mock mode.")


class GraphLoader:
    """
    Loads DEX price data into JanusGraph as a token-exchange graph.

    Parameters
    ----------
    config : dict
        Parsed YAML configuration.  Uses the ``janusgraph`` section.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        jg_conf = config.get("janusgraph", {})
        self.host: str = jg_conf.get("host", "localhost")
        self.port: int = int(jg_conf.get("port", 8182))
        self.traversal_source: str = jg_conf.get("traversal_source", "g")

        self._connection: Optional[Any] = None
        self.g: Optional[Any] = None
        self._mock_mode = not GREMLIN_AVAILABLE

        # In-memory store for mock mode
        self._mock_tokens: Dict[str, Dict[str, Any]] = {}
        self._mock_pools: list = []

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """
        Open WebSocket connection to JanusGraph / Gremlin Server.

        Returns
        -------
        bool  True if connected, False if mock mode.
        """
        if self._mock_mode:
            logger.info("Running in mock mode (gremlinpython unavailable).")
            return False

        url = f"ws://{self.host}:{self.port}/gremlin"
        try:
            self._connection = DriverRemoteConnection(
                url,
                self.traversal_source,
                message_serializer=serializer.GraphSONSerializersV2d0(),
            )
            self.g = traversal().withRemote(self._connection)
            logger.info("Connected to JanusGraph at %s", url)
            return True
        except Exception as exc:
            logger.warning(
                "Cannot connect to JanusGraph (%s). Switching to mock mode.", exc
            )
            self._mock_mode = True
            return False

    def disconnect(self) -> None:
        """Close the Gremlin connection."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as exc:
                logger.debug("Error closing Gremlin connection: %s", exc)
            self._connection = None
            self.g = None
        logger.info("Disconnected from JanusGraph.")

    def get_connection_status(self) -> Dict[str, Any]:
        """Return current connection metadata."""
        return {
            "host": self.host,
            "port": self.port,
            "connected": self._connection is not None and not self._mock_mode,
            "mock_mode": self._mock_mode,
        }

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "GraphLoader":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.disconnect()

    # ------------------------------------------------------------------
    # Vertex / edge operations
    # ------------------------------------------------------------------

    def upsert_token(self, symbol: str, address: str, decimals: int = 18) -> Any:
        """
        Create or retrieve a Token vertex.

        Parameters
        ----------
        symbol : str    Token symbol (e.g. "ETH").
        address : str   Token contract address.
        decimals : int  Token decimals (default 18).

        Returns
        -------
        Vertex id (real graph) or dict (mock mode).
        """
        if self._mock_mode:
            if symbol not in self._mock_tokens:
                self._mock_tokens[symbol] = {
                    "symbol": symbol,
                    "address": address,
                    "decimals": decimals,
                    "id": len(self._mock_tokens),
                }
                logger.debug("Mock: upserted token %s", symbol)
            return self._mock_tokens[symbol]

        try:
            # Check if vertex already exists
            existing = (
                self.g.V()
                .hasLabel("Token")
                .has("symbol", symbol)
                .toList()
            )
            if existing:
                vertex = existing[0]
                # Update address in case it changed
                self.g.V(vertex).property("address", address).iterate()
                return vertex

            # Create new vertex
            vertex = (
                self.g.addV("Token")
                .property("symbol", symbol)
                .property("address", address)
                .property("decimals", decimals)
                .next()
            )
            logger.debug("Created Token vertex: %s", symbol)
            return vertex
        except Exception as exc:
            logger.error("Error upserting token %s: %s", symbol, exc)
            raise

    def upsert_pool(
        self,
        token0_symbol: str,
        token1_symbol: str,
        dex: str,
        price: float,
        reserve0: float = 0.0,
        reserve1: float = 0.0,
        fee: int = 0,
    ) -> Any:
        """
        Create or update a Pool edge between two Token vertices.

        Parameters
        ----------
        token0_symbol : str
        token1_symbol : str
        dex : str            DEX name (e.g. "uniswap_v2").
        price : float        Current price of token1 in terms of token0.
        reserve0 : float     Liquidity reserve for token0.
        reserve1 : float     Liquidity reserve for token1.
        fee : int            Pool fee in basis points.

        Returns
        -------
        Edge object or dict (mock mode).
        """
        now_str = datetime.now(timezone.utc).isoformat()

        if self._mock_mode:
            record = {
                "from": token0_symbol,
                "to": token1_symbol,
                "dex": dex,
                "price": price,
                "reserve0": reserve0,
                "reserve1": reserve1,
                "fee": fee,
                "last_updated": now_str,
            }
            # Replace existing record for same (from, to, dex) triple
            self._mock_pools = [
                p for p in self._mock_pools
                if not (
                    p["from"] == token0_symbol
                    and p["to"] == token1_symbol
                    and p["dex"] == dex
                )
            ]
            self._mock_pools.append(record)
            return record

        try:
            v0 = self.g.V().hasLabel("Token").has("symbol", token0_symbol).next()
            v1 = self.g.V().hasLabel("Token").has("symbol", token1_symbol).next()

            # Check for existing edge
            existing = (
                self.g.V(v0)
                .outE("Pool")
                .has("dex", dex)
                .where(__.inV().is_(v1))
                .toList()
            )

            if existing:
                edge = existing[0]
                (
                    self.g.E(edge)
                    .property("price", price)
                    .property("reserve0", reserve0)
                    .property("reserve1", reserve1)
                    .property("fee", fee)
                    .property("last_updated", now_str)
                    .iterate()
                )
                return edge

            # Create new edge
            edge = (
                self.g.V(v0)
                .addE("Pool")
                .to(v1)
                .property("dex", dex)
                .property("price", price)
                .property("reserve0", reserve0)
                .property("reserve1", reserve1)
                .property("fee", fee)
                .property("last_updated", now_str)
                .next()
            )
            logger.debug(
                "Created Pool edge: %s -[%s]-> %s @ %.6f",
                token0_symbol, dex, token1_symbol, price,
            )
            return edge
        except Exception as exc:
            logger.error(
                "Error upserting pool %s/%s on %s: %s",
                token0_symbol, token1_symbol, dex, exc,
            )
            raise

    def load_prices(self, prices_dict: Dict[str, Dict[str, float]]) -> int:
        """
        Bulk-load a prices dictionary into the graph.

        Parameters
        ----------
        prices_dict : dict
            Output of ``DEXFetcher.fetch_all_prices()``.

        Returns
        -------
        int  Number of edges upserted.
        """
        count = 0
        # Collect all unique token symbols
        symbols: set = set()
        for pair in prices_dict:
            t0, t1 = pair.split("/")
            symbols.update([t0, t1])

        # Ensure all token vertices exist
        for sym in symbols:
            self.upsert_token(sym, address=sym)  # address is mocked here

        # Upsert pool edges
        for pair, dex_prices in prices_dict.items():
            t0, t1 = pair.split("/")
            for dex, price in dex_prices.items():
                self.upsert_pool(t0, t1, dex, price)
                count += 1

        logger.info("Loaded %d pool edges into graph.", count)
        return count
