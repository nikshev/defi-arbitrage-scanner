"""
JanusGraph schema definitions for the DeFi Arbitrage Scanner.

Vertices
--------
Token : symbol (str), address (str), decimals (int)

Edges
-----
Pool  : dex (str), price (float), reserve0 (float), reserve1 (float),
        fee (int), last_updated (str, ISO-8601)
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GraphSchema:
    """Creates and manages the JanusGraph schema via Gremlin."""

    # Vertex labels
    LABEL_TOKEN = "Token"

    # Edge labels
    LABEL_POOL = "Pool"

    # Token properties
    PROP_SYMBOL = "symbol"
    PROP_ADDRESS = "address"
    PROP_DECIMALS = "decimals"

    # Pool (edge) properties
    PROP_DEX = "dex"
    PROP_PRICE = "price"
    PROP_RESERVE0 = "reserve0"
    PROP_RESERVE1 = "reserve1"
    PROP_FEE = "fee"
    PROP_LAST_UPDATED = "last_updated"

    def create_schema(self, g: Any) -> None:
        """
        Initialise JanusGraph schema (management API).

        Parameters
        ----------
        g : GraphTraversalSource
            Active Gremlin traversal source connected to JanusGraph.
            The JanusGraph management API is accessed via the Gremlin
            server-side script execution channel.

        Notes
        -----
        This method uses JanusGraph's ``JanusGraphManagement`` API via
        Gremlin server-side scripts.  When running against a plain
        TinkerGraph (unit tests) the calls are silently skipped.
        """
        try:
            self._create_schema_janusgraph(g)
        except Exception as exc:
            logger.warning(
                "Could not apply JanusGraph management schema (%s). "
                "Proceeding without explicit schema — TinkerGraph or "
                "schema already exists.",
                exc,
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _create_schema_janusgraph(self, g: Any) -> None:
        """
        Execute JanusGraph management Groovy script via the traversal source.

        The script is submitted using the Gremlin client's ``submit`` method
        so that it runs server-side where JanusGraph classes are available.
        """
        schema_script = """
            mgmt = graph.openManagement()

            // --- Vertex labels ---
            if (!mgmt.containsVertexLabel('Token')) {
                mgmt.makeVertexLabel('Token').make()
            }

            // --- Property keys ---
            def makeOrGet = { name, type ->
                if (!mgmt.containsPropertyKey(name)) {
                    mgmt.makePropertyKey(name).dataType(type).make()
                } else {
                    mgmt.getPropertyKey(name)
                }
            }

            def symbolKey    = makeOrGet('symbol',       String.class)
            def addressKey   = makeOrGet('address',      String.class)
            def decimalsKey  = makeOrGet('decimals',     Integer.class)
            def dexKey       = makeOrGet('dex',          String.class)
            def priceKey     = makeOrGet('price',        Double.class)
            def reserve0Key  = makeOrGet('reserve0',     Double.class)
            def reserve1Key  = makeOrGet('reserve1',     Double.class)
            def feeKey       = makeOrGet('fee',          Integer.class)
            def updatedKey   = makeOrGet('last_updated', String.class)

            // --- Edge labels ---
            if (!mgmt.containsEdgeLabel('Pool')) {
                mgmt.makeEdgeLabel('Pool').multiplicity(MULTI).make()
            }

            // --- Indexes ---
            if (!mgmt.containsGraphIndex('byTokenSymbol')) {
                mgmt.buildIndex('byTokenSymbol', Vertex.class)
                    .addKey(symbolKey)
                    .indexOnly(mgmt.getVertexLabel('Token'))
                    .buildCompositeIndex()
            }
            if (!mgmt.containsGraphIndex('byTokenAddress')) {
                mgmt.buildIndex('byTokenAddress', Vertex.class)
                    .addKey(addressKey)
                    .unique()
                    .indexOnly(mgmt.getVertexLabel('Token'))
                    .buildCompositeIndex()
            }

            mgmt.commit()
            'Schema created'
        """
        # Access the underlying Gremlin client via the traversal source
        client = g.remote_connection._client  # type: ignore[attr-defined]
        result = client.submit(schema_script).all().result()
        logger.info("JanusGraph schema result: %s", result)
