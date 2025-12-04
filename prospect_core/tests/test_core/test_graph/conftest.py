import pytest

from prospect_core.core.graph import Edge

from ...utils import CanSpecListOfStr
from .components import (
    SpyAggMethods,
    SpyNode,
    SpyPullMethods,
)


@pytest.fixture
def spy_agg_methods() -> SpyAggMethods:
    return SpyAggMethods()


@pytest.fixture
def spy_pull_methods() -> SpyPullMethods:
    return SpyPullMethods()


@pytest.fixture
def node00() -> SpyNode:
    return SpyNode(
        id=0,
        name="node00",
        base_variables={
            "base_var0": 10,
            "base_var1": 100,
        },
        pulled_variables={
            "pulled_var0": None,
            "pulled_var1": None,
        },
        metadata={"metadata_var0": "node00"},
        pull_from_downstream_agg_key="pass_through",
        pull_from_upstream_agg_key="pass_through",
    )


@pytest.fixture
def node01() -> SpyNode:
    return SpyNode(
        id=1,
        name="node01",
        base_variables={
            "base_var0": 10,
            "base_var1": 100,
        },
        pulled_variables={
            "pulled_var0": None,
            "pulled_var1": None,
        },
        metadata={"metadata_var0": "node01"},
        pull_from_downstream_agg_key="pass_through",
        pull_from_upstream_agg_key="pass_through",
    )


@pytest.fixture
def edge10() -> Edge:
    """Edge node00->node01"""
    return Edge(
        id=10,
        name="edge_node00->node01",
        upstream_node_id=0,
        downstream_node_id=1,
        downstream_method_key="get",
        upstream_method_key="get",
    )


@pytest.fixture
def edge20() -> Edge:
    """Edge node00->node01"""
    return Edge(
        id=20,
        name="edge_node01->node00",
        upstream_node_id=1,
        downstream_node_id=0,
        downstream_method_key="get",
        upstream_method_key="get",
    )


@pytest.fixture
def nodes(
    node00: SpyNode,
    node01: SpyNode,
    request: CanSpecListOfStr,
) -> list[SpyNode]:
    return [_ for _ in [node00, node01] if _.name in request.param]


@pytest.fixture
def edges(
    edge10: Edge,
    request: CanSpecListOfStr,
) -> list[Edge]:
    return [_ for _ in [edge10] if _.name in request.param]
