import pytest

from prospect_core.core.graph import Edge

from ...utils import CanSpecGlobals, CanSpecListOfStr
from .components import GlobalVariables, SpyAggMethods, SpyNode, SpyPullMethods


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
            "base_var0": 20,
            "base_var1": 200,
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
def node02() -> SpyNode:
    return SpyNode(
        id=2,
        name="node02",
        base_variables={
            "base_var0": 30,
            "base_var1": 300,
        },
        pulled_variables={
            "pulled_var0": None,
            "pulled_var1": None,
        },
        metadata={"metadata_var0": "node02"},
        pull_from_downstream_agg_key="pass_through",
        pull_from_upstream_agg_key="pass_through",
    )


@pytest.fixture
def edge00_01() -> Edge:
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
def edge01_02() -> Edge:
    """Edge node00->node01"""
    return Edge(
        id=20,
        name="edge_node01->node02",
        upstream_node_id=1,
        downstream_node_id=2,
        downstream_method_key="get",
        upstream_method_key="get",
    )


@pytest.fixture
def edge00_02() -> Edge:
    """Edge node00->node02"""
    return Edge(
        id=30,
        name="edge_node00->node02",
        upstream_node_id=0,
        downstream_node_id=2,
        downstream_method_key="get",
        upstream_method_key="get",
    )


@pytest.fixture
def edge02_00() -> Edge:
    """Edge node00->node02"""
    return Edge(
        id=40,
        name="edge_node02->node00",
        upstream_node_id=2,
        downstream_node_id=0,
        downstream_method_key="get",
        upstream_method_key="get",
    )


@pytest.fixture
def nodes(
    node00: SpyNode,
    node01: SpyNode,
    node02: SpyNode,
    request: CanSpecListOfStr,
) -> list[SpyNode]:
    """Returns a filtered list of nodes."""
    return [_ for _ in [node00, node01, node02] if _.name in request.param]


@pytest.fixture
def edges(
    edge00_01: Edge,
    edge00_02: Edge,
    edge01_02: Edge,
    edge02_00: Edge,
    request: CanSpecListOfStr,
) -> list[Edge]:
    """Returns a filtered list of edges."""
    return [
        _
        for _ in [edge00_01, edge00_02, edge01_02, edge02_00]
        if _.name in request.param
    ]


@pytest.fixture
def global_variables(
    request: CanSpecGlobals,
) -> GlobalVariables:
    default = {"global_var0": 75}
    if hasattr(request, "param"):
        return GlobalVariables(**request.param)  # type: ignore
    return GlobalVariables(**default)  # type: ignore
