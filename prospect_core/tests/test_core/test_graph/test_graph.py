import pytest

from prospect_core.core.graph import Edge, Graph, Node

from .components import (
    BaseVariables,
    Globals,
    Metadata,
    PulledVariables,
)


@pytest.mark.parametrize(
    "nodes, edges, graph",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            Globals(global_var0=25),
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges", "graph"],
)
def test_can_make_graph(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    graph: Graph[BaseVariables, PulledVariables, Metadata, Globals],
) -> None:
    assert isinstance(graph, Graph)
