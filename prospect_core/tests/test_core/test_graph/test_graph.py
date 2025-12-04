from copy import deepcopy

import pytest

from prospect_core.core.graph import Edge, Graph, Node

from ...utils import escape_braces
from .components import (
    BaseVariables,
    Globals,
    Metadata,
    PulledVariables,
    SpyAggMethods,
    SpyPullMethods,
)


@pytest.mark.parametrize(
    "nodes, edges, globals",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            Globals(global_var0=25),
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges", "globals"],
)
def test_can_make_graph(
    graph: Graph[BaseVariables, PulledVariables, Metadata, Globals],
) -> None:
    """Test basic graph creation."""
    assert isinstance(graph, Graph)


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "name_or_id, expected_match",
    [
        pytest.param(
            "id",
            r"1 validation error for Graph\nnodes\n  Value error, Duplicated ids: {0: [(0, 'node00'), (0, 'node1000')]}",
            id="id",
        ),
        pytest.param(
            "name",
            r"1 validation error for Graph\nnodes\n  Value error, Duplicated names: {'node00': [(0, 'node00'), (1000, 'node00')]} ",
            id="name",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\nnodes\n  Value error, Duplicated ids: {0: [(0, 'node00'), (0, 'node00')]}.\nDuplicated names: {'node00': [(0, 'node00'), (0, 'node00')]}",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_nodes_id_name_not_unique(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    name_or_id: str,
    expected_match: str,
) -> None:
    """Test unique node id and name checking behaviour for nodes."""

    # add a node with a duplicated id, name or both
    duped_node = deepcopy(nodes[0])
    if name_or_id == "name":
        duped_node.id = 1000
    elif name_or_id == "id":
        duped_node.name = "node1000"
    nodes.append(duped_node)

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "name_or_id, expected_match",
    [
        pytest.param(
            "id",
            r"1 validation error for Graph\nedges\n  Value error, Duplicated ids: {10: [(10, 'edge_node00->node01'), (10, 'edge_node00->node02')]}",
            id="id",
        ),
        pytest.param(
            "name",
            r"1 validation error for Graph\nedges\n  Value error, Duplicated names: {'edge_node00->node01': [(10, 'edge_node00->node01'), (1000, 'edge_node00->node01')]}",
            id="name",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\nedges\n  Value error, Duplicated ids: {10: [(10, 'edge_node00->node01'), (10, 'edge_node00->node01')]}.\nDuplicated names: {'edge_node00->node01': [(10, 'edge_node00->node01'), (10, 'edge_node00->node01')]}.",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_edge_id_name_not_unique(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    name_or_id: str,
    expected_match: str,
) -> None:
    """Test unique node id and name checking behaviour for edges."""

    # add a edge with a duplicated id, name or both
    duped_edge = deepcopy(edges[0])
    if name_or_id == "name":
        duped_edge.id = 1000
    elif name_or_id == "id":
        duped_edge.name = "edge_node00->node02"
    edges.append(duped_edge)

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "direction, expected_match",
    [
        pytest.param(
            "same_direction",
            r"1 validation error for Graph\nedges\n  Value error, Duplicate edges in same direction: [(20, 'edge_node00->node01_dup')].",
            id="same_direction",
        ),
        pytest.param(
            "opposite_direction",
            r"1 validation error for Graph\nedges\n  Value error, Bidirectional edges found (violates acyclic property): [(20, 'edge_node01->node00')].",
            id="opposite_direction",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\nedges\n  Value error, Duplicate edges in same direction: [(20, 'edge_node00->node01_dup')].\nBidirectional edges found (violates acyclic property): [(30, 'edge_node01->node00')].",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_edge_duplicated(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    direction: str,
    expected_match: str,
) -> None:
    """Test unique edge checking behaviour."""

    # Add duplicate edge(s) based on direction parameter
    if direction == "same_direction":
        # Add another edge in the same direction (node00->node01)
        duped_edge = Edge(
            id=20,
            name="edge_node00->node01_dup",
            upstream_node_id=0,
            downstream_node_id=1,
            downstream_method_key="get",
            upstream_method_key="get",
        )
        edges.append(duped_edge)
    elif direction == "opposite_direction":
        # Add an edge in the opposite direction (node01->node00)
        opposite_edge = Edge(
            id=20,
            name="edge_node01->node00",
            upstream_node_id=1,
            downstream_node_id=0,
            downstream_method_key="get",
            upstream_method_key="get",
        )
        edges.append(opposite_edge)
    elif direction == "both":
        # Add both: a same direction duplicate and an opposite direction edge
        duped_edge = Edge(
            id=20,
            name="edge_node00->node01_dup",
            upstream_node_id=0,
            downstream_node_id=1,
            downstream_method_key="get",
            upstream_method_key="get",
        )
        opposite_edge = Edge(
            id=30,
            name="edge_node01->node00",
            upstream_node_id=1,
            downstream_node_id=0,
            downstream_method_key="get",
            upstream_method_key="get",
        )
        edges.append(duped_edge)
        edges.append(opposite_edge)

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "direction, expected_match",
    [
        pytest.param(
            "upstream",
            r"1 validation error for Graph\n  Value error, The following edges have missing upstream keys: [(10, 'edge_node00->node01', 'missing_upstream_method')].\nThe following edges have missing downstream keys: [].",
            id="upstream",
        ),
        pytest.param(
            "downstream",
            r"1 validation error for Graph\n  Value error, The following edges have missing upstream keys: [].\nThe following edges have missing downstream keys: [(10, 'edge_node00->node01', 'missing_downstream_method')].",
            id="downstream",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\n  Value error, The following edges have missing upstream keys: [(10, 'edge_node00->node01', 'missing_upstream_method')].\nThe following edges have missing downstream keys: [(10, 'edge_node00->node01', 'missing_downstream_method')].",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_missing_pull_method_key(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    direction: str,
    expected_match: str,
) -> None:
    """Test that Graph validates pull method keys exist in pull_methods dict."""

    # Modify edge to reference missing pull method key(s)
    if direction == "upstream":
        edges[0].upstream_method_key = "missing_upstream_method"
    elif direction == "downstream":
        edges[0].downstream_method_key = "missing_downstream_method"
    elif direction == "both":
        edges[0].upstream_method_key = "missing_upstream_method"
        edges[0].downstream_method_key = "missing_downstream_method"

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "direction, expected_match",
    [
        pytest.param(
            "downstream",
            r"1 validation error for Graph\n  Value error, The following nodes have missing downstream aggregation keys: [(0, 'node00', 'missing_downstream_agg')].\nThe following nodes have missing upstream aggregation keys: [].",
            id="downstream",
        ),
        pytest.param(
            "upstream",
            r"1 validation error for Graph\n  Value error, The following nodes have missing downstream aggregation keys: [].\nThe following nodes have missing upstream aggregation keys: [(0, 'node00', 'missing_upstream_agg')].",
            id="upstream",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\n  Value error, The following nodes have missing downstream aggregation keys: [(0, 'node00', 'missing_downstream_agg')].\nThe following nodes have missing upstream aggregation keys: [(0, 'node00', 'missing_upstream_agg')].",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_missing_agg_method_key(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    direction: str,
    expected_match: str,
) -> None:
    """Test that Graph validates aggregation method keys exist in agg_methods dict."""

    # Modify node to reference missing aggregation method key(s)
    if direction == "downstream":
        nodes[0].pull_from_downstream_agg_key = "missing_downstream_agg"
    elif direction == "upstream":
        nodes[0].pull_from_upstream_agg_key = "missing_upstream_agg"
    elif direction == "both":
        nodes[0].pull_from_downstream_agg_key = "missing_downstream_agg"
        nodes[0].pull_from_upstream_agg_key = "missing_upstream_agg"

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00", "node01"],
            ["edge_node00->node01"],
            id="basic_graph",
        )
    ],
    indirect=["nodes", "edges"],
)
@pytest.mark.parametrize(
    "direction, expected_match",
    [
        pytest.param(
            "upstream",
            r"1 validation error for Graph\n  Value error, The following edges have invalid upstream node IDs: [(10, 'edge_node00->node01', 999)].\nThe following edges have invalid downstream node IDs: [].",
            id="upstream",
        ),
        pytest.param(
            "downstream",
            r"1 validation error for Graph\n  Value error, The following edges have invalid upstream node IDs: [].\nThe following edges have invalid downstream node IDs: [(10, 'edge_node00->node01', 999)].",
            id="downstream",
        ),
        pytest.param(
            "both",
            r"1 validation error for Graph\n  Value error, The following edges have invalid upstream node IDs: [(10, 'edge_node00->node01', 998)].\nThe following edges have invalid downstream node IDs: [(10, 'edge_node00->node01', 999)].",
            id="both",
        ),
    ],
)
def test_Graph_raises_ValueError_if_missing_node(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    direction: str,
    expected_match: str,
) -> None:
    """Test that Graph validates edge node IDs reference existing nodes."""

    # Modify edge to reference non-existent node ID(s)
    if direction == "upstream":
        edges[0].upstream_node_id = 999
    elif direction == "downstream":
        edges[0].downstream_node_id = 999
    elif direction == "both":
        edges[0].upstream_node_id = 998
        edges[0].downstream_node_id = 999

    with pytest.raises(ValueError, match=escape_braces(expected_match)):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )


@pytest.mark.parametrize(
    "nodes, edges",
    [
        pytest.param(
            ["node00"],
            [],
            id="singe_node",
        )
    ],
    indirect=["nodes", "edges"],
)
def test_Graph_warns_if_orphan_node(
    nodes: list[Node[BaseVariables, PulledVariables, Metadata]],
    edges: list[Edge],
    globals: Globals,
    spy_agg_methods: SpyAggMethods,
    spy_pull_methods: SpyPullMethods,
    node01: Node[BaseVariables, PulledVariables, Metadata],
) -> None:
    """Test that Graph warns about orphaned nodes (nodes with no edges)."""

    # Test that a warning is raised with the expected message
    with pytest.warns(
        UserWarning,
        match=r"The following nodes have no edges \(orphaned nodes\): \[\(0, 'node00'\)\]",
    ):
        _ = Graph(
            nodes=nodes,
            edges=edges,
            globals=globals,
            pull_methods=spy_pull_methods.as_dict(),
            agg_methods=spy_agg_methods.as_dict(),
        )
