import warnings
from enum import StrEnum
from functools import cached_property
from typing import (
    Annotated,
    Any,
    Generic,
    Mapping,
    Protocol,
    Self,
    Sequence,
    TypeVar,
    runtime_checkable,
)

from pydantic import AfterValidator, BaseModel, Field, model_validator

# TypeVar for dict-like types, particularly TypedDict subclasses
# Using Mapping as the bound since TypedDict is compatible with Mapping
BaseVariablesT = TypeVar("BaseVariablesT", bound=Mapping[str, Any])
PulledVariablesT = TypeVar("PulledVariablesT", bound=Mapping[str, Any])
MetadataT = TypeVar("MetadataT", bound=Mapping[str, Any])
GlobalVariablesT = TypeVar("GlobalVariablesT", bound=Mapping[str, Any])


class Direction(StrEnum):
    """Enum representing the direction of data flow in a graph edge."""

    from_upstream = "FROM_UPSTREAM"
    from_downstream = "FROM_DOWNSTREAM"


@runtime_checkable
class PullMethod(
    Protocol, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
):
    """Protocol for methods that pull variables from connected nodes via edges.

    A pull method retrieves data from a neighboring node (either upstream or downstream)
    through an edge connection, returning the pulled variables.
    """

    def __call__(
        self,
        direction: Direction,
        edge: "Edge",
        graph: "Graph[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]",
    ) -> PulledVariablesT: ...


@runtime_checkable
class AggregationMethod(
    Protocol, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
):
    """Protocol for methods that aggregate pulled variables into a node.

    An aggregation method takes a list of pulled variables from multiple edges
    and combines them to update the node's state.
    """

    def __call__(
        self,
        node: "Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]",
        pulled_variables: list[PulledVariablesT],
    ) -> "Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]": ...


class HasIdAndName(Protocol):
    """Protocol for objects that have both an id and name property."""

    @property
    def id(self) -> int: ...

    @property
    def name(self) -> str: ...


def _validate_id_and_name_unique(value: list[HasIdAndName]) -> list[HasIdAndName]:
    """Validate that all items in the list have unique IDs and names.

    Args:
        value: List of items that have id and name properties.

    Returns:
        The input list if validation passes.

    Raises:
        ValueError: If duplicate IDs or names are found.
    """
    id_v_dict: dict[int, list[tuple[int, str]]] = {_.id: [] for _ in value}
    for v in value:
        id_v_dict[v.id].append((v.id, v.name))
    dups_ids = {k: v for k, v in id_v_dict.items() if len(v) > 1}

    name_v_dict: dict[str, list[tuple[int, str]]] = {_.name: [] for _ in value}
    for v in value:
        name_v_dict[v.name].append((v.id, v.name))
    dups_names = {k: v for k, v in name_v_dict.items() if len(v) > 1}

    if dups_ids and not dups_names:
        raise ValueError(f"Duplicated ids: {dups_ids}")

    if dups_names and not dups_ids:
        raise ValueError(f"Duplicated names: {dups_names}")

    if dups_names and dups_ids:
        raise ValueError(
            f"Duplicated ids: {dups_ids}.\n" f"Duplicated names: {dups_names}.\n",
        )

    return value


def _validate_no_duped_edges(edges: list["Edge"]) -> list["Edge"]:
    """Validate that no duplicate edges exist and the graph is acyclic.

    Checks for:
    - Duplicate edges in the same direction (same upstream and downstream node pair)
    - Bidirectional edges that would create cycles

    Args:
        edges: List of edges to validate.

    Returns:
        The input list if validation passes.

    Raises:
        ValueError: If duplicate or bidirectional edges are found.
    """
    # Track edges by (upstream_node_id, downstream_node_id) tuple
    seen_edges: dict[tuple[int, int], list[tuple[int, str]]] = {}
    duplicate_same_direction = []
    bidirectional_edges = []

    for edge in edges:
        edge_tuple = (edge.upstream_node_id, edge.downstream_node_id)
        reverse_tuple = (edge.downstream_node_id, edge.upstream_node_id)

        # Check for duplicate in same direction
        if edge_tuple in seen_edges:
            duplicate_same_direction.append((edge.id, edge.name))
        else:
            seen_edges[edge_tuple] = []

        seen_edges[edge_tuple].append((edge.id, edge.name))

        # Check for bidirectional edge (reverse direction already exists)
        if reverse_tuple in seen_edges:
            bidirectional_edges.append((edge.id, edge.name))

    if duplicate_same_direction or bidirectional_edges:
        error_msg = ""
        if duplicate_same_direction:
            error_msg += (
                f"Duplicate edges in same direction: {duplicate_same_direction}.\n"
            )
        if bidirectional_edges:
            error_msg += f"Bidirectional edges found (violates acyclic property): {bidirectional_edges}.\n"
        raise ValueError(error_msg.strip())

    return edges


class Node(
    BaseModel, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
):
    """Represents a node in a directed acyclic graph.

    A node stores base variables, pulled variables from connected nodes,
    metadata, and configuration for aggregation methods.

    Attributes:
        id: Unique integer identifier for the node.
        name: Unique string name for the node.
        base_variables: The node's core data.
        pulled_variables: Variables pulled from connected nodes via edges.
        metadata: Additional metadata associated with the node.
        pull_from_downstream_agg_key: Key for the aggregation method when pulling from downstream.
        pull_from_upstream_agg_key: Key for the aggregation method when pulling from upstream.
        pulled_from_downstream: Flag indicating if data has been pulled from downstream nodes.
        pulled_from_upstream: Flag indicating if data has been pulled from upstream nodes.
    """

    id: int
    name: str
    base_variables: BaseVariablesT
    pulled_variables: PulledVariablesT
    metadata: MetadataT
    pull_from_downstream_agg_key: str
    pull_from_upstream_agg_key: str
    pulled_from_downstream: bool = False
    pulled_from_upstream: bool = False

    def discover_upstream(
        self,
        graph: "Graph[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]",
    ) -> list[int]:
        """Recursively discover all upstream nodes connected to this node.

        Traverses the graph recursively following edges where this node is downstream,
        collecting all ancestor node IDs in the dependency chain.

        Args:
            graph: The graph containing this node.

        Returns:
            Sorted list of unique node IDs for all upstream nodes.
        """
        my_edges = [_ for _ in graph.edges if _.downstream_node_id == self.id]
        my_upstream_nodes: list[int] = []
        for edge in my_edges:
            node = graph.nodes_as_dict[edge.upstream_node_id]
            output = node.discover_upstream(graph=graph)
            output.append(edge.upstream_node_id)
            my_upstream_nodes.extend(output)
        return sorted(set(my_upstream_nodes))

    def discover_downstream(
        self,
        graph: "Graph[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]",
    ) -> list[int]:
        """Recursively discover all downstream nodes connected to this node.

        Traverses the graph recursively following edges where this node is upstream,
        collecting all descendant node IDs in the dependency chain.

        Args:
            graph: The graph containing this node.

        Returns:
            Sorted list of unique node IDs for all downstream nodes.
        """
        my_edges = [_ for _ in graph.edges if _.upstream_node_id == self.id]
        my_downstream_nodes: list[int] = []
        for edge in my_edges:
            node = graph.nodes_as_dict[edge.downstream_node_id]
            output = node.discover_downstream(graph=graph)
            output.append(edge.downstream_node_id)
            my_downstream_nodes.extend(output)
        return sorted(set(my_downstream_nodes))


class Edge(BaseModel):
    """Represents a directed edge connecting two nodes in a graph.

    An edge defines the connection between an upstream node and a downstream node,
    along with the methods to use for pulling data in each direction.

    Attributes:
        id: Unique integer identifier for the edge.
        name: Unique string name for the edge.
        downstream_node_id: ID of the node receiving data through this edge.
        upstream_node_id: ID of the node providing data through this edge.
        downstream_method_key: Key for the pull method to use when pulling from downstream.
        upstream_method_key: Key for the pull method to use when pulling from upstream.
    """

    id: int
    name: str
    downstream_node_id: int
    upstream_node_id: int
    downstream_method_key: str
    upstream_method_key: str

    @model_validator(mode="after")
    def _no_dup_node_ids(self) -> Self:
        """Validate that upstream and downstream node IDs are different to prevent self-loops."""
        if self.downstream_node_id == self.upstream_node_id:
            raise ValueError(f"edge: {self.id, self.name} has a cyclical dependency.")
        return self


class Graph(
    BaseModel, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
):
    """Represents a directed acyclic graph (DAG) with nodes and edges.

    The Graph class manages a collection of nodes connected by edges, along with
    methods for pulling and aggregating data between connected nodes. It enforces
    several constraints:
    - Node and edge IDs and names must be unique
    - No duplicate or bidirectional edges (maintains acyclic property)
    - All referenced pull and aggregation method keys must exist
    - All edge node IDs must reference existing nodes

    Attributes:
        nodes: Sequence of Node objects in the graph.
        edges: Sequence of Edge objects connecting the nodes.
        global_variables: Global variables accessible to all nodes and edges.
        pull_methods: Dictionary mapping method keys to pull method implementations.
        agg_methods: Dictionary mapping method keys to aggregation method implementations.
    """

    model_config = {"arbitrary_types_allowed": True}

    nodes: Annotated[
        Sequence[Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]],
        AfterValidator(_validate_id_and_name_unique),
    ]
    edges: Annotated[
        Sequence[Edge],
        AfterValidator(_validate_id_and_name_unique),
        AfterValidator(_validate_no_duped_edges),
    ]
    global_variables: GlobalVariablesT
    pull_methods: dict[
        str, PullMethod[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
    ] = Field(exclude=True)
    agg_methods: dict[
        str,
        AggregationMethod[
            BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT
        ],
    ] = Field(exclude=True)

    @model_validator(mode="after")
    def _validate_pull_method_keys(self) -> Self:
        """Validate that all edge pull method keys exist in pull_methods dict."""
        keys = self.pull_methods.keys()
        edges_with_missing_upstream_keys: list[Edge] = []
        for edge in self.edges:
            if edge.upstream_method_key not in keys:
                edges_with_missing_upstream_keys.append(edge)

        edges_with_missing_downstream_keys: list[Edge] = []
        for edge in self.edges:
            if edge.downstream_method_key not in keys:
                edges_with_missing_downstream_keys.append(edge)

        if edges_with_missing_upstream_keys or edges_with_missing_downstream_keys:
            raise ValueError(
                "The following edges have missing upstream keys: "
                f"{[(edge.id, edge.name, edge.upstream_method_key) for edge in edges_with_missing_upstream_keys]}.\n"
                "The following edges have missing downstream keys: "
                f"{[(edge.id, edge.name, edge.downstream_method_key) for edge in edges_with_missing_downstream_keys]}.\n"
            )

        return self

    @model_validator(mode="after")
    def _validate_agg_method_keys(self) -> Self:
        """Validate that all node aggregation method keys exist in agg_methods dict."""
        keys = self.agg_methods.keys()
        nodes_with_missing_downstream_keys: list[
            Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
        ] = []
        for node in self.nodes:
            if node.pull_from_downstream_agg_key not in keys:
                nodes_with_missing_downstream_keys.append(node)

        nodes_with_missing_upstream_keys: list[
            Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
        ] = []
        for node in self.nodes:
            if node.pull_from_upstream_agg_key not in keys:
                nodes_with_missing_upstream_keys.append(node)

        if nodes_with_missing_downstream_keys or nodes_with_missing_upstream_keys:
            raise ValueError(
                "The following nodes have missing downstream aggregation keys: "
                f"{[(node.id, node.name, node.pull_from_downstream_agg_key) for node in nodes_with_missing_downstream_keys]}.\n"
                "The following nodes have missing upstream aggregation keys: "
                f"{[(node.id, node.name, node.pull_from_upstream_agg_key) for node in nodes_with_missing_upstream_keys]}.\n"
            )

        return self

    @model_validator(mode="after")
    def _validate_edge_node_ids(self) -> Self:
        """Validate that all edge node IDs reference existing nodes in the graph."""
        node_ids = {node.id for node in self.nodes}
        edges_with_invalid_upstream_node_ids: list[Edge] = []
        for edge in self.edges:
            if edge.upstream_node_id not in node_ids:
                edges_with_invalid_upstream_node_ids.append(edge)

        edges_with_invalid_downstream_node_ids: list[Edge] = []
        for edge in self.edges:
            if edge.downstream_node_id not in node_ids:
                edges_with_invalid_downstream_node_ids.append(edge)

        if (
            edges_with_invalid_upstream_node_ids
            or edges_with_invalid_downstream_node_ids
        ):
            raise ValueError(
                "The following edges have invalid upstream node IDs: "
                f"{[(edge.id, edge.name, edge.upstream_node_id) for edge in edges_with_invalid_upstream_node_ids]}.\n"
                "The following edges have invalid downstream node IDs: "
                f"{[(edge.id, edge.name, edge.downstream_node_id) for edge in edges_with_invalid_downstream_node_ids]}.\n"
            )

        return self

    @model_validator(mode="after")
    def _warn_orphaned_nodes(self) -> Self:
        """Warn about orphaned nodes (nodes with no edges connected to them)."""
        # Find all node IDs that appear in edges (either as upstream or downstream)
        nodes_with_edges = set()
        for edge in self.edges:
            nodes_with_edges.add(edge.upstream_node_id)
            nodes_with_edges.add(edge.downstream_node_id)

        # Find orphaned nodes (nodes with no edges)
        orphaned_nodes: list[
            Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]
        ] = []
        for node in self.nodes:
            if node.id not in nodes_with_edges:
                orphaned_nodes.append(node)

        if orphaned_nodes:
            warnings.warn(
                "The following nodes have no edges (orphaned nodes): "
                f"{[(node.id, node.name) for node in orphaned_nodes]}",
                UserWarning,
                stacklevel=2,
            )

        return self

    @model_validator(mode="after")
    def _validate_acyclic(self) -> Self:
        """Validate that the graph is acyclic by attempting to compute all node dependencies.

        Recursively computes upstream and downstream node IDs for all nodes. If cycles exist,
        this will trigger a RecursionError, which is caught and converted to a descriptive
        ValueError.

        Raises:
            ValueError: If cycles are detected in the graph structure.
        """
        try:
            _ = self.upstream_node_ids
        except RecursionError:
            raise ValueError(
                "Maximum recursion depth reached when identifying downstream nodes. "
                "This indicates that there are cyclic properties in your graph."
            )

        try:
            _ = self.downstream_node_ids
        except RecursionError:
            raise ValueError(
                "Maximum recursion depth reached when identifying upstream nodes. "
                "This indicates that there are cyclic properties in your graph."
            )

        return self

    @cached_property
    def node_ids(self) -> list[int]:
        """Return a sorted list of all node IDs in the graph."""
        return sorted([_.id for _ in self.nodes])

    @cached_property
    def edge_ids(self) -> list[int]:
        """Return a sorted list of all edge IDs in the graph."""
        return sorted([_.id for _ in self.edges])

    @cached_property
    def nodes_as_dict(
        self,
    ) -> dict[int, Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]]:
        """Return a dictionary mapping node IDs to Node objects."""
        return {_.id: _ for _ in self.nodes}

    @cached_property
    def edges_as_dict(self) -> dict[int, Edge]:
        """Return a dictionary mapping edge IDs to Edge objects."""
        return {_.id: _ for _ in self.edges}

    @cached_property
    def root_nodes(
        self,
    ) -> list[Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]]:
        """Return nodes with no incoming edges (never appear as downstream_node_id)."""
        # find nodes with no edges pointing upstream (eg, they are never present in downstream_node_id)
        # these are the root nodes
        non_root_node_ids = {_.downstream_node_id for _ in self.edges}
        root_node_ids = set(self.node_ids) - non_root_node_ids
        return [_ for _ in self.nodes if _.id in root_node_ids]

    @cached_property
    def leaf_nodes(
        self,
    ) -> list[Node[BaseVariablesT, PulledVariablesT, MetadataT, GlobalVariablesT]]:
        """Return nodes with no outgoing edges (never appear as upstream_node_id)."""
        # find nodes with no edges pointing downstream (eg, they are never present in upstream_node_id)
        # these are the leaf nodes
        non_leaf_node_ids = {_.upstream_node_id for _ in self.edges}
        leaf_node_ids = set(self.node_ids) - non_leaf_node_ids
        return [_ for _ in self.nodes if _.id in leaf_node_ids]

    @cached_property
    def upstream_node_ids(self) -> dict[int, list[int]]:
        """Return mapping of each node ID to all its upstream (ancestor) node IDs."""
        return {_.id: _.discover_upstream(graph=self) for _ in self.nodes}

    @cached_property
    def downstream_node_ids(self) -> dict[int, list[int]]:
        """Return mapping of each node ID to all its downstream (descendant) node IDs."""
        return {_.id: _.discover_downstream(graph=self) for _ in self.nodes}
