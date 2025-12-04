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

# edges should make their own names
# edges should not be able to have upstream and downstream node idsequal


# TypeVar for dict-like types, particularly TypedDict subclasses
# Using Mapping as the bound since TypedDict is compatible with Mapping
BaseVariablesT = TypeVar("BaseVariablesT", bound=Mapping[str, Any])
PulledVariablesT = TypeVar("PulledVariablesT", bound=Mapping[str, Any])
MetadataT = TypeVar("MetadataT", bound=Mapping[str, Any])
GlobalsT = TypeVar("GlobalsT", bound=Mapping[str, Any])


class Direction(StrEnum):
    from_upstream = "FROM_UPSTREAM"
    from_downstream = "FROM_DOWNSTREAM"


@runtime_checkable
class PullMethod(
    Protocol, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalsT]
):
    def __call__(
        self,
        direction: Direction,
        edge: "Edge",
        graph: "Graph[BaseVariablesT, PulledVariablesT, MetadataT, GlobalsT]",
    ) -> PulledVariablesT: ...


@runtime_checkable
class AggregationMethod(Protocol, Generic[BaseVariablesT, PulledVariablesT, MetadataT]):
    def __call__(
        self,
        node: "Node[BaseVariablesT, PulledVariablesT, MetadataT]",
        pulled_variables: list[PulledVariablesT],
    ) -> "Node[BaseVariablesT, PulledVariablesT, MetadataT]": ...


class HasIdAndName(Protocol):
    @property
    def id(self) -> int: ...

    @property
    def name(self) -> str: ...


def _validate_id_and_name_unique(value: list[HasIdAndName]) -> list[HasIdAndName]:

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


class Node(BaseModel, Generic[BaseVariablesT, PulledVariablesT, MetadataT]):
    id: int
    name: str
    base_variables: BaseVariablesT
    pulled_variables: PulledVariablesT
    metadata: MetadataT
    pull_from_downstream_agg_key: str
    pull_from_upstream_agg_key: str
    pulled_from_downstream: bool = False
    pulled_from_upstream: bool = False


class Edge(BaseModel):
    id: int
    name: str
    downstream_node_id: int
    upstream_node_id: int
    downstream_method_key: str
    upstream_method_key: str

    @model_validator(mode="after")
    def _no_dup_node_ids(self) -> Self:
        if self.downstream_node_id == self.upstream_node_id:
            raise ValueError(f"edge: {self.id, self.name} has a cyclical dependency.")
        return self


class Graph(BaseModel, Generic[BaseVariablesT, PulledVariablesT, MetadataT, GlobalsT]):

    model_config = {"arbitrary_types_allowed": True}

    nodes: Annotated[
        Sequence[Node[BaseVariablesT, PulledVariablesT, MetadataT]],
        AfterValidator(_validate_id_and_name_unique),
    ]
    edges: Annotated[
        Sequence[Edge],
        AfterValidator(_validate_id_and_name_unique),
        AfterValidator(_validate_no_duped_edges),
    ]
    globals: GlobalsT
    pull_methods: dict[
        str, PullMethod[BaseVariablesT, PulledVariablesT, MetadataT, GlobalsT]
    ] = Field(exclude=True)
    agg_methods: dict[
        str, AggregationMethod[BaseVariablesT, PulledVariablesT, MetadataT]
    ] = Field(exclude=True)

    @model_validator(mode="after")
    def _validate_pull_method_keys(self) -> Self:
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
        keys = self.agg_methods.keys()
        nodes_with_missing_downstream_keys: list[
            Node[BaseVariablesT, PulledVariablesT, MetadataT]
        ] = []
        for node in self.nodes:
            if node.pull_from_downstream_agg_key not in keys:
                nodes_with_missing_downstream_keys.append(node)

        nodes_with_missing_upstream_keys: list[
            Node[BaseVariablesT, PulledVariablesT, MetadataT]
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
        # Find all node IDs that appear in edges (either as upstream or downstream)
        nodes_with_edges = set()
        for edge in self.edges:
            nodes_with_edges.add(edge.upstream_node_id)
            nodes_with_edges.add(edge.downstream_node_id)

        # Find orphaned nodes (nodes with no edges)
        orphaned_nodes: list[Node[BaseVariablesT, PulledVariablesT, MetadataT]] = []
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

    @cached_property
    def node_ids(self) -> list[int]:
        return sorted([_.id for _ in self.nodes])

    @cached_property
    def edge_ids(self) -> list[int]:
        return sorted([_.id for _ in self.edges])

    @cached_property
    def nodes_as_dict(
        self,
    ) -> dict[int, Node[BaseVariablesT, PulledVariablesT, MetadataT]]:
        return {_.id: _ for _ in self.nodes}

    @cached_property
    def edges_as_dict(self) -> dict[int, Edge]:
        return {_.id: _ for _ in self.edges}

    @cached_property
    def root_nodes(self) -> list[Node[BaseVariablesT, PulledVariablesT, MetadataT]]:
        # find nodes with no edges pointing upstream (eg, they are never present in downstream_node_id)
        # these are the root nodes
        non_root_node_ids = {_.downstream_node_id for _ in self.edges}
        root_node_ids = set(self.node_ids) - non_root_node_ids
        return [_ for _ in self.nodes if _.id in root_node_ids]

    @cached_property
    def leaf_nodes(self) -> list[Node[BaseVariablesT, PulledVariablesT, MetadataT]]:
        # find nodes with no edges pointing downstream (eg, they are never present in upstream_node_id)
        # these are the leaf nodes
        non_leaf_node_ids = {_.upstream_node_id for _ in self.edges}
        leaf_node_ids = set(self.node_ids) - non_leaf_node_ids
        return [_ for _ in self.nodes if _.id in leaf_node_ids]
