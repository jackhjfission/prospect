from typing import Any, TypedDict

from pydantic import BaseModel, PrivateAttr

from prospect_core.core.graph import (
    AggregationMethod,
    Direction,
    Edge,
    Graph,
    Node,
    PullMethod,
)


class BaseVariables(TypedDict):
    base_var0: int
    base_var1: int


class PulledVariables(TypedDict):
    pulled_var0: int | None
    pulled_var1: int | None


class Metadata(TypedDict):
    metadata_var0: str


class Globals(TypedDict):
    global_var0: int


class SpyAggMethods(BaseModel):

    _call_logs: list[tuple[str, dict[str, Any]]] = PrivateAttr(default_factory=list)

    def pass_through(
        self,
        node: Node[BaseVariables, PulledVariables, Metadata],
        pulled_variables: list[PulledVariables],
    ) -> Node[BaseVariables, PulledVariables, Metadata]:

        self._call_logs.append(
            ("sum_var0", {"node": node, "pulled_variables": pulled_variables})
        )
        return node

    def as_dict(
        self,
    ) -> dict[str, AggregationMethod[BaseVariables, PulledVariables, Metadata]]:
        return {"pass_through": self.pass_through}


class SpyPullMethods(BaseModel):

    _call_logs: list[tuple[str, dict[str, Any]]] = PrivateAttr(default_factory=list)

    def get(
        self,
        direction: Direction,
        edge: Edge,
        graph: Graph[BaseVariables, PulledVariables, Metadata, Globals],
    ) -> PulledVariables:

        if direction == Direction.from_upstream:
            node_id = edge.upstream_node_id
        else:
            node_id = edge.downstream_node_id

        pulled_variables = graph.nodes_as_dict[node_id].pulled_variables
        return pulled_variables

    def as_dict(
        self,
    ) -> dict[str, PullMethod[BaseVariables, PulledVariables, Metadata, Globals]]:
        return {"get": self.get}


class SpyNode(Node[BaseVariables, PulledVariables, Metadata]):
    _call_log: list[tuple[str, dict[str, Any]]] = PrivateAttr(default_factory=list)
