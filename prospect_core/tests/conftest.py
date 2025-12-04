from typing import Callable, Protocol

import pytest

from prospect_core.core.graph import Edge, Graph, Node

from .utils import CanSpecGlobals


class HasAsDict(Protocol):
    def as_dict(self) -> dict[str, Callable]: ...  # type: ignore[type-arg]


@pytest.fixture
def graph(
    spy_agg_methods: HasAsDict,
    spy_pull_methods: HasAsDict,
    nodes: list[Node],  # type: ignore[type-arg]
    edges: list[Edge],
    request: CanSpecGlobals,
) -> Graph:  # type: ignore[type-arg]

    return Graph(
        nodes=nodes,
        edges=edges,
        globals=request.param,
        pull_methods=spy_pull_methods.as_dict(),
        agg_methods=spy_agg_methods.as_dict(),
    )
