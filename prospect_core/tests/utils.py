from typing import Any, Protocol


class CanSpecListOfStr(Protocol):
    @property
    def param(self) -> list[str]: ...


class CanSpecGlobals(Protocol):
    @property
    def param(self) -> dict[str, Any]: ...
