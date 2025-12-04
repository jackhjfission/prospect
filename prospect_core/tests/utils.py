from typing import Any, Protocol


class CanSpecListOfStr(Protocol):
    @property
    def param(self) -> list[str]: ...


class CanSpecGlobals(Protocol):
    @property
    def param(self) -> dict[str, Any]: ...


def escape_braces(regex: str) -> str:
    return (
        regex.replace("(", r"\(")
        .replace(")", r"\)")
        .replace("[", r"\[")
        .replace("]", r"\]")
        .replace("{", r"\{")
        .replace("}", r"\}")
    )
