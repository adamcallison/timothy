from typing import ParamSpec, Protocol, TypeAlias, TypeVar

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)


Obj: TypeAlias = object


class StageFunction(Protocol[P, R_co]):
    __name__: str

    def __call__(*args: P.args, **kwargs: P.kwargs) -> R_co: ...
