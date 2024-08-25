from typing import ParamSpec, Protocol, TypeVar

from timothy.core._typedefs import Singleton

T = TypeVar("T")
P = ParamSpec("P")


class EmptyPipelineIOType(Singleton):
    def __bool__(self) -> bool:
        return False

    def __eq__(self, _: object) -> bool:
        return False


class PipelineIO(Protocol[T]):
    def load(self) -> T | EmptyPipelineIOType: ...
    def save(self, obj: T) -> None: ...


EmptyPipelineIO = EmptyPipelineIOType()
