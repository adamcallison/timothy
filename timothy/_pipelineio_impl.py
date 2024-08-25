"""Implementations of PipelineIO."""

from typing import Generic, TypeVar

from timothy.core import EmptyPipelineIO, EmptyPipelineIOType

T = TypeVar("T")


class MemoryPipelineIO(Generic[T]):
    def __init__(
        self,
        initial_value: T | EmptyPipelineIOType = EmptyPipelineIO,
    ) -> None:
        self._storage: T | EmptyPipelineIOType = initial_value

    def load(self) -> T | EmptyPipelineIOType:
        return self._storage

    def save(self, obj: T) -> None:
        self._storage = obj
