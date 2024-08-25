from collections import Counter
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from timothy.core._exceptions import (
    CannotSaveObjectError,
    DuplicateObjectError,
    MissingPipelineObjectError,
)
from timothy.core._pipelinecomponentset import PipelineComponent, PipelineComponentSet
from timothy.core._pipelineio import EmptyPipelineIOType, PipelineIO

T = TypeVar("T")


class PipelineObject(Generic[T], PipelineComponent):
    def __init__(self, name: str, io: PipelineIO[T]) -> None:
        self._io = io
        self._name = name
        self._has_obj = False
        self._obj: T | None = None

    @property
    def name(self) -> str:
        return self._name

    def load(self) -> T | EmptyPipelineIOType:
        return self._io.load()

    def save(self, obj: T) -> None:
        self._io.save(obj)


class PipelineObjectSet(PipelineComponentSet[PipelineObject]):
    _missing_component_error = MissingPipelineObjectError

    @staticmethod
    def _validate(*pipeline_objects: PipelineObject) -> None:
        name_counts = Counter(p.name for p in pipeline_objects)
        if duplicate_names := tuple(name for name, count in name_counts.items() if count > 1):
            msg = f"Pipeline object names {duplicate_names} appear more than once."
            raise DuplicateObjectError(msg)

    def load(self) -> list[Any]:
        return [p_obj.load() for p_obj in self.values()]

    def save(self, values: Sequence[Any]) -> None:
        if (num_values := len(values)) != (num_obj := len(self._components)):
            msg = f"Cannot save {num_values} value(s) to {num_obj} pipeline object(s)."
            raise CannotSaveObjectError(msg)
        for value, pipeline_object in zip(values, self.values(), strict=True):
            pipeline_object.save(value)
