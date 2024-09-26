from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from timothy.core._exceptions import (
    CannotSaveObjectError,
    DuplicateObjectError,
    MissingPipelineObjectError,
)
from timothy.core._pipelinecomponentset import PipelineComponent, PipelineComponentSet
from timothy.core._typedefs import Singleton

T = TypeVar("T")


class EmptyPipelineObjectType(Singleton):
    def __bool__(self) -> bool:
        return False

    def __eq__(self, _: object) -> bool:
        return False


class PipelineObject(Generic[T], PipelineComponent, ABC):
    _name: str

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def load(self) -> T | EmptyPipelineObjectType: ...
    @abstractmethod
    def save(self, obj: T) -> None: ...

    def configure(self, **kwargs: Any) -> None:
        del kwargs


EmptyPipelineObject = EmptyPipelineObjectType()


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

    def configure(self, **kwargs: Any) -> None:
        for pipeline_object in self.values():
            pipeline_object.configure(**kwargs)
