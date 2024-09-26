"""Implementations of PipelineObject."""

import json
from contextlib import suppress
from pathlib import Path
from typing import Any, TypeVar

from timothy._utils.common import not_none
from timothy.core import EmptyPipelineObject, EmptyPipelineObjectType, PipelineObject
from timothy.exceptions import PipelineConfigError

T = TypeVar("T")


class MemoryPipelineObject(PipelineObject[T]):
    def __init__(
        self,
        name: str,
        initial_value: T | EmptyPipelineObjectType = EmptyPipelineObject,
    ) -> None:
        self._name = name
        self._storage: T | EmptyPipelineObjectType = initial_value

    def load(self) -> T | EmptyPipelineObjectType:
        return self._storage

    def save(self, obj: T) -> None:
        self._storage = obj


class JSONFilePipelineObject(PipelineObject[T]):
    def __init__(self, name: str, location: Path | None = None) -> None:
        self._name = name
        self._location_or_none = location

    @property
    def _path(self) -> Path:
        error = PipelineConfigError
        msg = f"A pipeline object of type {self.__class__.__name__} has no location set."
        return not_none(self._location_or_none, error=error, msg=msg) / f"{self.name}.json"

    def load(self) -> T | EmptyPipelineObjectType:
        if not (self._path.is_file()):
            return EmptyPipelineObject
        with self._path.open("r") as f:
            return json.load(f)

    def save(self, obj: T) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w") as f:
            return json.dump(obj, f, indent=4)

    def configure(self, **kwargs: Any) -> None:
        with suppress(KeyError):
            self._location_or_none = kwargs["location"]
