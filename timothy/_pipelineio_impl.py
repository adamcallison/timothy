"""Implementations of PipelineIO."""

import json
from pathlib import Path
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


class JSONFilePipelineIO(Generic[T]):
    def __init__(self, file_path: Path | str) -> None:
        self._path = Path(file_path)

    def load(self) -> T | EmptyPipelineIOType:
        if not (self._path.is_file()):
            return EmptyPipelineIO
        with self._path.open("r") as f:
            return json.load(f)

    def save(self, obj: T) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w") as f:
            return json.dump(obj, f, indent=4)
