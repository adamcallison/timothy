from pathlib import Path
from typing import ParamSpec, TypeVar

from timothy._pipelineobject_impl import JSONFilePipelineObject, MemoryPipelineObject
from timothy.core import Pipeline
from timothy.exceptions import PipelineConfigError

T = TypeVar("T")
P = ParamSpec("P")


class MemoryPipeline(Pipeline):
    def _object_factory(self, name: str) -> MemoryPipelineObject:
        return MemoryPipelineObject(name)


class JSONPipeline(Pipeline):
    _location: Path | None = None

    @property
    def location(self) -> Path:
        if self._location is None:
            msg = "Location not set."
            raise PipelineConfigError(msg)
        return self._location

    def set_location(self, location: Path) -> None:
        self._location = location
        self._core.objects.configure(location=location)

    def _object_factory(self, name: str) -> JSONFilePipelineObject:
        return JSONFilePipelineObject(name)
