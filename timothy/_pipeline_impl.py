from pathlib import Path
from typing import Any, TypeVar

from timothy._pipelineio_impl import JSONFilePipelineIO, MemoryPipelineIO
from timothy.core import PipelineBase, PipelineStage
from timothy.exceptions import MissingPipelineObjectError, PipelineConfigError

T = TypeVar("T")


class Pipeline(PipelineBase):
    def _register_stage_hook(self, stage: PipelineStage) -> None:
        self._check_names_are_registered_objects(list(stage.params))
        self._check_names_are_registered_objects(list(stage.returns))

    def _check_names_are_registered_objects(self, names: list[str]) -> None:
        self.objects[names]  # raises an error if missing


class MemoryPipeline(PipelineBase):
    def _register_stage_hook(self, stage: PipelineStage) -> None:
        for param_or_return in stage.params + stage.returns:
            if param_or_return in self._objs:
                continue
            self.register_object(param_or_return, MemoryPipelineIO())

    def set_initial_values(self, **kwargs: Any) -> None:  # noqa: ANN401
        for objname, objval in kwargs.items():
            try:
                self.objects[objname].save(objval)
            except MissingPipelineObjectError:
                self.register_object(objname, MemoryPipelineIO(objval))


class JSONPipeline(PipelineBase):
    location: Path | None = None
    input_names: list[str] | None = None

    def set_location(self, location: Path) -> None:
        self.location = location

    def set_initial_values(self, **kwargs: Any) -> None:  # noqa: ANN401
        if not self.location:
            msg = "Pipeline storage location not set."
            raise PipelineConfigError(msg)
        for objname, objval in kwargs.items():
            try:
                self.objects[objname].save(objval)
            except MissingPipelineObjectError:
                self.register_object(objname, JSONFilePipelineIO(self.location / f"{objname}.json"))

    def _register_stage_hook(self, stage: PipelineStage) -> None:
        if not self.location:
            msg = "Pipeline storage location not set."
            raise PipelineConfigError(msg)
        for param_or_return in stage.params + stage.returns:
            if param_or_return in self._objs:
                continue
            self.register_object(
                param_or_return,
                JSONFilePipelineIO(self.location / f"{param_or_return}.json"),
            )
