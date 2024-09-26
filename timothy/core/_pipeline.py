from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any, ParamSpec, TypeVar

from timothy.core._exceptions import DuplicateObjectError
from timothy.core._pipelinecore import PipelineCore
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


class Pipeline(ABC):
    def __init__(self, name: str, stage_runner: PipelineStageRunner) -> None:
        self._name = name
        self._core = PipelineCore(stage_runner)

    @abstractmethod
    def _object_factory(self, name: str) -> PipelineObject: ...

    @property
    def name(self) -> str:
        return self._name

    @property
    def objects(self) -> PipelineObjectSet:
        return self._core.objects

    @property
    def stages(self) -> PipelineStageSet:
        return self._core.stages

    def register(
        self,
        returns: Sequence[str],
        name: str | None = None,
        params: Sequence[str] | None = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        def dec(f: Callable[P, T]) -> Callable[P, T]:
            stage = PipelineStage(f, returns, name=name, params=params)
            for param_or_return in stage.params + stage.returns:
                if param_or_return in self._core.objects:
                    continue
                obj = self._object_factory(param_or_return)
                self._core.add_object(obj)

            self._core.add_stage(stage)
            return f

        return dec

    def run(self) -> None:
        self._core.run()

    def set_values(self, **kwargs: Any) -> None:
        for objname, objval in kwargs.items():
            with suppress(DuplicateObjectError):
                obj = self._object_factory(objname)
                self._core.add_object(obj)
            self._core.objects[objname].save(objval)
