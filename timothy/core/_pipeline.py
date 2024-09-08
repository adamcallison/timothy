from collections.abc import Callable, Sequence
from typing import ParamSpec, TypeVar

from timothy.core._pipelineio import PipelineIO
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner

T = TypeVar("T")
P = ParamSpec("P")


class Pipeline:
    def __init__(self, name: str, stage_runner: PipelineStageRunner) -> None:
        self._name = name
        self._objs = PipelineObjectSet()
        self._stages = PipelineStageSet()
        self._stage_runner = stage_runner

    @property
    def name(self) -> str:
        return self._name

    @property
    def objects(self) -> PipelineObjectSet:
        return self._objs

    @property
    def stages(self) -> PipelineStageSet:
        return self._stages

    def register_object(self, name: str, io: PipelineIO[T]) -> None:
        self._objs += PipelineObject[T](name, io)

    def register(
        self,
        returns: Sequence[str],
        name: str | None = None,
        params: Sequence[str] | None = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        def dec(f: Callable[P, T]) -> Callable[P, T]:
            stage = PipelineStage(f, returns, name=name, params=params)
            self._check_names_are_registered_objects(list(stage.params))
            self._check_names_are_registered_objects(list(returns))
            self._stages += stage
            return f

        return dec

    def run(self) -> None:
        self._stage_runner(self._stages, self._objs)

    def _check_names_are_registered_objects(self, names: list[str]) -> None:
        self._objs[names]  # raises an error if missing
