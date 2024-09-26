from typing import ParamSpec, TypeVar

from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner

T = TypeVar("T")
P = ParamSpec("P")


class PipelineCore:
    def __init__(self, stage_runner: PipelineStageRunner) -> None:
        self._objs = PipelineObjectSet()
        self._stages = PipelineStageSet()
        self._stage_runner = stage_runner

    @property
    def objects(self) -> PipelineObjectSet:
        return self._objs

    @property
    def stages(self) -> PipelineStageSet:
        return self._stages

    def add_object(self, obj: PipelineObject[T]) -> None:
        self._objs += obj

    def add_stage(self, stage: PipelineStage) -> None:
        self._stages += stage

    def run(self) -> None:
        self._stage_runner(self._stages, self._objs)
