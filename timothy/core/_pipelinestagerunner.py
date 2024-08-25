from typing import Protocol

from timothy.core._pipelineobject import PipelineObjectSet
from timothy.core._pipelinestage import PipelineStageSet


class PipelineStageRunner(Protocol):
    def __call__(self, stages: PipelineStageSet, objects: PipelineObjectSet) -> None: ...
