from typing import Protocol

from timothy.core._pipelinestage import PipelineStageSet
from timothy.core._pipelinestorage import PipelineStorage


class PipelineStageRunner(Protocol):
    def __call__(self, stages: PipelineStageSet, storage: PipelineStorage) -> None: ...
