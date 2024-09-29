from pathlib import Path
from typing import ParamSpec, TypeVar

from timothy._pipelinestagerunner_impl import DAGPipelineStageRunner
from timothy._pipelinestorage_impl import JSONFilePipelineStorage, MemoryPipelineStorage
from timothy.core import Pipeline

T = TypeVar("T")
P = ParamSpec("P")


class MemoryPipeline(Pipeline[DAGPipelineStageRunner, MemoryPipelineStorage]):
    @staticmethod
    def init_stagerunner() -> DAGPipelineStageRunner:
        return DAGPipelineStageRunner()

    @staticmethod
    def init_storage() -> MemoryPipelineStorage:
        return MemoryPipelineStorage()


class JSONPipeline(Pipeline[DAGPipelineStageRunner, JSONFilePipelineStorage]):
    @staticmethod
    def init_stagerunner() -> DAGPipelineStageRunner:
        return DAGPipelineStageRunner()

    @staticmethod
    def init_storage() -> JSONFilePipelineStorage:
        return JSONFilePipelineStorage()

    def set_location(self, location: Path) -> None:
        self._storage.set_location(location)
