from copy import deepcopy
from pathlib import Path
from typing import ParamSpec, TypeVar

from timothy._pipelinestagerunner_impl import DAGPipelineStageRunner
from timothy._pipelinestorage_impl import JSONFilePipelineStorage, MemoryPipelineStorage
from timothy.core import Pipeline

T = TypeVar("T")
P = ParamSpec("P")
_Pipeline = TypeVar("_Pipeline", bound=Pipeline)


def memory_pipeline(pipeline: _Pipeline) -> _Pipeline:
    pipeline = deepcopy(pipeline)
    pipeline.storage = MemoryPipelineStorage()
    pipeline.stagerunner = DAGPipelineStageRunner()
    return pipeline


def json_pipeline(pipeline: _Pipeline, location: Path) -> _Pipeline:
    pipeline = deepcopy(pipeline)
    pipeline.storage = JSONFilePipelineStorage(location)
    pipeline.stagerunner = DAGPipelineStageRunner()
    return pipeline
