from timothy._pipeline_impl import json_pipeline, memory_pipeline
from timothy._pipelinestagerunner_impl import DAGPipelineStageRunner
from timothy._pipelinestorage_impl import JSONFilePipelineStorage, MemoryPipelineStorage
from timothy.core import Pipeline


def test_memory_pipeline_produces_pipeline_with_correct_attributes():
    pipeline = memory_pipeline(Pipeline("a_pipeline"))
    assert isinstance(pipeline.stagerunner, DAGPipelineStageRunner)
    assert isinstance(pipeline.storage, MemoryPipelineStorage)


def test_json_pipeline_produces_pipeline_with_correct_attributes(tmp_path):
    pipeline = json_pipeline(Pipeline("a_pipeline"), tmp_path)
    assert isinstance(pipeline.stagerunner, DAGPipelineStageRunner)
    assert isinstance(pipeline.storage, JSONFilePipelineStorage)
    assert pipeline.storage.location == tmp_path
