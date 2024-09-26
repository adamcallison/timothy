from copy import deepcopy

import pytest

from tests.stubs import StubPipelineStageRunner, StubPipelineStorage
from timothy.core._pipeline import Pipeline
from timothy.exceptions import PipelineConfigError


def concrete_pipeline(pipeline: Pipeline) -> Pipeline:
    pipeline = deepcopy(pipeline)
    pipeline.storage = StubPipelineStorage()
    pipeline.stagerunner = StubPipelineStageRunner()
    return pipeline


class TestPipeline:
    @pytest.fixture()
    def ready_made_pipeline(self) -> Pipeline:
        p = concrete_pipeline(Pipeline("test_pipeline"))

        @p.register(returns=["num5"])
        def add_num3_and_num4(num3: int, num4: float) -> float:
            return num3 + num4

        @p.register(returns=["num4"])
        def cube_num2(num2: float) -> float:
            return num2**3

        @p.register(returns=["num3"])
        def square_num1(num1: int) -> int:
            return num1**2

        return p

    def test_function_can_be_registered_as_stage_and_retrieved(self):
        p = concrete_pipeline(Pipeline("test_pipeline"))

        @p.register(returns=[])
        def some_func() -> None: ...

        stage = p.stages[some_func.__name__]
        assert stage.name == some_func.__name__
        assert stage.func is some_func

    def test_pipeline_name_is_correct(self, ready_made_pipeline: Pipeline):
        assert ready_made_pipeline.name == "test_pipeline"

    def test_values_can_be_set_and_get(self, ready_made_pipeline: Pipeline):
        ready_made_pipeline.set_values(value1="hello", value2="world")
        values = ready_made_pipeline.get_values()
        assert values == {"value1": "hello", "value2": "world"}

    def test_run_runs_pipeline_and_stores_correct_values(self, ready_made_pipeline: Pipeline):
        ready_made_pipeline.set_values(num1=5, num2=7.3)
        ready_made_pipeline.run()
        values = ready_made_pipeline.get_values()
        assert values == {"num1": 5, "num2": 7.3, "num3": 25, "num4": 389.017, "num5": 414.017}

    def test_pipeline_can_be_instantiated_with_existing_stages(self, ready_made_pipeline: Pipeline):
        new_pipeline = concrete_pipeline(
            Pipeline("new_pipeline", stages=ready_made_pipeline.stages),
        )
        for stage in ready_made_pipeline.stages:
            assert new_pipeline.stages[stage.name].func is stage.func

    def test_accessing_storage_attribute_raises_if_storage_not_set(self):
        new_pipeline = Pipeline("new_pipeline")
        with pytest.raises(PipelineConfigError):
            new_pipeline.storage

    def test_accessing_stagerunner_attribute_raises_if_storage_not_set(self):
        new_pipeline = Pipeline("new_pipeline")
        with pytest.raises(PipelineConfigError):
            new_pipeline.stagerunner
