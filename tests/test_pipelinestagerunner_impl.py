import pytest

from timothy import MemoryPipelineObject
from timothy._pipelinestagerunner_impl import DAGPipelineStageRunner
from timothy.core import (
    PipelineObjectSet,
    PipelineStage,
    PipelineStageSet,
)
from timothy.exceptions import CannotRunPipelineError


class TestDAGPipelineStageRunner:
    def test_call_raises_if_stage_dependency_cycle_detected(self):
        runner = DAGPipelineStageRunner()

        def doubles_num1_to_get_num2(num1: int) -> int:
            return num1 * 2

        def halves_num2_to_get_num1(num2: int) -> int:
            return num2 // 2

        stages = PipelineStageSet(
            PipelineStage(doubles_num1_to_get_num2, ["num2"]),
            PipelineStage(halves_num2_to_get_num1, ["num1"]),
        )
        objects = PipelineObjectSet(
            MemoryPipelineObject("num1"),
            MemoryPipelineObject("num2"),
        )

        with pytest.raises(CannotRunPipelineError):
            runner(stages, objects)

    def test_call_raises_if_registered_object_unused(self):
        runner = DAGPipelineStageRunner()

        def foo(num1: int, num2: int) -> int:
            return num1 + num2

        def bar(num2: int, num3: int) -> int:
            return num2 + num3

        stages = PipelineStageSet(PipelineStage(foo, ["num3"]), PipelineStage(bar, ["num4"]))
        objects = PipelineObjectSet(
            *(MemoryPipelineObject(f"num{j}") for j in range(1, 6)),
        )

        with pytest.raises(CannotRunPipelineError) as e:
            runner(stages, objects)

        assert str(e.value) == (
            "Pipeline object(s) ('num5',) are not used as params or return values."
        )
