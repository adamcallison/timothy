import pytest

from tests.stubs import StubPipelineStorage
from timothy._pipelinestagerunner_impl import DAGPipelineStageRunner
from timothy.core import (
    PipelineStage,
    PipelineStageSet,
)
from timothy.exceptions import CannotRunPipelineError


class TestDAGPipelineStageRunner:
    @pytest.fixture()
    def runner_input_and_call_list(
        self,
    ) -> tuple[
        StubPipelineStorage,
        PipelineStageSet,
        list[tuple[int, str]],
    ]:
        storage = StubPipelineStorage()
        storage.store_many(num1=123, str1="helloworld")

        calls: list[tuple[int, str]] = []

        def some_func(num1: int, str1: str) -> str:
            nonlocal calls
            calls.append((num1, str1))
            return f"called with {num1} and {str1}"

        stages = PipelineStageSet(PipelineStage(some_func, ["str2"]))

        return storage, stages, calls

    def test_call_raises_if_stage_dependency_cycle_detected(self):
        runner = DAGPipelineStageRunner()
        storage = StubPipelineStorage()

        def doubles_num1_to_get_num2(num1: int) -> int:
            return num1 * 2

        def halves_num2_to_get_num1(num2: int) -> int:
            return num2 // 2

        stages = PipelineStageSet(
            PipelineStage(doubles_num1_to_get_num2, ["num2"]),
            PipelineStage(halves_num2_to_get_num1, ["num1"]),
        )

        with pytest.raises(CannotRunPipelineError):
            runner(stages, storage)

    def test_call_stagerunner_calls_stages_with_correct_arguments(self, runner_input_and_call_list):
        runner = DAGPipelineStageRunner()
        storage, stages, calls = runner_input_and_call_list

        runner(stages, storage)

        assert calls == [(123, "helloworld")]

    def test_call_stagerunner_stores_return_value_in_storage(self, runner_input_and_call_list):
        runner = DAGPipelineStageRunner()
        storage, stages, _ = runner_input_and_call_list

        runner(stages, storage)

        assert storage.fetch_one("str2") == "called with 123 and helloworld"
