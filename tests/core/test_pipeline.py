import io
from typing import TypeVar

import pytest

from timothy import DAGPipelineStageRunner, MemoryPipelineObject
from timothy.core._pipeline import Pipeline
from timothy.core._pipelineobject import PipelineObject

T = TypeVar("T")


class ConcretePipeline(Pipeline):
    def _object_factory(self, name: str) -> PipelineObject:
        return MemoryPipelineObject(name)


class TestPipeline:
    @pytest.mark.parametrize("name", ["iamaname", "iamanothername"])
    def test_name_is_correct(self, name):
        pipeline = ConcretePipeline(name, DAGPipelineStageRunner())
        assert pipeline.name == name

    def test_registered_function_can_be_retrieved_by_name(self):
        pipeline = ConcretePipeline("name", DAGPipelineStageRunner())

        @pipeline.register(returns=["foo", "bar"])
        def iamafunction(hello: str, world: int) -> tuple[list[str], dict[str, int]]:
            return [hello, hello], {"avalue": world, "anothervalue": world}

        stage = pipeline.stages["iamafunction"]

        assert stage.func is iamafunction
        assert stage.params == ["hello", "world"]
        assert stage.returns == ["foo", "bar"]

    def test_register_respects_specified_name(self):
        pipeline = ConcretePipeline("name", DAGPipelineStageRunner())

        @pipeline.register(name="iamadifferentname", returns=[])
        def iamafunction() -> None: ...

        assert pipeline.stages["iamadifferentname"].name == "iamadifferentname"

    def test_register_respects_specified_params(self):
        pipeline = ConcretePipeline("name", DAGPipelineStageRunner())

        @pipeline.register(params=["iamavalue"], returns=[])
        def iamafunction(hello_world: int) -> None:
            del hello_world

        assert pipeline.stages["iamafunction"].params == ["iamavalue"]

    def test_run_works_correctly(self):
        pipeline = ConcretePipeline("test_pipeline", DAGPipelineStageRunner())
        pipeline.set_values(num1=5, num2=7.3)
        print_to = io.StringIO()

        @pipeline.register(returns=["num5"])
        def add_num3_and_num4(num3: int, num4: float) -> float:
            """Add num3 and num 4."""
            num5 = num3 + num4
            print(f"Added num3 ({num3}) and num4 ({num4}) to get num5 ({num5}).", file=print_to)
            return num5

        @pipeline.register(returns=["num4"])
        def cube_num2(num2: float) -> float:
            """Cube num2."""
            num4 = num2**3
            print(f"Cubed num2 ({num2}) to get num4 ({num4}).", file=print_to)
            return num4

        @pipeline.register(returns=["num3"])
        def square_num1(num1: int) -> int:
            """Square num1."""
            num3 = num1**2
            print(f"Squared num1 ({num1}) to get num3 ({num3}).", file=print_to)
            return num3

        pipeline.run()

        values_expected = {"num1": 5, "num2": 7.3, "num3": 25, "num4": 389.017, "num5": 414.017}
        first_prints_expected = [
            "Squared num1 (5) to get num3 (25).",
            "Cubed num2 (7.3) to get num4 (389.017).",
        ]
        second_prints_expected = [
            "Added num3 (25) and num4 (389.017) to get num5 (414.017).",
        ]
        values_actual = {k: v.load() for k, v in pipeline.objects.items()}
        prints_actual = print_to.getvalue().strip().split("\n")
        first_prints_actual = prints_actual[: len(first_prints_expected)]
        second_prints_actual = prints_actual[
            len(first_prints_expected) : len(first_prints_expected) + len(second_prints_expected)
        ]

        assert values_actual == values_expected
        assert sorted(first_prints_actual) == sorted(first_prints_expected)
        assert sorted(second_prints_actual) == sorted(second_prints_expected)
