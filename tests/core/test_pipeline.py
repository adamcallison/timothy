import io

import pytest

from timothy import DAGPipelineStageRunner, MemoryPipelineIO
from timothy.core._pipeline import Pipeline


class TestPipeline:
    @pytest.mark.parametrize("name", ["iamaname", "iamanothername"])
    def test_name_is_correct(self, name):
        pipeline = Pipeline(name, DAGPipelineStageRunner())
        assert pipeline.name == name

    @pytest.mark.parametrize("object_name", ["iamaname", "iamanothername"])
    def test_registered_object_can_be_retrieved_by_name(self, object_name):
        pipeline = Pipeline("name", DAGPipelineStageRunner())
        pipeline.register_object(object_name, MemoryPipelineIO())
        pipeline_object = pipeline.objects[object_name]
        assert pipeline_object.name == object_name

    def test_registered_function_can_be_retrieved_by_name(self):
        pipeline = Pipeline("name", DAGPipelineStageRunner())
        for obj_name in ("hello", "world", "foo", "bar"):
            pipeline.register_object(obj_name, MemoryPipelineIO())

        @pipeline.register(returns=["foo", "bar"])
        def iamafunction(hello: str, world: int) -> tuple[list[str], dict[str, int]]:
            return [hello, hello], {"avalue": world, "anothervalue": world}

        stage = pipeline.stages["iamafunction"]

        assert stage.func is iamafunction
        assert stage.params == ["hello", "world"]
        assert stage.returns == ["foo", "bar"]

    def test_run_works_correctly(self):
        pipeline = Pipeline("test_pipeline", DAGPipelineStageRunner())
        pipeline.register_object("num1", MemoryPipelineIO(initial_value=5))
        pipeline.register_object("num2", MemoryPipelineIO(initial_value=7.3))
        pipeline.register_object("num3", MemoryPipelineIO())
        pipeline.register_object("num4", MemoryPipelineIO())
        pipeline.register_object("num5", MemoryPipelineIO())

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
