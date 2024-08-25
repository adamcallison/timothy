import pytest

from timothy._pipelineio_impl import MemoryPipelineIO
from timothy.core._exceptions import (
    CannotCallStageError,
    DuplicateObjectError,
    DuplicateStageError,
    InvalidResultsError,
)
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet


class TestPipelineStage:
    @pytest.fixture(scope="module")
    def return_zero_func(self):
        def return_zero(foo, bar) -> tuple[int, str]:
            del foo
            del bar
            return 0, "zero"

        return return_zero

    @pytest.fixture(scope="module")
    def return_zero_stage(self, return_zero_func):
        return PipelineStage(return_zero_func, returns=["baz", "bazstr"])

    def test_name_is_correct(self, return_zero_stage):
        assert return_zero_stage.name == "return_zero"

    def test_func_is_correct(self, return_zero_stage, return_zero_func):
        assert return_zero_stage.func is return_zero_func

    def test_params_are_correct(self, return_zero_stage):
        assert return_zero_stage.params == ["foo", "bar"]

    def test_returns_are_correct(self, return_zero_stage):
        assert return_zero_stage.returns == ["baz", "bazstr"]

    @pytest.mark.parametrize(
        ("raw_return_value", "declared_returns", "expected_call_return_value"),
        [
            ((1, 2.0, "three"), ["int", "float", "str"], [1, 2.0, "three"]),
            (1, ["int"], [1]),
            (None, [], []),
            ((1, 2.0, "three"), ["tuple"], [(1, 2.0, "three")]),
            ([1, 2.0, "three"], ["list"], [[1, 2.0, "three"]]),
            (None, ["none"], [None]),
        ],
    )
    def test_call_calls_function_and_returns_correct_values(
        self,
        raw_return_value,
        declared_returns,
        expected_call_return_value,
    ):
        calls = []

        def some_function(param_a, param_b) -> tuple[int, float, str]:
            nonlocal calls
            nonlocal raw_return_value
            calls.append((param_a, param_b))
            return raw_return_value

        pipeline_stage = PipelineStage(some_function, returns=declared_returns)

        return_object_set = PipelineObjectSet(
            *(PipelineObject(r_name, MemoryPipelineIO()) for r_name in declared_returns),
        )

        param_object_set1 = PipelineObjectSet(
            PipelineObject("param_a", MemoryPipelineIO(initial_value="param_a_v1")),
            PipelineObject("param_b", MemoryPipelineIO(initial_value="param_b_v1")),
        )
        pipeline_stage.call(param_object_set1, return_object_set)
        assert return_object_set.load() == expected_call_return_value

        param_object_set2 = PipelineObjectSet(
            PipelineObject("param_a", MemoryPipelineIO(initial_value="param_a_v2")),
            PipelineObject("param_b", MemoryPipelineIO(initial_value="param_b_v2")),
        )
        pipeline_stage.call(param_object_set2, return_object_set)
        assert return_object_set.load() == expected_call_return_value

        assert calls == [("param_a_v1", "param_b_v1"), ("param_a_v2", "param_b_v2")]

    @pytest.mark.parametrize(
        ("raw_return_value", "declared_returns"),
        [
            ((1, 2.0, "three"), ["int", "float", "str", "fish"]),
            ((1, 2.0, "three"), ["int", "float"]),
            ((1, 2.0, "three"), []),
            (1, ["int", "fish"]),
            (1, []),
            (None, ["none", "fish"]),
            ([1, 2.0, "three"], ["list", "fish"]),
        ],
    )
    def test_call_raises_if_return_values_dont_mach_declared(
        self,
        raw_return_value,
        declared_returns,
    ):
        def some_function(param_a, param_b) -> tuple[int, float, str]:
            nonlocal raw_return_value
            del param_a
            del param_b
            return raw_return_value

        pipeline_stage = PipelineStage(some_function, returns=declared_returns)
        param_object_set = PipelineObjectSet(
            PipelineObject("param_a", MemoryPipelineIO(initial_value="param_a_val")),
            PipelineObject("param_b", MemoryPipelineIO(initial_value="param_b_val")),
        )
        return_object_set = PipelineObjectSet(
            *(PipelineObject(r_name, MemoryPipelineIO()) for r_name in declared_returns),
        )

        with pytest.raises(InvalidResultsError):
            pipeline_stage.call(param_object_set, return_object_set)

    def test_call_raises_if_a_pipeline_object_has_no_value(self):
        def does_nothing(num1: int) -> None:
            pass

        param_object_set = PipelineObjectSet(PipelineObject("num1", MemoryPipelineIO()))
        return_object_set = PipelineObjectSet()
        stage = PipelineStage(does_nothing, [])

        with pytest.raises(CannotCallStageError) as e:
            stage.call(param_object_set, return_object_set)

        assert str(e.value) == "Cannot call 'does_nothing' due to valueless params ('num1',)."

    @pytest.mark.parametrize(
        "call_with",
        [
            (),
            ("param_a",),
            ("param_c",),
            ("param_a", "param_c"),
            ("param_c", "param_d"),
            ("param_b", "param_a"),
            ("param_a", "param_b", "param_c"),
            ("param_c", "param_d", "param_e"),
        ],
    )
    def test_call_raises_if_params_not_correct(self, call_with):
        def some_function(param_a, param_b) -> None:
            pass

        pipeline_stage = PipelineStage(some_function, returns=[])
        params = PipelineObjectSet(*(PipelineObject(cw, MemoryPipelineIO()) for cw in call_with))
        returns = PipelineObjectSet()
        with pytest.raises(CannotCallStageError):
            pipeline_stage.call(params, returns)

    @pytest.mark.parametrize(
        "call_with",
        [
            (),
            ("return_a",),
            ("return_c",),
            ("return_a", "return_c"),
            ("return_c", "return_d"),
            ("return_b", "return_a"),
            ("return_a", "return_b", "return_c"),
            ("return_c", "return_d", "return_e"),
        ],
    )
    def test_call_raises_if_returns_not_correct(self, call_with):
        def some_function() -> tuple[str, str]:
            return "value_a", "value_b"

        pipeline_stage = PipelineStage(some_function, returns=["return_a", "return_b"])
        params = PipelineObjectSet()
        returns = PipelineObjectSet(*(PipelineObject(cw, MemoryPipelineIO()) for cw in call_with))
        with pytest.raises(CannotCallStageError):
            pipeline_stage.call(params, returns)


class TestPipelineStageSet:
    def test_init_raises_if_duplicate_name(self):
        def foo() -> None:
            return None

        with pytest.raises(DuplicateStageError):
            PipelineStageSet(PipelineStage(foo, []), PipelineStage(foo, []))

    def test_init_raises_if_return_object_already_returned_by_other_stage(self):
        def foo() -> str:
            return "hello"

        def bar() -> str:
            return "world"

        with pytest.raises(DuplicateObjectError):
            PipelineStageSet(PipelineStage(foo, ["baz"]), PipelineStage(bar, ["baz"]))
