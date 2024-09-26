import pytest

from timothy.core._exceptions import (
    CannotCallStageError,
    DuplicateReturnError,
    DuplicateStageError,
    InvalidParamsError,
    InvalidResultsError,
    MissingPipelineStageError,
)
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet


def zero_func(foo, bar) -> tuple[int, str]:
    del foo
    del bar
    return 0, "zero"


class TestPipelineStage:
    def test_name_is_correct(self):
        zero_stage = PipelineStage(zero_func, returns=["baz", "bazstr"])
        assert zero_stage.name == zero_func.__name__

    def test_func_is_correct(self):
        zero_stage = PipelineStage(zero_func, returns=["baz", "bazstr"])
        assert zero_stage.func is zero_func

    @pytest.mark.parametrize(
        ("params_specified", "params_expected"),
        [
            (None, ["foo", "bar"]),
            (["foo", "bar"], ["foo", "bar"]),
            (["bar", "foo"], ["bar", "foo"]),
            (["foo", "baz"], ["foo", "baz"]),
            (["baz", "boz"], ["baz", "boz"]),
        ],
    )
    def test_params_are_correct(self, params_specified, params_expected):
        zero_stage = PipelineStage(
            zero_func,
            params=params_specified,
            returns=["baz", "bazstr"],
        )
        assert zero_stage.params == params_expected

    def test_returns_are_correct(self):
        zero_stage = PipelineStage(zero_func, returns=["baz", "bazstr"])
        assert zero_stage.returns == ["baz", "bazstr"]

    @pytest.mark.parametrize(
        "params_specified",
        [
            ["hello"],
            ["hello", "world", "helloworld"],
        ],
    )
    def test_init_raises_if_incorrect_number_of_params(self, params_specified):
        with pytest.raises(InvalidParamsError):
            PipelineStage(
                zero_func,
                params=params_specified,
                returns=["baz", "bazstr"],
            )

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

        return_objs = pipeline_stage.call(["param_a_v1", "param_b_v1"])
        assert return_objs == expected_call_return_value

        return_objs = pipeline_stage.call(["param_a_v2", "param_b_v2"])
        assert return_objs == expected_call_return_value

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
    def test_call_raises_if_number_of_returns_doesnt_match_declared(
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

        with pytest.raises(InvalidResultsError):
            pipeline_stage.call(["param_a", "param_b"])

    @pytest.mark.parametrize(
        "call_with",
        [
            (),
            ("param_a",),
            ("param_a", "param_b", "param_c"),
        ],
    )
    def test_call_raises_if_number_of_params_not_correct(self, call_with):
        def some_function(param_a, param_b) -> None:
            pass

        pipeline_stage = PipelineStage(some_function, returns=[])
        with pytest.raises(CannotCallStageError):
            pipeline_stage.call(call_with)


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

        with pytest.raises(DuplicateReturnError):
            PipelineStageSet(PipelineStage(foo, ["baz"]), PipelineStage(bar, ["baz"]))

    def test_getitem_returns_stage_if_given_string(self):
        def a_name() -> None:
            pass

        fc = PipelineStage(a_name, [])
        pipeline_stage_set = PipelineStageSet(fc)
        returned = pipeline_stage_set["a_name"]
        assert returned is fc

    def test_getitem_returns_componentset_if_given_list(self):
        def a_name() -> None:
            pass

        fc = PipelineStage(a_name, [])
        pipeline_stage_set = PipelineStageSet(fc)
        returned = pipeline_stage_set[["a_name"]]
        assert isinstance(returned, PipelineStageSet)
        assert returned["a_name"] is fc

    def test_getitem_raises_if_not_present(self):
        def a_name() -> None:
            pass

        pipeline_stage_set = PipelineStageSet(PipelineStage(a_name, []))
        with pytest.raises(MissingPipelineStageError):
            pipeline_stage_set["a_different_name"]

    def test_adding_set_and_object_produces_new_set_with_correct_components(self):
        def name1() -> None:
            pass

        def name2() -> None:
            pass

        stage1 = PipelineStage(name1, [])
        stage2 = PipelineStage(name2, [])
        pipeline_stage_set1 = PipelineStageSet(stage1)

        pipeline_stage_set2 = pipeline_stage_set1 + stage2

        assert pipeline_stage_set2 is not pipeline_stage_set1
        assert list(pipeline_stage_set2) == [stage1, stage2]

    def test_names_produce_correct_names(self):
        def foo() -> None:
            pass

        def bar() -> None:
            pass

        foostage = PipelineStage(foo, [])
        barstage = PipelineStage(bar, [])
        pipeline_stage_set = PipelineStageSet(foostage, barstage)

        assert tuple(pipeline_stage_set.names()) == ("foo", "bar")

    def test_instance_is_iterator_over_stages(self):
        def foo() -> None:
            pass

        def bar() -> None:
            pass

        foostage = PipelineStage(foo, [])
        barstage = PipelineStage(bar, [])
        pipeline_stage_set = PipelineStageSet(foostage, barstage)

        assert tuple(pipeline_stage_set) == (foostage, barstage)

    def test_returns_produces_correct_mapping(self):
        def foo() -> str:
            return "1"

        def bar() -> int:
            return 2

        foostage = PipelineStage(foo, ["one"])
        barstage = PipelineStage(bar, ["two"])
        pipeline_stage_set = PipelineStageSet(foostage, barstage)

        assert dict(**pipeline_stage_set.returns) == {"one": foostage, "two": barstage}

    def test_params_produces_correct_mapping(self):
        def foo(hello: str, world: int) -> None:
            del hello, world

        def bar(hello: str, there: int) -> None:
            del hello, there

        foostage = PipelineStage(foo, [])
        barstage = PipelineStage(bar, [])
        pipeline_stage_set = PipelineStageSet(foostage, barstage)

        assert {k: set(v) for k, v in pipeline_stage_set.params.items()} == {
            "hello": {foostage, barstage},
            "world": {foostage},
            "there": {barstage},
        }
