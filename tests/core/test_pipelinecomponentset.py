from collections.abc import Sequence

import pytest

from timothy.core._pipelinecomponentset import PipelineComponent, PipelineComponentSet


class FakeMissingCompError(Exception): ...


class FakeComponent(PipelineComponent):
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class FakeCompSet(PipelineComponentSet[FakeComponent]):
    _missing_component_error = FakeMissingCompError
    validation_calls: list[Sequence[FakeComponent]]

    @staticmethod
    def _validate(*fake_comps: FakeComponent) -> None:
        if fake_comps[0].name == "should_raise":
            raise FakeMissingCompError


class TestPipelineComponentSet:
    def test_getitem_raises_if_not_present(self):
        pipeline_object_set = FakeCompSet(FakeComponent("a_name"))
        with pytest.raises(FakeMissingCompError):
            pipeline_object_set["a_different_name"]

    def test_init_calls_validate(self):
        with pytest.raises(FakeMissingCompError):
            FakeCompSet(FakeComponent("should_raise"))
        FakeCompSet(FakeComponent("should_not_raise"))

    def test_adding_two_sets_other_set_produces_new_set_with_correct_components(self):
        obj1 = FakeComponent("name1")
        obj2 = FakeComponent("name2")
        pipeline_obj_set1 = FakeCompSet(obj1)
        pipeline_obj_set2 = FakeCompSet(obj2)

        pipeline_obj_set3 = pipeline_obj_set1 + pipeline_obj_set2

        assert pipeline_obj_set3 is not pipeline_obj_set1
        assert pipeline_obj_set3 is not pipeline_obj_set2
        assert list(pipeline_obj_set3.values()) == [obj1, obj2]

    def test_adding_set_and_object_produces_new_set_with_correct_components(self):
        obj1 = FakeComponent("name1")
        obj2 = FakeComponent("name2")
        pipeline_obj_set1 = FakeCompSet(obj1)

        pipeline_obj_set2 = pipeline_obj_set1 + obj2

        assert pipeline_obj_set2 is not pipeline_obj_set1
        assert list(pipeline_obj_set2.values()) == [obj1, obj2]

    def test_adding_set_and_component_sequence_produces_new_set_with_correct_components(self):
        obj1 = FakeComponent("name1")
        obj2 = FakeComponent("name2")
        pipeline_obj_set1 = FakeCompSet(obj1)

        pipeline_obj_set2 = pipeline_obj_set1 + [obj2]  # noqa: RUF005

        assert pipeline_obj_set2 is not pipeline_obj_set1
        assert list(pipeline_obj_set2.values()) == [obj1, obj2]

    def test_keys_produce_correct_names(self):
        fake_comp_set = FakeCompSet(FakeComponent("foo"), FakeComponent("bar"))
        assert tuple(fake_comp_set.keys()) == ("foo", "bar")

    def test_iterating_produces_correct_names(self):
        fake_comp_set = FakeCompSet(FakeComponent("foo"), FakeComponent("bar"))
        assert tuple(fake_comp_set) == ("foo", "bar")
