import pytest

from tests.stubs import StubComponent, StubComponentSet, StubMissingCompError


class TestPipelineComponentSet:
    def test_getitem_returns_component_if_given_string(self):
        fc = StubComponent("a_name")
        pipeline_component_set = StubComponentSet(fc)
        returned = pipeline_component_set["a_name"]
        assert returned is fc

    def test_getitem_returns_componentset_if_given_list(self):
        fc = StubComponent("a_name")
        pipeline_component_set = StubComponentSet(fc)
        returned = pipeline_component_set[["a_name"]]
        assert isinstance(returned, StubComponentSet)
        assert returned["a_name"] is fc

    @pytest.mark.parametrize(
        ("name_to_check", "expected_result"),
        [
            ("a_name", True),
            ("another_name", False),
        ],
    )
    def test_contains_comparison_is_correct(self, name_to_check, expected_result):
        pipeline_component_set = StubComponentSet(
            StubComponent("a_name"),
            StubComponent("a_different_name"),
        )
        assert (name_to_check in pipeline_component_set) == expected_result

    def test_getitem_raises_if_not_present(self):
        pipeline_object_set = StubComponentSet(StubComponent("a_name"))
        with pytest.raises(StubMissingCompError):
            pipeline_object_set["a_different_name"]

    def test_init_calls_validate(self):
        with pytest.raises(StubMissingCompError):
            StubComponentSet(StubComponent("should_raise"))
        StubComponentSet(StubComponent("should_not_raise"))

    def test_adding_two_sets_other_set_produces_new_set_with_correct_components(self):
        obj1 = StubComponent("name1")
        obj2 = StubComponent("name2")
        pipeline_obj_set1 = StubComponentSet(obj1)
        pipeline_obj_set2 = StubComponentSet(obj2)

        pipeline_obj_set3 = pipeline_obj_set1 + pipeline_obj_set2

        assert pipeline_obj_set3 is not pipeline_obj_set1
        assert pipeline_obj_set3 is not pipeline_obj_set2
        assert list(pipeline_obj_set3.values()) == [obj1, obj2]

    def test_adding_set_and_object_produces_new_set_with_correct_components(self):
        obj1 = StubComponent("name1")
        obj2 = StubComponent("name2")
        pipeline_obj_set1 = StubComponentSet(obj1)

        pipeline_obj_set2 = pipeline_obj_set1 + obj2

        assert pipeline_obj_set2 is not pipeline_obj_set1
        assert list(pipeline_obj_set2.values()) == [obj1, obj2]

    def test_adding_set_and_component_sequence_produces_new_set_with_correct_components(self):
        obj1 = StubComponent("name1")
        obj2 = StubComponent("name2")
        pipeline_obj_set1 = StubComponentSet(obj1)

        pipeline_obj_set2 = pipeline_obj_set1 + [obj2]  # noqa: RUF005

        assert pipeline_obj_set2 is not pipeline_obj_set1
        assert list(pipeline_obj_set2.values()) == [obj1, obj2]

    def test_keys_produce_correct_names(self):
        fake_comp_set = StubComponentSet(StubComponent("foo"), StubComponent("bar"))
        assert tuple(fake_comp_set.keys()) == ("foo", "bar")

    def test_iterating_produces_correct_names(self):
        fake_comp_set = StubComponentSet(StubComponent("foo"), StubComponent("bar"))
        assert tuple(fake_comp_set) == ("foo", "bar")

    def test_items_produces_iterable_over_correct_tuples(self):
        fc_foo, fc_bar = StubComponent("foo"), StubComponent("bar")
        fake_comp_set = StubComponentSet(fc_foo, fc_bar)
        assert tuple(fake_comp_set.items()) == (("foo", fc_foo), ("bar", fc_bar))
