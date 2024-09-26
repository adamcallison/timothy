from unittest.mock import MagicMock

import pytest

from timothy._pipelineobject_impl import MemoryPipelineObject
from timothy.core._exceptions import CannotSaveObjectError, DuplicateObjectError
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet


class TestPipelineObject:
    @staticmethod
    def create_pipelineobject(name="name") -> PipelineObject:
        return MemoryPipelineObject(name)

    @pytest.mark.parametrize("name", ["iamaname", "iamanothername"])
    def test_name_is_correct(self, name):
        pipeline_object = self.create_pipelineobject(name=name)
        assert pipeline_object.name == name

    def test_save_and_load_work_together_correctly(self):
        obj = MagicMock()
        pipeline_object = self.create_pipelineobject()
        pipeline_object.save(obj)
        loaded_obj = pipeline_object.load()
        assert loaded_obj is obj


class TestPipelineObjectSet:
    def test_init_raises_if_duplicate_name(self):
        with pytest.raises(DuplicateObjectError):
            PipelineObjectSet(
                MemoryPipelineObject("a_name"),
                MemoryPipelineObject("a_name"),
            )

    def test_save_raises_if_mismatched_number_of_objects(self):
        pipeline_object_set = PipelineObjectSet(
            MemoryPipelineObject("a_name"),
            MemoryPipelineObject("another_name"),
        )
        with pytest.raises(CannotSaveObjectError):
            pipeline_object_set.save(["a_value", "another_value", "a_third_value"])
