from unittest.mock import MagicMock

import pytest

from timothy._pipelineio_impl import MemoryPipelineIO
from timothy.core._exceptions import CannotSaveObjectError, DuplicateObjectError
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet


class TestPipelineObject:
    @staticmethod
    def create_pipelineobject(name="name") -> PipelineObject:
        pipeline_object: PipelineObject = PipelineObject(name, MemoryPipelineIO())
        return pipeline_object

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
                PipelineObject("a_name", MemoryPipelineIO()),
                PipelineObject("a_name", MemoryPipelineIO()),
            )

    def test_save_raises_if_mismatched_number_of_objects(self):
        pipeline_object_set = PipelineObjectSet(
            PipelineObject("a_name", MemoryPipelineIO()),
            PipelineObject("another_name", MemoryPipelineIO()),
        )
        with pytest.raises(CannotSaveObjectError):
            pipeline_object_set.save(["a_value", "another_value", "a_third_value"])
