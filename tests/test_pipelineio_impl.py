from unittest.mock import MagicMock

import pytest

from timothy._pipelineio_impl import JSONFilePipelineIO, MemoryPipelineIO
from timothy.core import EmptyPipelineIO


class TestMemoryPipelineIO:
    def test_save_and_load_work_together(self):
        obj = MagicMock()
        memory_pipeline_io = MemoryPipelineIO()

        memory_pipeline_io.save(obj)
        loaded_obj = memory_pipeline_io.load()

        assert loaded_obj is obj

    def test_load_returns_empty_pipeline_io_if_not_saved(self):
        memory_pipeline_io = MemoryPipelineIO()
        loaded_obj = memory_pipeline_io.load()
        assert loaded_obj is EmptyPipelineIO


class TestJSONFilePipelineIO:
    @pytest.mark.parametrize(
        "obj",
        [
            1,
            2.0,
            "three",
            [1, 2.0, "three", None],
            [1, [2.0, "three", None]],
            {"one": 1, "two": 2.0, "three": "three"},
            {"one_and_two": [1, 2.0], "three": "three"},
            None,
        ],
    )
    def test_save_and_load_work_together(self, tmp_path, obj):
        json_file_pipeline_io = JSONFilePipelineIO(tmp_path / "f.json")

        json_file_pipeline_io.save(obj)
        loaded_obj = json_file_pipeline_io.load()

        assert (loaded_obj is None) if obj is None else (loaded_obj == obj)

    def test_load_returns_empty_pipeline_io_if_not_saved(self, tmp_path):
        json_file_pipeline_io = JSONFilePipelineIO(tmp_path / "f.json")
        loaded_obj = json_file_pipeline_io.load()
        assert loaded_obj is EmptyPipelineIO
