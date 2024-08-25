from unittest.mock import MagicMock

from timothy._pipelineio_impl import MemoryPipelineIO
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
