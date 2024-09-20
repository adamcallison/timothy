import pytest

from timothy import DAGPipelineStageRunner, MemoryPipelineIO
from timothy._pipeline_impl import Pipeline
from timothy.exceptions import MissingPipelineObjectError


class TestPipeline:
    @pytest.mark.parametrize("skip_obj", ["hello", "world", "foo", "bar"])
    def test_register_raises_if_object_not_registered(self, skip_obj):
        pipeline = Pipeline("name", DAGPipelineStageRunner())
        for obj_name in ("hello", "world", "foo", "bar"):
            if obj_name == skip_obj:
                continue
            pipeline.register_object(obj_name, MemoryPipelineIO())

        with pytest.raises(MissingPipelineObjectError):

            @pipeline.register(returns=["foo", "bar"])
            def iamafunction(hello: str, world: int) -> tuple[list[str], dict[str, int]]:
                return [hello, hello], {"avalue": world, "anothervalue": world}
