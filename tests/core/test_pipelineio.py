from timothy.core._pipelineio import EmptyPipelineIO


def test_empty_pipeline_io_is_false():
    assert not EmptyPipelineIO


def test_empty_pipeline_io_is_not_equal_to_itself():
    assert EmptyPipelineIO != EmptyPipelineIO  # noqa: PLR0124
