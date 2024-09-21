"""Core functionality."""

from timothy.core._pipeline import PipelineBase
from timothy.core._pipelineio import (
    EmptyPipelineIO,
    EmptyPipelineIOType,
    PipelineIO,
)
from timothy.core._pipelineobject import PipelineObject, PipelineObjectSet
from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner

__all__ = [
    "PipelineObject",  # TODO: can I avoid exposing this?
    "PipelineObjectSet",
    "PipelineStage",  # TODO: can I avoid exposing this?
    "PipelineStageSet",
    "PipelineBase",
    "PipelineIO",
    "EmptyPipelineIOType",
    "EmptyPipelineIO",
    "PipelineStageRunner",
]
