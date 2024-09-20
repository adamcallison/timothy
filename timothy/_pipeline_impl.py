from typing import TypeVar

from timothy.core import PipelineBase, PipelineStage

T = TypeVar("T")


class Pipeline(PipelineBase):
    def _register_stage_hook(self, stage: PipelineStage) -> None:
        self._check_names_are_registered_objects(list(stage.params))
        self._check_names_are_registered_objects(list(stage.returns))

    def _check_names_are_registered_objects(self, names: list[str]) -> None:
        self._objs[names]  # raises an error if missing
