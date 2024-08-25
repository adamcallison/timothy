from graphlib import CycleError, TopologicalSorter
from typing import cast

from timothy.core import PipelineObjectSet, PipelineStageSet
from timothy.core._exceptions import CannotRunPipelineError


class DAGPipelineStageRunner:
    def __call__(self, stages: PipelineStageSet, objects: PipelineObjectSet) -> None:
        self._check_all_objects_are_used(stages, objects)

        dag: TopologicalSorter = TopologicalSorter()

        # TODO: some of the logic in these 2 loops can go into pipelinestageset
        rn_to_sn = {}
        for stage_name, stage in stages.items():
            for return_name in stage.returns:
                rn_to_sn[return_name] = stage_name

        for stage_name, stage in stages.items():
            depends_on = tuple(pred for pn in stage.params if (pred := rn_to_sn.get(pn)))
            dag.add(stage_name, *depends_on)

        try:
            dag.prepare()
        except CycleError as e:
            msg = "Cycle detected in pipeline stage dependency graph."
            raise CannotRunPipelineError(msg) from e

        while dag.is_active():
            stage_group_names = list(cast(tuple[str], dag.get_ready()))
            stage_group = stages[stage_group_names]
            for stage in stage_group.values():
                stage.call(objects[stage.params], objects[stage.returns])
            dag.done(*stage_group_names)

    @staticmethod
    def _check_all_objects_are_used(stages: PipelineStageSet, objects: PipelineObjectSet) -> None:
        # TODO: consider moving to a base class
        used_objects = set().union(
            *(set(stage.params + stage.returns) for stage in stages.values()),
        )
        unused_objects = tuple(set(objects) - used_objects)
        if unused_objects:
            msg = f"Pipeline object(s) {unused_objects} are not used as params or return values."
            raise CannotRunPipelineError(msg)
