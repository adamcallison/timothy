from graphlib import CycleError, TopologicalSorter
from typing import cast

from timothy.core import PipelineStageSet, PipelineStorage
from timothy.exceptions import CannotRunPipelineError


class DAGPipelineStageRunner:
    def __call__(self, stages: PipelineStageSet, storage: PipelineStorage) -> None:
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
                return_objs = stage.call(storage.fetch_many(*stage.params))
                storage.store_many(**dict(zip(stage.returns, return_objs, strict=True)))
            dag.done(*stage_group_names)
