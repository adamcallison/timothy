from graphlib import CycleError, TopologicalSorter
from typing import cast

from timothy.core import PipelineStageSet, PipelineStorage
from timothy.exceptions import CannotRunPipelineError


class DAGPipelineStageRunner:
    def __call__(self, stages: PipelineStageSet, storage: PipelineStorage) -> None:
        dag: TopologicalSorter = TopologicalSorter()

        for stage in stages:
            depends_on = tuple(pred.name for pn in stage.params if (pred := stages.returns.get(pn)))
            dag.add(stage.name, *depends_on)

        try:
            dag.prepare()
        except CycleError as e:
            msg = "Cycle detected in pipeline stage dependency graph."
            raise CannotRunPipelineError(msg) from e

        while dag.is_active():
            stage_group_names = list(cast(tuple[str], dag.get_ready()))
            stage_group = stages[stage_group_names]
            for stage in stage_group:
                return_objs = stage.call(storage.fetch_many(*stage.params))
                storage.store_many(**dict(zip(stage.returns, return_objs, strict=True)))
            dag.done(*stage_group_names)
