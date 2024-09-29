from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Generic, ParamSpec, TypeVar

from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner
from timothy.core._pipelinestorage import PipelineStorage
from timothy.core._typedefs import Obj

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")
_PipelineStageRunner = TypeVar("_PipelineStageRunner", bound=PipelineStageRunner)
_PipelineStorage = TypeVar("_PipelineStorage", bound=PipelineStorage)


class Pipeline(ABC, Generic[_PipelineStageRunner, _PipelineStorage]):
    def __init__(self, name: str, *, stages: PipelineStageSet | None = None) -> None:
        self._name = name
        self._stages = stages or PipelineStageSet()
        self._stagerunner = self.init_stagerunner()
        self._storage = self.init_storage()

    @staticmethod
    @abstractmethod
    def init_storage() -> _PipelineStorage: ...

    @staticmethod
    @abstractmethod
    def init_stagerunner() -> _PipelineStageRunner: ...

    @property
    def name(self) -> str:
        return self._name

    @property
    def stages(self) -> PipelineStageSet:
        return self._stages

    def add_stage(self, stage: PipelineStage) -> None:
        self._stages += stage

    def run(self) -> None:
        self._stagerunner(self._stages, self._storage)

    def register(
        self,
        returns: Sequence[str],
        name: str | None = None,
        params: Sequence[str] | None = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        def dec(f: Callable[P, T]) -> Callable[P, T]:
            stage = PipelineStage(f, returns, name=name, params=params)
            self.add_stage(stage)
            return f

        return dec

    def set_values(self, **kwargs: Any) -> None:
        self._storage.store_many(**kwargs)

    def get_values(self, *names: str) -> Mapping[str, Obj]:
        if not names:
            names = tuple(self._storage.list_names())
        return dict(zip(names, self._storage.fetch_many(*names), strict=True))
