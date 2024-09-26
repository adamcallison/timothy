from collections.abc import Callable, Mapping, Sequence
from typing import Any, Generic, ParamSpec, TypeVar

from timothy.core._pipelinestage import PipelineStage, PipelineStageSet
from timothy.core._pipelinestagerunner import PipelineStageRunner
from timothy.core._pipelinestorage import PipelineStorage
from timothy.core._typedefs import Obj
from timothy.exceptions import PipelineConfigError

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


class _PipelineMaybeAttribute(Generic[T]):
    def __init__(self, attrname: str, attrtype: Callable[..., T]) -> None:
        del attrtype
        self._attrname = attrname
        self._maybe_name = f"_maybe_{attrname}"

    def __get__(self, obj: "Pipeline", objtype: type["Pipeline"] | None = None) -> T:
        maybe_attr = getattr(obj, self._maybe_name)
        if maybe_attr is not None:
            return maybe_attr
        msg = f"Pipeline {self._attrname} not set"
        raise PipelineConfigError(msg)

    def __set__(self, obj: "Pipeline", value: T) -> None:
        setattr(obj, self._maybe_name, value)


class Pipeline:
    storage = _PipelineMaybeAttribute("storage", PipelineStorage)
    stagerunner = _PipelineMaybeAttribute("stagerunner", PipelineStageRunner)

    def __init__(self, name: str, *, stages: PipelineStageSet | None = None) -> None:
        self._name = name
        self._stages = stages or PipelineStageSet()
        self._maybe_storage: PipelineStorage | None = None
        self._maybe_stagerunner: PipelineStageRunner | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def stages(self) -> PipelineStageSet:
        return self._stages

    def add_stage(self, stage: PipelineStage) -> None:
        self._stages += stage

    def run(self) -> None:
        self.stagerunner(self._stages, self.storage)

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
        self.storage.store_many(**kwargs)

    def get_values(self, *names: str) -> Mapping[str, Obj]:
        if not names:
            names = tuple(self.storage.list_names())
        return dict(zip(names, self.storage.fetch_many(*names), strict=True))
