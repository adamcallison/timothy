from collections.abc import Sequence
from contextlib import suppress

from timothy.core import (
    Obj,
    PipelineComponent,
    PipelineComponentSet,
    PipelineStageSet,
    PipelineStorage,
)


class StubPipelineStorage(dict):
    def fetch_one(self, name: str) -> Obj:
        return self[name]

    def fetch_many(self, *names: str) -> Sequence[Obj]:
        return [self.fetch_one(name) for name in names]

    def store_many(self, **name_to_obj_map: Obj) -> None:
        self.update(name_to_obj_map)

    def store_one(self, name: str, obj: Obj) -> None:
        self.store_many(**{name: obj})

    def list_names(self) -> Sequence[str]:
        return sorted(self.keys())


class StubPipelineStageRunner:
    def __call__(self, stages: PipelineStageSet, storage: PipelineStorage) -> None:
        stage_names = list(stages.keys())
        called: list[str] = []
        attempts = 100
        # TODO: make this stub less hacky
        while (set(called) != set(stage_names)) and attempts > 0:
            for stage in stages.values():
                attempts -= 1
                if stage.name in called:
                    continue
                with suppress(KeyError):
                    storage.store_many(
                        **dict(
                            zip(
                                stage.returns,
                                stage.call(storage.fetch_many(*stage.params)),
                                strict=True,
                            ),
                        ),
                    )
                    called.append(stage.name)


class StubMissingCompError(Exception): ...


class StubComponent(PipelineComponent):
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class StubComponentSet(PipelineComponentSet[StubComponent]):
    _missing_component_error = StubMissingCompError
    validation_calls: list[Sequence[StubComponent]]

    @staticmethod
    def _validate(*fake_comps: StubComponent) -> None:
        if any(fake_comp.name == "should_raise" for fake_comp in fake_comps):
            raise StubMissingCompError
