import json
from collections.abc import MutableMapping, Sequence
from pathlib import Path

from timothy.core import Obj


class MemoryPipelineStorage:
    def __init__(self) -> None:
        self._storage: MutableMapping[str, Obj] = {}

    def fetch_one(self, name: str) -> Obj:
        return self._storage[name]

    def fetch_many(self, *names: str) -> Sequence[Obj]:
        return [self.fetch_one(name) for name in names]

    def store_many(self, **name_to_obj_map: Obj) -> None:
        self._storage.update(name_to_obj_map)

    def store_one(self, name: str, obj: Obj) -> None:
        self.store_many(**{name: obj})

    def list_names(self) -> Sequence[str]:
        return sorted(self._storage.keys())


class JSONFilePipelineStorage:
    def __init__(self, location: Path) -> None:
        self._location = location

    @property
    def location(self) -> Path:
        return self._location

    def fetch_one(self, name: str) -> Obj:
        with (self.location / f"{name}.json").open("r") as f:
            return json.load(f)

    def fetch_many(self, *names: str) -> Sequence[Obj]:
        return [self.fetch_one(name) for name in names]

    def store_one(self, name: str, obj: Obj) -> None:
        self.location.mkdir(parents=True, exist_ok=True)
        with (self.location / f"{name}.json").open("w") as f:
            return json.dump(obj, f)

    def store_many(self, **name_to_obj_map: Obj) -> None:
        for name, obj in name_to_obj_map.items():
            self.store_one(name, obj)

    def list_names(self) -> Sequence[str]:
        return [p.stem for p in self.location.glob("*")]
