import json
from typing import Generic, TypeVar

import pytest

from timothy._pipelinestorage_impl import JSONFilePipelineStorage, MemoryPipelineStorage
from timothy.core import PipelineStorage

_Storage = TypeVar("_Storage", bound=PipelineStorage)


class BaseTestAnyPipelineStorage(Generic[_Storage]):
    def test_obj_stored_as_one_can_be_fetched_as_one(self, empty_storage: PipelineStorage):
        obj = "hello"
        empty_storage.store_one("name", obj)
        assert empty_storage.fetch_one("name") == obj

    def test_obj_stored_as_one_can_be_fetched_as_many(self, empty_storage: PipelineStorage):
        obj1 = "hello"
        obj2 = "world"
        empty_storage.store_one("name1", obj1)
        empty_storage.store_one("name2", obj2)
        assert list(empty_storage.fetch_many("name1", "name2")) == [obj1, obj2]

    def test_obj_stored_as_many_can_be_fetched_as_one(self, empty_storage: PipelineStorage):
        obj1 = "hello"
        obj2 = "world"
        empty_storage.store_many(name1=obj1, name2=obj2)
        assert empty_storage.fetch_one("name1") == obj1
        assert empty_storage.fetch_one("name2") == obj2

    def test_obj_stored_as_many_can_be_fetched_as_many(self, empty_storage: PipelineStorage):
        obj1 = "hello"
        obj2 = "world"
        empty_storage.store_many(name1=obj1, name2=obj2)
        assert list(empty_storage.fetch_many("name1", "name2")) == [obj1, obj2]

    def test_list_names_correctly_lists_names(self, empty_storage: PipelineStorage):
        obj1 = "hello"
        obj2 = "world"
        empty_storage.store_many(name1=obj1, name2=obj2)
        assert list(empty_storage.list_names()) == ["name1", "name2"]


class TestMemoryPipelineStorage(BaseTestAnyPipelineStorage[MemoryPipelineStorage]):
    @pytest.fixture()
    def empty_storage(self) -> MemoryPipelineStorage:
        return MemoryPipelineStorage()


class TestJSONFilePipelineStorage(BaseTestAnyPipelineStorage[JSONFilePipelineStorage]):
    @pytest.fixture()
    def empty_storage(self, tmp_path) -> JSONFilePipelineStorage:
        return JSONFilePipelineStorage(tmp_path)

    def test_json_file_is_stored_in_correct_location(self, empty_storage: JSONFilePipelineStorage):
        obj = {"hello": "world"}
        empty_storage.store_one("name", obj)
        with (empty_storage.location / "name.json").open("r") as f:
            stored = json.load(f)
        assert stored == obj
