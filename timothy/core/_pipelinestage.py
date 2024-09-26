from collections import Counter, defaultdict
from collections.abc import Collection, Iterator, Mapping, Sequence
from inspect import signature
from itertools import chain
from typing import Any, Self, TypeVar, cast, overload

from timothy.core._exceptions import (
    CannotCallStageError,
    DuplicateReturnError,
    DuplicateStageError,
    InvalidParamsError,
    InvalidResultsError,
    MissingPipelineStageError,
)
from timothy.core._typedefs import Obj, StageFunction

T = TypeVar("T")


class PipelineStage:
    def __init__(
        self,
        func: StageFunction,
        returns: Sequence[str],
        name: str | None = None,
        params: Sequence[str] | None = None,
    ) -> None:
        self._func = func
        self._name = name if name is not None else self._func.__name__

        base_params = list(signature(func).parameters)
        if params is None:
            self._params = base_params
        elif len(params) != len(base_params):
            msg = f"Cannot rename {tuple(base_params)} to {tuple(params)}"
            raise InvalidParamsError(msg)
        else:
            self._params = list(params)

        self._returns = list(returns)

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> StageFunction:
        return self._func

    @property
    def params(self) -> list[str]:
        return self._params

    @property
    def returns(self) -> list[str]:
        return self._returns

    def call(self, param_objs: Sequence[Obj]) -> Sequence[Obj]:
        if (n_param_objs := len(param_objs)) != (n_params := len(self._params)):
            msg = f"Stage '{self.name}' has {n_params} param(s) but called with {n_param_objs}."
            raise CannotCallStageError(msg)

        return self._ensure_valid_results(self.func(*param_objs))

    def _ensure_valid_results(self, raw_result: T | list[Any]) -> T | list[Any]:
        if raw_result is None:
            result: Any = self._validate_returns_none(raw_result)
        elif type(raw_result) is tuple:
            result = self._validate_returns_tuple(raw_result)
        else:
            result = self._validate_returns_not_none_or_tuple(raw_result)

        if len(self.returns) == 0:
            results: list[Any] = []
        elif len(self.returns) == 1:
            results = [result]
        else:
            results = list(result)

        return results

    def _validate_returns_none(self, return_value: T) -> T:
        assert return_value is None
        if (exprlen := len(self.returns)) > 1:
            msg = f"'Stage {self.name}' should return {exprlen} value(s) but returned 'None'."
            raise InvalidResultsError(msg)
        return cast(T, return_value)

    def _validate_returns_tuple(self, return_value: T) -> T:
        assert type(return_value) is tuple
        if (exprlen := len(self.returns)) not in (1, (rlen := len(return_value))):
            msg = f"'Stage {self.name}' should return {exprlen} value(s) but returned {rlen}."
            raise InvalidResultsError(msg)
        return cast(T, return_value)

    def _validate_returns_not_none_or_tuple(self, return_value: T) -> T:
        assert return_value is not None
        assert type(return_value) is not tuple
        if (exprlen := len(self.returns)) != 1:
            msg = f"'Stage {self.name}' should return {exprlen} value(s) but returned 1."
            raise InvalidResultsError(msg)
        return return_value


class PipelineStageSet:
    def __init__(self, *stages: PipelineStage) -> None:
        name_counts = Counter(p.name for p in stages)
        if duplicate_names := tuple(name for name, count in name_counts.items() if count > 1):
            msg = f"Pipeline stage names {duplicate_names} appear more than once."
            raise DuplicateStageError(msg)
        return_counts = Counter(chain(*(p.returns for p in stages)))
        if duplicate_returns := tuple(name for name, count in return_counts.items() if count > 1):
            msg = f"Objects {duplicate_returns} should not be returned by multiple stages."
            raise DuplicateReturnError(msg)

        self._stages: dict[str, PipelineStage] = {s.name: s for s in stages}

    def names(self) -> Iterator[str]:
        yield from self._stages.keys()

    def __iter__(self) -> Iterator[PipelineStage]:
        yield from self._stages.values()

    def __add__(self, other: PipelineStage) -> Self:
        return self.__class__(*self._stages.values(), other)

    @overload
    def __getitem__(self, names: str) -> PipelineStage: ...

    @overload
    def __getitem__(self, names: list[str]) -> Self: ...

    def __getitem__(self, names: str | list[str]) -> PipelineStage | Self:
        if isinstance(names, str):
            for first in self[[names]]:
                return first
        if missing_stages := tuple(set(names) - set(self.names())):
            msg = f"No such pipeline stages {missing_stages}."
            raise MissingPipelineStageError(msg)
        return self.__class__(*(self._stages[n] for n in names))

    @property
    def returns(self) -> Mapping[str, PipelineStage]:
        returns: dict[str, PipelineStage] = {}
        for stage in self._stages.values():
            for return_name in stage.returns:
                returns[return_name] = stage
        return returns

    @property
    def params(self) -> Mapping[str, Collection[PipelineStage]]:
        params: dict[str, set[PipelineStage]] = defaultdict(set)
        for stage in self._stages.values():
            for param_name in stage.params:
                params[param_name].add(stage)
        return params
