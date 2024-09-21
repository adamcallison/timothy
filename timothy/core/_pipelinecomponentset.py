from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from typing import Generic, Self, TypeVar, cast, overload


class PipelineComponent(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...


_PipelineComponent = TypeVar("_PipelineComponent", bound=PipelineComponent)


class PipelineComponentSet(Generic[_PipelineComponent], ABC):
    _missing_component_error: type[Exception]

    def __init__(self, *components: _PipelineComponent) -> None:
        self._validate(*components)
        self._components: dict[str, _PipelineComponent] = {p.name: p for p in components}

    @staticmethod
    @abstractmethod
    def _validate(*components: _PipelineComponent) -> None: ...

    def __iter__(self) -> Iterator[str]:
        yield from self.keys()

    def keys(self) -> Iterator[str]:
        yield from self._components.keys()

    def values(self) -> Iterator[_PipelineComponent]:
        yield from self._components.values()

    def items(self) -> Iterator[tuple[str, _PipelineComponent]]:
        yield from self._components.items()

    @overload
    def __getitem__(self, name: str) -> _PipelineComponent: ...

    @overload
    def __getitem__(self, names: list[str]) -> Self: ...

    def __getitem__(self, names: str | list[str]) -> _PipelineComponent | Self:
        if isinstance(names, str):
            return next(self[[names]].values())
        if missing_obj := tuple(set(names) - set(self._components)):
            msg = f"No such pipeline component {missing_obj} in collection."
            raise self._missing_component_error(msg)
        return self.__class__(*(self._components[n] for n in names))

    @overload
    def __add__(self, other: _PipelineComponent) -> Self: ...

    @overload
    def __add__(self, other: Self) -> Self: ...

    @overload
    def __add__(self, other: Sequence[_PipelineComponent]) -> Self: ...

    def __add__(self, other: _PipelineComponent | Self | Sequence[_PipelineComponent]) -> Self:
        if isinstance(other, type(self)):
            return self + list(other._components.values())
        if isinstance(other, PipelineComponent):
            return self + [cast(_PipelineComponent, other)]  # noqa: RUF005
        return self.__class__(*self._components.values(), *other)

    def __contains__(self, item: str) -> bool:
        return any(item == component_name for component_name in self.keys())
