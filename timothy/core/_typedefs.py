"""Type definitions."""

from typing import Any, ParamSpec, Protocol

P = ParamSpec("P")


class StageFunction(Protocol):
    __name__: str

    def __call__(self, *args: Any) -> Any: ...  # noqa: ANN401


class _SingletonMeta(type):
    _instance: "_SingletonMeta | None" = None

    def __call__(cls, *args: P.args, **kwargs: P.kwargs) -> "_SingletonMeta":
        if cls._instance is not None:
            msg = f"Class '{cls.__name__}' should not be instantiated more than once."
            raise RuntimeError(msg)
        cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class Singleton(metaclass=_SingletonMeta): ...
