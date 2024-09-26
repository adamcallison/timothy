from typing import TypeVar

from timothy.exceptions import PipelineError

T = TypeVar("T")


def not_none(obj: T | None, error: type[PipelineError] = PipelineError, msg: str = "") -> T:
    if obj is None:
        raise error(msg)
    return obj
